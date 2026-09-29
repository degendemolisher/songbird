use crate::input::{
    AsyncAdapterStream,
    AsyncMediaSource,
    AudioStream,
    AudioStreamError,
    Compose,
    Input,
};
use async_trait::async_trait;
use futures::{ready, TryStreamExt};
use reqwest::{
    header::{HeaderMap, ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, RANGE, RETRY_AFTER},
    Client,
    StatusCode,
};
use std::{
    future::Future,
    io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult, SeekFrom},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use symphonia_core::io::MediaSource;
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};
use tokio_util::io::StreamReader;

/// A lazily instantiated HTTP request.
#[derive(Clone, Debug)]
pub struct HttpRequest {
    /// A reqwest client instance used to send the HTTP GET request.
    pub client: Client,
    /// The target URL of the required resource.
    pub request: String,
    /// HTTP header fields to add to any created requests.
    pub headers: HeaderMap,
    /// Content length, used as an upper bound in range requests if known.
    ///
    /// This is only needed for certain domains who expect to see a value like
    /// `range: bytes=0-1023` instead of the simpler `range: bytes=0-` (such as
    /// Youtube).
    pub content_length: Option<u64>,
}

impl HttpRequest {
    #[must_use]
    /// Create a lazy HTTP request.
    pub fn new(client: Client, request: String) -> Self {
        Self::new_with_headers(client, request, HeaderMap::default())
    }

    #[must_use]
    /// Create a lazy HTTP request.
    pub fn new_with_headers(client: Client, request: String, headers: HeaderMap) -> Self {
        HttpRequest {
            client,
            request,
            headers,
            content_length: None,
        }
    }

    async fn create_stream(&mut self, offset: Option<u64>) -> Result<HttpStream, AudioStreamError> {
        let mut resp = self.client.get(&self.request).headers(self.headers.clone());

        match (offset, self.content_length) {
            (Some(offset), None) => {
                resp = resp.header(RANGE, format!("bytes={offset}-"));
            },
            (offset, Some(total)) => {
                let start = offset.unwrap_or(0);
                let end = total.min(start.saturating_add(MAX_RANGE));
                resp = resp.header(RANGE, format!("bytes={start}-{}", end.saturating_sub(1)));
            },
            _ => {},
        }

        let resp = resp
            .send()
            .await
            .map_err(|e| AudioStreamError::Fail(Box::new(e)))?;

        if !resp.status().is_success() {
            let msg: Box<dyn std::error::Error + Send + Sync + 'static> =
                format!("failed with http status code: {}", resp.status()).into();
            return Err(AudioStreamError::Fail(msg));
        }

        let offset = offset.unwrap_or(0);
        if offset > 0 && resp.status() != StatusCode::PARTIAL_CONTENT {
            let msg: Box<dyn std::error::Error + Send + Sync + 'static> =
                "server ignored the requested byte range".into();
            return Err(AudioStreamError::Fail(msg));
        }

        if let Some(t) = resp.headers().get(RETRY_AFTER) {
            t.to_str()
                .map_err(|_| {
                    let msg: Box<dyn std::error::Error + Send + Sync + 'static> =
                        "Retry-after field contained non-ASCII data.".into();
                    AudioStreamError::Fail(msg)
                })
                .and_then(|str_text| {
                    str_text.parse().map_err(|_| {
                        let msg: Box<dyn std::error::Error + Send + Sync + 'static> =
                            "Retry-after field was non-numeric.".into();
                        AudioStreamError::Fail(msg)
                    })
                })
                .and_then(|t| Err(AudioStreamError::RetryIn(Duration::from_secs(t))))
        } else {
            let headers = resp.headers();

            let len = total_len(headers, offset);

            let accepts_ranges = headers
                .get(ACCEPT_RANGES)
                .and_then(|a| a.to_str().ok())
                .is_some_and(|a| a == "bytes")
                || resp.status() == StatusCode::PARTIAL_CONTENT;
            let resume = accepts_ranges.then(|| HttpRequest {
                content_length: self.content_length.or(len),
                ..self.clone()
            });

            let stream = Box::new(StreamReader::new(
                resp.bytes_stream().map_err(IoError::other),
            ));

            Ok(HttpStream {
                stream,
                len,
                pos: offset,
                resume,
                pending: None,
                skip: 0,
            })
        }
    }
}

/// Full size of the resource. A ranged response's `Content-Length` only covers
/// the requested slice, so the total in `Content-Range` wins.
fn total_len(headers: &HeaderMap, offset: u64) -> Option<u64> {
    let from_range = headers
        .get(CONTENT_RANGE)
        .and_then(|val| val.to_str().ok())
        .and_then(|val| val.rsplit('/').next())
        .and_then(|total| total.parse().ok());

    from_range.or_else(|| {
        if offset > 0 {
            return None;
        }
        headers
            .get(CONTENT_LENGTH)
            .and_then(|val| val.to_str().ok())
            .and_then(|val| val.parse().ok())
    })
}

type PendingRequest =
    Pin<Box<dyn Future<Output = Result<HttpStream, AudioStreamError>> + Send + Sync>>;

/// googlevideo throttles any response larger than this to about the media bitrate.
const MAX_RANGE: u64 = 10 * 1024 * 1024;

/// Short hops forward are cheaper to read through than to reconnect for.
const MAX_FORWARD_SKIP: u64 = 256 * 1024;

struct HttpStream {
    stream: Box<dyn AsyncRead + Send + Sync + Unpin>,
    len: Option<u64>,
    pos: u64,
    resume: Option<HttpRequest>,
    pending: Option<(u64, PendingRequest)>,
    skip: u64,
}

impl HttpStream {
    fn seek_target(&self, position: SeekFrom) -> IoResult<u64> {
        let target = match position {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::Current(delta) => self.pos.checked_add_signed(delta),
            SeekFrom::End(delta) => self
                .len
                .ok_or_else(|| IoError::new(IoErrorKind::Unsupported, "stream length unknown"))?
                .checked_add_signed(delta),
        };
        let target = target.ok_or_else(|| {
            IoError::new(IoErrorKind::InvalidInput, "seek before start of stream")
        })?;

        if self.len.is_some_and(|len| target > len) {
            return Err(IoError::new(
                IoErrorKind::InvalidInput,
                "seek past end of stream",
            ));
        }

        Ok(target)
    }

    /// The current response ended short of the resource, so the next window is fetched.
    fn more_to_fetch(&self) -> bool {
        self.resume.is_some() && self.len.is_some_and(|len| self.pos < len)
    }

    fn request_from(&mut self, target: u64) -> IoResult<()> {
        let mut request = self.resume.clone().ok_or_else(|| {
            IoError::new(
                IoErrorKind::Unsupported,
                "server does not accept byte ranges",
            )
        })?;
        let fut = async move { request.create_stream(Some(target)).await };
        self.pending = Some((target, Box::pin(fut)));
        // free the connection now; the body is unusable once a new request starts
        self.stream = Box::new(tokio::io::empty());

        Ok(())
    }

    fn poll_pending(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        let Some((target, fut)) = self.pending.as_mut() else {
            return Poll::Ready(Ok(()));
        };

        let res = ready!(fut.as_mut().poll(cx));
        let target = *target;
        self.pending = None;

        let new = res.map_err(|e| IoError::other(e.to_string()))?;
        self.stream = new.stream;
        self.len = new.len.or(self.len);
        self.pos = target;

        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for HttpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        let this = self.get_mut();

        loop {
            ready!(this.poll_pending(cx))?;

            let before = buf.filled().len();
            ready!(Pin::new(&mut this.stream).poll_read(cx, buf))?;
            let read = (buf.filled().len() - before) as u64;
            this.pos += read;

            if read > 0 || !this.more_to_fetch() {
                return Poll::Ready(Ok(()));
            }
            this.request_from(this.pos)?;
        }
    }
}

impl AsyncSeek for HttpStream {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> IoResult<()> {
        let this = self.get_mut();
        let target = this.seek_target(position)?;

        if Some(target) == this.len {
            this.stream = Box::new(tokio::io::empty());
            this.pending = None;
            this.pos = target;
            return Ok(());
        }

        if target >= this.pos && target - this.pos <= MAX_FORWARD_SKIP {
            this.skip = target - this.pos;
            return Ok(());
        }

        this.request_from(target)
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<u64>> {
        let this = self.get_mut();

        while this.skip > 0 {
            let mut scratch = [0u8; 8 * 1024];
            let want = usize::try_from(this.skip.min(scratch.len() as u64)).unwrap_or(scratch.len());
            let mut buf = ReadBuf::new(&mut scratch[..want]);
            ready!(Pin::new(&mut *this).poll_read(cx, &mut buf))?;
            let n = buf.filled().len() as u64;
            if n == 0 {
                this.skip = 0;
                return Poll::Ready(Err(IoErrorKind::UnexpectedEof.into()));
            }
            this.skip -= n;
        }

        ready!(this.poll_pending(cx))?;

        Poll::Ready(Ok(this.pos))
    }
}

#[async_trait]
impl AsyncMediaSource for HttpStream {
    fn is_seekable(&self) -> bool {
        self.resume.is_some() && self.len.is_some()
    }

    async fn byte_len(&self) -> Option<u64> {
        self.len
    }

    async fn try_resume(
        &mut self,
        offset: u64,
    ) -> Result<Box<dyn AsyncMediaSource>, AudioStreamError> {
        if let Some(resume) = &mut self.resume {
            resume
                .create_stream(Some(offset))
                .await
                .map(|a| Box::new(a) as Box<dyn AsyncMediaSource>)
        } else {
            Err(AudioStreamError::Unsupported)
        }
    }
}

#[async_trait]
impl Compose for HttpRequest {
    fn create(&mut self) -> Result<AudioStream<Box<dyn MediaSource>>, AudioStreamError> {
        Err(AudioStreamError::Unsupported)
    }

    async fn create_async(
        &mut self,
    ) -> Result<AudioStream<Box<dyn MediaSource>>, AudioStreamError> {
        self.create_stream(None).await.map(|input| {
            let stream = AsyncAdapterStream::new(Box::new(input), 64 * 1024);

            AudioStream {
                input: Box::new(stream) as Box<dyn MediaSource>,
            }
        })
    }

    fn should_create_async(&self) -> bool {
        true
    }
}

impl From<HttpRequest> for Input {
    fn from(val: HttpRequest) -> Self {
        Input::Lazy(Box::new(val))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        constants::test_data::{HTTP_OPUS_TARGET, HTTP_TARGET, HTTP_WEBM_TARGET},
        input::input_tests::*,
    };
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    // GitHub's CDN stops answering on an HTTP/2 connection once a partially
    // read body on it has been dropped, which every seek does.
    fn client() -> Client {
        Client::builder().http1_only().build().unwrap()
    }

    /// `HTTP_TEST_BASE=http://host:port` serves the `resources/` files locally.
    fn target(default: &str) -> String {
        match std::env::var("HTTP_TEST_BASE") {
            Ok(base) => format!("{base}/{}", default.rsplit('/').next().unwrap()),
            Err(_) => default.to_string(),
        }
    }

    #[tokio::test]
    #[ntest::timeout(10_000)]
    async fn http_track_plays() {
        track_plays_mixed(|| HttpRequest::new(client(), target(HTTP_TARGET))).await;
    }

    #[tokio::test]
    #[ntest::timeout(10_000)]
    async fn http_forward_seek_correct() {
        forward_seek_correct(|| HttpRequest::new(client(), target(HTTP_TARGET))).await;
    }

    #[tokio::test]
    #[ntest::timeout(10_000)]
    async fn http_backward_seek_correct() {
        backward_seek_correct(|| HttpRequest::new(client(), target(HTTP_TARGET))).await;
    }

    #[tokio::test]
    #[ntest::timeout(20_000)]
    async fn http_stream_seeks_by_byte_range() {
        let mut req = HttpRequest::new(client(), target(HTTP_WEBM_TARGET));
        let mut stream = req.create_stream(None).await.expect("first request");
        assert!(stream.is_seekable());
        let len = stream.byte_len().await.expect("length known");

        let mut buf = vec![0u8; 16 * 1024];
        // backward, short forward (read through), long forward, back again
        for target in [0u64, 4096, 1_000_000, len / 2, 300, len, len - 1] {
            let mut got = 0;
            while got < 70_000 {
                let n = stream.read(&mut buf).await.expect("read");
                assert!(n > 0, "unexpected eof");
                got += n;
            }
            let landed = stream.seek(SeekFrom::Start(target)).await.expect("seek");
            assert_eq!(landed, target);
            assert_eq!(stream.byte_len().await, Some(len));
            if target == len {
                assert_eq!(stream.read(&mut buf).await.expect("eof"), 0);
                stream.seek(SeekFrom::Start(0)).await.expect("seek back");
            }
        }

        let n = stream.read(&mut buf).await.expect("read at end");
        assert_eq!(n, 1);
        assert_eq!(stream.read(&mut buf).await.expect("eof"), 0);
    }

    #[tokio::test]
    #[ntest::timeout(60_000)]
    async fn http_stream_chains_bounded_windows() {
        let mut req = HttpRequest::new(client(), target(HTTP_WEBM_TARGET));
        let mut stream = req.create_stream(None).await.expect("first request");
        let len = stream.byte_len().await.expect("length known");
        assert!(len > MAX_RANGE, "resource must span more than one window");
        let mut whole = Vec::new();
        stream.read_to_end(&mut whole).await.expect("read whole");

        let mut req = HttpRequest {
            content_length: Some(len),
            ..HttpRequest::new(client(), target(HTTP_WEBM_TARGET))
        };
        let mut stream = req.create_stream(None).await.expect("first window");
        let mut windowed = Vec::new();
        stream.read_to_end(&mut windowed).await.expect("read windows");

        assert_eq!(windowed.len(), whole.len());
        assert!(windowed == whole);
    }

    // NOTE: this covers youtube audio in a non-copyright-violating way, since
    // those depend on an HttpRequest internally anyhow.
    #[tokio::test]
    #[ntest::timeout(10_000)]
    async fn http_opus_track_plays() {
        track_plays_passthrough(|| HttpRequest::new(client(), target(HTTP_OPUS_TARGET))).await;
    }

    #[tokio::test]
    #[ntest::timeout(10_000)]
    async fn http_opus_forward_seek_correct() {
        forward_seek_correct(|| HttpRequest::new(client(), target(HTTP_OPUS_TARGET))).await;
    }

    #[tokio::test]
    #[ntest::timeout(10_000)]
    async fn http_opus_backward_seek_correct() {
        backward_seek_correct(|| HttpRequest::new(client(), target(HTTP_OPUS_TARGET))).await;
    }

    #[tokio::test]
    #[ntest::timeout(10_000)]
    async fn http_webm_track_plays() {
        track_plays_passthrough(|| HttpRequest::new(client(), target(HTTP_WEBM_TARGET))).await;
    }

    #[tokio::test]
    #[ntest::timeout(10_000)]
    async fn http_webm_forward_seek_correct() {
        forward_seek_correct(|| HttpRequest::new(client(), target(HTTP_WEBM_TARGET))).await;
    }

    #[tokio::test]
    #[ntest::timeout(10_000)]
    async fn http_webm_backward_seek_correct() {
        backward_seek_correct(|| HttpRequest::new(client(), target(HTTP_WEBM_TARGET))).await;
    }
}
