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
use pin_project::pin_project;
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
            (offset, Some(max)) => {
                resp = resp.header(
                    RANGE,
                    format!("bytes={}-{}", offset.unwrap_or(0), max.saturating_sub(1)),
                );
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

            let resume = headers
                .get(ACCEPT_RANGES)
                .and_then(|a| a.to_str().ok())
                .and_then(|a| {
                    if a == "bytes" {
                        Some(self.clone())
                    } else {
                        None
                    }
                });

            let stream = Box::new(StreamReader::new(
                resp.bytes_stream().map_err(IoError::other),
            ));

            Ok(HttpStream {
                stream,
                len,
                pos: offset,
                resume,
                pending_seek: None,
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

type PendingSeek =
    Pin<Box<dyn Future<Output = Result<HttpStream, AudioStreamError>> + Send + Sync>>;

/// Short hops forward are cheaper to read through than to reconnect for.
const MAX_FORWARD_SKIP: u64 = 256 * 1024;

#[pin_project]
struct HttpStream {
    #[pin]
    stream: Box<dyn AsyncRead + Send + Sync + Unpin>,
    len: Option<u64>,
    pos: u64,
    resume: Option<HttpRequest>,
    pending_seek: Option<(u64, PendingSeek)>,
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
}

impl AsyncRead for HttpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        let this = self.project();
        let before = buf.filled().len();
        let res = ready!(AsyncRead::poll_read(this.stream, cx, buf));
        *this.pos += (buf.filled().len() - before) as u64;
        Poll::Ready(res)
    }
}

impl AsyncSeek for HttpStream {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> IoResult<()> {
        let this = self.get_mut();
        let target = this.seek_target(position)?;

        if Some(target) == this.len {
            this.stream = Box::new(tokio::io::empty());
            this.pos = target;
            return Ok(());
        }

        if target >= this.pos && target - this.pos <= MAX_FORWARD_SKIP {
            this.skip = target - this.pos;
            return Ok(());
        }

        let mut request = this.resume.clone().ok_or_else(|| {
            IoError::new(
                IoErrorKind::Unsupported,
                "server does not accept byte ranges",
            )
        })?;
        let fut = async move { request.create_stream(Some(target)).await };
        this.pending_seek = Some((target, Box::pin(fut)));
        // free the connection now; the body is unusable once a seek starts
        this.stream = Box::new(tokio::io::empty());

        Ok(())
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

        let Some((target, fut)) = this.pending_seek.as_mut() else {
            return Poll::Ready(Ok(this.pos));
        };

        let res = ready!(fut.as_mut().poll(cx));
        let target = *target;
        this.pending_seek = None;

        let new = res.map_err(|e| IoError::other(e.to_string()))?;
        this.stream = new.stream;
        this.len = new.len.or(this.len);
        this.pos = target;

        Poll::Ready(Ok(target))
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
