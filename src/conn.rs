//! Per-connection IO layer.
//!
//! `ConnIo` wraps the `TcpStream` handed to hyper's HTTP/1 connection driver.
//! Everything hyper writes (response headers) goes through it untouched, but
//! it also records *when* a response's marker header has actually reached the
//! kernel socket buffer. That is the barrier that makes zero-copy sendfile
//! safe: file data is written directly to the socket fd with `sendfile(2)`,
//! bypassing hyper — so we must be certain the headers are already on the
//! wire before the first byte of file data, or the client would see garbage.
//!
//! The barrier works like this:
//!  * the handler stamps each download response with a unique marker header
//!    (`x-conn-marker: <seq>`) and registers it in `ConnShared`;
//!  * whenever hyper calls `poll_write`, `ConnIo` streams the bytes past a
//!    rolling subsequence search (handles headers split across writes);
//!  * once the marker is seen, `head_written` is set and the body's waker is
//!    fired — only then does `SendfileBody` start calling `sendfile`.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{ready, Context, Poll, Waker};

use hyper::rt::{Read, Write};
use tokio::io::{unix::AsyncFd, AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

/// Marker header name used for the sendfile barrier (value is a per-response
/// sequence number, so stale bytes from a previous keep-alive response can
/// never match).
pub(crate) const MARKER_HEADER: &str = "x-conn-marker";

static MARKER_SEQ: AtomicU64 = AtomicU64::new(0);

/// Allocate a fresh marker value for a response.
pub(crate) fn next_marker_value() -> u64 {
    MARKER_SEQ.fetch_add(1, Ordering::Relaxed)
}

struct ConnInner {
    /// Byte pattern to look for (`"<name>: <value>"`), empty when idle.
    marker: Vec<u8>,
    /// Tail of the previous write, kept so a marker straddling two writes is
    /// still found.
    carry: Vec<u8>,
    head_written: bool,
    waker: Option<Waker>,
}

/// State shared between hyper's IO wrapper and the sendfile body.
pub(crate) struct ConnShared {
    inner: Mutex<ConnInner>,
    /// dup()'d socket fd; lets the body wait for writability (EAGAIN on
    /// sendfile) without owning the stream hyper is using.
    sock: AsyncFd<std::fs::File>,
}

impl ConnShared {
    fn new(stream: &TcpStream) -> io::Result<Arc<Self>> {
        // dup shares the same open file description (i.e. the same socket);
        // sendfile through it writes the same connection.
        let dup = unsafe { libc::dup(stream.as_raw_fd()) };
        if dup < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Arc::new(Self {
            inner: Mutex::new(ConnInner {
                marker: Vec::new(),
                carry: Vec::new(),
                head_written: false,
                waker: None,
            }),
            sock: AsyncFd::new(unsafe { std::fs::File::from_raw_fd(dup) })?,
        }))
    }

    /// Called by the handler before returning a download response.
    pub(crate) fn arm(&self, marker_value: u64) {
        let mut g = self.inner.lock().unwrap();
        g.marker = format!("{MARKER_HEADER}: {marker_value}\r\n").into_bytes();
        // keep `carry`: it can only match the *previous* marker value
        g.head_written = false;
    }

    /// Called from `ConnIo::poll_write` after bytes reached the kernel.
    fn note_write(&self, buf: &[u8]) {
        let marker = self.inner.lock().unwrap().marker.clone();
        if marker.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        if g.head_written {
            return;
        }
        // search carry+buf so the marker can straddle a write boundary
        let mut hay = Vec::with_capacity(g.carry.len() + buf.len());
        hay.extend_from_slice(&g.carry);
        hay.extend_from_slice(buf);
        let found = hay.windows(marker.len()).any(|w| w == marker);
        if found {
            g.head_written = true;
            g.carry.clear();
            if let Some(w) = g.waker.take() {
                w.wake();
            }
            return;
        }
        // keep at most marker.len()-1 trailing bytes for the next round
        let keep = marker.len().saturating_sub(1).min(hay.len());
        let split = hay.len() - keep;
        g.carry.clear();
        g.carry.extend_from_slice(&hay[split..]);
    }

    /// Barrier check from the body side.
    pub(crate) fn head_written(&self) -> bool {
        self.inner.lock().unwrap().head_written
    }

    /// Register a waker to be fired when the headers reach the socket.
    pub(crate) fn register_waker(&self, waker: &Waker) {
        let mut g = self.inner.lock().unwrap();
        match &g.waker {
            Some(prev) if prev.will_wake(waker) => {}
            _ => g.waker = Some(waker.clone()),
        }
    }

    /// Raw socket fd for sendfile.
    pub(crate) fn socket_fd(&self) -> RawFd {
        self.sock.get_ref().as_raw_fd()
    }

    /// Wait for the socket to become writable (sendfile hit EAGAIN).
    pub(crate) fn poll_socket_writable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            let mut guard = ready!(self.sock.poll_write_ready(cx))?;
            guard.clear_ready();
            return Poll::Ready(Ok(()));
        }
    }
}

/// hyper IO wrapper around the TCP stream.
pub(crate) struct ConnIo {
    stream: TcpStream,
    shared: Arc<ConnShared>,
}

impl ConnIo {
    pub(crate) fn new(stream: TcpStream) -> io::Result<(Self, Arc<ConnShared>)> {
        let shared = ConnShared::new(&stream)?;
        Ok((Self { stream, shared: shared.clone() }, shared))
    }
}

impl Read for ConnIo {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        // Safety: hyper guarantees the cursor points at valid uninitialized
        // memory; tokio's ReadBuf only exposes the part it filled.
        let this = self.get_mut();
        let n = unsafe {
            let mut tbuf = tokio::io::ReadBuf::uninit(buf.as_mut());
            match std::pin::Pin::new(&mut this.stream).poll_read(cx, &mut tbuf) {
                Poll::Ready(Ok(())) => tbuf.filled().len(),
                other => return other,
            }
        };
        unsafe { buf.advance(n) };
        Poll::Ready(Ok(()))
    }
}

impl Write for ConnIo {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match std::pin::Pin::new(&mut this.stream).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                // only the bytes that actually reached the kernel count
                this.shared.note_write(&buf[..n]);
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        // keep hyper on the single-buffer path so headers flow through
        // poll_write (and the marker scan) as one contiguous region
        false
    }
}
