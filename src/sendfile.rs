//! Zero-copy download body.
//!
//! `SendfileBody` implements `http_body::Body` but never hands a single byte
//! of file data to hyper: `poll_frame` drives `sendfile(2)` directly against
//! the connection's socket fd (page cache → socket, no user-space copy) and
//! returns `Ready(Ok(None))` once the whole range has been handed to the
//! kernel. Combined with `Content-Length` set by the handler this yields a
//! fully valid HTTP response.
//!
//! One consequence of bypassing hyper's encoder: hyper has written 0 of the
//! advertised `Content-Length` bytes itself, so when the body completes it
//! reports "body write aborted" and closes the connection. The close is a
//! graceful one — all sendfile'd bytes are already in the kernel socket
//! buffer — so the client receives the complete file (verified by md5 in
//! testing). Non-download responses (directory listings, errors) use normal
//! in-memory bodies and keep-alive works for them as usual.

use std::io;
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use bytes::Bytes;
use http_body::Frame;

use crate::conn::ConnShared;

/// Cap a single sendfile(2) call: Linux internally limits one call to
/// 0x7ffff000 bytes; smaller chunks also keep one call from monopolizing the
/// runtime thread.
#[cfg(target_os = "linux")]
const MAX_CHUNK: u64 = 0x7fff_f000;
#[cfg(not(target_os = "linux"))]
const MAX_CHUNK: u64 = 64 * 1024 * 1024;

/// Push up to `len` bytes of `file` (starting at `offset`) straight into the
/// kernel socket buffer. Returns how many bytes were sent; `Ok(0)` means the
/// socket buffer is full (EAGAIN, no progress yet).
fn sendfile_once(sock: std::os::fd::RawFd, file: std::os::fd::RawFd, offset: u64, len: u64) -> io::Result<u64> {
    let len = len.min(MAX_CHUNK);

    #[cfg(target_os = "macos")]
    {
        // darwin: int sendfile(int fd, int s, off_t offset, off_t *len, ...)
        // returns -1 with EAGAIN and *len = bytes sent when it blocks partway
        let mut sent: libc::off_t = len as libc::off_t;
        let rc = unsafe {
            libc::sendfile(
                file,
                sock,
                offset as libc::off_t,
                &mut sent,
                std::ptr::null_mut(),
                0,
            )
        };
        if rc == 0 {
            // completed the whole request in one go
            Ok(sent as u64)
        } else {
            let err = io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::EAGAIN) => Ok(sent as u64),
                _ => Err(err),
            }
        }
    }

    #[cfg(target_os = "linux")]
    {
        // linux: ssize_t sendfile(int out, int in, off_t *offset, size_t count)
        let mut off: libc::off_t = offset as libc::off_t;
        let n = unsafe {
            libc::sendfile(sock, file, &mut off, len as libc::size_t)
        };
        if n >= 0 {
            Ok(n as u64)
        } else {
            let err = io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::EAGAIN) => Ok(0),
                _ => Err(err),
            }
        }
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = (sock, file, offset, len);
        compile_error!("sendfile zero-copy is only implemented for macOS and Linux");
    }
}

/// Response body that streams `len` bytes of `file` from `offset` via
/// sendfile(2) directly into the connection socket.
pub(crate) struct SendfileBody {
    file: std::fs::File,
    shared: Arc<ConnShared>,
    offset: u64,
    remaining: u64,
}

impl SendfileBody {
    pub(crate) fn new(file: std::fs::File, shared: Arc<ConnShared>, offset: u64, len: u64) -> Self {
        Self {
            file,
            shared,
            offset,
            remaining: len,
        }
    }
}

impl http_body::Body for SendfileBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        // Barrier: never let file data overtake the response headers.
        if !self.shared.head_written() {
            self.shared.register_waker(cx.waker());
            if !self.shared.head_written() {
                return Poll::Pending;
            }
        }

        let sock = self.shared.socket_fd();
        let file_fd = self.file.as_raw_fd();

        loop {
            if self.remaining == 0 {
                return Poll::Ready(None);
            }
            let want = self.remaining.min(MAX_CHUNK);
            let sent = match sendfile_once(sock, file_fd, self.offset, want) {
                Ok(n) => n,
                Err(e) => return Poll::Ready(Some(Err(e))),
            };
            if sent == 0 {
                // socket buffer full: wait for writability, retry
                if let Err(e) = ready!(self.shared.poll_socket_writable(cx)) {
                    return Poll::Ready(Some(Err(e)));
                }
                continue;
            }
            self.offset += sent;
            self.remaining -= sent;
            if sent < want {
                // partial write means the buffer filled up mid-call
                if let Err(e) = ready!(self.shared.poll_socket_writable(cx)) {
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.remaining)
    }
}
