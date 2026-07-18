//! Buffers and the out-of-band terminal primitive (design §5, §10).
//!
//! Home of the buffer-sizing constants, the send-side cursor helper that
//! partial-consumes an [`h3::quic::WriteBuf`] into `quiche::stream_send`, and
//! [`TerminalCell`] — the sticky, pollable, out-of-band one-shot the worker uses
//! to publish terminal reasons to synchronous `h3::quic` `poll_*` methods.
#![allow(dead_code)] // wired up incrementally across Phases 2–8

use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Buf;
use futures::task::AtomicWaker;

use crate::quiche;

/// Outbound packet buffer length backing `ApplicationOverQuic::buffer()`
/// (§5, T3). Sized for a full GSO send batch — at least
/// `max_send_udp_payload_size`, larger to amortize batched sends — and
/// deliberately **not** capped at [`MAX_CHUNK`], so UDP throughput is
/// independent of the per-stream read chunk size. Provisional (§12 C1).
pub(crate) const PKT_BUF_LEN: usize = 64 * 1024;

/// Cap on a single `stream_recv` into the receive scratch buffer (§5.1).
/// The per-stream in-flight memory bound is `channel_depth × MAX_CHUNK`.
/// Provisional (§12 S3).
pub(crate) const MAX_CHUNK: usize = 16 * 1024;

/// Drive **one** `stream_send` turn from any [`Buf`], advancing the cursor by
/// the number of bytes the transport accepted.
///
/// h3's [`WriteBuf`](h3::quic::WriteBuf) is itself a `Buf` and may be
/// non-contiguous (encoded frame header + payload), so `chunk()` yields only
/// the current contiguous segment; repeated turns walk across segments. This is
/// the partial-consume core of the §5.3 send state machine: the worker calls it
/// under whatever send capacity exists, re-arming on the writable edge.
///
/// `sink` performs the actual `quiche::Connection::stream_send(id, chunk, fin)`
/// and returns the accepted byte count (or a quiche error, propagated verbatim
/// so the caller can classify it via [`crate::error`]). Returns the number of
/// bytes consumed from `buf` this turn (`0` when there is nothing left to send).
pub(crate) fn send_from_buf<B, F>(buf: &mut B, mut sink: F) -> Result<usize, quiche::Error>
where
    B: Buf,
    F: FnMut(&[u8]) -> Result<usize, quiche::Error>,
{
    if !buf.has_remaining() {
        return Ok(0);
    }
    let chunk = buf.chunk();
    debug_assert!(!chunk.is_empty(), "Buf::has_remaining but empty chunk");
    let written = sink(chunk)?;
    debug_assert!(written <= chunk.len(), "sink accepted more than offered");
    buf.advance(written);
    Ok(written)
}

/// A sticky, out-of-band, *pollable* one-shot — conceptually
/// `Arc<(Mutex<Option<T>>, AtomicWaker)>`, **first-writer-wins** (§5).
///
/// The worker [`set`](TerminalCell::set)s the value once and wakes; the single
/// logical poller reads it through the race-free order in
/// [`poll`](TerminalCell::poll). Because an [`AtomicWaker`] holds exactly one
/// waker, each cell has exactly one poller (a stream half's own poll method, or
/// one per-direction accept method), so the two accept cells each get their own
/// waker even if polled on different tasks (§5 finding 5).
///
/// `tokio::sync::watch` is unsuitable because `changed()` is an async future
/// while the `h3::quic` methods are synchronous `poll_*(cx)` fns.
pub(crate) struct TerminalCell<T> {
    inner: Arc<TerminalInner<T>>,
}

struct TerminalInner<T> {
    value: Mutex<Option<T>>,
    waker: AtomicWaker,
}

impl<T> Clone for TerminalCell<T> {
    fn clone(&self) -> Self {
        TerminalCell {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: Clone> Default for TerminalCell<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> TerminalCell<T> {
    pub(crate) fn new() -> Self {
        TerminalCell {
            inner: Arc::new(TerminalInner {
                value: Mutex::new(None),
                waker: AtomicWaker::new(),
            }),
        }
    }

    /// Publish the terminal value. First-writer-wins: returns `true` if this
    /// call installed the value (and woke the poller), `false` if a value was
    /// already present (no-op, no second wake).
    pub(crate) fn set(&self, value: T) -> bool {
        {
            let mut slot = self.inner.value.lock().expect("TerminalCell poisoned");
            if slot.is_some() {
                return false;
            }
            *slot = Some(value);
        }
        self.inner.waker.wake();
        true
    }

    /// Clone out the current value, if any (fast path / non-registering read).
    pub(crate) fn get(&self) -> Option<T> {
        self.inner
            .value
            .lock()
            .expect("TerminalCell poisoned")
            .clone()
    }

    /// Race-free poll (§5 finding 1): register the waker, then re-read, so a
    /// value installed between the fast-path check and registration is never
    /// missed. The sticky value is cloned out on every ready poll.
    pub(crate) fn poll(&self, cx: &mut Context<'_>) -> Poll<T> {
        // 1. fast path
        if let Some(v) = self.get() {
            return Poll::Ready(v);
        }
        // 2. register
        self.inner.waker.register(cx.waker());
        // 3. re-read
        match self.get() {
            // 4. only Pending if still empty after registration
            Some(v) => Poll::Ready(v),
            None => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{RawWaker, RawWakerVTable, Waker};

    #[test]
    fn send_from_buf_partial_consume_contiguous() {
        let mut buf = Bytes::from_static(b"hello world"); // 11 bytes
                                                          // First turn: transport accepts only 5 bytes.
        let n = send_from_buf(&mut buf, |chunk| {
            assert_eq!(chunk, b"hello world");
            Ok(5)
        })
        .unwrap();
        assert_eq!(n, 5);
        assert_eq!(buf.remaining(), 6);
        // Second turn: accepts the rest.
        let n = send_from_buf(&mut buf, |chunk| {
            assert_eq!(chunk, b" world");
            Ok(chunk.len())
        })
        .unwrap();
        assert_eq!(n, 6);
        assert_eq!(buf.remaining(), 0);
        // Nothing left: a further turn is a no-op and never calls the sink.
        let n = send_from_buf(&mut buf, |_| panic!("sink must not be called")).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn send_from_buf_walks_noncontiguous_segments() {
        // A chained Buf mimics h3's WriteBuf (header + frame): chunk() yields
        // only the current segment, so the cursor must walk across segments.
        let mut buf = Bytes::from_static(b"AAA").chain(Bytes::from_static(b"BBBB"));
        let mut sent = Vec::new();
        while buf.has_remaining() {
            send_from_buf(&mut buf, |chunk| {
                sent.extend_from_slice(chunk);
                Ok(chunk.len())
            })
            .unwrap();
        }
        assert_eq!(sent, b"AAABBBB");
    }

    #[test]
    fn send_from_buf_propagates_quiche_error() {
        let mut buf = Bytes::from_static(b"x");
        let err = send_from_buf(&mut buf, |_| Err(quiche::Error::Done)).unwrap_err();
        assert!(matches!(err, quiche::Error::Done));
        // Cursor is not advanced on error.
        assert_eq!(buf.remaining(), 1);
    }

    #[test]
    fn terminal_cell_first_writer_wins() {
        let cell = TerminalCell::<u32>::new();
        assert!(cell.set(1));
        assert!(!cell.set(2)); // second writer loses
        assert_eq!(cell.get(), Some(1));
    }

    #[test]
    fn terminal_cell_fast_path_ready() {
        let cell = TerminalCell::<u32>::new();
        cell.set(7);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(cell.poll(&mut cx), Poll::Ready(7));
    }

    #[test]
    fn terminal_cell_pending_then_woken() {
        let cell = TerminalCell::<u32>::new();
        let woken = Arc::new(AtomicBool::new(false));
        let waker = flag_waker(woken.clone());
        let mut cx = Context::from_waker(&waker);
        // Empty → Pending, waker registered.
        assert_eq!(cell.poll(&mut cx), Poll::Pending);
        assert!(!woken.load(Ordering::SeqCst));
        // Worker sets → registered waker fires.
        assert!(cell.set(99));
        assert!(
            woken.load(Ordering::SeqCst),
            "set must wake the registered waker"
        );
        // Re-poll observes the sticky value.
        assert_eq!(cell.poll(&mut cx), Poll::Ready(99));
    }

    // §11 "TerminalCell set-vs-register race": the value is installed exactly
    // between the poll's fast-path check and its waker registration. The
    // race-free order (register → re-read) must yield Ready, never Pending.
    //
    // We drive this deterministically: AtomicWaker::register clones the passed
    // waker, and our waker's clone hook installs the value at that instant —
    // i.e. after the fast-path check (which saw empty) but as part of
    // registration. A non-rechecking poll would incorrectly return Pending.
    #[test]
    fn terminal_cell_set_between_check_and_register() {
        let cell = TerminalCell::<u32>::new();
        let hook = Box::new(RaceHook {
            cell: cell.clone(),
            value: 55,
        });
        let hook_ptr = Box::into_raw(hook);
        let waker = unsafe { Waker::from_raw(RawWaker::new(hook_ptr as *const (), &RACE_VTABLE)) };
        let mut cx = Context::from_waker(&waker);
        // Fast path sees empty; register clones the waker → hook sets value;
        // recheck must observe it.
        assert_eq!(cell.poll(&mut cx), Poll::Ready(55));
        drop(waker);
        unsafe { drop(Box::from_raw(hook_ptr)) };
    }

    // ---- test waker plumbing ----

    fn flag_waker(flag: Arc<AtomicBool>) -> Waker {
        let ptr = Arc::into_raw(flag) as *const ();
        unsafe { Waker::from_raw(RawWaker::new(ptr, &FLAG_VTABLE)) }
    }

    static FLAG_VTABLE: RawWakerVTable = RawWakerVTable::new(
        |p| unsafe {
            let arc = Arc::from_raw(p as *const AtomicBool);
            let cloned = arc.clone();
            std::mem::forget(arc);
            RawWaker::new(Arc::into_raw(cloned) as *const (), &FLAG_VTABLE)
        },
        |p| unsafe {
            let arc = Arc::from_raw(p as *const AtomicBool);
            arc.store(true, Ordering::SeqCst);
        },
        |p| unsafe {
            let arc = Arc::from_raw(p as *const AtomicBool);
            arc.store(true, Ordering::SeqCst);
            std::mem::forget(arc);
        },
        |p| unsafe {
            drop(Arc::from_raw(p as *const AtomicBool));
        },
    );

    struct RaceHook {
        cell: TerminalCell<u32>,
        value: u32,
    }

    static RACE_VTABLE: RawWakerVTable = RawWakerVTable::new(
        // clone: install the value at registration time, then hand back a
        // no-op waker so subsequent clones don't re-fire.
        |p| unsafe {
            let hook = &*(p as *const RaceHook);
            hook.cell.set(hook.value);
            RawWaker::new(std::ptr::null(), &NOOP_VTABLE)
        },
        |_| {},
        |_| {},
        |_| {},
    );

    static NOOP_VTABLE: RawWakerVTable = RawWakerVTable::new(
        |_| RawWaker::new(std::ptr::null(), &NOOP_VTABLE),
        |_| {},
        |_| {},
        |_| {},
    );
}
