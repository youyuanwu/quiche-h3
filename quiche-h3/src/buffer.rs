//! Buffers and the out-of-band terminal primitive (design §5, §10).
//!
//! Home of the buffer-sizing constants, the send-side cursor helper that
//! partial-consumes an [`h3::quic::WriteBuf`] into `quiche::stream_send`, and
//! [`TerminalCell`] — the sticky, pollable, out-of-band one-shot the worker uses
//! to publish terminal reasons to synchronous `h3::quic` `poll_*` methods.
#![allow(dead_code)] // wired up incrementally across Phases 2–8

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

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

/// The outcome of a single outstanding write, as observed by the front-end
/// [`H3SendStream`](crate::stream::H3SendStream) (SF-3). Mirrors the two ways a
/// per-write `oneshot` used to resolve: a delivered `Result` (worker completed
/// the op) or `Cancelled` (the carrier dropped without completing — e.g. an
/// unapplied `Send` at connection close, matching `oneshot::Sender`'s drop).
#[cfg_attr(test, derive(Debug))]
pub(crate) enum WriteOutcome<E> {
    /// The worker resolved the write with a concrete result.
    Done(Result<(), E>),
    /// The completion carrier was dropped without completing; the front end
    /// resolves through its sticky terminal instead (never a bare cancel).
    Cancelled,
}

/// A per-stream, **reusable** write-completion cell (SF-3). Replaces the
/// per-write `oneshot` so K sequential writes on one stream reuse a single
/// `Arc`-shared cell (a refcount bump per write) instead of heap-allocating a
/// channel per chunk.
///
/// Reuse across writes is made safe by a **generation counter** with
/// **set-if-current-generation** completion (synthesis MF-B). [`TerminalCell`]'s
/// exactly-once safety derives precisely from *never resetting*, so it is the
/// wrong template for a cell that must be reused; a reused cell reintroduces a
/// worker-set ↔ front-end-reset race unless synchronized. Here:
///
/// - Each new write [`begin`](WriteCompletion::begin)s a fresh generation and
///   clears any stale slot; the generation is stamped into the enqueued op.
/// - The worker completes through a [`WriteCompleter`] whose
///   [`set_if_current`](WriteCompletion::set_if_current) only stores when the
///   op's stamped generation still matches the cell's current generation, so a
///   stale/superseded completion is a no-op rather than clobbering the next
///   write's slot.
/// - The front end consumes the completion for its generation exactly once via
///   [`poll`](WriteCompletion::poll) (which empties the slot), then `begin`s the
///   next generation.
///
/// The single-outstanding-write-per-stream contract (h3's `send_data`↔
/// `poll_ready` handshake) guarantees at most one in-flight generation, so reset
/// and worker-set never target the same generation concurrently, and exactly-once
/// holds *per generation*.
pub(crate) struct WriteCompletion<E> {
    inner: Arc<WriteCompletionInner<E>>,
}

struct WriteCompletionInner<E> {
    /// The cell's current generation. Bumped by the front end before each write.
    generation: AtomicU64,
    /// `(generation, outcome)` for the completion, if one has been stored. The
    /// paired generation lets the front end reject a stale slot defensively.
    slot: Mutex<Option<(u64, WriteOutcome<E>)>>,
    waker: AtomicWaker,
}

impl<E> Clone for WriteCompletion<E> {
    fn clone(&self) -> Self {
        WriteCompletion {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<E> Default for WriteCompletion<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> WriteCompletion<E> {
    pub(crate) fn new() -> Self {
        WriteCompletion {
            inner: Arc::new(WriteCompletionInner {
                generation: AtomicU64::new(0),
                slot: Mutex::new(None),
                waker: AtomicWaker::new(),
            }),
        }
    }

    /// Front end: begin a new write. Advances to a fresh generation, clears any
    /// stale slot, and returns the new generation to stamp into the enqueued op.
    /// Safe to clear here because the single-outstanding-write contract
    /// guarantees the prior generation's completion was already consumed by
    /// [`poll`](WriteCompletion::poll).
    pub(crate) fn begin(&self) -> u64 {
        let generation = self.inner.generation.fetch_add(1, Ordering::AcqRel) + 1;
        *self.inner.slot.lock().expect("WriteCompletion poisoned") = None;
        generation
    }

    /// Build a one-shot [`WriteCompleter`] for `generation`, to hand to the
    /// worker in the enqueued op. Dropping it without completing signals
    /// [`WriteOutcome::Cancelled`] (mirroring `oneshot::Sender`'s drop).
    pub(crate) fn completer(&self, generation: u64) -> WriteCompleter<E> {
        WriteCompleter {
            cell: self.clone(),
            generation,
            completed: false,
        }
    }

    /// Worker: store `outcome` for `generation`, but only if it is still the
    /// current generation and the slot is empty (set-if-current-generation). A
    /// stale/superseded or duplicate completion is dropped. Wakes the front end
    /// when a value is installed.
    fn set_if_current(&self, generation: u64, outcome: WriteOutcome<E>) {
        {
            let mut slot = self.inner.slot.lock().expect("WriteCompletion poisoned");
            if self.inner.generation.load(Ordering::Acquire) != generation || slot.is_some() {
                return; // stale generation or already completed: no-op, no wake
            }
            *slot = Some((generation, outcome));
        }
        self.inner.waker.wake();
    }

    /// Front end: race-free poll for `generation`'s completion. Registers the
    /// waker, then reads the slot; a value for a matching generation is taken
    /// (consumed) exactly once. A slot bearing a different generation is ignored
    /// (defensive — the single-outstanding contract makes this unreachable).
    pub(crate) fn poll(&self, generation: u64, cx: &mut Context<'_>) -> Poll<WriteOutcome<E>> {
        self.inner.waker.register(cx.waker());
        let mut slot = self.inner.slot.lock().expect("WriteCompletion poisoned");
        if matches!(slot.as_ref(), Some((g, _)) if *g == generation) {
            let (_, outcome) = slot.take().expect("slot just matched");
            return Poll::Ready(outcome);
        }
        Poll::Pending
    }

    /// Test-only: non-registering take of a completed outcome for `generation`.
    #[cfg(test)]
    pub(crate) fn try_take(&self, generation: u64) -> Option<WriteOutcome<E>> {
        let mut slot = self.inner.slot.lock().expect("WriteCompletion poisoned");
        if matches!(slot.as_ref(), Some((g, _)) if *g == generation) {
            return Some(slot.take().expect("slot just matched").1);
        }
        None
    }

    /// Test-only: the cell's current generation (advances once per `begin`).
    /// A single cell reused for K writes ends at generation K (SF-3 / SC-004).
    #[cfg(test)]
    pub(crate) fn generation(&self) -> u64 {
        self.inner.generation.load(Ordering::Acquire)
    }

    /// Test-only: identity check proving two handles share the *same* underlying
    /// cell (no per-write allocation) rather than merely being equal.
    #[cfg(test)]
    pub(crate) fn same_cell(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

/// Worker-side one-shot completion carrier for one generation of a
/// [`WriteCompletion`] (SF-3). Carries an `Arc` clone of the cell plus the
/// stamped generation. [`complete`](WriteCompleter::complete) resolves the write;
/// dropping it without completing signals [`WriteOutcome::Cancelled`] so an
/// unapplied/dropped `Send` never hangs the front end (matching the old
/// `oneshot::Sender` drop semantics). Both paths honor set-if-current-generation,
/// so a stale carrier for a superseded generation is a no-op.
pub(crate) struct WriteCompleter<E> {
    cell: WriteCompletion<E>,
    generation: u64,
    completed: bool,
}

impl<E> WriteCompleter<E> {
    /// Resolve the write with `result`, exactly once for this generation.
    pub(crate) fn complete(mut self, result: Result<(), E>) {
        self.cell
            .set_if_current(self.generation, WriteOutcome::Done(result));
        self.completed = true;
    }
}

impl<E> Drop for WriteCompleter<E> {
    fn drop(&mut self) {
        if !self.completed {
            self.cell
                .set_if_current(self.generation, WriteOutcome::Cancelled);
        }
    }
}

/// Aggregate buffered-send-byte accounting with an optional finite cap (SF-6,
/// FR-010/FR-011, §12 S3). Bounds the total bytes resident in the unbounded
/// command channel / per-stream `SendOp` queues so a slow/stalled peer cannot
/// drive send-side memory without limit.
///
/// The accounting domain is **admitted** bytes only — a byte enters residency
/// when the front end reserves it just before enqueuing a `Send` command, and
/// leaves when the carrying [`SendBytesPermit`] drops (the SF-3 completion
/// chokepoint: write completion, terminal drain, unapplied-close, or enqueue
/// rollback). The front-end per-stream `stash` is **excluded** — it is a single
/// not-yet-admitted `WriteBuf` bounded by the h3 single-outstanding-write
/// contract, not aggregate buffering.
///
/// `cap == None` (the default) means unlimited: `try_reserve` always succeeds
/// and no admission ever parks, so behavior is byte-for-byte unchanged from
/// before SF-6 apart from one atomic reserve/release pair per write (plus one
/// uncontended waiter-lock acquisition per release that drains an always-empty
/// waiter list — no task ever parks). A finite cap bounds
/// residency to at most `cap + one admission unit` (the oversize/`cap == 0`
/// exception below guarantees forward progress).
pub(crate) struct SendAccounting {
    /// Resident admitted buffered send bytes.
    resident: AtomicUsize,
    /// Optional finite cap. `None` = unlimited (default; behavior unchanged).
    cap: Option<usize>,
    /// Parked admissions waiting for residency to drop under the cap. A
    /// hand-rolled multi-waiter (NOT a single [`AtomicWaker`], which would drop
    /// other streams' wakers when several stalled streams contend — SH-E).
    ///
    /// `tokio::sync::Notify` is deliberately **not** used here: its `Notified`
    /// future borrows the `Notify` and must be held across polls to stay
    /// registered, which is infeasible inside the synchronous `poll_ready`
    /// (§12 S3 trade-off note).
    waiters: Mutex<Vec<Waker>>,
}

impl SendAccounting {
    /// Create shared accounting with an optional finite `cap`.
    pub(crate) fn new(cap: Option<usize>) -> Arc<Self> {
        Arc::new(SendAccounting {
            resident: AtomicUsize::new(0),
            cap,
            waiters: Mutex::new(Vec::new()),
        })
    }

    /// Currently resident admitted send bytes (inspection / tests).
    pub(crate) fn resident(&self) -> usize {
        self.resident.load(Ordering::Acquire)
    }

    /// The configured cap, if finite.
    pub(crate) fn cap(&self) -> Option<usize> {
        self.cap
    }

    /// Try to admit `bytes`. Reserves atomically iff the new residency fits
    /// under the cap, **or** if nothing is currently resident — the
    /// one-in-flight-unit exception that lets an oversize buffer (or a `cap == 0`
    /// configuration) still make progress, preserving liveness. Returns a
    /// [`SendBytesPermit`] whose `Drop` releases the reservation exactly once.
    ///
    /// Unlimited (`cap == None`) always succeeds. `bytes == 0` always succeeds
    /// with a zero-cost permit.
    pub(crate) fn try_reserve(self: &Arc<Self>, bytes: usize) -> Option<SendBytesPermit> {
        let outcome = self
            .resident
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                let next = cur.checked_add(bytes)?;
                match self.cap {
                    // Over cap, and something is already resident: reject and let
                    // the caller park until a permit drops.
                    Some(cap) if next > cap && cur != 0 => None,
                    _ => Some(next),
                }
            });
        match outcome {
            Ok(_) => Some(SendBytesPermit {
                accounting: Arc::clone(self),
                bytes,
            }),
            Err(_) => None,
        }
    }

    /// Register `waker` to be woken when residency next drops (multi-waiter).
    /// Callers MUST register *before* the final over-cap re-check so a release
    /// racing between check and park cannot be missed (SF-2 discipline, N-1).
    pub(crate) fn register_waiter(&self, waker: &Waker) {
        let mut waiters = self.waiters.lock().expect("send-accounting waiters lock");
        if !waiters.iter().any(|w| w.will_wake(waker)) {
            waiters.push(waker.clone());
        }
    }

    /// Release `bytes` back to the pool and wake every parked admission (they
    /// re-check under the atomic). Invoked only by [`SendBytesPermit::drop`].
    fn release(&self, bytes: usize) {
        if bytes != 0 {
            // Saturating guard: each permit releases exactly once, so this never
            // underflows in practice, but guard defensively rather than wrap.
            let _ = self
                .resident
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                    Some(cur.saturating_sub(bytes))
                });
        }
        // Wake all waiters even for a zero-byte release: a zero-byte permit still
        // signals an admission slot may have opened elsewhere. Drain-and-wake so
        // a re-parking admission re-registers freshly.
        let wakers: Vec<Waker> = {
            let mut waiters = self.waiters.lock().expect("send-accounting waiters lock");
            std::mem::take(&mut *waiters)
        };
        for w in wakers {
            w.wake();
        }
    }
}

/// RAII reservation of `bytes` against a [`SendAccounting`] pool. Created by
/// [`SendAccounting::try_reserve`] at admission and carried with the outbound
/// `Send` command / `SendOp::Write`; its `Drop` decrements residency exactly
/// once and wakes parked admissions. Moving the permit (through the command
/// channel into the worker's op queue) never releases — only the final drop at
/// the SF-3 completion chokepoint (or an unapplied-command / rollback drop)
/// does.
pub(crate) struct SendBytesPermit {
    accounting: Arc<SendAccounting>,
    bytes: usize,
}

impl SendBytesPermit {
    /// The number of bytes this permit holds resident (inspection / tests).
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for SendBytesPermit {
    fn drop(&mut self) {
        self.accounting.release(self.bytes);
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

    /// SF-3 / SC-004: a single reusable cell services K sequential writes with
    /// no per-write allocation — the same underlying cell instance is reused
    /// across generations (identity-checked), advancing one generation per write.
    #[test]
    fn write_completion_reuses_one_cell_across_generations() {
        let cell = WriteCompletion::<i32>::new();
        assert_eq!(cell.generation(), 0);
        for expected_gen in 1..=8u64 {
            let generation = cell.begin();
            assert_eq!(generation, expected_gen);
            let completer = cell.completer(generation);
            // The worker's completer shares the *same* cell (a refcount bump).
            assert!(completer.cell.same_cell(&cell), "completer reuses the cell");
            completer.complete(Ok(()));
            // Front end consumes the completion for this generation exactly once.
            assert!(matches!(
                cell.try_take(generation),
                Some(WriteOutcome::Done(Ok(())))
            ));
            assert!(cell.try_take(generation).is_none(), "consumed exactly once");
        }
        assert_eq!(cell.generation(), 8, "one generation per write, one cell");
    }

    /// SF-3: set-if-current-generation drops a stale completion for a superseded
    /// generation (synthesis MF-B). Worker sets `Ok` for generation g; the front
    /// end consumes it, begins g+1, and enqueues the next write; a lingering
    /// completer for g must NOT clobber the g+1 slot, and g+1 completes exactly
    /// once with its own (reset) terminal.
    #[test]
    fn write_completion_set_if_current_drops_stale_generation() {
        let cell = WriteCompletion::<i32>::new();
        let g1 = cell.begin();
        // Fabricate a lingering carrier for g1 that has NOT completed yet.
        let stale = cell.completer(g1);
        // Front end already consumed g1 and advanced to g2 (begin clears the slot).
        let g2 = cell.begin();
        assert_eq!(g2, g1 + 1);
        let current = cell.completer(g2);
        // The stale g1 completion arrives late: set-if-current-generation drops it.
        stale.complete(Ok(()));
        assert!(cell.try_take(g1).is_none(), "stale g1 store dropped");
        assert!(cell.try_take(g2).is_none(), "stale store must not fill g2");
        // The real g2 completion (a reset terminal) resolves exactly once.
        current.complete(Err(-7));
        assert!(matches!(
            cell.try_take(g2),
            Some(WriteOutcome::Done(Err(-7)))
        ));
        assert!(cell.try_take(g2).is_none(), "g2 consumed exactly once");
    }

    /// SF-3: a completer dropped without completing signals `Cancelled` (matching
    /// the old `oneshot::Sender` drop), so an unapplied/dropped `Send` never hangs
    /// the front end.
    #[test]
    fn write_completion_drop_signals_cancelled() {
        let cell = WriteCompletion::<i32>::new();
        let generation = cell.begin();
        let completer = cell.completer(generation);
        drop(completer);
        assert!(matches!(
            cell.try_take(generation),
            Some(WriteOutcome::Cancelled)
        ));
    }

    /// SF-3: a stale carrier dropped after the cell advanced does NOT inject a
    /// `Cancelled` into the newer generation's slot (set-if-current on drop too).
    #[test]
    fn write_completion_stale_drop_does_not_cancel_new_generation() {
        let cell = WriteCompletion::<i32>::new();
        let g1 = cell.begin();
        let stale = cell.completer(g1);
        let g2 = cell.begin();
        let current = cell.completer(g2);
        drop(stale); // stale Cancelled for g1 is dropped (generation mismatch)
        assert!(cell.try_take(g2).is_none(), "stale drop must not fill g2");
        current.complete(Ok(()));
        assert!(matches!(
            cell.try_take(g2),
            Some(WriteOutcome::Done(Ok(())))
        ));
    }

    /// SF-3: `poll` registers the waker and the worker's completion wakes it
    /// exactly once, race-free (register-then-recheck like [`TerminalCell`]).
    #[test]
    fn write_completion_poll_registers_and_wakes() {
        let cell = WriteCompletion::<i32>::new();
        let generation = cell.begin();
        let flag = Arc::new(AtomicBool::new(false));
        let waker = flag_waker(flag.clone());
        let mut cx = Context::from_waker(&waker);
        // Nothing completed yet: Pending, waker registered, not yet woken.
        assert!(matches!(cell.poll(generation, &mut cx), Poll::Pending));
        assert!(!flag.load(Ordering::SeqCst));
        // Worker completes → the registered waker fires.
        cell.completer(generation).complete(Ok(()));
        assert!(flag.load(Ordering::SeqCst), "completion woke the poller");
        match cell.poll(generation, &mut cx) {
            Poll::Ready(WriteOutcome::Done(Ok(()))) => {}
            other => panic!("expected Ready(Done(Ok)), got {other:?}"),
        }
    }

    // ---- SF-6 send-byte accounting ----

    #[test]
    fn send_accounting_unlimited_increments_and_releases_once() {
        // Default (cap == None): every reserve succeeds and residency tracks the
        // outstanding bytes, returning to zero after the permits drop.
        let acct = SendAccounting::new(None);
        assert_eq!(acct.resident(), 0);
        assert_eq!(acct.cap(), None);

        let p1 = acct.try_reserve(1000).expect("unlimited reserve");
        let p2 = acct.try_reserve(2500).expect("unlimited reserve");
        assert_eq!(acct.resident(), 3500);
        assert_eq!(p1.bytes(), 1000);

        drop(p1);
        assert_eq!(acct.resident(), 2500);
        drop(p2);
        assert_eq!(acct.resident(), 0);
    }

    #[test]
    fn send_accounting_capped_rejects_over_cap_then_admits_after_release() {
        // A finite cap parks admissions that would exceed it, and re-admits once
        // an outstanding permit drops (the front end retries after a wake).
        let acct = SendAccounting::new(Some(100));
        let p1 = acct.try_reserve(60).expect("fits under cap");
        assert_eq!(acct.resident(), 60);

        // 60 + 60 = 120 > 100 and residency is non-zero → rejected (park).
        assert!(acct.try_reserve(60).is_none());
        assert_eq!(
            acct.resident(),
            60,
            "rejected reserve must not mutate residency"
        );

        // A smaller reserve that still fits is admitted.
        let p2 = acct.try_reserve(40).expect("40 fits (100 total == cap)");
        assert_eq!(acct.resident(), 100);
        assert!(acct.try_reserve(1).is_none(), "at cap, nothing more admits");

        drop(p1);
        // 40 resident now; the 60 retry fits.
        let _p3 = acct.try_reserve(60).expect("fits after release");
        assert_eq!(acct.resident(), 100);
        drop(p2);
    }

    #[test]
    fn send_accounting_oversize_admits_one_unit_and_bounds_at_cap_plus_unit() {
        // The `current == 0` exception lets a single oversize buffer through so
        // an item larger than the cap can still make progress, but only one such
        // unit is ever admitted: residency is bounded by cap + one unit.
        let acct = SendAccounting::new(Some(100));
        let big = acct
            .try_reserve(250)
            .expect("oversize admits when nothing resident");
        assert_eq!(acct.resident(), 250);

        // With the oversize unit resident, nothing else admits until it drops.
        assert!(acct.try_reserve(1).is_none());
        assert!(acct.try_reserve(250).is_none());
        assert_eq!(acct.resident(), 250);

        drop(big);
        assert_eq!(acct.resident(), 0);
        // cap == 0 is the degenerate oversize case: only the single-unit
        // exception ever admits.
        let acct0 = SendAccounting::new(Some(0));
        let unit = acct0
            .try_reserve(10)
            .expect("cap==0 admits one in-flight unit");
        assert!(acct0.try_reserve(1).is_none());
        drop(unit);
        assert!(acct0.try_reserve(10).is_some());
    }

    #[test]
    fn send_accounting_release_wakes_parked_waiter() {
        // A parked admission registers a waker; dropping a permit wakes it so it
        // can retry. Register-before-recheck (SF-2) means a release racing the
        // park is never missed.
        let acct = SendAccounting::new(Some(100));
        let p = acct.try_reserve(100).expect("fills cap");

        let flag = Arc::new(AtomicBool::new(false));
        let waker = flag_waker(flag.clone());
        // Over-cap attempt fails; the front end registers then re-checks.
        assert!(acct.try_reserve(50).is_none());
        acct.register_waiter(&waker);
        assert!(acct.try_reserve(50).is_none());
        assert!(!flag.load(Ordering::SeqCst));

        drop(p);
        assert!(
            flag.load(Ordering::SeqCst),
            "release must wake the parked admission"
        );
        // After the wake the retry now fits.
        assert!(acct.try_reserve(50).is_some());
    }

    #[test]
    fn send_accounting_register_waiter_dedups_equal_waker() {
        // Repeated registration of the same waker (a stream polled repeatedly)
        // must not grow the waiter list without bound.
        let acct = SendAccounting::new(Some(10));
        let _p = acct.try_reserve(10).expect("fills cap");
        let flag = Arc::new(AtomicBool::new(false));
        let waker = flag_waker(flag.clone());
        acct.register_waiter(&waker);
        acct.register_waiter(&waker);
        acct.register_waiter(&waker);
        assert_eq!(acct.waiters.lock().unwrap().len(), 1);
    }

    #[test]
    fn send_accounting_zero_byte_reserve_is_free() {
        // A zero-byte write still yields a permit (uniform enqueue path) without
        // perturbing residency.
        let acct = SendAccounting::new(Some(100));
        let p = acct
            .try_reserve(0)
            .expect("zero-byte reserve always admits");
        assert_eq!(acct.resident(), 0);
        assert_eq!(p.bytes(), 0);
        drop(p);
        assert_eq!(acct.resident(), 0);
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
