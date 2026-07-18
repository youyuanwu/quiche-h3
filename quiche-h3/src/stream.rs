//! Front end: streams and connection objects implementing the `h3::quic`
//! traits (`H3Stream`, `H3SendStream`, `H3RecvStream`, `Connection`,
//! `StreamOpener`) — design §6.
//!
//! Every method here is a **synchronous** `poll_*(cx)` that must never block:
//! bytes/handoffs are read through non-blocking channel `poll_recv`, terminals
//! through the race-free [`TerminalCell::poll`], and control commands are sent
//! over the unbounded control channel (`send`, never `try_send`). Correctness
//! rests on: exactly-once completion, first-writer-wins terminal cells, the
//! §5.1 sealing edge (a single byte/accept recheck after observing a terminal),
//! and producer-coalesced resume bits flipped only on the false→true edge.
#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use tokio::sync::{mpsc, oneshot};

use h3::quic::{self, ConnectionErrorIncoming, StreamErrorIncoming, StreamId, WriteBuf};

use crate::buffer::TerminalCell;
use crate::driver::{BidiHandoff, ConnShared, DriverCommand, RecvHandoff, SendHandoff};
use crate::error::{internal_stream_error, ConnTerminal, RecvEnd, SendEnd};

/// Convert a worker `u64` stream id into the h3 [`StreamId`]. The worker only
/// ever allocates/admits valid QUIC varint ids, so this never fails.
fn stream_id(id: u64) -> StreamId {
    StreamId::try_from(id).expect("worker allocates only valid QUIC stream ids")
}

/// Map a published connection terminal to the stream-level h3 error used when a
/// stream operation is resolved by a connection close (§8.4).
fn conn_terminal_stream_err(term: &Arc<ConnTerminal>) -> StreamErrorIncoming {
    StreamErrorIncoming::ConnectionErrorIncoming {
        connection_error: term.to_h3(),
    }
}

// ===================================================================
// Receive half
// ===================================================================

/// The `h3::quic::RecvStream` front-end half (§6). Drains the bounded byte
/// channel first, then reads the out-of-band terminal; a producer-coalesced
/// resume bit is flipped false→true when capacity is freed. `B` appears only in
/// the `cmd_tx` type — the received `Buf` is always [`Bytes`].
pub struct H3RecvStream<B: Buf> {
    id: u64,
    bytes: mpsc::Receiver<Bytes>,
    terminal: TerminalCell<RecvEnd>,
    resume: Arc<AtomicBool>,
    cmd_tx: mpsc::UnboundedSender<DriverCommand<B>>,
    /// A terminal has been observed and returned; `Drop` need not stop-send.
    terminal_seen: bool,
    /// A `StopSending` was already enqueued (explicitly or by a prior drop path).
    stop_sent: bool,
}

impl<B: Buf> H3RecvStream<B> {
    pub(crate) fn from_handoff(h: RecvHandoff<B>) -> Self {
        H3RecvStream {
            id: h.id,
            bytes: h.bytes,
            terminal: h.terminal,
            resume: h.resume,
            cmd_tx: h.cmd_tx,
            terminal_seen: false,
            stop_sent: false,
        }
    }

    /// Freed one byte-channel slot: flip the shared resume bit and nudge the
    /// worker **only** on the false→true edge (§5.1 coalescing).
    fn signal_resume(&self) {
        if !self.resume.swap(true, Ordering::Relaxed) {
            let _ = self.cmd_tx.send(DriverCommand::RecvResume { id: self.id });
        }
    }

    /// Cache and map an observed terminal: `Fin` → `Ok(None)`, otherwise the
    /// stream error (§8.4).
    fn resolve_terminal(
        &mut self,
        end: RecvEnd,
    ) -> Poll<Result<Option<Bytes>, StreamErrorIncoming>> {
        self.terminal_seen = true;
        match end.to_h3() {
            None => Poll::Ready(Ok(None)),
            Some(err) => Poll::Ready(Err(err)),
        }
    }
}

impl<B: Buf> quic::RecvStream for H3RecvStream<B> {
    type Buf = Bytes;

    fn poll_data(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::Buf>, StreamErrorIncoming>> {
        // 1. Drain buffered bytes first.
        match self.bytes.poll_recv(cx) {
            Poll::Ready(Some(b)) => {
                self.signal_resume();
                Poll::Ready(Ok(Some(b)))
            }
            Poll::Ready(None) => {
                // Channel closed. The worker publishes the terminal *before*
                // dropping the byte sender (§5.1 sealing), so it must be present;
                // its absence is an adapter bug.
                match self.terminal.poll(cx) {
                    Poll::Ready(end) => self.resolve_terminal(end),
                    Poll::Pending => Poll::Ready(Err(internal_stream_error(
                        "recv byte channel closed without a published terminal",
                    ))),
                }
            }
            Poll::Pending => {
                // Channel open but empty: consult the out-of-band terminal.
                match self.terminal.poll(cx) {
                    Poll::Ready(end) => {
                        // Sealing-edge single recheck (M1): a byte may have raced
                        // in just before the terminal was observed — yield it
                        // first so accepted bytes are never truncated by EOF.
                        if let Poll::Ready(Some(b)) = self.bytes.poll_recv(cx) {
                            self.signal_resume();
                            return Poll::Ready(Ok(Some(b)));
                        }
                        self.resolve_terminal(end)
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }

    fn stop_sending(&mut self, error_code: u64) {
        self.stop_sent = true;
        let _ = self
            .cmd_tx
            .send(DriverCommand::StopSending { id: self.id, code: error_code });
    }

    fn recv_id(&self) -> StreamId {
        stream_id(self.id)
    }
}

impl<B: Buf> Drop for H3RecvStream<B> {
    fn drop(&mut self) {
        // Normal local abandonment of an unread recv half → STOP_SENDING(0),
        // unless it was already stopped or has already ended (§6.2).
        if self.stop_sent || self.terminal_seen || self.terminal.get().is_some() {
            return;
        }
        let _ = self
            .cmd_tx
            .send(DriverCommand::StopSending { id: self.id, code: 0 });
    }
}

// ===================================================================
// Send half
// ===================================================================

/// The `h3::quic::SendStream` front-end half (§6). Follows the h3 single-slot
/// send contract: `send_data` stashes exactly one `WriteBuf`, `poll_ready`
/// flushes it through the worker and reports the recorded completion once, and
/// `poll_finish`/`reset` drive an idempotent finalization state machine.
pub struct H3SendStream<B: Buf> {
    id: u64,
    status: TerminalCell<SendEnd>,
    cmd_tx: mpsc::UnboundedSender<DriverCommand<B>>,
    /// The single pending `WriteBuf` awaiting a `poll_ready` flush.
    stash: Option<WriteBuf<B>>,
    /// Completion of the in-flight `Send`, resolved exactly once by the worker.
    send_completion: Option<oneshot::Receiver<Result<(), SendEnd>>>,
    /// Completion of the in-flight `Finish`.
    finish_completion: Option<oneshot::Receiver<Result<(), SendEnd>>>,
    /// Retained `poll_finish` result, returned on every later poll.
    finish_result: Option<Result<(), SendEnd>>,
    /// A FIN/reset/terminal has been chosen: no further op may be enqueued.
    finalized: bool,
    /// A locally-issued `reset` terminal, visible immediately (the worker's
    /// `status` cell is only set asynchronously afterward).
    local_terminal: Option<SendEnd>,
}

impl<B: Buf> H3SendStream<B> {
    pub(crate) fn from_handoff(h: SendHandoff<B>) -> Self {
        H3SendStream {
            id: h.id,
            status: h.status,
            cmd_tx: h.cmd_tx,
            stash: None,
            send_completion: None,
            finish_completion: None,
            finish_result: None,
            finalized: false,
            local_terminal: None,
        }
    }

    /// The sticky send terminal visible right now: a local reset outranks the
    /// worker's `status` cell, which is consulted race-free (register + recheck).
    fn terminal_now(&self, cx: &mut Context<'_>) -> Option<SendEnd> {
        if let Some(end) = &self.local_terminal {
            return Some(end.clone());
        }
        match self.status.poll(cx) {
            Poll::Ready(end) => Some(end),
            Poll::Pending => None,
        }
    }

    /// The sticky send terminal without a context (for `Drop`).
    fn terminal_now_noctx(&self) -> Option<SendEnd> {
        self.local_terminal.clone().or_else(|| self.status.get())
    }

    /// Resolve a failed/cancelled completion through the sticky terminal, or an
    /// adapter-bug `InternalError` — never a bare cancel (§5.2 M3).
    fn sticky_or_internal(&self, cx: &mut Context<'_>, msg: &'static str) -> StreamErrorIncoming {
        match self.terminal_now(cx) {
            Some(end) => end.to_h3(),
            None => internal_stream_error(msg),
        }
    }

    /// Like [`sticky_or_internal`](Self::sticky_or_internal) but yields a
    /// [`SendEnd`] so the failure can be **retained** (e.g. as `finish_result`),
    /// ensuring a later poll returns the same error and never defaults to `Ok`.
    /// The `Internal` fallback is modeled as `SendEnd::Conn(Internal)`, which
    /// maps to the same `InternalError` as [`internal_stream_error`].
    fn sticky_send_end_or_internal(&self, cx: &mut Context<'_>, msg: &'static str) -> SendEnd {
        self.terminal_now(cx)
            .unwrap_or_else(|| SendEnd::Conn(Arc::new(ConnTerminal::Internal(msg))))
    }
}

impl<B: Buf> quic::SendStream<B> for H3SendStream<B> {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        // (1) An in-flight write completion outranks everything: report it once.
        if self.send_completion.is_some() {
            match Pin::new(self.send_completion.as_mut().unwrap()).poll(cx) {
                Poll::Ready(Ok(result)) => {
                    self.send_completion = None;
                    return Poll::Ready(result.map_err(|e| e.to_h3()));
                }
                Poll::Ready(Err(_)) => {
                    self.send_completion = None;
                    return Poll::Ready(Err(self.sticky_or_internal(
                        cx,
                        "send completion cancelled without a terminal",
                    )));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        // (2) A sticky terminal rejects idle or new work.
        if let Some(end) = self.terminal_now(cx) {
            return Poll::Ready(Err(end.to_h3()));
        }
        // (3) Nothing stashed → idle readiness fast path (§2.1).
        let buf = match self.stash.take() {
            None => return Poll::Ready(Ok(())),
            Some(buf) => buf,
        };
        // (4) Flush the stash as exactly one `Send`, store + poll its completion.
        let (done_tx, done_rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(DriverCommand::Send { id: self.id, buf, done: done_tx })
            .is_err()
        {
            return Poll::Ready(Err(
                self.sticky_or_internal(cx, "send channel closed without a terminal")
            ));
        }
        self.send_completion = Some(done_rx);
        match Pin::new(self.send_completion.as_mut().unwrap()).poll(cx) {
            Poll::Ready(Ok(result)) => {
                self.send_completion = None;
                Poll::Ready(result.map_err(|e| e.to_h3()))
            }
            Poll::Ready(Err(_)) => {
                self.send_completion = None;
                Poll::Ready(Err(
                    self.sticky_or_internal(cx, "send completion cancelled without a terminal")
                ))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn send_data<T: Into<WriteBuf<B>>>(&mut self, data: T) -> Result<(), StreamErrorIncoming> {
        if self.stash.is_some() {
            // The h3 contract requires a `poll_ready` flush between sends.
            return Err(internal_stream_error(
                "send_data called while a previous write is still pending poll_ready",
            ));
        }
        self.stash = Some(data.into());
        Ok(())
    }

    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        // Retained result: reuse it on every later poll (idempotent).
        if let Some(result) = &self.finish_result {
            return Poll::Ready(result.clone().map_err(|e| e.to_h3()));
        }
        // In-flight finish completion: poll before sticky status.
        if self.finish_completion.is_some() {
            match Pin::new(self.finish_completion.as_mut().unwrap()).poll(cx) {
                Poll::Ready(Ok(result)) => {
                    self.finish_completion = None;
                    self.finish_result = Some(result.clone());
                    return Poll::Ready(result.map_err(|e| e.to_h3()));
                }
                Poll::Ready(Err(_)) => {
                    self.finish_completion = None;
                    // Persist the failure so a later poll cannot default to Ok
                    // via the `finalized` branch below.
                    let end =
                        self.sticky_send_end_or_internal(cx, "finish completion cancelled without a terminal");
                    self.finish_result = Some(Err(end.clone()));
                    return Poll::Ready(Err(end.to_h3()));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        // Finalized by a prior `reset` (or a channel-closed finish): return the
        // sticky local terminal rather than enqueueing.
        if self.finalized {
            return Poll::Ready(match self.terminal_now(cx) {
                Some(end) => Err(end.to_h3()),
                None => Ok(()),
            });
        }
        // First finish: consult sticky status first.
        if let Some(end) = self.terminal_now(cx) {
            self.finalized = true;
            self.finish_result = Some(Err(end.clone()));
            return Poll::Ready(Err(end.to_h3()));
        }
        // Enqueue exactly one `Finish`.
        let (done_tx, done_rx) = oneshot::channel();
        self.finalized = true;
        if self
            .cmd_tx
            .send(DriverCommand::Finish { id: self.id, done: done_tx })
            .is_err()
        {
            let end = self.sticky_send_end_or_internal(cx, "finish channel closed without a terminal");
            self.finish_result = Some(Err(end.clone()));
            return Poll::Ready(Err(end.to_h3()));
        }
        self.finish_completion = Some(done_rx);
        match Pin::new(self.finish_completion.as_mut().unwrap()).poll(cx) {
            Poll::Ready(Ok(result)) => {
                self.finish_completion = None;
                self.finish_result = Some(result.clone());
                Poll::Ready(result.map_err(|e| e.to_h3()))
            }
            Poll::Ready(Err(_)) => {
                self.finish_completion = None;
                let end =
                    self.sticky_send_end_or_internal(cx, "finish completion cancelled without a terminal");
                self.finish_result = Some(Err(end.clone()));
                Poll::Ready(Err(end.to_h3()))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn reset(&mut self, reset_code: u64) {
        // One reset only; never overwrite an already-finalized direction (§6.2).
        if self.finalized {
            return;
        }
        self.finalized = true;
        self.local_terminal = Some(SendEnd::Reset { error_code: reset_code });
        // Does not drop an existing send/finish completion receiver (§5.3a).
        let _ = self
            .cmd_tx
            .send(DriverCommand::Reset { id: self.id, code: reset_code });
    }

    fn send_id(&self) -> StreamId {
        stream_id(self.id)
    }
}

impl<B: Buf> Drop for H3SendStream<B> {
    fn drop(&mut self) {
        // Graceful finish-on-drop for an unfinished send half (§6.2). A dropped
        // completion receiver is harmless: the worker's `reply.send` just fails.
        if self.finalized || self.terminal_now_noctx().is_some() {
            return;
        }
        self.finalized = true;
        let (done_tx, _done_rx) = oneshot::channel();
        let _ = self
            .cmd_tx
            .send(DriverCommand::Finish { id: self.id, done: done_tx });
    }
}

// ===================================================================
// Bidirectional stream
// ===================================================================

/// A bidirectional stream: an `H3SendStream` + `H3RecvStream` that also
/// implements `BidiStream` so h3 can `split()` it into its two halves (§6).
pub struct H3Stream<B: Buf> {
    send: H3SendStream<B>,
    recv: H3RecvStream<B>,
}

impl<B: Buf> H3Stream<B> {
    pub(crate) fn from_handoff(h: BidiHandoff<B>) -> Self {
        H3Stream {
            send: H3SendStream::from_handoff(h.send),
            recv: H3RecvStream::from_handoff(h.recv),
        }
    }
}

impl<B: Buf> quic::SendStream<B> for H3Stream<B> {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        self.send.poll_ready(cx)
    }
    fn send_data<T: Into<WriteBuf<B>>>(&mut self, data: T) -> Result<(), StreamErrorIncoming> {
        self.send.send_data(data)
    }
    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        self.send.poll_finish(cx)
    }
    fn reset(&mut self, reset_code: u64) {
        self.send.reset(reset_code)
    }
    fn send_id(&self) -> StreamId {
        self.send.send_id()
    }
}

impl<B: Buf> quic::RecvStream for H3Stream<B> {
    type Buf = Bytes;
    fn poll_data(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::Buf>, StreamErrorIncoming>> {
        self.recv.poll_data(cx)
    }
    fn stop_sending(&mut self, error_code: u64) {
        self.recv.stop_sending(error_code)
    }
    fn recv_id(&self) -> StreamId {
        self.recv.recv_id()
    }
}

impl<B: Buf> quic::BidiStream<B> for H3Stream<B> {
    type SendStream = H3SendStream<B>;
    type RecvStream = H3RecvStream<B>;
    fn split(self) -> (Self::SendStream, Self::RecvStream) {
        (self.send, self.recv)
    }
}

// ===================================================================
// Stream opener
// ===================================================================

/// The `h3::quic::OpenStreams` front-end (§6.1). Stream-ID allocation is
/// worker-owned; `poll_open_*` only submit an `OpenBidi`/`OpenUni` request
/// through the close-admission submit helper and await the worker's handoff.
/// A single-slot `pending_*` receiver makes repeated polls idempotent.
pub struct StreamOpener<B: Buf> {
    cmd_tx: mpsc::UnboundedSender<DriverCommand<B>>,
    shared: Arc<ConnShared>,
    pending_bidi: Option<oneshot::Receiver<Result<BidiHandoff<B>, Arc<ConnTerminal>>>>,
    pending_uni: Option<oneshot::Receiver<Result<SendHandoff<B>, Arc<ConnTerminal>>>>,
}

impl<B: Buf> StreamOpener<B> {
    pub(crate) fn from_parts(
        cmd_tx: mpsc::UnboundedSender<DriverCommand<B>>,
        shared: Arc<ConnShared>,
    ) -> Self {
        StreamOpener {
            cmd_tx,
            shared,
            pending_bidi: None,
            pending_uni: None,
        }
    }

    /// The terminal handed to a submitter the worker declined: the published
    /// connection terminal if present, else an adapter-bug `InternalError`
    /// (never a bare cancel, §5.2 M3).
    fn submit_terminal(&self) -> StreamErrorIncoming {
        match self.shared.conn_terminal.get() {
            Some(term) => conn_terminal_stream_err(&term),
            None => internal_stream_error("open declined without a published terminal"),
        }
    }
}

impl<B: Buf> Clone for StreamOpener<B> {
    fn clone(&self) -> Self {
        // Fresh empty pending slots: an in-flight open belongs to the original
        // clone (§6.1). This is the exact late-open race the M3 gate closes.
        StreamOpener {
            cmd_tx: self.cmd_tx.clone(),
            shared: Arc::clone(&self.shared),
            pending_bidi: None,
            pending_uni: None,
        }
    }
}

impl<B: Buf> quic::OpenStreams<B> for StreamOpener<B> {
    type BidiStream = H3Stream<B>;
    type SendStream = H3SendStream<B>;

    fn poll_open_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::BidiStream, StreamErrorIncoming>> {
        if self.pending_bidi.is_none() {
            // Close-admission submit helper (§5.2 M3): a preset terminal or a
            // failed send resolves *this* poll locally, never stores a doomed
            // receiver.
            if let Some(term) = self.shared.conn_terminal.get() {
                return Poll::Ready(Err(conn_terminal_stream_err(&term)));
            }
            let (reply_tx, reply_rx) = oneshot::channel();
            if self
                .cmd_tx
                .send(DriverCommand::OpenBidi { reply: reply_tx })
                .is_err()
            {
                return Poll::Ready(Err(self.submit_terminal()));
            }
            self.pending_bidi = Some(reply_rx);
        }
        match Pin::new(self.pending_bidi.as_mut().unwrap()).poll(cx) {
            Poll::Ready(Ok(Ok(handoff))) => {
                self.pending_bidi = None;
                Poll::Ready(Ok(H3Stream::from_handoff(handoff)))
            }
            Poll::Ready(Ok(Err(term))) => {
                self.pending_bidi = None;
                Poll::Ready(Err(conn_terminal_stream_err(&term)))
            }
            Poll::Ready(Err(_)) => {
                // The worker dropped the reply without answering: fall back to
                // the published terminal (else InternalError), never a cancel.
                self.pending_bidi = None;
                Poll::Ready(Err(self.submit_terminal()))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_open_send(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::SendStream, StreamErrorIncoming>> {
        if self.pending_uni.is_none() {
            if let Some(term) = self.shared.conn_terminal.get() {
                return Poll::Ready(Err(conn_terminal_stream_err(&term)));
            }
            let (reply_tx, reply_rx) = oneshot::channel();
            if self
                .cmd_tx
                .send(DriverCommand::OpenUni { reply: reply_tx })
                .is_err()
            {
                return Poll::Ready(Err(self.submit_terminal()));
            }
            self.pending_uni = Some(reply_rx);
        }
        match Pin::new(self.pending_uni.as_mut().unwrap()).poll(cx) {
            Poll::Ready(Ok(Ok(handoff))) => {
                self.pending_uni = None;
                Poll::Ready(Ok(H3SendStream::from_handoff(handoff)))
            }
            Poll::Ready(Ok(Err(term))) => {
                self.pending_uni = None;
                Poll::Ready(Err(conn_terminal_stream_err(&term)))
            }
            Poll::Ready(Err(_)) => {
                self.pending_uni = None;
                Poll::Ready(Err(self.submit_terminal()))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn close(&mut self, code: h3::error::Code, reason: &[u8]) {
        let _ = self.cmd_tx.send(DriverCommand::Close {
            code: code.value(),
            reason: Bytes::copy_from_slice(reason),
        });
    }
}

// ===================================================================
// Connection
// ===================================================================

/// The `h3::quic::Connection` front-end (§6): the two bounded accept receivers,
/// their per-direction accept-terminal cells and resume bits, and an embedded
/// `StreamOpener` it delegates `OpenStreams` to.
pub struct Connection<B: Buf> {
    accept_bidi_rx: mpsc::Receiver<BidiHandoff<B>>,
    accept_uni_rx: mpsc::Receiver<RecvHandoff<B>>,
    accept_terminal_bidi: TerminalCell<Arc<ConnTerminal>>,
    accept_terminal_uni: TerminalCell<Arc<ConnTerminal>>,
    accept_bidi_resume: Arc<AtomicBool>,
    accept_uni_resume: Arc<AtomicBool>,
    opener: StreamOpener<B>,
}

impl<B: Buf> Connection<B> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        accept_bidi_rx: mpsc::Receiver<BidiHandoff<B>>,
        accept_uni_rx: mpsc::Receiver<RecvHandoff<B>>,
        accept_terminal_bidi: TerminalCell<Arc<ConnTerminal>>,
        accept_terminal_uni: TerminalCell<Arc<ConnTerminal>>,
        accept_bidi_resume: Arc<AtomicBool>,
        accept_uni_resume: Arc<AtomicBool>,
        opener: StreamOpener<B>,
    ) -> Self {
        Connection {
            accept_bidi_rx,
            accept_uni_rx,
            accept_terminal_bidi,
            accept_terminal_uni,
            accept_bidi_resume,
            accept_uni_resume,
            opener,
        }
    }

    /// Freed one bidi accept-queue slot: flip the bidi accept-resume bit and
    /// nudge the worker only on the false→true edge (§5.1 coalescing).
    fn signal_accept_bidi_resume(&self) {
        if !self.accept_bidi_resume.swap(true, Ordering::Relaxed) {
            let _ = self.opener.cmd_tx.send(DriverCommand::AcceptBidiResume);
        }
    }

    /// Freed one uni accept-queue slot: flip the uni accept-resume bit.
    fn signal_accept_uni_resume(&self) {
        if !self.accept_uni_resume.swap(true, Ordering::Relaxed) {
            let _ = self.opener.cmd_tx.send(DriverCommand::AcceptUniResume);
        }
    }
}

impl<B: Buf> quic::OpenStreams<B> for Connection<B> {
    type BidiStream = H3Stream<B>;
    type SendStream = H3SendStream<B>;

    fn poll_open_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::BidiStream, StreamErrorIncoming>> {
        self.opener.poll_open_bidi(cx)
    }

    fn poll_open_send(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::SendStream, StreamErrorIncoming>> {
        self.opener.poll_open_send(cx)
    }

    fn close(&mut self, code: h3::error::Code, reason: &[u8]) {
        self.opener.close(code, reason)
    }
}

impl<B: Buf> quic::Connection<B> for Connection<B> {
    type RecvStream = H3RecvStream<B>;
    type OpenStreams = StreamOpener<B>;

    fn poll_accept_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::RecvStream, ConnectionErrorIncoming>> {
        match self.accept_uni_rx.poll_recv(cx) {
            Poll::Ready(Some(handoff)) => {
                self.signal_accept_uni_resume();
                Poll::Ready(Ok(H3RecvStream::from_handoff(handoff)))
            }
            Poll::Ready(None) => match self.accept_terminal_uni.poll(cx) {
                Poll::Ready(term) => Poll::Ready(Err(term.to_h3())),
                Poll::Pending => Poll::Ready(Err(ConnectionErrorIncoming::InternalError(
                    "uni accept channel closed without a published terminal".to_string(),
                ))),
            },
            Poll::Pending => match self.accept_terminal_uni.poll(cx) {
                Poll::Ready(term) => {
                    // Sealing-edge single recheck (M1): an accepted stream may
                    // have raced in just before the accept terminal.
                    if let Poll::Ready(Some(handoff)) = self.accept_uni_rx.poll_recv(cx) {
                        self.signal_accept_uni_resume();
                        return Poll::Ready(Ok(H3RecvStream::from_handoff(handoff)));
                    }
                    Poll::Ready(Err(term.to_h3()))
                }
                Poll::Pending => Poll::Pending,
            },
        }
    }

    fn poll_accept_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::BidiStream, ConnectionErrorIncoming>> {
        match self.accept_bidi_rx.poll_recv(cx) {
            Poll::Ready(Some(handoff)) => {
                self.signal_accept_bidi_resume();
                Poll::Ready(Ok(H3Stream::from_handoff(handoff)))
            }
            Poll::Ready(None) => match self.accept_terminal_bidi.poll(cx) {
                Poll::Ready(term) => Poll::Ready(Err(term.to_h3())),
                Poll::Pending => Poll::Ready(Err(ConnectionErrorIncoming::InternalError(
                    "bidi accept channel closed without a published terminal".to_string(),
                ))),
            },
            Poll::Pending => match self.accept_terminal_bidi.poll(cx) {
                Poll::Ready(term) => {
                    if let Poll::Ready(Some(handoff)) = self.accept_bidi_rx.poll_recv(cx) {
                        self.signal_accept_bidi_resume();
                        return Poll::Ready(Ok(H3Stream::from_handoff(handoff)));
                    }
                    Poll::Ready(Err(term.to_h3()))
                }
                Poll::Pending => Poll::Pending,
            },
        }
    }

    fn opener(&self) -> Self::OpenStreams {
        // Clone → fresh pending slots (§6.1).
        self.opener.clone()
    }
}

impl<B: Buf> Drop for Connection<B> {
    fn drop(&mut self) {
        // Enqueue before the accept receivers close, so the worker cleans up
        // parked peer streams promptly (§6.2, iter9 finding 4).
        let _ = self.opener.cmd_tx.send(DriverCommand::ConnectionDropped);
    }
}

// ===================================================================
// §11 compile-time trait gate
// ===================================================================

/// Static assertion that every `h3::quic` trait the bridge must provide is
/// implemented by the front-end types (design §11). Never called; it fails to
/// compile if any signature drifts from h3 0.0.8.
fn _assert_h3_traits<B: Buf>() {
    fn is_connection<B: Buf, T: quic::Connection<B>>() {}
    fn is_open_streams<B: Buf, T: quic::OpenStreams<B>>() {}
    fn is_bidi_stream<B: Buf, T: quic::BidiStream<B>>() {}
    fn is_send_stream<B: Buf, T: quic::SendStream<B>>() {}
    fn is_recv_stream<T: quic::RecvStream>() {}

    is_connection::<B, Connection<B>>();
    is_open_streams::<B, StreamOpener<B>>();
    is_bidi_stream::<B, H3Stream<B>>();
    is_send_stream::<B, H3SendStream<B>>();
    is_recv_stream::<H3RecvStream<B>>();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CloseOrigin;
    use h3::quic::{Connection as _, OpenStreams as _, RecvStream as _, SendStream as _};
    use std::task::{RawWaker, RawWakerVTable, Waker};

    // ---- test plumbing ----

    fn noop_cx() -> Context<'static> {
        Context::from_waker(noop_waker_ref())
    }

    fn noop_waker_ref() -> &'static Waker {
        static VTABLE: RawWakerVTable =
            RawWakerVTable::new(|_| RawWaker::new(std::ptr::null(), &VTABLE), |_| {}, |_| {}, |_| {});
        static WAKER: std::sync::OnceLock<Waker> = std::sync::OnceLock::new();
        WAKER.get_or_init(|| unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) })
    }

    fn recv_channel() -> (
        mpsc::Sender<Bytes>,
        TerminalCell<RecvEnd>,
        Arc<AtomicBool>,
        H3RecvStream<Bytes>,
        mpsc::UnboundedReceiver<DriverCommand<Bytes>>,
    ) {
        let (btx, brx) = mpsc::channel(4);
        let (ctx, crx) = mpsc::unbounded_channel();
        let terminal = TerminalCell::new();
        let resume = Arc::new(AtomicBool::new(false));
        let recv = H3RecvStream::from_handoff(RecvHandoff {
            id: 0,
            bytes: brx,
            terminal: terminal.clone(),
            resume: Arc::clone(&resume),
            cmd_tx: ctx,
        });
        (btx, terminal, resume, recv, crx)
    }

    fn send_half(
        id: u64,
    ) -> (
        TerminalCell<SendEnd>,
        H3SendStream<Bytes>,
        mpsc::UnboundedReceiver<DriverCommand<Bytes>>,
    ) {
        let (ctx, crx) = mpsc::unbounded_channel();
        let status = TerminalCell::new();
        let send = H3SendStream::from_handoff(SendHandoff {
            id,
            status: status.clone(),
            cmd_tx: ctx,
        });
        (status, send, crx)
    }

    fn wbuf(payload: &'static [u8]) -> WriteBuf<Bytes> {
        WriteBuf::from(h3::proto::frame::Frame::Data(Bytes::from_static(payload)))
    }

    // ---- H3RecvStream ----

    #[test]
    fn poll_data_delivers_buffered_bytes_before_terminal() {
        let (btx, terminal, _resume, mut recv, _crx) = recv_channel();
        // A byte is buffered AND the terminal is set: bytes win (§5.1 sealing).
        btx.try_send(Bytes::from_static(b"hi")).unwrap();
        terminal.set(RecvEnd::Fin);
        let mut cx = noop_cx();
        match recv.poll_data(&mut cx) {
            Poll::Ready(Ok(Some(b))) => assert_eq!(&b[..], b"hi"),
            other => panic!("expected buffered bytes first, got {other:?}"),
        }
        // Now the queue is drained: the sticky terminal maps to clean EOF.
        assert!(matches!(recv.poll_data(&mut cx), Poll::Ready(Ok(None))));
    }

    #[test]
    fn poll_data_maps_fin_reset_conn() {
        let mut cx = noop_cx();
        // Fin → Ok(None)
        {
            let (_btx, terminal, _r, mut recv, _c) = recv_channel();
            terminal.set(RecvEnd::Fin);
            assert!(matches!(recv.poll_data(&mut cx), Poll::Ready(Ok(None))));
        }
        // Reset → StreamTerminated
        {
            let (_btx, terminal, _r, mut recv, _c) = recv_channel();
            terminal.set(RecvEnd::Reset { error_code: 42 });
            match recv.poll_data(&mut cx) {
                Poll::Ready(Err(StreamErrorIncoming::StreamTerminated { error_code })) => {
                    assert_eq!(error_code, 42)
                }
                other => panic!("expected StreamTerminated, got {other:?}"),
            }
        }
        // Conn → ConnectionErrorIncoming
        {
            let (_btx, terminal, _r, mut recv, _c) = recv_channel();
            terminal.set(RecvEnd::Conn(Arc::new(ConnTerminal::Timeout)));
            match recv.poll_data(&mut cx) {
                Poll::Ready(Err(StreamErrorIncoming::ConnectionErrorIncoming {
                    connection_error: ConnectionErrorIncoming::Timeout,
                })) => {}
                other => panic!("expected ConnectionErrorIncoming::Timeout, got {other:?}"),
            }
        }
    }

    #[test]
    fn poll_data_closed_channel_without_terminal_is_internal_error() {
        let (btx, _terminal, _r, mut recv, _c) = recv_channel();
        drop(btx); // channel closed, no terminal published: adapter bug.
        let mut cx = noop_cx();
        match recv.poll_data(&mut cx) {
            Poll::Ready(Err(StreamErrorIncoming::ConnectionErrorIncoming {
                connection_error: ConnectionErrorIncoming::InternalError(_),
            })) => {}
            other => panic!("expected InternalError, got {other:?}"),
        }
    }

    #[test]
    fn recv_resume_sent_once_on_false_to_true() {
        let (btx, _terminal, resume, mut recv, mut crx) = recv_channel();
        btx.try_send(Bytes::from_static(b"a")).unwrap();
        btx.try_send(Bytes::from_static(b"b")).unwrap();
        let mut cx = noop_cx();
        // First drain flips false→true and sends one RecvResume.
        assert!(matches!(recv.poll_data(&mut cx), Poll::Ready(Ok(Some(_)))));
        assert!(resume.load(Ordering::Relaxed));
        match crx.try_recv() {
            Ok(DriverCommand::RecvResume { id: 0 }) => {}
            other => panic!("expected one RecvResume, got {other:?}"),
        }
        // Second drain: bit already true → no duplicate.
        assert!(matches!(recv.poll_data(&mut cx), Poll::Ready(Ok(Some(_)))));
        assert!(crx.try_recv().is_err(), "must not resend RecvResume");
    }

    #[test]
    fn recv_drop_enqueues_stop_sending_zero() {
        let (_btx, _terminal, _r, recv, mut crx) = recv_channel();
        drop(recv);
        match crx.try_recv() {
            Ok(DriverCommand::StopSending { id: 0, code: 0 }) => {}
            other => panic!("expected StopSending(0), got {other:?}"),
        }
    }

    #[test]
    fn recv_drop_after_terminal_does_not_stop_send() {
        let (_btx, terminal, _r, recv, mut crx) = recv_channel();
        terminal.set(RecvEnd::Fin);
        drop(recv);
        assert!(crx.try_recv().is_err(), "terminal recv must not stop-send on drop");
    }

    // ---- H3SendStream ----

    #[test]
    fn send_data_single_slot_errors_on_double_stash() {
        let (_status, mut send, _crx) = send_half(0);
        assert!(send.send_data(wbuf(b"one")).is_ok());
        match send.send_data(wbuf(b"two")) {
            Err(StreamErrorIncoming::ConnectionErrorIncoming {
                connection_error: ConnectionErrorIncoming::InternalError(_),
            }) => {}
            other => panic!("expected InternalError on double stash, got {other:?}"),
        }
    }

    #[test]
    fn poll_ready_returns_recorded_completion_once_then_sticky() {
        let (status, mut send, mut crx) = send_half(0);
        let mut cx = noop_cx();
        // Idle readiness with no stash.
        assert!(matches!(send.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        // Stash + poll_ready enqueues a Send and awaits its completion.
        send.send_data(wbuf(b"body")).unwrap();
        assert!(matches!(send.poll_ready(&mut cx), Poll::Pending));
        let done = match crx.try_recv() {
            Ok(DriverCommand::Send { id: 0, done, .. }) => done,
            other => panic!("expected Send, got {other:?}"),
        };
        // Worker records success; even if a terminal arrives afterward, the
        // recorded result is reported once.
        done.send(Ok(())).unwrap();
        status.set(SendEnd::Stopped { error_code: 7 });
        assert!(matches!(send.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        // Subsequent idle poll now sees the sticky terminal.
        match send.poll_ready(&mut cx) {
            Poll::Ready(Err(StreamErrorIncoming::StreamTerminated { error_code: 7 })) => {}
            other => panic!("expected sticky StreamTerminated, got {other:?}"),
        }
    }

    #[test]
    fn poll_finish_idempotent_one_finish() {
        let (_status, mut send, mut crx) = send_half(0);
        let mut cx = noop_cx();
        assert!(matches!(send.poll_finish(&mut cx), Poll::Pending));
        let done = match crx.try_recv() {
            Ok(DriverCommand::Finish { id: 0, done }) => done,
            other => panic!("expected Finish, got {other:?}"),
        };
        // No second Finish is enqueued while the first is in flight.
        assert!(matches!(send.poll_finish(&mut cx), Poll::Pending));
        assert!(crx.try_recv().is_err(), "must not enqueue a second Finish");
        done.send(Ok(())).unwrap();
        assert!(matches!(send.poll_finish(&mut cx), Poll::Ready(Ok(()))));
        // Retained result on every later poll.
        assert!(matches!(send.poll_finish(&mut cx), Poll::Ready(Ok(()))));
        assert!(crx.try_recv().is_err());
    }

    // Regression (review finding): a failed poll_finish (command channel closed
    // with no sticky terminal) must RETAIN its error; a later poll must not
    // default to Ok via the `finalized` branch.
    #[test]
    fn poll_finish_failure_is_retained_not_success() {
        let (_status, mut send, crx) = send_half(0);
        drop(crx); // close the control channel → the Finish send fails
        let mut cx = noop_cx();
        match send.poll_finish(&mut cx) {
            Poll::Ready(Err(StreamErrorIncoming::ConnectionErrorIncoming {
                connection_error: ConnectionErrorIncoming::InternalError(_),
            })) => {}
            other => panic!("expected InternalError on first poll, got {other:?}"),
        }
        // The next poll must return the SAME error, never Ok.
        match send.poll_finish(&mut cx) {
            Poll::Ready(Err(StreamErrorIncoming::ConnectionErrorIncoming {
                connection_error: ConnectionErrorIncoming::InternalError(_),
            })) => {}
            other => panic!("finalized failure must not become Ok, got {other:?}"),
        }
    }

    #[test]
    fn reset_enqueues_once_and_finalizes() {
        let (_status, mut send, mut crx) = send_half(4);
        send.reset(7);
        match crx.try_recv() {
            Ok(DriverCommand::Reset { id: 4, code: 7 }) => {}
            other => panic!("expected Reset(7), got {other:?}"),
        }
        // Idempotent: a second reset enqueues nothing.
        send.reset(9);
        assert!(crx.try_recv().is_err(), "must not enqueue a second Reset");
        // poll_finish after reset returns the sticky local terminal.
        let mut cx = noop_cx();
        match send.poll_finish(&mut cx) {
            Poll::Ready(Err(StreamErrorIncoming::StreamTerminated { error_code: 7 })) => {}
            other => panic!("expected sticky reset terminal, got {other:?}"),
        }
    }

    #[test]
    fn send_drop_enqueues_graceful_finish() {
        let (_status, send, mut crx) = send_half(0);
        drop(send);
        match crx.try_recv() {
            Ok(DriverCommand::Finish { id: 0, .. }) => {}
            other => panic!("expected graceful Finish on drop, got {other:?}"),
        }
    }

    #[test]
    fn send_drop_after_finalize_does_not_finish() {
        let (_status, mut send, mut crx) = send_half(0);
        send.reset(3);
        let _ = crx.try_recv(); // the Reset
        drop(send);
        assert!(crx.try_recv().is_err(), "finalized send must not finish on drop");
    }

    // ---- StreamOpener ----

    fn opener() -> (StreamOpener<Bytes>, mpsc::UnboundedReceiver<DriverCommand<Bytes>>, Arc<ConnShared>) {
        let (ctx, crx) = mpsc::unbounded_channel();
        let shared = ConnShared::new();
        (StreamOpener::from_parts(ctx, Arc::clone(&shared)), crx, shared)
    }

    #[test]
    fn stream_opener_submit_helper_resolves_terminal_when_conn_terminal_preset() {
        let (mut op, mut crx, shared) = opener();
        shared
            .conn_terminal
            .set(Arc::new(ConnTerminal::AppClose {
                origin: CloseOrigin::Peer,
                error_code: 0x101,
                reason: Bytes::new(),
            }));
        let mut cx = noop_cx();
        match op.poll_open_bidi(&mut cx) {
            Poll::Ready(Err(StreamErrorIncoming::ConnectionErrorIncoming {
                connection_error: ConnectionErrorIncoming::ApplicationClose { error_code: 0x101 },
            })) => {}
            _ => panic!("expected preset terminal resolution"),
        }
        // No doomed OpenBidi was enqueued.
        assert!(crx.try_recv().is_err(), "must not submit under a preset terminal");
    }

    #[test]
    fn cloned_opener_has_fresh_pending_slots() {
        let (mut op, mut crx, _shared) = opener();
        let mut cx = noop_cx();
        // Submit stores a pending receiver in the original.
        assert!(matches!(op.poll_open_bidi(&mut cx), Poll::Pending));
        assert!(op.pending_bidi.is_some());
        assert!(matches!(crx.try_recv(), Ok(DriverCommand::OpenBidi { .. })));
        // The clone starts empty (§6.1).
        let clone = op.clone();
        assert!(clone.pending_bidi.is_none());
        assert!(clone.pending_uni.is_none());
    }

    #[test]
    fn opener_open_bidi_resolves_handoff_into_stream() {
        let (mut op, mut crx, _shared) = opener();
        let mut cx = noop_cx();
        assert!(matches!(op.poll_open_bidi(&mut cx), Poll::Pending));
        let reply = match crx.try_recv() {
            Ok(DriverCommand::OpenBidi { reply }) => reply,
            other => panic!("expected OpenBidi, got {other:?}"),
        };
        // Fabricate a worker handoff.
        let (_btx, brx) = mpsc::channel(1);
        let (ictx, _icrx) = mpsc::unbounded_channel();
        let handoff = BidiHandoff {
            send: SendHandoff { id: 0, status: TerminalCell::new(), cmd_tx: ictx.clone() },
            recv: RecvHandoff {
                id: 0,
                bytes: brx,
                terminal: TerminalCell::new(),
                resume: Arc::new(AtomicBool::new(false)),
                cmd_tx: ictx,
            },
        };
        reply.send(Ok(handoff)).ok().expect("deliver handoff");
        match op.poll_open_bidi(&mut cx) {
            Poll::Ready(Ok(_stream)) => {}
            _ => panic!("expected resolved H3Stream"),
        }
        assert!(op.pending_bidi.is_none(), "slot cleared after resolution");
    }

    // ---- Connection ----

    fn connection() -> (
        Connection<Bytes>,
        mpsc::Sender<BidiHandoff<Bytes>>,
        TerminalCell<Arc<ConnTerminal>>,
        Arc<AtomicBool>,
        mpsc::UnboundedReceiver<DriverCommand<Bytes>>,
    ) {
        let (btx, brx) = mpsc::channel(4);
        let (_utx, urx) = mpsc::channel(4);
        let (ctx, crx) = mpsc::unbounded_channel();
        let at_bidi = TerminalCell::new();
        let at_uni = TerminalCell::new();
        let rb = Arc::new(AtomicBool::new(false));
        let ru = Arc::new(AtomicBool::new(false));
        let shared = ConnShared::new();
        let opener = StreamOpener::from_parts(ctx, shared);
        let conn = Connection::from_parts(
            brx,
            urx,
            at_bidi.clone(),
            at_uni,
            Arc::clone(&rb),
            ru,
            opener,
        );
        (conn, btx, at_bidi, rb, crx)
    }

    fn make_bidi_handoff() -> BidiHandoff<Bytes> {
        let (_btx, brx) = mpsc::channel(1);
        let (ictx, _icrx) = mpsc::unbounded_channel();
        BidiHandoff {
            send: SendHandoff { id: 0, status: TerminalCell::new(), cmd_tx: ictx.clone() },
            recv: RecvHandoff {
                id: 0,
                bytes: brx,
                terminal: TerminalCell::new(),
                resume: Arc::new(AtomicBool::new(false)),
                cmd_tx: ictx,
            },
        }
    }

    #[test]
    fn poll_accept_bidi_delivers_then_maps_terminal() {
        let (mut conn, btx, at_bidi, rb, mut crx) = connection();
        let mut cx = noop_cx();
        // A queued accepted stream is delivered, flipping the accept-resume bit.
        btx.try_send(make_bidi_handoff()).unwrap();
        match conn.poll_accept_bidi(&mut cx) {
            Poll::Ready(Ok(_stream)) => {}
            _ => panic!("expected accepted stream"),
        }
        assert!(rb.load(Ordering::Relaxed));
        match crx.try_recv() {
            Ok(DriverCommand::AcceptBidiResume) => {}
            other => panic!("expected AcceptBidiResume, got {other:?}"),
        }
        // Empty queue + accept terminal → mapped connection error.
        at_bidi.set(Arc::new(ConnTerminal::Timeout));
        match conn.poll_accept_bidi(&mut cx) {
            Poll::Ready(Err(ConnectionErrorIncoming::Timeout)) => {}
            _ => panic!("expected Timeout"),
        }
    }

    #[test]
    fn poll_accept_bidi_sealing_recheck_yields_queued_stream_before_terminal() {
        let (mut conn, btx, at_bidi, _rb, _crx) = connection();
        let mut cx = noop_cx();
        // Both a queued stream AND the accept terminal are present: the stream
        // must win (M1 sealing-edge recheck).
        btx.try_send(make_bidi_handoff()).unwrap();
        at_bidi.set(Arc::new(ConnTerminal::Timeout));
        match conn.poll_accept_bidi(&mut cx) {
            Poll::Ready(Ok(_stream)) => {}
            _ => panic!("expected queued stream ahead of terminal"),
        }
    }

    #[test]
    fn connection_drop_enqueues_connection_dropped() {
        let (conn, _btx, _at, _rb, mut crx) = connection();
        drop(conn);
        match crx.try_recv() {
            Ok(DriverCommand::ConnectionDropped) => {}
            other => panic!("expected ConnectionDropped, got {other:?}"),
        }
    }
}
