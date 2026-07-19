//! Back end: [`QuicheDriver`] — the [`tokio_quiche::ApplicationOverQuic`] impl
//! that owns all cross-task state and is the sole toucher of quiche (design §5).
//!
//! Phase 2 lays down the structural skeleton: the command channel plumbing
//! (unbounded control ingress + a weak worker sender, §5.2), the bounded accept
//! channels (created but not yet driven), and the `ApplicationOverQuic`
//! callbacks with the `wait_for_data` pending-work fast path (§5, finding 2) and
//! the per-iteration read-pump invocation contract (§2.3). The receive/send/
//! close *stages* are stubbed here and filled in Phases 3–5.
#![allow(dead_code)] // stages wired up across Phases 3–5

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::{Buf, Bytes, BytesMut};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};

use tokio_quiche::quic::HandshakeInfo;
use tokio_quiche::quic::QuicheConnection;
use tokio_quiche::{ApplicationOverQuic, QuicResult};

use crate::buffer::{
    send_from_buf, SendAccounting, SendBytesPermit, TerminalCell, WriteCompleter, MAX_CHUNK,
    PKT_BUF_LEN,
};
use crate::conn::QuicConn;
use crate::error::{
    classify_stream_recv_error, classify_stream_send_error, conn_terminal_from_error, CloseOrigin,
    ConnTerminal, RecvEnd, SendEnd, StreamRecvClass, StreamSendClass, H3_NO_ERROR,
    H3_REQUEST_CANCELLED,
};
use crate::quiche::{self, Shutdown};

/// Shared per-iteration chunk budget for the read pump (§5.1, provisional §12
/// S3). One `ReadBudget` is threaded through all receive draining in a single
/// helper invocation.
const READ_BUDGET: usize = 32;

/// Max `stream_readable_next()`/`stream_writable_next()` ids consumed for
/// destructive intake in one pump invocation (§5.1, provisional §12 S3).
const DISCOVERY_BUDGET: usize = 32;

/// Max distinct resumed (bit-transitioned) recv ids drained per pump (§5.1).
const RECV_RESUME_BUDGET: usize = 16;

/// Max registered-drain id-attempts (phase 1) per pump (§5.1).
const READABLE_BUDGET: usize = 32;

/// Max admission attempts (phase 2) per pump (§5.1).
const ADMIT_BUDGET: usize = 32;

/// Max parked-stream promotions per pump (§5.1).
const PROMOTE_BUDGET: usize = 32;

/// Max `stream_recv` chunks drained from one stream per pump (§5.1); the body
/// remainder is requeued so a large body drains across bounded callbacks.
const CHUNK_BUDGET: usize = 16;

/// Bounded per-recv byte-channel depth; the per-stream in-flight memory bound is
/// `BYTE_CHANNEL_DEPTH × MAX_CHUNK` (§5.1, provisional §12 S3). This is the
/// **default** depth; it is configurable per connection via [`DriverBufferConfig`]
/// (SF-4) — see [`H3QuicheServerConfig`](crate::listener::H3QuicheServerConfig)
/// and [`H3QuicheClientConfig`](crate::connector::H3QuicheClientConfig).
pub(crate) const BYTE_CHANNEL_DEPTH: usize = 64;

/// Per-connection buffer sizing knobs (SF-4 / SF-5-pkt_buf). Both default to the
/// historical constants so out-of-the-box behavior is byte-for-byte unchanged;
/// callers may raise/lower them to trade memory against throughput.
///
/// These are **trade-offs**, not free wins: shrinking `recv_channel_depth`
/// reduces per-stream buffering (throughput) to save memory, and the packet
/// buffer must NOT be shrunk below a full GSO batch without a datapath
/// assessment (§5, §12).
#[derive(Clone, Copy, Debug)]
pub(crate) struct DriverBufferConfig {
    /// Bounded per-recv byte-channel depth (default [`BYTE_CHANNEL_DEPTH`]).
    /// Effective value is clamped to at least 1.
    pub recv_channel_depth: usize,
    /// Outbound packet-buffer size in bytes (default [`PKT_BUF_LEN`]).
    /// Effective value is clamped to at least 1.
    pub packet_buffer_size: usize,
    /// Optional aggregate cap (bytes) on buffered outbound send data admitted to
    /// the worker command/op queues (SF-6, FR-010/FR-011). `None` (default) =
    /// unlimited, preserving the historical unbounded behavior. A finite cap
    /// bounds resident admitted send bytes to at most `cap + one admission unit`.
    pub max_buffered_send_bytes: Option<usize>,
}

impl Default for DriverBufferConfig {
    fn default() -> Self {
        Self {
            recv_channel_depth: BYTE_CHANNEL_DEPTH,
            packet_buffer_size: PKT_BUF_LEN,
            max_buffered_send_bytes: None,
        }
    }
}

/// Max commands applied per `process_writes` stage (a) (§5.2). Excess stays in
/// `inbox` (relative order preserved) and re-forces an iteration.
const CMD_BUDGET: usize = 64;

/// Max `stream_writable_next()` ids consumed by stage (d) per iteration (§5.5).
const WRITABLE_BUDGET: usize = 32;

/// Max round-robin runnable-send turns per stage (e) (§5.3a, invariant 12).
const WRITE_BUDGET: usize = 32;

/// Cap on the bytes offered to a single `stream_send` turn (§5.3a "one bounded
/// transport call").
const MAX_WRITE_CHUNK: usize = MAX_CHUNK;

/// Small low-water re-arm progress threshold (§5.3): any capacity gain wakes a
/// blocked write, rather than starving it on the full remaining length.
const REARM_THRESHOLD: usize = 1;

/// Max locally-requested stream opens materialized per iteration (§6.1). Excess
/// stays queued and re-forces an iteration.
const OPEN_BUDGET: usize = 32;

/// Stream-class helper: a bidirectional stream has bit 0x2 clear (§5.5).
#[inline]
fn is_bidi(id: u64) -> bool {
    id & 0x2 == 0
}

/// Front-end → worker control commands, carried over the single unbounded
/// control channel (§5.2). Unbounded because the emitting trait methods cannot
/// exert backpressure or fail (`reset`/`stop_sending` return `()`), and the
/// resume signals are correctness-critical and must never be dropped.
// The `Send` variant is intentionally larger than the lifecycle variants: it
// carries the caller's `WriteBuf` inline. Boxing it would add a heap allocation
// per queued write on the send hot path, which we deliberately avoid (cf.
// `SendOp`).
#[allow(clippy::large_enum_variant)]
pub(crate) enum DriverCommand<B: Buf> {
    /// `OpenStreams::poll_open_bidi` — returns both halves.
    OpenBidi {
        reply: oneshot::Sender<Result<BidiHandoff<B>, Arc<ConnTerminal>>>,
    },
    /// `OpenStreams::poll_open_send` — returns only a send half. Its cleanup
    /// only ever touches `Shutdown::Write` (§5.2, finding 4).
    OpenUni {
        reply: oneshot::Sender<Result<SendHandoff<B>, Arc<ConnTerminal>>>,
    },
    /// `SendStream::send_data` — stash one `WriteBuf` in the stream's send slot.
    /// The completion is delivered through the stream's **reusable**
    /// [`WriteCompleter`] (SF-3), stamped with the write's generation, rather
    /// than a freshly allocated per-chunk `oneshot`.
    Send {
        id: u64,
        buf: h3::quic::WriteBuf<B>,
        done: WriteCompleter<SendEnd>,
        /// Aggregate send-byte reservation for this write (SF-6). Held for the
        /// buffer's whole worker lifetime; its `Drop` releases the reserved
        /// bytes exactly once at the SF-3 completion chokepoint (or on an
        /// unapplied-command / rollback drop). `None` only for synthetic
        /// internal sends that bypass front-end admission.
        permit: Option<SendBytesPermit>,
    },
    /// `SendStream::poll_finish` — queue a FIN after any buffered writes.
    Finish {
        id: u64,
        done: oneshot::Sender<Result<(), SendEnd>>,
    },
    /// `SendStream::reset` — emit `RESET_STREAM`, preempting unaccepted writes.
    Reset { id: u64, code: u64 },
    /// `RecvStream::stop_sending` — emit `STOP_SENDING`.
    StopSending { id: u64, code: u64 },
    /// `OpenStreams::close` — explicit local connection close.
    Close { code: u64, reason: Bytes },
    /// A blocked recv half regained channel capacity (§5.1). Sent only on a
    /// false→true resume-bit transition, so duplicates never reach the channel.
    RecvResume { id: u64 },
    /// The consumer freed BIDI accept-queue capacity (§5.2, finding 2).
    AcceptBidiResume,
    /// The consumer freed UNI accept-queue capacity (§5.2, finding 2).
    AcceptUniResume,
    /// `Connection::drop`, sent *before* the accept receivers close, so parked
    /// peer streams can be cleaned up (§5.2, finding 4).
    ConnectionDropped,
}

impl<B: Buf> std::fmt::Debug for DriverCommand<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DriverCommand::OpenBidi { .. } => f.write_str("OpenBidi"),
            DriverCommand::OpenUni { .. } => f.write_str("OpenUni"),
            DriverCommand::Send { id, .. } => write!(f, "Send {{ id: {id} }}"),
            DriverCommand::Finish { id, .. } => write!(f, "Finish {{ id: {id} }}"),
            DriverCommand::Reset { id, code } => write!(f, "Reset {{ id: {id}, code: {code} }}"),
            DriverCommand::StopSending { id, code } => {
                write!(f, "StopSending {{ id: {id}, code: {code} }}")
            }
            DriverCommand::Close { code, .. } => write!(f, "Close {{ code: {code} }}"),
            DriverCommand::RecvResume { id } => write!(f, "RecvResume {{ id: {id} }}"),
            DriverCommand::AcceptBidiResume => f.write_str("AcceptBidiResume"),
            DriverCommand::AcceptUniResume => f.write_str("AcceptUniResume"),
            DriverCommand::ConnectionDropped => f.write_str("ConnectionDropped"),
        }
    }
}

/// Cross-task shared state (§5). Holds the connection-level terminal cell the
/// close-admission gate publishes once and every submitter reads (§5.2, M3).
pub(crate) struct ConnShared {
    /// Published exactly once at the connection-terminal edge.
    pub(crate) conn_terminal: TerminalCell<Arc<ConnTerminal>>,
    /// Aggregate buffered-send-byte accounting shared with every front-end
    /// `H3SendStream` for cap admission (SF-6, §12 S3). `cap == None` (default)
    /// leaves admission unbounded (behavior unchanged).
    pub(crate) send_accounting: Arc<SendAccounting>,
}

impl ConnShared {
    pub(crate) fn new(max_buffered_send_bytes: Option<usize>) -> Arc<Self> {
        Arc::new(ConnShared {
            conn_terminal: TerminalCell::new(),
            send_accounting: SendAccounting::new(max_buffered_send_bytes),
        })
    }
}

/// Raw receive-half state the worker hands to the front end at admission/open.
/// Phase 6 wraps this into an `H3RecvStream`. The worker initializes the sticky
/// `terminal` cell atomically during registration (§5.4 invariant 7) before
/// handoff, so no retained terminal is lost.
pub(crate) struct RecvHandoff<B: Buf> {
    pub(crate) id: u64,
    /// Bounded byte channel; the worker reserves a permit before `stream_recv`.
    pub(crate) bytes: mpsc::Receiver<Bytes>,
    /// Out-of-band sticky end reason (§8.2).
    pub(crate) terminal: TerminalCell<RecvEnd>,
    /// Producer-coalesced resume bit shared with the worker (§5.1).
    pub(crate) resume: Arc<AtomicBool>,
    /// Worker "parked on a full byte channel" flag shared with the worker so the
    /// consumer only emits a resume when the worker had genuinely blocked (SF-2).
    pub(crate) blocked: Arc<AtomicBool>,
    /// For `stop_sending` and drop cleanup.
    pub(crate) cmd_tx: mpsc::UnboundedSender<DriverCommand<B>>,
    /// Armed cleanup if this handoff is dropped before conversion (§6.2).
    pub(crate) cleanup: HandoffCleanup<B>,
}

/// Raw send-half state the worker hands to the front end at open. Phase 6 wraps
/// this into an `H3SendStream`.
pub(crate) struct SendHandoff<B: Buf> {
    pub(crate) id: u64,
    /// Out-of-band sticky end reason (§8.2), mirrors the worker's terminal.
    pub(crate) status: TerminalCell<SendEnd>,
    /// For `send_data`/`poll_finish`/`reset` and drop cleanup.
    pub(crate) cmd_tx: mpsc::UnboundedSender<DriverCommand<B>>,
    /// Shared aggregate send-byte accounting for cap admission (SF-6, §12 S3).
    pub(crate) send_accounting: Arc<SendAccounting>,
    /// Armed cleanup if this handoff is dropped before conversion (§6.2).
    pub(crate) cleanup: HandoffCleanup<B>,
}

/// A peer/opened bidi stream handed to the front end: both halves.
pub(crate) struct BidiHandoff<B: Buf> {
    pub(crate) send: SendHandoff<B>,
    pub(crate) recv: RecvHandoff<B>,
}

/// Armed, direction-aware cleanup for a **materialized** handoff that is dropped
/// before the front end converts it into a stream object (§6.2). This closes
/// the open-cancel-after-materialize window (the worker's `reply.send(Ok(..))`
/// can succeed yet the receiver drops the handoff before polling) and the
/// accept-drop window (a queued accepted handoff dropped when `Connection`
/// drops): without it the peer/local stream would leak `MAX_STREAMS` credit and
/// its worker registry entry until connection close.
///
/// [`disarm`](HandoffCleanup::disarm) is called by `from_handoff` on successful
/// conversion, after which the front-end stream object's own `Drop` owns
/// cleanup. Matches the front-end drop policy (§6.2): a recv half enqueues
/// `StopSending(0)`, a send half a graceful `Finish` (never inferring
/// cancellation).
pub(crate) struct HandoffCleanup<B: Buf> {
    id: u64,
    is_recv: bool,
    cmd_tx: mpsc::UnboundedSender<DriverCommand<B>>,
    armed: bool,
}

impl<B: Buf> HandoffCleanup<B> {
    pub(crate) fn new(
        id: u64,
        is_recv: bool,
        cmd_tx: mpsc::UnboundedSender<DriverCommand<B>>,
    ) -> Self {
        HandoffCleanup {
            id,
            is_recv,
            cmd_tx,
            armed: true,
        }
    }

    /// Called by `from_handoff` on conversion: the front-end stream object now
    /// owns drop cleanup, so this guard becomes a no-op.
    pub(crate) fn disarm(mut self) {
        self.armed = false;
    }
}

impl<B: Buf> Drop for HandoffCleanup<B> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if self.is_recv {
            let _ = self.cmd_tx.send(DriverCommand::StopSending {
                id: self.id,
                code: 0,
            });
        } else {
            let (done, _rx) = oneshot::channel();
            let _ = self
                .cmd_tx
                .send(DriverCommand::Finish { id: self.id, done });
        }
    }
}

/// Worker-owned receive registry state for a live recv half (§5, §5.1). The
/// front end never touches quiche; the worker moves bytes into `bytes` under a
/// reserve-before-read discipline and publishes the sealing terminal into the
/// out-of-band `terminal` cell.
pub(crate) struct StreamRecvState {
    /// Bounded byte channel sender; the worker reserves a permit before
    /// `stream_recv` (§5.1 rule, invariant 6).
    pub(crate) bytes: mpsc::Sender<Bytes>,
    /// Out-of-band sticky end reason; a full byte channel can never hide it.
    pub(crate) terminal: TerminalCell<RecvEnd>,
    /// Producer-coalesced resume bit shared with the front end (§5.1, finding 3).
    pub(crate) resume: Arc<AtomicBool>,
    /// The byte channel was full on the last drain; parked until `RecvResume`.
    /// Shared `Arc<AtomicBool>` with the front-end handle so the consumer can
    /// gate its resume signal on genuine worker blocking (SF-2): the worker
    /// publishes `true` (Release) when it parks and the consumer observes-and-
    /// clears it (AcqRel swap) when it frees a slot.
    pub(crate) blocked: Arc<AtomicBool>,
}

/// One ordered send operation queued for a stream (§5.3a). `Write` carries the
/// caller's `WriteBuf` cursor (partial-consumed across turns) and its reusable
/// [`WriteCompleter`] (SF-3); `Finish` carries only its completion oneshot.
// The `Write` variant is intentionally larger than `Finish`: boxing the
// `WriteBuf` would add a heap allocation per queued write on the send hot path,
// which we deliberately avoid.
#[allow(clippy::large_enum_variant)]
pub(crate) enum SendOp<B: Buf> {
    Write {
        buf: h3::quic::WriteBuf<B>,
        done: WriteCompleter<SendEnd>,
        /// SF-6 byte reservation; released on drop (completion / drain / reset).
        permit: Option<SendBytesPermit>,
    },
    Finish {
        done: oneshot::Sender<Result<(), SendEnd>>,
    },
}

impl<B: Buf> SendOp<B> {
    /// Resolve this op's completion exactly once (§5.3a exactly-once). A `Write`
    /// completes through its reusable [`WriteCompleter`] (set-if-current-
    /// generation, SF-3); a `Finish` stays a per-stream one-shot `oneshot`.
    fn complete(self, result: Result<(), SendEnd>) {
        match self {
            // Dropping the destructured `permit` here releases the SF-6 byte
            // reservation at the exactly-once completion chokepoint.
            SendOp::Write { done, .. } => done.complete(result),
            // Ignore send error: the front end may have stopped polling (drop).
            SendOp::Finish { done } => {
                let _ = done.send(result);
            }
        }
    }
}

/// Worker-owned send-registry state for a live send half (§5.3a). Holds the
/// ordered `send_ops` queue, a possible `pending_reset` (serviced before any
/// generic terminal/stale eviction, invariant 11), a local sticky `terminal`
/// copy for fast checks, and the `status` cell shared with the front-end handle
/// so a published send terminal is observable out of band (§8.2).
pub(crate) struct StreamSendState<B: Buf> {
    pub(crate) send_ops: VecDeque<SendOp<B>>,
    pub(crate) pending_reset: Option<u64>,
    pub(crate) terminal: Option<SendEnd>,
    pub(crate) status: TerminalCell<SendEnd>,
    /// Set once the send direction is fully terminal (FIN accepted, local reset
    /// serviced, peer `STOP_SENDING`, or connection close). Used by the
    /// drop-driven cleanup to reclaim a retained entry once both directions of a
    /// stream are terminal and its front-end halves are gone (§6.2, invariant 8).
    pub(crate) finished: bool,
}

impl<B: Buf> StreamSendState<B> {
    /// A fresh live send half with its own status cell (lazy-create path for a
    /// locally-materialized stream; Phase 6 shares the cell with the handle).
    fn new() -> Self {
        StreamSendState {
            send_ops: VecDeque::new(),
            pending_reset: None,
            terminal: None,
            status: TerminalCell::new(),
            finished: false,
        }
    }
}

/// Admission state of an *observed* peer stream id (§5, invariant 7). `admit`
/// holds only ids we have actually seen — there is no high-watermark and no
/// reclaim subsystem: contract A drops the entry at the terminal edge (§5.5).
pub(crate) enum AdmitState {
    /// Accept-queue capacity was unavailable at discovery; the captured
    /// `PeerStream` (with any retained terminals) awaits promotion (§5, finding 4).
    Parked(PeerStream),
    /// Handed to the front end. Per-direction completion is tracked so the
    /// stream is reclaimed on *any* clean or abrupt end (§5 terminal transition).
    Registered { send_done: bool, recv_done: bool },
}

/// A discovered peer stream awaiting admission. It **owns** any terminal seen
/// before accept capacity existed (e.g. a writable-path `STOP_SENDING` code), so
/// a deferred admission never loses it — quiche is not guaranteed to re-surface
/// the event (§5, finding 4; iter10 finding 3).
pub(crate) struct PeerStream {
    pub(crate) id: u64,
    pub(crate) pending_send_terminal: Option<SendEnd>,
    pub(crate) pending_recv_terminal: Option<RecvEnd>,
}

impl PeerStream {
    fn new(id: u64) -> Self {
        PeerStream {
            id,
            pending_send_terminal: None,
            pending_recv_terminal: None,
        }
    }
}

/// The outcome of a single `admit_one` attempt. `Full`/`TornDown` hand the owned
/// `PeerStream` back (or drop it) so the caller can park or abandon it (§5).
enum AdmitResult {
    /// Registered and handed over the accept channel.
    Registered,
    /// The accept channel is full; the `PeerStream` is returned to be parked.
    Full(PeerStream),
    /// The accept receiver is closed or teardown is underway; the stream was
    /// shut down by directionality and dropped (§5, invariant 1).
    TornDown,
}

/// Why the connection setup failed before establishment (§7.1, §8.4). Surfaced
/// log-only; carries no fabricated transport code.
#[derive(Debug)]
pub(crate) enum SetupFailure {
    /// The worker exited before `on_conn_established` ran. Resolved by
    /// `QuicheDriver::drop`, since a pre-handshake exit never calls
    /// `on_conn_close` (tokio-quiche gates it on `should_act()`, §14 T2a).
    PreHandshakeWorkerExit,
}

impl std::fmt::Display for SetupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetupFailure::PreHandshakeWorkerExit => write!(
                f,
                "connection setup failed: worker exited before the handshake completed"
            ),
        }
    }
}

impl std::error::Error for SetupFailure {}

/// The front-end-facing handles produced alongside a [`QuicheDriver`]. The
/// driver itself is moved into `tokio_quiche` (`start`/`connect_with_config`);
/// these handles are what the acceptor/connector (Phase 7) hand to the front
/// end (Phase 6).
pub(crate) struct DriverHandles<B: Buf> {
    /// The strong control sender every front-end handle clones (§5.2). When the
    /// last strong clone drops, `cmd_rx` sees EOF → last-handle teardown.
    pub(crate) cmd_tx: mpsc::UnboundedSender<DriverCommand<B>>,
    /// Accept side of peer-initiated bidi streams (§6).
    pub(crate) accept_bidi_rx: mpsc::Receiver<BidiHandoff<B>>,
    /// Accept side of peer-initiated uni streams (§6).
    pub(crate) accept_uni_rx: mpsc::Receiver<RecvHandoff<B>>,
    /// Resolves when the handshake completes (`Ok`) or setup fails (`Err`).
    pub(crate) established_rx: oneshot::Receiver<Result<(), SetupFailure>>,
    /// Shared connection state (holds the terminal cell).
    pub(crate) shared: Arc<ConnShared>,
    /// Clone of the BIDI accept-terminal cell so `Connection::poll_accept_bidi`
    /// resolves a blocked accept when the connection terminates (§5).
    pub(crate) accept_terminal_bidi: TerminalCell<Arc<ConnTerminal>>,
    /// Clone of the UNI accept-terminal cell so `Connection::poll_accept_uni`
    /// resolves a blocked accept when the connection terminates (§5).
    pub(crate) accept_terminal_uni: TerminalCell<Arc<ConnTerminal>>,
    /// Producer-coalesced BIDI accept-resume bit shared with the worker (§5.1,
    /// finding 3): `Connection::poll_accept_bidi` flips it false→true and sends
    /// `AcceptBidiResume` so the worker drains the parked bidi queue.
    pub(crate) accept_bidi_resume: Arc<AtomicBool>,
    /// Producer-coalesced UNI accept-resume bit shared with the worker (§5.1).
    pub(crate) accept_uni_resume: Arc<AtomicBool>,
}

impl<B: Buf + Send + 'static> DriverHandles<B> {
    /// Materialize the front-end [`crate::stream::Connection`] from these handles
    /// (Phase 7 wiring, §6). Consumes the accept channels, per-direction accept
    /// terminals, and resume bits, and builds a [`crate::stream::StreamOpener`]
    /// over the control channel + shared state. The `established_rx` is not
    /// carried into the connection: handshake readiness is awaited separately by
    /// the acceptor/connector before the connection is handed to `h3`.
    pub(crate) fn into_connection(self) -> crate::stream::Connection<B> {
        let opener = crate::stream::StreamOpener::from_parts(self.cmd_tx, self.shared);
        crate::stream::Connection::from_parts(
            self.accept_bidi_rx,
            self.accept_uni_rx,
            self.accept_terminal_bidi,
            self.accept_terminal_uni,
            self.accept_bidi_resume,
            self.accept_uni_resume,
            opener,
        )
    }

    /// Await handshake establishment, then materialize the front-end
    /// [`crate::stream::Connection`] (§7.1, §7.2). Resolves the `established_rx`
    /// oneshot: `Ok(())` builds the connection, `Err(SetupFailure)` is returned
    /// verbatim, and a *cancelled* oneshot (the worker dropped the sender
    /// without sending — an adapter inconsistency the worker's `Drop` is
    /// designed to prevent) is mapped to
    /// [`SetupFailure::PreHandshakeWorkerExit`] (§8.4).
    pub(crate) async fn into_established_connection(
        self,
    ) -> Result<crate::stream::Connection<B>, SetupFailure> {
        let DriverHandles {
            cmd_tx,
            accept_bidi_rx,
            accept_uni_rx,
            established_rx,
            shared,
            accept_terminal_bidi,
            accept_terminal_uni,
            accept_bidi_resume,
            accept_uni_resume,
        } = self;

        match established_rx.await {
            Ok(res) => res?,
            Err(_cancelled) => return Err(SetupFailure::PreHandshakeWorkerExit),
        }

        let opener = crate::stream::StreamOpener::from_parts(cmd_tx, shared);
        Ok(crate::stream::Connection::from_parts(
            accept_bidi_rx,
            accept_uni_rx,
            accept_terminal_bidi,
            accept_terminal_uni,
            accept_bidi_resume,
            accept_uni_resume,
            opener,
        ))
    }
}

/// A staged explicit local `Close` awaiting the mandatory close barrier (§5.2,
/// invariant 10). First-close-wins: stage (a) records only the first effective
/// one; the barrier applies it after at most one bounded write batch.
pub(crate) struct PendingClose {
    pub(crate) code: u64,
    pub(crate) reason: Bytes,
}

/// A recorded successful `qconn.close` (explicit or synthetic last-handle
/// `H3_NO_ERROR`), used by `on_conn_close` to classify the terminal (§8.3).
/// Its presence outranks `local_error()` and suppresses a second close call.
pub(crate) struct RecordedLocalClose {
    pub(crate) code: u64,
    pub(crate) reason: Bytes,
}

/// The decision `wait_for_data` makes before awaiting (§5, finding 2). Factored
/// out for unit testing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WaitDecision {
    /// Closing/draining: stay pending; only packet/timer events drive the loop.
    Pending,
    /// Runnable work is deferred: force a no-packet acting iteration.
    Yield,
    /// Idle: await the command channel.
    Recv,
}

/// The bridge back end: owns all cross-task state and is the sole toucher of
/// quiche (§5). Generic over the send buffer `B` (defaults to [`Bytes`]).
pub(crate) struct QuicheDriver<B: Buf = Bytes> {
    shared: Arc<ConnShared>,

    // ----- command ingress (§5.2) -----
    /// Unbounded control ingress; closed by the worker at the terminal edge to
    /// make the `on_conn_close` drain finite (M3).
    cmd_rx: mpsc::UnboundedReceiver<DriverCommand<B>>,
    /// Weak sender the worker upgrades to build handles for accepted peer
    /// streams; `upgrade() == None` means teardown is in progress (finding 2).
    cmd_tx_weak: mpsc::WeakUnboundedSender<DriverCommand<B>>,
    /// Commands pulled off `cmd_rx`, applied in `process_writes`.
    inbox: VecDeque<DriverCommand<B>>,

    // ----- accept side (§6), bounded (finding 3) -----
    accept_bidi: mpsc::Sender<BidiHandoff<B>>,
    accept_uni: mpsc::Sender<RecvHandoff<B>>,
    /// Producer-coalesced accept-resume bits (§5.1, finding 3): the front end
    /// sets these true on a false→true edge and issues `Accept*Resume`; the
    /// worker gates parked-stream promotion on them and clears before retry.
    accept_bidi_resume: Arc<AtomicBool>,
    accept_uni_resume: Arc<AtomicBool>,

    // ----- locally-initiated stream opening (§6.1) -----
    /// True for the acceptor (server) side; selects the QUIC stream-id parity
    /// convention for locally-opened streams (§6.1).
    is_server: bool,
    /// Next locally-initiable bidi id (client `0,4,8…`; server `1,5,9…`),
    /// advanced by 4 only after `stream_priority` materialization succeeds.
    next_bidi_id: u64,
    /// Next locally-initiable uni id (client `2,6,10…`; server `3,7,11…`).
    next_uni_id: u64,
    /// Queued `OpenStreams::poll_open_bidi` requests awaiting materialization
    /// under peer flow control (§6.1). Each holds only its reply oneshot; a
    /// request whose poller dropped (`reply.is_closed()`) burns no id.
    open_bidi: VecDeque<oneshot::Sender<Result<BidiHandoff<B>, Arc<ConnTerminal>>>>,
    /// Queued `OpenStreams::poll_open_send` requests awaiting materialization.
    open_uni: VecDeque<oneshot::Sender<Result<SendHandoff<B>, Arc<ConnTerminal>>>>,

    // ----- per-stream receive registry + admission bookkeeping (§5, §5.1) -----
    /// Live recv halves: bounded byte sender + out-of-band terminal + resume bit.
    recv: HashMap<u64, StreamRecvState>,
    /// Live send halves: ordered `send_ops`, `pending_reset`, sticky terminal,
    /// and the front-end-shared `status` cell (§5.3a).
    send: HashMap<u64, StreamSendState<B>>,
    /// Round-robin queue of stream ids with runnable send work, with a
    /// membership set for exact-once queueing (§5.3a, invariant 12).
    runnable_send: VecDeque<u64>,
    runnable_send_set: HashSet<u64>,
    /// Admission state of every *observed* peer stream id (invariant 7). No
    /// high-watermark; contract A drops entries at the terminal edge (§5.5).
    admit: HashMap<u64, AdmitState>,
    /// Registered ids awaiting a receive-drain, with a membership set for
    /// exact-once queueing (§5, iter9 finding 2).
    pending_readable: VecDeque<u64>,
    readable_set: HashSet<u64>,
    /// Resumed (bit-transitioned) recv ids awaiting a retry drain (§5.1).
    pending_resume: VecDeque<u64>,
    resume_set: HashSet<u64>,
    /// New peer ids awaiting admission. `pending_admit` **owns** each captured
    /// `PeerStream`; the per-class `pending_admit_bidi`/`pending_admit_uni`
    /// queues define bounded admission order and its membership. Both are
    /// updated atomically on every exit (iter11 f6). Admission order is split
    /// per class so a class that parks on a full accept channel can be skipped
    /// without rescanning its backlog, bounding per-pump work to O(ADMIT_BUDGET)
    /// regardless of how many same-class ids are blocked (MF-1).
    pending_admit: HashMap<u64, PeerStream>,
    pending_admit_bidi: VecDeque<u64>,
    pending_admit_uni: VecDeque<u64>,
    /// Per-class parked promotion queues (parking is independent per class, §5).
    parked_bidi: VecDeque<u64>,
    parked_uni: VecDeque<u64>,
    /// Test-only counter of ids popped/examined by `phase2_admission` in the
    /// most recent pump. Proves the MF-1 fix bounds per-pump admission work to
    /// O(ADMIT_BUDGET) instead of re-scanning the accept-blocked backlog.
    #[cfg(test)]
    phase2_pops: usize,

    // ----- setup signalling (§7.1) -----
    established: Option<oneshot::Sender<Result<(), SetupFailure>>>,

    // ----- worker loop flags / buffers (§2.3, §5) -----
    /// `should_act()` result: true once established.
    acting: bool,
    /// Outbound packet buffer backing `buffer()` (§5, T3). Sized from
    /// [`DriverBufferConfig::packet_buffer_size`] (SF-5-pkt_buf).
    pkt_buf: Vec<u8>,
    /// Configured recv byte-channel depth applied in `build_recv` (SF-4).
    recv_channel_depth: usize,
    /// Reusable `stream_recv` target (SF-1). A single `BytesMut` grown to
    /// `MAX_CHUNK` per read; each chunk is handed out via `split_to(len).freeze()`
    /// (O(1) refcounted share of the backing allocation, no per-chunk memcpy).
    /// Lazily allocated on first receive (SF-5): a connection that never receives
    /// data never allocates it (`None`).
    recv_buf: Option<BytesMut>,
    /// Counts hoisted per-stream sender lookup+clone operations (SF-7): with the
    /// clone hoisted out of the chunk loop this increments once per `drain_stream`.
    #[cfg(test)]
    recv_lookup_count: usize,
    /// A stage deferred runnable work under a per-iteration quota: force another
    /// iteration via the `wait_for_data` fast path (§5, finding 5).
    needs_iteration: bool,
    /// Set after the graceful last-handle close is issued: `wait_for_data` then
    /// stays pending instead of re-polling the disconnected receiver (§5.2).
    graceful_close_issued: bool,
    /// Set on `cmd_rx` EOF (last strong sender dropped): one normal iteration
    /// then `process_writes` attempts graceful close (§5.2).
    last_handle_teardown: bool,
    /// Selects the single read-pump invocation per acting iteration (§2.3).
    reads_ran_this_iter: bool,
    /// Shared chunk counter for the read pump (§5.1).
    read_budget: usize,

    // ----- connection close / teardown (§5.2, §8.3) -----
    /// The first effective explicit `Close`, staged in stage (a) and applied at
    /// the mandatory close barrier after the bounded write stage (invariant 10).
    pending_close: Option<PendingClose>,
    /// Set once the barrier calls `qconn.close` (explicit or synthetic): a
    /// second call is never issued, and any attempt suppresses synthetic
    /// `H3_NO_ERROR` (§5.2).
    explicit_close_attempted: bool,
    /// A recorded successful `qconn.close` (§8.3); outranks `local_error()`.
    local_close: Option<RecordedLocalClose>,
    /// A `qconn.close` result our barrier could not classify (`Err` other than
    /// `Done`): an adapter bug that fails the callback as `Internal` (§8.3).
    close_bug: Option<&'static str>,
    /// Accept-terminal cell shared with `poll_accept_bidi` (§5): published once
    /// at the connection-terminal edge so a blocked accept resolves.
    accept_terminal_bidi: TerminalCell<Arc<ConnTerminal>>,
    /// Accept-terminal cell shared with `poll_accept_uni` (§5).
    accept_terminal_uni: TerminalCell<Arc<ConnTerminal>>,
    /// Endpoint deregistration guard (server accept path only, §5.3/§5.5). When
    /// present, its `Drop` — reached here at `QuicheDriver::drop`, the single
    /// worker-exit funnel for both pre- and post-handshake exits — deregisters
    /// this worker from the endpoint registry and fires the idle notify at the
    /// `live` 1→0 edge. `None` on the connector path, which is unregistered.
    conn_registration: Option<crate::endpoint::ConnRegistration>,
}

impl<B: Buf + Send + 'static> QuicheDriver<B> {
    /// Create a driver and its front-end handles. `is_server` selects the QUIC
    /// stream-id parity for locally-opened streams (§6.1); `accept_bidi_cap` /
    /// `accept_uni_cap` bound the respective accept queues (§5.2, provisional
    /// §12 S3). Buffer sizes take the historical defaults ([`DriverBufferConfig`]);
    /// use [`with_buffers`](Self::with_buffers) to override them (SF-4/SF-5).
    pub(crate) fn new(
        is_server: bool,
        accept_bidi_cap: usize,
        accept_uni_cap: usize,
    ) -> (Self, DriverHandles<B>) {
        Self::with_buffers(
            is_server,
            accept_bidi_cap,
            accept_uni_cap,
            DriverBufferConfig::default(),
        )
    }

    /// Like [`new`](Self::new) but with explicit per-connection buffer sizing
    /// (SF-4 recv channel depth, SF-5 packet buffer size). Passing
    /// `DriverBufferConfig::default()` is identical to [`new`](Self::new).
    pub(crate) fn with_buffers(
        is_server: bool,
        accept_bidi_cap: usize,
        accept_uni_cap: usize,
        buffers: DriverBufferConfig,
    ) -> (Self, DriverHandles<B>) {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let cmd_tx_weak = cmd_tx.downgrade();
        let (accept_bidi_tx, accept_bidi_rx) = mpsc::channel(accept_bidi_cap.max(1));
        let (accept_uni_tx, accept_uni_rx) = mpsc::channel(accept_uni_cap.max(1));
        let (est_tx, est_rx) = oneshot::channel();
        let shared = ConnShared::new(buffers.max_buffered_send_bytes);
        let accept_terminal_bidi = TerminalCell::new();
        let accept_terminal_uni = TerminalCell::new();
        let accept_bidi_resume = Arc::new(AtomicBool::new(false));
        let accept_uni_resume = Arc::new(AtomicBool::new(false));

        let driver = QuicheDriver {
            shared: Arc::clone(&shared),
            cmd_rx,
            cmd_tx_weak,
            inbox: VecDeque::new(),
            accept_bidi: accept_bidi_tx,
            accept_uni: accept_uni_tx,
            accept_bidi_resume: Arc::clone(&accept_bidi_resume),
            accept_uni_resume: Arc::clone(&accept_uni_resume),
            is_server,
            // Seed the id counters by QUIC convention (§6.1): client bidi
            // `0,4,8…`/uni `2,6,10…`; server bidi `1,5,9…`/uni `3,7,11…`.
            next_bidi_id: if is_server { 1 } else { 0 },
            next_uni_id: if is_server { 3 } else { 2 },
            open_bidi: VecDeque::new(),
            open_uni: VecDeque::new(),
            recv: HashMap::new(),
            send: HashMap::new(),
            runnable_send: VecDeque::new(),
            runnable_send_set: HashSet::new(),
            admit: HashMap::new(),
            pending_readable: VecDeque::new(),
            readable_set: HashSet::new(),
            pending_resume: VecDeque::new(),
            resume_set: HashSet::new(),
            pending_admit: HashMap::new(),
            pending_admit_bidi: VecDeque::new(),
            pending_admit_uni: VecDeque::new(),
            parked_bidi: VecDeque::new(),
            parked_uni: VecDeque::new(),
            #[cfg(test)]
            phase2_pops: 0,
            established: Some(est_tx),
            acting: false,
            pkt_buf: vec![0u8; buffers.packet_buffer_size.max(1)],
            recv_channel_depth: buffers.recv_channel_depth,
            recv_buf: None,
            #[cfg(test)]
            recv_lookup_count: 0,
            needs_iteration: false,
            graceful_close_issued: false,
            last_handle_teardown: false,
            reads_ran_this_iter: false,
            read_budget: READ_BUDGET,
            pending_close: None,
            explicit_close_attempted: false,
            local_close: None,
            close_bug: None,
            accept_terminal_bidi: accept_terminal_bidi.clone(),
            accept_terminal_uni: accept_terminal_uni.clone(),
            // Unregistered by default; the server accept path attaches a guard
            // via `set_conn_registration` before `start` (§5.3).
            conn_registration: None,
        };

        let handles = DriverHandles {
            cmd_tx,
            accept_bidi_rx,
            accept_uni_rx,
            established_rx: est_rx,
            shared,
            accept_terminal_bidi,
            accept_terminal_uni,
            accept_bidi_resume,
            accept_uni_resume,
        };

        (driver, handles)
    }

    /// Attach the endpoint deregistration guard to this driver (server accept
    /// path, §5.3). Called by the acceptor after a successful `try_register`
    /// and before `start`, so the guard's `Drop` runs at worker exit via
    /// `QuicheDriver::drop`.
    pub(crate) fn set_conn_registration(&mut self, reg: crate::endpoint::ConnRegistration) {
        self.conn_registration = Some(reg);
    }

    /// The `wait_for_data` pre-await decision (§5, finding 2).
    fn wait_decision(&self) -> WaitDecision {
        if self.graceful_close_issued {
            // EOF/close already consumed: polling the disconnected receiver
            // would return None forever, so stay pending and let packet/timer
            // events drive closing/draining. No hot spin (§5.2, §9).
            WaitDecision::Pending
        } else if !self.inbox.is_empty() || self.needs_iteration {
            // Deferred runnable work: force a no-packet acting iteration so it
            // isn't stranded until the idle timeout (§2.3).
            WaitDecision::Yield
        } else {
            WaitDecision::Recv
        }
    }

    /// The shared read pump (§5.1), the sole **readable-path** discovery +
    /// admission + receive engine, run **exactly once per acting iteration**
    /// (§2.3, invariant 9). It is generic over [`QuicConn`] so it is
    /// unit-testable against `MockConn`; the concrete `QuicheConnection`
    /// satisfies the bound, so the `ApplicationOverQuic` callbacks pass
    /// `&mut QuicheConnection` unchanged.
    ///
    /// Control flow: bounded destructive readable intake → resumed-read drain →
    /// phase-1 registered-drain → phase-2 admission → parked promotion. The
    /// **writable**-path scan is stage (d), run from `process_writes`
    /// (§5.3a, §5.5), not here — the read pump owns the readable path only. All
    /// receive chunk work shares the single [`read_budget`](Self::read_budget).
    fn run_read_pump<C: QuicConn>(&mut self, qconn: &mut C) {
        self.intake_readable(qconn);
        self.drain_resumed(qconn);
        self.phase1_registered_drain(qconn);
        self.phase2_admission(qconn);
        self.promote_parked(qconn, true);
        self.promote_parked(qconn, false);
    }

    /// Packet-driven acting-iteration body (generic for testing; the
    /// `ApplicationOverQuic::process_reads` callback delegates here). Resets the
    /// per-iteration signals at the iteration's start, claims the single pump
    /// invocation, and runs it (§2.3, §5.1). `needs_iteration` is reset here —
    /// not in `do_process_writes` — so a deferral the pump records survives to
    /// `wait_for_data`.
    fn do_process_reads<C: QuicConn>(&mut self, qconn: &mut C) {
        self.needs_iteration = false;
        self.read_budget = READ_BUDGET;
        self.reads_ran_this_iter = true;
        self.run_read_pump(qconn);
    }

    /// Every-iteration acting body (generic for testing; the
    /// `ApplicationOverQuic::process_writes` callback delegates here). Stage
    /// order (§5 process_writes): (a) apply queued commands; (b) on the no-packet
    /// path (process_reads skipped) run the readable read pump — this is the
    /// iteration start, so `needs_iteration`/`read_budget` are reset here; on a
    /// packet iteration the pump already ran and its deferral is preserved;
    /// (d) the single destructive **writable** scan; (e) the round-robin
    /// runnable-send drain; (f) the mandatory close barrier. Stages (d)/(e) run
    /// once per iteration on both paths, preserving the single-writable-scan-
    /// per-iteration contract (§5.3a).
    fn do_process_writes<C: QuicConn>(&mut self, qconn: &mut C) -> QuicResult<()> {
        let no_packet = !self.reads_ran_this_iter;
        if no_packet {
            // No-packet iteration start: reset the per-iteration signals before
            // any stage that may set them (§2.3; do NOT blanket-clear).
            self.needs_iteration = false;
            self.read_budget = READ_BUDGET;
        }
        self.apply_inbox(qconn);
        if no_packet {
            self.run_read_pump(qconn);
        }
        // (c) Open-materialization stage (§6.1): worker-owned id allocation for
        // locally-requested opens, bounded by OPEN_BUDGET and gated on peer
        // flow control. Placed before the send stages so a freshly opened
        // stream can be written in the same iteration.
        self.stage_open(qconn);
        self.stage_writable(qconn);
        self.stage_send(qconn);
        // Common per-iteration boundary: clear only the pump-selection flag.
        self.reads_ran_this_iter = false;
        // Recompute `needs_iteration` from the runnable stage remainders (§5.2
        // progress bound): a command applied here (e.g. RecvResume /
        // Accept*Resume) on a PACKET iteration is queued but its stage does not
        // re-run this iteration, so without this the resumed work would strand
        // until unrelated traffic. `|=` never clears a pessimistic signal a
        // stage already set; the predicates below are all "definitely runnable,
        // not blocked on credit/capacity" so they cannot hot-spin.
        self.needs_iteration |= self.has_runnable_remainder();
        // (f) The mandatory non-write-budgeted close barrier, AFTER stage (e)
        // (§5.2, invariant 10): applies a staged explicit `Close` or the
        // synthetic last-handle `H3_NO_ERROR` close even after a saturated
        // write batch.
        self.apply_close_barrier(qconn)
    }

    /// Whether any stage has runnable work left that is NOT blocked purely on
    /// channel capacity or stream credit (§5.2). Used to recompute
    /// `needs_iteration` at the callback boundary so a resume/command applied on
    /// a packet iteration is serviced on the next one instead of stranding. A
    /// blocked recv (byte channel full) is removed from `pending_readable`; a
    /// blocked send (no capacity) is removed from `runnable_send`; a parked
    /// stream only counts once its accept-resume bit is set; a credit-blocked
    /// open counts nothing here (its backlog signal is set in `stage_open`).
    fn has_runnable_remainder(&self) -> bool {
        !self.pending_resume.is_empty()
            || !self.pending_readable.is_empty()
            // A class with pending admits re-arms the worker only if it is not
            // currently accept-blocked-waiting: either it has never parked
            // (`parked_*` empty — capacity may be free) or its accept-resume bit
            // has since been set. A class that parked on a full accept channel
            // (parked non-empty, resume bit still clear) is NOT reported runnable
            // until the matching `Accept*Resume` arrives, so a capacity-blocked
            // class can no longer self-reschedule the worker (MF-1, FR-002). A
            // fresh never-parked class with pending ids still reports runnable so
            // no admission is stalled. The accept-resume bit is set by the accept
            // consumer (`stream.rs`) and read here on the worker; a Relaxed load
            // suffices because the actual happens-before + wakeup is carried by
            // the paired `Accept*Resume` command over the control channel (the
            // bit only coalesces re-arm signals), matching the parked-promotion
            // checks below.
            || (!self.pending_admit_bidi.is_empty()
                && (self.parked_bidi.is_empty()
                    || self.accept_bidi_resume.load(Ordering::Relaxed)))
            || (!self.pending_admit_uni.is_empty()
                && (self.parked_uni.is_empty()
                    || self.accept_uni_resume.load(Ordering::Relaxed)))
            || !self.runnable_send.is_empty()
            || (!self.parked_bidi.is_empty() && self.accept_bidi_resume.load(Ordering::Relaxed))
            || (!self.parked_uni.is_empty() && self.accept_uni_resume.load(Ordering::Relaxed))
    }

    /// Route a discovered peer id into its per-class admission queue (§5.1). The
    /// split lets `phase2_admission` skip a parked (accept-full) class without
    /// rescanning its backlog (MF-1); FIFO order is preserved within each class.
    fn push_pending_admit(&mut self, id: u64) {
        if is_bidi(id) {
            self.pending_admit_bidi.push_back(id);
        } else {
            self.pending_admit_uni.push_back(id);
        }
    }

    /// The mandatory explicit-close barrier (§5.2, §8.3, invariant 10), run
    /// after the bounded stream-write stage and bypassing `WRITE_BUDGET`. It
    /// applies at most one `qconn.close` per connection and classifies its
    /// result:
    /// - a staged explicit `Close` takes precedence and, on `Ok`, records the
    ///   exact `local_close`; `Done` defers to the pre-existing quiche terminal;
    /// - otherwise, on last-handle teardown with no peer/local/timeout terminal
    ///   and no prior recorded close, it issues the synthetic
    ///   `qconn.close(true, H3_NO_ERROR, b"")`.
    ///
    /// Either `Ok` or `Done` sets `graceful_close_issued` so `wait_for_data`
    /// stays pending; any other `Err` is an adapter bug that fails the callback
    /// as `Internal` (returned as an error from `process_writes`).
    fn apply_close_barrier<C: QuicConn>(&mut self, qconn: &mut C) -> QuicResult<()> {
        if self.explicit_close_attempted {
            return Ok(());
        }
        if let Some(pc) = self.pending_close.take() {
            self.explicit_close_attempted = true;
            match qconn.close(true, pc.code, &pc.reason) {
                Ok(()) => {
                    self.local_close = Some(RecordedLocalClose {
                        code: pc.code,
                        reason: pc.reason,
                    });
                    self.graceful_close_issued = true;
                }
                Err(quiche::Error::Done) => {
                    // A pre-existing quiche terminal supplies the cause; do not
                    // fabricate acceptance (§8.3).
                    self.graceful_close_issued = true;
                }
                Err(_) => {
                    self.close_bug = Some("explicit qconn.close returned an unexpected error");
                    return Err(self.close_bug.unwrap().into());
                }
            }
            return Ok(());
        }
        // Synthetic last-handle teardown close: only when no explicit close was
        // staged/attempted and no quiche terminal (peer/local/timeout) or prior
        // recorded close exists (§5.2, §8.3).
        if self.last_handle_teardown
            && qconn.peer_error().is_none()
            && qconn.local_error().is_none()
            && !qconn.is_timed_out()
            && self.local_close.is_none()
        {
            self.explicit_close_attempted = true;
            match qconn.close(true, H3_NO_ERROR, b"") {
                Ok(()) => {
                    self.local_close = Some(RecordedLocalClose {
                        code: H3_NO_ERROR,
                        reason: Bytes::new(),
                    });
                    self.graceful_close_issued = true;
                }
                Err(quiche::Error::Done) => {
                    self.graceful_close_issued = true;
                }
                Err(_) => {
                    self.close_bug = Some("last-handle qconn.close returned an unexpected error");
                    return Err(self.close_bug.unwrap().into());
                }
            }
        }
        Ok(())
    }

    /// Bounded **destructive** readable intake (§5.1, iter9 finding 2): each id
    /// returned by `stream_readable_next()` is dearmed in quiche, so it is
    /// transferred before any fallible work into exactly one bridge-owned slot,
    /// guarded by membership. A readable discovery of an id already queued from
    /// the writable path merges without dropping its retained send terminal.
    fn intake_readable<C: QuicConn>(&mut self, qconn: &mut C) {
        let mut n = 0;
        while n < DISCOVERY_BUDGET {
            let id = match qconn.stream_readable_next() {
                Some(id) => id,
                None => break,
            };
            n += 1;
            if self.recv.contains_key(&id) {
                // Registered live half: route to the registered-drain cursor.
                self.requeue_readable(id);
            } else if self.admit.contains_key(&id) || self.pending_admit.contains_key(&id) {
                // Parked / Registered-recv-done / already-queued: membership
                // merge is a no-op (its owned PeerStream/terminals are retained).
            } else {
                // A new peer id: own a fresh PeerStream awaiting admission.
                self.pending_admit.insert(id, PeerStream::new(id));
                self.push_pending_admit(id);
            }
        }
        if n == DISCOVERY_BUDGET {
            // Pessimistic re-probe is cheaper than losing an id (§5.1).
            self.needs_iteration = true;
        }
    }

    /// Stage (d): the single destructive **writable** scan per acting iteration
    /// (§5 process_writes, §5.3a, §5.5). Bounded by [`WRITABLE_BUDGET`], it
    /// destructively drains `stream_writable_next()` and routes each id:
    ///
    /// - **registered send half** (`self.send`): probe `stream_capacity`. A
    ///   `StreamStopped(code)` (and no earlier local reset owning the terminal)
    ///   runs the send-terminal transition — draining `send_ops` once before any
    ///   runnable eviction (invariant 13). Otherwise, if capacity is available
    ///   and the stream has pending send work, mark it runnable exactly once.
    /// - **new / parked / pending peer bidi id**: the same admission-capture path
    ///   the old readable/writable intake used, including the `STOP_SENDING`
    ///   `stream_capacity` probe for a not-yet-registered id (§5.5, §14 Q4).
    ///
    /// Per outcome 3, only ids `stream_writable_next()` actually returns are
    /// handled — no discovery is synthesized at zero connection send capacity.
    fn stage_writable<C: QuicConn>(&mut self, qconn: &mut C) {
        let mut n = 0;
        while n < WRITABLE_BUDGET {
            let id = match qconn.stream_writable_next() {
                Some(id) => id,
                None => break,
            };
            n += 1;
            // A registered local send half (any class): the send machine owns it.
            if self.send.contains_key(&id) {
                match qconn.stream_capacity(id) {
                    Err(quiche::Error::StreamStopped(code)) => {
                        let owns_reset = self
                            .send
                            .get(&id)
                            .map(|s| s.pending_reset.is_some() || s.terminal.is_some())
                            .unwrap_or(false);
                        if !owns_reset {
                            self.send_terminal_transition(
                                id,
                                SendEnd::Stopped { error_code: code },
                            );
                        }
                    }
                    _ => {
                        let has_work = self
                            .send
                            .get(&id)
                            .map(|s| !s.send_ops.is_empty() || s.pending_reset.is_some())
                            .unwrap_or(false);
                        if has_work {
                            self.mark_send_runnable(id);
                        }
                    }
                }
                continue;
            }
            // Peer-stream discovery: only a peer bidi id enters admission via the
            // writable path (a peer uni stream is receive-only locally).
            if !is_bidi(id) {
                continue;
            }
            if matches!(self.admit.get(&id), Some(AdmitState::Registered { .. })) {
                // Registered with the send half already done: nothing to do.
                continue;
            }
            // Probe for a peer STOP_SENDING (Q4: `stream_capacity` reports
            // `StreamStopped(code)` immediately after the frame). Merge it into
            // the owned PeerStream so admission never loses the send terminal.
            let stopped = match qconn.stream_capacity(id) {
                Err(quiche::Error::StreamStopped(code)) => {
                    Some(SendEnd::Stopped { error_code: code })
                }
                _ => None,
            };
            if let Some(peer) = self.pending_admit.get_mut(&id) {
                if peer.pending_send_terminal.is_none() {
                    peer.pending_send_terminal = stopped;
                }
            } else if let Some(AdmitState::Parked(peer)) = self.admit.get_mut(&id) {
                if peer.pending_send_terminal.is_none() {
                    peer.pending_send_terminal = stopped;
                }
            } else {
                let mut peer = PeerStream::new(id);
                peer.pending_send_terminal = stopped;
                self.pending_admit.insert(id, peer);
                self.push_pending_admit(id);
                // A new admission is pending: ensure stage (b) runs next iteration.
                self.needs_iteration = true;
            }
        }
        if n == WRITABLE_BUDGET {
            // Pessimistic re-probe is cheaper than stranding a ready write (§5.5).
            self.needs_iteration = true;
        }
    }

    /// Drain up to `RECV_RESUME_BUDGET` distinct resumed ids (§5.1). The worker
    /// clears the resume bit **before** retrying (clearing after would drop a
    /// wakeup if the consumer freed more capacity during the retry).
    fn drain_resumed<C: QuicConn>(&mut self, qconn: &mut C) {
        let mut budget = RECV_RESUME_BUDGET;
        while budget > 0 {
            let id = match self.pending_resume.pop_front() {
                Some(id) => id,
                None => break,
            };
            self.resume_set.remove(&id);
            budget -= 1;
            match self.recv.get_mut(&id) {
                Some(state) => {
                    state.resume.store(false, Ordering::Relaxed);
                    // Defensive: the consumer already cleared `blocked` via its
                    // AcqRel swap when it emitted this resume; reset it here too
                    // so a fresh park cycle starts from a known-clear flag (SF-2).
                    state.blocked.store(false, Ordering::Relaxed);
                }
                None => continue,
            }
            self.drain_stream(qconn, id);
        }
    }

    /// Phase 1: registered-drain up to `READABLE_BUDGET` id-attempts from
    /// `pending_readable` (§5.1). Known streams are drained before new peer
    /// streams are admitted (invariant 5).
    fn phase1_registered_drain<C: QuicConn>(&mut self, qconn: &mut C) {
        let mut attempts = READABLE_BUDGET;
        while attempts > 0 {
            if self.read_budget == 0 {
                // Shared budget exhausted: leave the remainder queued.
                if !self.pending_readable.is_empty() {
                    self.needs_iteration = true;
                }
                break;
            }
            let id = match self.pending_readable.pop_front() {
                Some(id) => id,
                None => break,
            };
            self.readable_set.remove(&id);
            attempts -= 1;
            if !self.recv.contains_key(&id) {
                // Terminal/abandoned since queueing: drop it (membership gone).
                continue;
            }
            self.drain_stream(qconn, id);
        }
    }

    /// Reserve-before-read drain of one registered stream up to `CHUNK_BUDGET`
    /// chunks, decrementing the shared `read_budget` per chunk (§5.1). Publishes
    /// `RecvEnd::Fin` only after `Permit::send`-ing the final byte (sealing edge).
    fn drain_stream<C: QuicConn>(&mut self, qconn: &mut C, id: u64) {
        // SF-7: hoist the per-stream sender lookup+clone out of the chunk loop.
        // Cloning the `Sender` (a) frees `&mut self` for the receive buffer borrow
        // and (b) is done once per drain instead of once per chunk. The clone
        // stays valid even if the `recv` entry is later removed on a terminal
        // path (we `return` on those paths anyway).
        let tx = match self.recv.get(&id) {
            Some(state) => state.bytes.clone(),
            None => return,
        };
        #[cfg(test)]
        {
            self.recv_lookup_count += 1;
        }
        for _ in 0..CHUNK_BUDGET {
            if self.read_budget == 0 {
                // Requeue keeping membership so the remainder drains next pump.
                self.requeue_readable(id);
                self.needs_iteration = true;
                return;
            }
            let permit = match tx.try_reserve() {
                Ok(permit) => permit,
                Err(TrySendError::Full(())) => {
                    // Full: leave bytes in quiche (flow control backpressures the
                    // peer); park until RecvResume. SF-2 lost-wakeup closure
                    // (§5.1): publish `blocked=true` with Release BEFORE a final
                    // capacity re-check. This forms an atomic handshake with the
                    // consumer's `blocked.swap(false, AcqRel)`:
                    //   * if the consumer freed a slot and swapped before our
                    //     store, its swap saw `false` (no resume) — but our
                    //     re-check below then observes the freed slot and we do
                    //     NOT park (so no wakeup is needed);
                    //   * if the consumer frees after our store, its swap sees
                    //     `true` and emits exactly one resume.
                    // Either way we never park with a slot already free, so a
                    // genuine resume can never be dropped (correctness > perf).
                    if let Some(state) = self.recv.get(&id) {
                        state.blocked.store(true, Ordering::Release);
                    }
                    match tx.try_reserve() {
                        Ok(permit) => {
                            // A slot was freed in the window: clear the park flag
                            // and proceed with the read instead of parking.
                            if let Some(state) = self.recv.get(&id) {
                                state.blocked.store(false, Ordering::Release);
                            }
                            permit
                        }
                        Err(TrySendError::Full(())) => return,
                        Err(TrySendError::Closed(())) => {
                            self.abandon_recv(qconn, id);
                            return;
                        }
                    }
                }
                Err(TrySendError::Closed(())) => {
                    // Dropped H3RecvStream: normal local abandonment (invariant 1).
                    self.abandon_recv(qconn, id);
                    return;
                }
            };
            // SF-1: read into the reusable `BytesMut` (lazily allocated per SF-5)
            // and hand the consumer an owned `Bytes` via `split_to(len).freeze()`.
            // `stream_recv` requires an *initialized* `&mut [u8]`, so the buffer is
            // grown to `MAX_CHUNK` with a safe zero-fill (`resize`) — NOT an unsafe
            // uninitialized-slice cast. The zero-fill of the reused window is the
            // accepted trade-off (D-1): this removes the per-chunk *memcpy*, not the
            // fill; only the freshly-written `[..len]` region is frozen, so no
            // uninitialized (or stale) byte is ever exposed to h3. The frozen slice
            // shares the backing allocation with the reader buffer (O(1)); a later
            // grow reallocates instead of overwriting bytes the consumer still holds.
            let read = {
                let buf = self.recv_buf.get_or_insert_with(BytesMut::new);
                buf.resize(MAX_CHUNK, 0);
                match qconn.stream_recv(id, &mut buf[..]) {
                    Ok((len, fin)) => {
                        let chunk = if len > 0 {
                            Some(buf.split_to(len).freeze())
                        } else {
                            None
                        };
                        Ok((chunk, fin))
                    }
                    Err(err) => Err(err),
                }
            };
            match read {
                Ok((chunk, fin)) => {
                    let had_bytes = chunk.is_some();
                    if let Some(chunk) = chunk {
                        permit.send(chunk);
                        self.read_budget -= 1;
                    } else {
                        drop(permit);
                    }
                    if fin {
                        // Sealing edge: Fin published only after the final byte.
                        self.publish_recv_terminal(id, RecvEnd::Fin);
                        self.mark_recv_done(id);
                        return;
                    }
                    // Mirror the original semantics: a zero-length read always
                    // stops this drain (avoids spinning on a readable-but-empty
                    // stream); otherwise stop once quiche reports no more data.
                    if !had_bytes || !qconn.stream_readable(id) {
                        return;
                    }
                }
                Err(err) => {
                    drop(permit);
                    match classify_stream_recv_error(&err) {
                        StreamRecvClass::Done => return,
                        StreamRecvClass::Reset(code) => {
                            self.publish_recv_terminal(id, RecvEnd::Reset { error_code: code });
                            self.mark_recv_done(id);
                            return;
                        }
                        StreamRecvClass::ConnGone => {
                            self.resolve_recv_via_conn(id);
                            return;
                        }
                        StreamRecvClass::Bug(msg) => {
                            // A stream-level invariant violation resolves via a
                            // connection terminal; the close machine (Phases 4–5)
                            // owns the connection edge.
                            self.publish_recv_terminal(
                                id,
                                RecvEnd::Conn(Arc::new(ConnTerminal::Internal(msg))),
                            );
                            self.mark_recv_done(id);
                            return;
                        }
                    }
                }
            }
        }
        // CHUNK_BUDGET exhausted: requeue if the body still has data (§5.1).
        if qconn.stream_readable(id) {
            self.requeue_readable(id);
            self.needs_iteration = true;
        }
    }

    /// Phase 2: admit up to `ADMIT_BUDGET` ids from the per-class admission
    /// queues (§5.1). A full accept queue parks **only that class**; the other
    /// class keeps admitting (parking is independent per class). The two queues
    /// are serviced in an alternating (round-robin) fashion so a flood of one
    /// class cannot starve the other. Crucially, once a class parks it is not
    /// serviced again this pump and its remaining backlog is left in place
    /// **without being rescanned** — this bounds per-pump work to O(ADMIT_BUDGET)
    /// regardless of how many same-class ids are accept-blocked (MF-1).
    fn phase2_admission<C: QuicConn>(&mut self, qconn: &mut C) {
        let mut budget = ADMIT_BUDGET;
        let mut bidi_blocked = false;
        let mut uni_blocked = false;
        #[cfg(test)]
        {
            self.phase2_pops = 0;
        }
        // Alternate the preferred class each iteration for fair cross-class
        // servicing (FR-003). Skip a class that is blocked (parked this pump) or
        // empty; stop when neither class can be served.
        let mut prefer_bidi = true;
        while budget > 0 {
            let bidi_ready = !bidi_blocked && !self.pending_admit_bidi.is_empty();
            let uni_ready = !uni_blocked && !self.pending_admit_uni.is_empty();
            let bidi = match (bidi_ready, uni_ready) {
                (false, false) => break,
                (true, false) => true,
                (false, true) => false,
                (true, true) => prefer_bidi,
            };
            // Alternate for the next iteration so the two queues share the budget.
            prefer_bidi = !bidi;
            let id = if bidi {
                self.pending_admit_bidi.pop_front()
            } else {
                self.pending_admit_uni.pop_front()
            };
            let id = match id {
                Some(id) => id,
                None => continue,
            };
            #[cfg(test)]
            {
                self.phase2_pops += 1;
            }
            // Already admitted (defensive; membership should prevent it).
            if self.admit.contains_key(&id) || self.recv.contains_key(&id) {
                self.pending_admit.remove(&id);
                continue;
            }
            let peer = match self.pending_admit.remove(&id) {
                Some(peer) => peer,
                None => continue,
            };
            budget -= 1;
            match self.admit_one(qconn, peer) {
                AdmitResult::Registered => {}
                AdmitResult::Full(peer) => {
                    self.admit.insert(id, AdmitState::Parked(peer));
                    if bidi {
                        self.parked_bidi.push_back(id);
                        bidi_blocked = true;
                    } else {
                        self.parked_uni.push_back(id);
                        uni_blocked = true;
                    }
                }
                AdmitResult::TornDown => {
                    if bidi {
                        bidi_blocked = true;
                    } else {
                        uni_blocked = true;
                    }
                }
            }
        }
    }

    /// Promote up to `PROMOTE_BUDGET` parked ids of one class, gated by that
    /// class's accept-resume bit (§5.1). The bit is cleared before retry; a
    /// re-`Full` promotion re-parks and stops the class (the front end re-signals
    /// on its next dequeue).
    fn promote_parked<C: QuicConn>(&mut self, qconn: &mut C, bidi: bool) {
        let armed = if bidi {
            self.accept_bidi_resume.load(Ordering::Relaxed)
        } else {
            self.accept_uni_resume.load(Ordering::Relaxed)
        };
        if !armed {
            return;
        }
        if bidi {
            self.accept_bidi_resume.store(false, Ordering::Relaxed);
        } else {
            self.accept_uni_resume.store(false, Ordering::Relaxed);
        }
        let mut budget = PROMOTE_BUDGET;
        while budget > 0 {
            let id = {
                let queue = if bidi {
                    &mut self.parked_bidi
                } else {
                    &mut self.parked_uni
                };
                match queue.pop_front() {
                    Some(id) => id,
                    None => break,
                }
            };
            budget -= 1;
            let peer = match self.admit.remove(&id) {
                Some(AdmitState::Parked(peer)) => peer,
                Some(other) => {
                    // Not parked (already registered/terminal): leave it be.
                    self.admit.insert(id, other);
                    continue;
                }
                None => continue,
            };
            match self.admit_one(qconn, peer) {
                AdmitResult::Registered => {}
                AdmitResult::Full(peer) => {
                    self.admit.insert(id, AdmitState::Parked(peer));
                    if bidi {
                        self.parked_bidi.push_front(id);
                    } else {
                        self.parked_uni.push_front(id);
                    }
                    break;
                }
                AdmitResult::TornDown => break,
            }
        }
    }

    /// The atomic `register_peer` transfer (§5.1). Selects the accept channel by
    /// class, reserves it, upgrades `cmd_tx_weak`, builds registry state and the
    /// handle's sticky cells from any retained terminals, hands the handle over,
    /// runs the terminal transition, then drains already-buffered data against
    /// the shared `read_budget`. One synchronous operation, no interleaving.
    fn admit_one<C: QuicConn>(&mut self, qconn: &mut C, mut peer: PeerStream) -> AdmitResult {
        let id = peer.id;
        let bidi = is_bidi(id);
        if bidi {
            // Clone the accept sender into a local so the reserved `Permit`
            // borrows the local — not `self` — leaving `self` free to mutate.
            let tx = self.accept_bidi.clone();
            let permit = match tx.try_reserve() {
                Ok(permit) => permit,
                Err(TrySendError::Full(())) => return AdmitResult::Full(peer),
                Err(TrySendError::Closed(())) => {
                    self.shutdown_peer_directions(qconn, id, bidi);
                    return AdmitResult::TornDown;
                }
            };
            let cmd_tx = match self.cmd_tx_weak.upgrade() {
                Some(cmd_tx) => cmd_tx,
                None => {
                    self.shutdown_peer_directions(qconn, id, bidi);
                    return AdmitResult::TornDown;
                }
            };
            let (recv_state, recv_handoff, recv_done) =
                self.build_recv(id, cmd_tx.clone(), peer.pending_recv_terminal.take());
            let (send_handoff, send_state, send_done) = build_send(
                id,
                cmd_tx,
                Arc::clone(&self.shared.send_accounting),
                peer.pending_send_terminal.take(),
            );
            if let Some(state) = recv_state {
                self.recv.insert(id, state);
            }
            // Retain the live send half so a later STOP_SENDING / Send / Reset
            // resolves through the SAME `status` cell the handle holds (§5.4
            // invariant 7). A send-terminal-at-admission bidi has no live half.
            if let Some(state) = send_state {
                self.send.insert(id, state);
            }
            self.admit.insert(
                id,
                AdmitState::Registered {
                    send_done,
                    recv_done,
                },
            );
            permit.send(BidiHandoff {
                send: send_handoff,
                recv: recv_handoff,
            });
        } else {
            let tx = self.accept_uni.clone();
            let permit = match tx.try_reserve() {
                Ok(permit) => permit,
                Err(TrySendError::Full(())) => return AdmitResult::Full(peer),
                Err(TrySendError::Closed(())) => {
                    self.shutdown_peer_directions(qconn, id, bidi);
                    return AdmitResult::TornDown;
                }
            };
            let cmd_tx = match self.cmd_tx_weak.upgrade() {
                Some(cmd_tx) => cmd_tx,
                None => {
                    self.shutdown_peer_directions(qconn, id, bidi);
                    return AdmitResult::TornDown;
                }
            };
            let (recv_state, recv_handoff, recv_done) =
                self.build_recv(id, cmd_tx, peer.pending_recv_terminal.take());
            if let Some(state) = recv_state {
                self.recv.insert(id, state);
            }
            // A peer uni stream is receive-only locally: send is n/a → done.
            self.admit.insert(
                id,
                AdmitState::Registered {
                    send_done: true,
                    recv_done,
                },
            );
            permit.send(recv_handoff);
        }
        // Run the terminal transition immediately (a fully-terminal retained
        // stream is reclaimed now; the handed-over cells are already populated).
        self.terminal_transition(id);
        // If the recv direction is still live, drain buffered data now.
        if self.recv.contains_key(&id) {
            self.drain_stream(qconn, id);
        }
        AdmitResult::Registered
    }

    /// Build a recv registry entry + handoff, initializing the sticky terminal
    /// cell from any retained `RecvEnd` (§5.1 atomic transfer). Returns `None`
    /// for the registry entry when the recv direction is already terminal.
    fn build_recv(
        &self,
        id: u64,
        cmd_tx: mpsc::UnboundedSender<DriverCommand<B>>,
        retained: Option<RecvEnd>,
    ) -> (Option<StreamRecvState>, RecvHandoff<B>, bool) {
        let (tx, rx) = mpsc::channel(self.recv_channel_depth.max(1));
        let terminal = TerminalCell::new();
        let resume = Arc::new(AtomicBool::new(false));
        let blocked = Arc::new(AtomicBool::new(false));
        let recv_done = retained.is_some();
        if let Some(end) = retained {
            terminal.set(end);
        }
        let handoff = RecvHandoff {
            id,
            bytes: rx,
            terminal: terminal.clone(),
            resume: Arc::clone(&resume),
            blocked: Arc::clone(&blocked),
            cmd_tx: cmd_tx.clone(),
            cleanup: HandoffCleanup::new(id, true, cmd_tx),
        };
        let state = if recv_done {
            None
        } else {
            Some(StreamRecvState {
                bytes: tx,
                terminal,
                resume,
                blocked,
            })
        };
        (state, handoff, recv_done)
    }

    /// Publish a recv terminal into the out-of-band cell, never the byte channel
    /// (§5.1). The sealing edge: no byte is enqueued for the stream afterward.
    fn publish_recv_terminal(&self, id: u64, end: RecvEnd) {
        if let Some(state) = self.recv.get(&id) {
            state.terminal.set(end);
        }
    }

    /// Mark the recv direction done: release the registry entry (sealing edge —
    /// no more bytes), drop cursor memberships, set `recv_done`, run the terminal
    /// transition, and reclaim a retained (finished) send entry now that both
    /// directions are terminal (§5.1 terminal transition, §6.2 invariant 8).
    fn mark_recv_done(&mut self, id: u64) {
        self.recv.remove(&id);
        self.drop_recv_memberships(id);
        if let Some(AdmitState::Registered { recv_done, .. }) = self.admit.get_mut(&id) {
            *recv_done = true;
        }
        self.terminal_transition(id);
        self.reclaim_finished_send(id);
    }

    /// Normal local abandonment of a recv half (dropped `H3RecvStream`): issue an
    /// idempotent `stop_sending`, release the entry, never `InternalError`
    /// (invariant 1).
    fn abandon_recv<C: QuicConn>(&mut self, qconn: &mut C, id: u64) {
        let _ = qconn.stream_shutdown(id, Shutdown::Read, H3_NO_ERROR);
        self.mark_recv_done(id);
    }

    /// A stream-level `ConnGone` (`InvalidState`/`FinalSize`/`FlowControl` while
    /// closing) resolves via the connection terminal. If the terminal is already
    /// published, seal the recv half now; otherwise **leave the recv entry in
    /// place** so `on_conn_close` publishes the final `RecvEnd::Conn` into its
    /// cell — removing it here (before the terminal exists) would strand the
    /// front end with a drained queue and no terminal (a hang). Cursor
    /// memberships are dropped either way so a dead stream is not re-drained.
    fn resolve_recv_via_conn(&mut self, id: u64) {
        match self.shared.conn_terminal.get() {
            Some(terminal) => {
                self.publish_recv_terminal(id, RecvEnd::Conn(terminal));
                self.mark_recv_done(id);
            }
            None => {
                self.drop_recv_memberships(id);
            }
        }
    }

    /// Direction-aware shutdown of an un-admitted peer stream (§5.1): peer bidi
    /// shuts down both directions; peer uni is receive-only, so `Shutdown::Read`
    /// only (`Shutdown::Write` would return `InvalidStreamState`).
    fn shutdown_peer_directions<C: QuicConn>(&self, qconn: &mut C, id: u64, bidi: bool) {
        let _ = qconn.stream_shutdown(id, Shutdown::Read, H3_NO_ERROR);
        if bidi {
            let _ = qconn.stream_shutdown(id, Shutdown::Write, H3_NO_ERROR);
        }
    }

    /// Contract A terminal transition (§5.5): when all applicable directions are
    /// terminal (peer uni: recv; peer bidi: both), drop `admit[id]` immediately
    /// and release cursor memberships. No reclaim subsystem, no `stream_closed`.
    fn terminal_transition(&mut self, id: u64) {
        let all_terminal = match self.admit.get(&id) {
            Some(AdmitState::Registered {
                send_done,
                recv_done,
            }) => {
                if is_bidi(id) {
                    *send_done && *recv_done
                } else {
                    *recv_done
                }
            }
            _ => false,
        };
        if all_terminal {
            self.admit.remove(&id);
            self.recv.remove(&id);
            self.drop_recv_memberships(id);
            self.drop_send_membership(id);
            // self.send is deliberately RETAINED: its sticky `terminal` still
            // resolves a Send/Finish that was deferred in `cmd_rx` before the
            // terminal edge (§5.3a "ops after a terminal"; §5.2 exactly-once).
            // The send registry entry is reclaimed by front-end drop cleanup
            // (§6.2, invariant 8), not by contract A.
        }
    }

    /// Add an id to the registered-drain cursor with exact-once membership.
    fn requeue_readable(&mut self, id: u64) {
        if self.readable_set.insert(id) {
            self.pending_readable.push_back(id);
        }
    }

    /// Drop an id's readable and resume cursor memberships (terminal edge).
    fn drop_recv_memberships(&mut self, id: u64) {
        if self.readable_set.remove(&id) {
            self.pending_readable.retain(|queued| *queued != id);
        }
        if self.resume_set.remove(&id) {
            self.pending_resume.retain(|queued| *queued != id);
        }
    }

    /// Add an id to the round-robin runnable-send queue with exact-once
    /// membership (§5.3a). Idempotent: the non-runnable→runnable edge is the set
    /// insert, so repeated calls never duplicate the id.
    fn mark_send_runnable(&mut self, id: u64) {
        if self.runnable_send_set.insert(id) {
            self.runnable_send.push_back(id);
        }
    }

    /// Drop an id's runnable-send membership (terminal / reset-serviced edge).
    fn drop_send_membership(&mut self, id: u64) {
        if self.runnable_send_set.remove(&id) {
            self.runnable_send.retain(|queued| *queued != id);
        }
    }

    /// Apply up to [`CMD_BUDGET`] queued control commands (§5.2 stage (a)):
    /// drain `self.inbox` first, then `cmd_rx.try_recv()`. `Send`/`Finish` append
    /// to the target stream's ordered `send_ops` and make it runnable on the
    /// non-runnable→runnable edge; `Reset` is the FIFO-preemption exception;
    /// `StopSending` shuts the recv half down like a local abandonment (§5.3a).
    /// Excess commands stay in `inbox` in receipt order and re-force an iteration.
    fn apply_inbox<C: QuicConn>(&mut self, qconn: &mut C) {
        let mut budget = CMD_BUDGET;
        while budget > 0 {
            let cmd = match self.inbox.pop_front() {
                Some(cmd) => cmd,
                None => match self.cmd_rx.try_recv() {
                    Ok(cmd) => cmd,
                    // Empty: nothing more this iteration. Disconnected (all
                    // senders dropped) is the last-handle teardown signal owned
                    // by Phase 5; stage (a) here just stops draining.
                    Err(_) => break,
                },
            };
            budget -= 1;
            match cmd {
                DriverCommand::RecvResume { id } => self.enqueue_resume(id),
                DriverCommand::AcceptBidiResume => {
                    self.accept_bidi_resume.store(true, Ordering::Relaxed);
                }
                DriverCommand::AcceptUniResume => {
                    self.accept_uni_resume.store(true, Ordering::Relaxed);
                }
                DriverCommand::Send {
                    id,
                    buf,
                    done,
                    permit,
                } => {
                    self.enqueue_send_op(id, SendOp::Write { buf, done, permit });
                }
                DriverCommand::Finish { id, done } => {
                    self.enqueue_send_op(id, SendOp::Finish { done });
                }
                DriverCommand::Reset { id, code } => self.apply_reset(id, code),
                DriverCommand::StopSending { id, code } => {
                    let _ = qconn.stream_shutdown(id, Shutdown::Read, code);
                    self.mark_recv_done(id);
                }
                DriverCommand::Close { code, reason }
                    if self.pending_close.is_none() && !self.explicit_close_attempted =>
                {
                    // First-close-wins (§5.2, invariant 10): stage only the first
                    // effective one; a later Close (or one after an attempt) is an
                    // idempotent no-op. The barrier applies it after the bounded
                    // write stage, bypassing WRITE_BUDGET.
                    self.pending_close = Some(PendingClose { code, reason });
                }
                DriverCommand::ConnectionDropped => {
                    // `Connection::drop`, sent before the accept receivers close:
                    // clean parked/pending peer streams (direction-aware shutdown,
                    // drop bookkeeping) since they can no longer be handed over
                    // (§5.2, finding 4).
                    self.clean_undelivered_peer_streams(qconn);
                }
                DriverCommand::OpenBidi { reply } => {
                    // Queued for the open-materialization stage (§6.1): id
                    // allocation is worker-owned, deferred until peer flow
                    // control permits it.
                    self.open_bidi.push_back(reply);
                }
                DriverCommand::OpenUni { reply } => {
                    self.open_uni.push_back(reply);
                }
                // A non-first `Close` (guard above failed) is an idempotent
                // no-op.
                DriverCommand::Close { .. } => {}
            }
        }
        if !self.inbox.is_empty() {
            // Budget-deferred commands remain in receipt order (§5.2).
            self.needs_iteration = true;
        }
    }

    /// `ConnectionDropped` cleanup (§5.2, finding 4): the front-end `Connection`
    /// is gone and the accept receivers are about to close, so no parked or
    /// pending peer stream can ever be handed over. Direction-aware `shutdown`
    /// each (peer bidi: both; peer uni: read-only) and drop all admission
    /// bookkeeping so nothing lingers.
    fn clean_undelivered_peer_streams<C: QuicConn>(&mut self, qconn: &mut C) {
        // Parked (accept-full) streams: their `admit` entry is `Parked`.
        let parked: Vec<u64> = self
            .parked_bidi
            .drain(..)
            .chain(self.parked_uni.drain(..))
            .collect();
        for id in parked {
            if let Some(AdmitState::Parked(_)) = self.admit.get(&id) {
                self.shutdown_peer_directions(qconn, id, is_bidi(id));
                self.admit.remove(&id);
            }
        }
        // Discovered-but-not-yet-admitted streams (both per-class queues).
        let pending: Vec<u64> = self
            .pending_admit_bidi
            .drain(..)
            .chain(self.pending_admit_uni.drain(..))
            .collect();
        for id in pending {
            if self.pending_admit.remove(&id).is_some() {
                self.shutdown_peer_directions(qconn, id, is_bidi(id));
            }
        }
    }

    /// Open-materialization stage (§6.1). For up to [`OPEN_BUDGET`] queued
    /// `OpenBidi`/`OpenUni` requests per direction: skip a cancelled poller
    /// (`reply.is_closed()`) burning no id; stop (leaving the request queued and
    /// forcing another iteration) when peer stream credit is exhausted; else
    /// allocate the next id by convention, materialize transport state with
    /// `stream_priority(id, 127, true)`, increment the counter **only after**
    /// success, build the halves retaining live registry state, and deliver them
    /// through the reply. A reply that fails *after* materialization (the poller
    /// cancelled in the window) triggers the §6.2 direction-aware cleanup so the
    /// stream credit is reclaimed.
    fn stage_open<C: QuicConn>(&mut self, qconn: &mut C) {
        self.stage_open_bidi(qconn);
        self.stage_open_uni(qconn);
    }

    fn stage_open_bidi<C: QuicConn>(&mut self, qconn: &mut C) {
        let mut budget = OPEN_BUDGET;
        while budget > 0 {
            let reply = match self.open_bidi.pop_front() {
                Some(reply) => reply,
                None => break,
            };
            budget -= 1;
            // (1) Cancelled before materialization: no id burned.
            if reply.is_closed() {
                continue;
            }
            // (2) No peer credit: leave queued and STOP, WITHOUT setting
            // needs_iteration — blocking on stream credit must not hot-spin
            // (§5.2 progress bound). The peer's MAX_STREAMS is always a packet,
            // which drives the retry via the next process_writes iteration. Use
            // `return` (not `break`) so the budget-backlog check below is skipped
            // on the credit-blocked path.
            if qconn.peer_streams_left_bidi() == 0 {
                self.open_bidi.push_front(reply);
                return;
            }
            // (3) Allocate the next bidi id by convention.
            let id = self.next_bidi_id;
            // (4) Materialize transport state deterministically (§6.1 spike Q1).
            let cmd_tx = match self.cmd_tx_weak.upgrade() {
                Some(cmd_tx) => cmd_tx,
                None => {
                    // Teardown underway: resolve locally with the terminal.
                    let _ = reply.send(Err(self.open_terminal()));
                    continue;
                }
            };
            if qconn.stream_priority(id, 127, true).is_err() {
                // Credit was reported available, so this is unexpected: resolve
                // locally without burning the id (counter not advanced).
                let _ = reply.send(Err(self.open_terminal()));
                continue;
            }
            // (5) Advance the counter only after materialization succeeded.
            self.next_bidi_id = id.wrapping_add(4);
            // (6) Build both halves, retaining live registry state.
            let (recv_state, recv_handoff, _recv_done) = self.build_recv(id, cmd_tx.clone(), None);
            let (send_handoff, send_state, _send_done) =
                build_send(id, cmd_tx, Arc::clone(&self.shared.send_accounting), None);
            if let Some(state) = recv_state {
                self.recv.insert(id, state);
            }
            if let Some(state) = send_state {
                self.send.insert(id, state);
            }
            let handoff = BidiHandoff {
                send: send_handoff,
                recv: recv_handoff,
            };
            if let Err(undelivered) = reply.send(Ok(handoff)) {
                // Cancelled after materialization (§6.2): reclaim both directions.
                drop(undelivered);
                self.cleanup_undeliverable_open(qconn, id, true);
            }
        }
        // Budget exhausted with eligible requests still queued (not blocked on
        // credit, which returns early): force another iteration so the backlog
        // drains instead of waiting for an unrelated event.
        if !self.open_bidi.is_empty() {
            self.needs_iteration = true;
        }
    }

    fn stage_open_uni<C: QuicConn>(&mut self, qconn: &mut C) {
        let mut budget = OPEN_BUDGET;
        while budget > 0 {
            let reply = match self.open_uni.pop_front() {
                Some(reply) => reply,
                None => break,
            };
            budget -= 1;
            if reply.is_closed() {
                continue;
            }
            // No peer credit: leave queued and STOP without hot-spinning; the
            // peer's MAX_STREAMS packet drives the retry (§5.2, §6.1).
            if qconn.peer_streams_left_uni() == 0 {
                self.open_uni.push_front(reply);
                return;
            }
            let id = self.next_uni_id;
            let cmd_tx = match self.cmd_tx_weak.upgrade() {
                Some(cmd_tx) => cmd_tx,
                None => {
                    let _ = reply.send(Err(self.open_terminal()));
                    continue;
                }
            };
            if qconn.stream_priority(id, 127, true).is_err() {
                let _ = reply.send(Err(self.open_terminal()));
                continue;
            }
            self.next_uni_id = id.wrapping_add(4);
            let (send_handoff, send_state, _send_done) =
                build_send(id, cmd_tx, Arc::clone(&self.shared.send_accounting), None);
            if let Some(state) = send_state {
                self.send.insert(id, state);
            }
            if let Err(undelivered) = reply.send(Ok(send_handoff)) {
                // Cancelled after materialization (§6.2): a locally-initiated uni
                // stream is send-only, so only `Shutdown::Write` is valid.
                drop(undelivered);
                self.cleanup_undeliverable_open(qconn, id, false);
            }
        }
        if !self.open_uni.is_empty() {
            self.needs_iteration = true;
        }
    }

    /// The terminal handed to an open reply that the worker declines to
    /// materialize (teardown / unexpected `stream_priority` failure): the
    /// published connection terminal if present, else an adapter-bug `Internal`
    /// (never a bare cancel, §5.2 M3).
    fn open_terminal(&self) -> Arc<ConnTerminal> {
        self.shared
            .conn_terminal
            .get()
            .unwrap_or_else(|| Arc::new(ConnTerminal::Internal("open declined without a terminal")))
    }

    /// Reclaim the transport credit of a just-materialized stream whose open
    /// reply became undeliverable (the poller cancelled in the window between
    /// `reply.is_closed()` and `reply.send`, §6.2). Direction-aware: a **bidi**
    /// open shuts down both directions; a **uni** (send-only, locally initiated)
    /// open shuts down only `Shutdown::Write` — `Shutdown::Read` on it returns
    /// `InvalidStreamState`. Drops any retained registry state first so no stale
    /// half lingers. Idempotent (safe with no registry entry).
    fn cleanup_undeliverable_open<C: QuicConn>(&mut self, qconn: &mut C, id: u64, bidi: bool) {
        self.send.remove(&id);
        let _ = qconn.stream_shutdown(id, Shutdown::Write, H3_REQUEST_CANCELLED);
        if bidi {
            self.recv.remove(&id);
            let _ = qconn.stream_shutdown(id, Shutdown::Read, H3_REQUEST_CANCELLED);
        }
    }

    /// Reclaim a retained send entry once its direction is terminal and the recv
    /// half is no longer live (§6.2, invariant 8). Called from the drop-driven
    /// cleanup edges where no further `Send`/`Finish` can arrive: a serviced
    /// graceful **FIN** (`service_finish_turn`, where the front-end half is
    /// `finalized`) and a **recv-half terminal** (`mark_recv_done`). It is
    /// deliberately NOT called from the local-**reset** edge (a worker-side
    /// duplicate `Reset` must still resolve idempotently against the sticky
    /// terminal) nor from the generic peer-driven send terminal (§5.3a, where a
    /// still-live send handle may enqueue a late op). The `finished`/`recv`
    /// guard keeps a bidi entry retained while its recv half is still live.
    fn reclaim_finished_send(&mut self, id: u64) {
        let finished = self.send.get(&id).map(|s| s.finished).unwrap_or(false);
        if finished && !self.recv.contains_key(&id) {
            self.send.remove(&id);
            self.admit.remove(&id);
        }
    }

    /// Append a `Write`/`Finish` op to a stream's send queue (§5.3a stage (a)).
    /// If the send half is already terminal, complete the op **immediately once**
    /// with the sticky terminal instead of enqueueing (never a bare cancel,
    /// never a fabricated `Ok`). Otherwise queue it and make the id runnable.
    fn enqueue_send_op(&mut self, id: u64, op: SendOp<B>) {
        let state = self.send.entry(id).or_insert_with(StreamSendState::new);
        if let Some(end) = state.terminal.clone() {
            op.complete(Err(end));
            return;
        }
        state.send_ops.push_back(op);
        self.mark_send_runnable(id);
    }

    /// Apply the first effective `Reset` for a stream (§5.3a preemption): install
    /// `pending_reset`, publish the sticky `SendEnd::Reset` (first-writer-wins),
    /// drain every not-yet-accepted `Write`/`Finish` once with the effective
    /// terminal, and make the id runnable so stage (e) emits `RESET_STREAM`
    /// before any generic terminal/stale eviction (invariant 11). A later reset
    /// is an idempotent no-op (does not replace the first effective reset).
    fn apply_reset(&mut self, id: u64, code: u64) {
        let state = self.send.entry(id).or_insert_with(StreamSendState::new);
        // Idempotent: once a reset is pending OR the send half is already
        // terminal (a prior reset serviced, or a peer STOP_SENDING/close), a
        // later reset is a no-op — it must never schedule a second RESET_STREAM
        // (§5.3a first-effective reset).
        if state.pending_reset.is_some() || state.terminal.is_some() {
            return;
        }
        state.pending_reset = Some(code);
        let end = SendEnd::Reset { error_code: code };
        // First-writer-wins: a prior peer STOP_SENDING keeps its `Stopped`.
        state.status.set(end.clone());
        if state.terminal.is_none() {
            state.terminal = Some(end);
        }
        let terminal = state.terminal.clone().expect("terminal just set");
        let ops: Vec<SendOp<B>> = state.send_ops.drain(..).collect();
        for op in ops {
            op.complete(Err(terminal.clone()));
        }
        self.mark_send_runnable(id);
    }

    /// The send-terminal transition (§5.3a, invariant 13): publish `end` to the
    /// sticky `terminal` + shared `status` cell (first-writer-wins), drain
    /// **every** not-yet-completed `send_ops` entry exactly once with `end`,
    /// then mark the send half done and release runnable membership. Reusable by
    /// stage (d) discovery, stage (e) `StreamStopped`, and (Phase 5) close.
    fn send_terminal_transition(&mut self, id: u64, end: SendEnd) {
        let ops: Vec<SendOp<B>> = match self.send.get_mut(&id) {
            Some(state) => {
                state.status.set(end.clone());
                if state.terminal.is_none() {
                    state.terminal = Some(end.clone());
                }
                state.send_ops.drain(..).collect()
            }
            None => return,
        };
        let terminal = self
            .send
            .get(&id)
            .and_then(|s| s.terminal.clone())
            .unwrap_or(end);
        for op in ops {
            op.complete(Err(terminal.clone()));
        }
        self.mark_send_done(id);
    }

    /// Mark the send direction done (§5.3a): flag the entry `finished`, release
    /// runnable membership, set `send_done`, and run the contract-A terminal
    /// transition. The `send` registry entry is **retained** (with its sticky
    /// `terminal`) so an op that arrives after the terminal still completes
    /// immediately once; the drop-driven cleanup (§6.2) removes it once both
    /// directions are terminal and the front-end halves are gone.
    fn mark_send_done(&mut self, id: u64) {
        if let Some(state) = self.send.get_mut(&id) {
            state.finished = true;
        }
        self.drop_send_membership(id);
        if let Some(AdmitState::Registered { send_done, .. }) = self.admit.get_mut(&id) {
            *send_done = true;
        }
        self.terminal_transition(id);
    }

    /// Stage (e): the round-robin runnable-send drain (§5.3a, invariant 12).
    /// Pop at most [`WRITE_BUDGET`] ids from `runnable_send`; each pop (including
    /// a stale/terminal id removed lazily) consumes one turn. A still-runnable id
    /// returns at the tail, so a continuously-writable bulk stream cannot starve
    /// later work. `needs_iteration` is set while runnable work remains.
    fn stage_send<C: QuicConn>(&mut self, qconn: &mut C) {
        let mut turns = WRITE_BUDGET;
        while turns > 0 {
            let id = match self.runnable_send.pop_front() {
                Some(id) => id,
                None => break,
            };
            self.runnable_send_set.remove(&id);
            turns -= 1;
            match self.service_send_turn(qconn, id) {
                TurnOutcome::Requeue => {
                    self.mark_send_runnable(id);
                    self.needs_iteration = true;
                }
                TurnOutcome::Park | TurnOutcome::Drop => {}
            }
        }
        if !self.runnable_send.is_empty() {
            self.needs_iteration = true;
        }
    }

    /// Service one round-robin turn for `id` (§5.3a). Order is mandatory:
    /// `pending_reset` is checked **before** any generic terminal/stale eviction
    /// (invariant 11); then the head `send_op` gets at most one bounded transport
    /// call. Returns whether the id should return to the round-robin tail.
    fn service_send_turn<C: QuicConn>(&mut self, qconn: &mut C, id: u64) -> TurnOutcome {
        // 1. pending_reset FIRST (before terminal/stale eviction, invariant 11).
        let pending_reset = self.send.get(&id).and_then(|s| s.pending_reset);
        if let Some(code) = pending_reset {
            if let Some(state) = self.send.get_mut(&id) {
                state.pending_reset = None;
            }
            // Emit RESET_STREAM even at zero send capacity (§14 Q3 assumption).
            let _ = qconn.stream_shutdown(id, Shutdown::Write, code);
            self.mark_send_done(id);
            // Note: the retained `self.send` entry is deliberately NOT reclaimed
            // here — a still-live handle may issue a late `Send` (or a duplicate
            // `Reset`) that must resolve idempotently against the sticky terminal
            // (§5.3a). Reclamation happens once the recv half is also terminal
            // (`mark_recv_done`) or via a graceful drop-`Finish`.
            return TurnOutcome::Drop;
        }
        // 2. Stale/terminal id: nothing to send. Evicted (already popped).
        let has_ops = match self.send.get(&id) {
            Some(state) => state.terminal.is_none() && !state.send_ops.is_empty(),
            None => false,
        };
        if !has_ops {
            return TurnOutcome::Drop;
        }
        // 3. Service the head op with exactly one transport call.
        let is_write = matches!(
            self.send.get(&id).and_then(|s| s.send_ops.front()),
            Some(SendOp::Write { .. })
        );
        if is_write {
            self.service_write_turn(qconn, id)
        } else {
            self.service_finish_turn(qconn, id)
        }
    }

    /// One `Write` turn: a single bounded `stream_send(id, chunk, fin=false)`
    /// honoring partial writes (§5.3a). Full acceptance pops the op and completes
    /// `Ok(())`; a capacity-exhausted partial / `Done` re-arms the low-water mark
    /// (§5.3) and parks; `StreamStopped` runs the send-terminal transition.
    fn service_write_turn<C: QuicConn>(&mut self, qconn: &mut C, id: u64) -> TurnOutcome {
        let mut offered = 0usize;
        let result = {
            let state = self.send.get_mut(&id).expect("send state present");
            match state.send_ops.front_mut() {
                Some(SendOp::Write { buf, .. }) => send_from_buf(buf, |chunk| {
                    let n = chunk.len().min(MAX_WRITE_CHUNK);
                    offered = n;
                    qconn.stream_send(id, &chunk[..n], false)
                }),
                _ => return TurnOutcome::Drop,
            }
        };
        match result {
            Ok(written) => {
                let has_remaining = self
                    .send
                    .get(&id)
                    .and_then(|s| s.send_ops.front())
                    .map(|op| match op {
                        SendOp::Write { buf, .. } => buf.has_remaining(),
                        SendOp::Finish { .. } => false,
                    })
                    .unwrap_or(false);
                if !has_remaining {
                    // Whole buffer accepted: pop + complete Ok exactly once.
                    if let Some(state) = self.send.get_mut(&id) {
                        if let Some(op) = state.send_ops.pop_front() {
                            op.complete(Ok(()));
                        }
                    }
                    // Still runnable if more ops remain behind it.
                    return self.runnable_after_pop(id);
                }
                if written == offered {
                    // Made full progress on the offered chunk; more buffer (our
                    // own chunking / next segment) with capacity likely present.
                    TurnOutcome::Requeue
                } else {
                    // Capacity exhausted mid-write: re-arm low-water and park.
                    self.rearm_send(qconn, id)
                }
            }
            Err(err) => self.classify_send_err(qconn, id, &err),
        }
    }

    /// One `Finish` turn: `stream_send(id, &[], fin=true)` (§5.3a). Accepted even
    /// at zero send capacity (§14 Q5); on acceptance pop + complete `Ok(())`.
    fn service_finish_turn<C: QuicConn>(&mut self, qconn: &mut C, id: u64) -> TurnOutcome {
        match qconn.stream_send(id, &[], true) {
            Ok(_) => {
                if let Some(state) = self.send.get_mut(&id) {
                    if let Some(op) = state.send_ops.pop_front() {
                        op.complete(Ok(()));
                    }
                }
                // A sent FIN closes the send direction: mark it done so contract
                // A reclaims an admitted bidi once its recv half also ends.
                self.mark_send_done(id);
                // Reclaim the retained send entry now IF the recv half is already
                // gone (the normal server order: request read to FIN → recv
                // reclaimed, then response + FIN). The send half is `finalized`
                // after a graceful FIN, so no late Send/Finish can legitimately
                // arrive; the guard keeps it retained while a bidi recv half is
                // still live (client order → reclaimed at the recv-done edge).
                // Without this, one send entry leaks per handled request (§6.2
                // invariant 8).
                self.reclaim_finished_send(id);
                TurnOutcome::Drop
            }
            Err(err) => self.classify_send_err(qconn, id, &err),
        }
    }

    /// Classify a `stream_send` error into a turn outcome (§8.3): `StreamStopped`
    /// drains all ops via the send-terminal transition; `Done` (blocked) re-arms
    /// and parks; a connection-gone / bug resolves via a sticky terminal so the
    /// op never spins or leaks a bare cancel.
    fn classify_send_err<C: QuicConn>(
        &mut self,
        qconn: &mut C,
        id: u64,
        err: &quiche::Error,
    ) -> TurnOutcome {
        match classify_stream_send_error(err) {
            StreamSendClass::Stopped(code) => {
                self.send_terminal_transition(id, SendEnd::Stopped { error_code: code });
                TurnOutcome::Drop
            }
            StreamSendClass::Blocked => self.rearm_send(qconn, id),
            StreamSendClass::ConnGone => {
                match self.shared.conn_terminal.get() {
                    Some(t) => self.send_terminal_transition(id, SendEnd::Conn(t)),
                    // Closing window before on_conn_close classified the terminal:
                    // leave the send_ops in place (do NOT pin a fabricated
                    // `Internal` via the first-writer-wins `status` cell).
                    // on_conn_close drains them with the final terminal (§5.2).
                    None => self.drop_send_membership(id),
                }
                TurnOutcome::Drop
            }
            StreamSendClass::Limit | StreamSendClass::Bug(_) => {
                let end = SendEnd::Conn(Arc::new(ConnTerminal::Internal(
                    "unexpected stream_send error",
                )));
                self.send_terminal_transition(id, end);
                TurnOutcome::Drop
            }
        }
    }

    /// Low-water re-arm a blocked send half (§5.3): set a SMALL progress
    /// threshold so any capacity gain re-surfaces the id via stage (d), and park
    /// it (do not requeue — no spin). A `StreamStopped` observed here runs the
    /// send-terminal transition instead.
    fn rearm_send<C: QuicConn>(&mut self, qconn: &mut C, id: u64) -> TurnOutcome {
        match qconn.stream_writable(id, REARM_THRESHOLD) {
            Err(quiche::Error::StreamStopped(code)) => {
                self.send_terminal_transition(id, SendEnd::Stopped { error_code: code });
                TurnOutcome::Drop
            }
            _ => TurnOutcome::Park,
        }
    }

    /// After popping a completed op: the id stays runnable iff more ops remain
    /// and no terminal was published (§5.3a round-robin tail).
    fn runnable_after_pop(&mut self, id: u64) -> TurnOutcome {
        let more = self
            .send
            .get(&id)
            .map(|s| s.terminal.is_none() && !s.send_ops.is_empty())
            .unwrap_or(false);
        if more {
            TurnOutcome::Requeue
        } else {
            TurnOutcome::Drop
        }
    }

    /// Apply queued control commands (§5.2). Route a `RecvResume` id onto the
    /// resume cursor with exact-once membership.
    fn enqueue_resume(&mut self, id: u64) {
        if self.resume_set.insert(id) {
            self.pending_resume.push_back(id);
        }
    }

    /// Classify the connection terminal at worker exit by the §8.3 PRECEDENCE:
    /// `Internal` (our own recorded bug) → `peer_error` → `Timeout` → recorded
    /// explicit local close (or successful last-handle `H3_NO_ERROR`) →
    /// `local_error`. A last-handle `H3_NO_ERROR` and an explicit local `Close`
    /// are both carried in `local_close`; both outrank `local_error()` and are
    /// only reachable when no peer terminal/timeout preempts them.
    fn classify_conn_terminal<C: QuicConn>(&self, qconn: &C) -> ConnTerminal {
        if let Some(msg) = self.close_bug {
            return ConnTerminal::Internal(msg);
        }
        if let Some(pe) = qconn.peer_error() {
            return conn_terminal_from_error(CloseOrigin::Peer, pe);
        }
        if qconn.is_timed_out() {
            return ConnTerminal::Timeout;
        }
        if let Some(lc) = &self.local_close {
            return ConnTerminal::AppClose {
                origin: CloseOrigin::Local,
                error_code: lc.code,
                reason: lc.reason.clone(),
            };
        }
        if let Some(le) = qconn.local_error() {
            return conn_terminal_from_error(CloseOrigin::Local, le);
        }
        // No recorded cause after establishment is an adapter contract break.
        ConnTerminal::Internal("connection closed without a recorded terminal")
    }

    /// The single finite-cut teardown funnel (§9, §8.3, invariant 14), generic
    /// over [`QuicConn`] for mock testing. It (1) classifies the connection
    /// terminal once, (2) publishes it into `shared.conn_terminal`, both
    /// accept-terminal cells, and every live recv/send cell, (3) closes command
    /// ingress and drains the finite remaining command set — completing every
    /// reply/completion channel with the terminal, never a bare oneshot cancel.
    /// It never calls `qconn.close` (§8.3).
    fn do_on_conn_close<C: QuicConn>(&mut self, qconn: &mut C) {
        let terminal = Arc::new(self.classify_conn_terminal(qconn));

        // (2) Publish the terminal to every out-of-band cell (first-writer-wins).
        self.shared.conn_terminal.set(Arc::clone(&terminal));
        self.accept_terminal_bidi.set(Arc::clone(&terminal));
        self.accept_terminal_uni.set(Arc::clone(&terminal));
        for state in self.recv.values() {
            state.terminal.set(RecvEnd::Conn(Arc::clone(&terminal)));
        }
        // Publish the send terminal into each live half AND drain every
        // registry-held `send_ops` remainder so a flushing Send/Finish does not
        // leak a bare cancel (§5.3a).
        let mut pending_ops: Vec<SendOp<B>> = Vec::new();
        for state in self.send.values_mut() {
            let end = SendEnd::Conn(Arc::clone(&terminal));
            state.status.set(end.clone());
            if state.terminal.is_none() {
                state.terminal = Some(end);
            }
            pending_ops.extend(state.send_ops.drain(..));
        }
        for op in pending_ops {
            op.complete(Err(SendEnd::Conn(Arc::clone(&terminal))));
        }

        // (3) Close command ingress, then drain the now-finite command set:
        // `self.inbox` first, then `cmd_rx.try_recv()` until empty (§5.2, M3).
        self.cmd_rx.close();
        loop {
            let cmd = match self.inbox.pop_front() {
                Some(cmd) => cmd,
                None => match self.cmd_rx.try_recv() {
                    Ok(cmd) => cmd,
                    Err(_) => break,
                },
            };
            self.complete_command_on_close(cmd, &terminal);
        }
        // Also resolve open requests already moved into the staging queues by
        // stage (a) — their reply senders are not in cmd_rx (§5.2 finite cut,
        // §6.1). Every pending opener is woken with the classified terminal
        // rather than only later via sender-drop cancellation.
        for reply in self.open_bidi.drain(..) {
            let _ = reply.send(Err(Arc::clone(&terminal)));
        }
        for reply in self.open_uni.drain(..) {
            let _ = reply.send(Err(Arc::clone(&terminal)));
        }
    }

    /// Resolve one drained command against the published connection terminal
    /// (§5.2). A command owning a reply/completion channel is completed with the
    /// terminal; a reply-free lifecycle command is dropped because the stream's
    /// own cells already carry it. Never a bare oneshot cancel.
    fn complete_command_on_close(&self, cmd: DriverCommand<B>, terminal: &Arc<ConnTerminal>) {
        match cmd {
            // A `Send` still queued at close resolves its generation exactly once
            // through its reusable completer (set-if-current-generation); a
            // `Finish` resolves its per-stream oneshot. Never a bare cancel.
            DriverCommand::Send { done, .. } => {
                done.complete(Err(SendEnd::Conn(Arc::clone(terminal))));
            }
            DriverCommand::Finish { done, .. } => {
                let _ = done.send(Err(SendEnd::Conn(Arc::clone(terminal))));
            }
            DriverCommand::OpenBidi { reply } => {
                let _ = reply.send(Err(Arc::clone(terminal)));
            }
            DriverCommand::OpenUni { reply } => {
                let _ = reply.send(Err(Arc::clone(terminal)));
            }
            // Reply-free lifecycle commands: the stream/connection cells already
            // carry the terminal, so these are dropped.
            DriverCommand::Reset { .. }
            | DriverCommand::StopSending { .. }
            | DriverCommand::RecvResume { .. }
            | DriverCommand::AcceptBidiResume
            | DriverCommand::AcceptUniResume
            | DriverCommand::ConnectionDropped
            | DriverCommand::Close { .. } => {}
        }
    }
}

/// The disposition of one stage-(e) round-robin turn (§5.3a).
enum TurnOutcome {
    /// Still immediately runnable: return to the round-robin tail.
    Requeue,
    /// Blocked on send capacity: re-armed via low-water, do not requeue (no spin).
    Park,
    /// Terminal / stale / reset-serviced: release the turn, do not requeue.
    Drop,
}

/// Build a send handoff and, for a still-live send half, the worker-retained
/// [`StreamSendState`] that **shares** the handoff's sticky `status` cell (§5.1
/// atomic transfer, §5.4 invariant 7). When the send half is already terminal
/// (a retained `SendEnd`), the cell is pre-set and no registry state is created.
/// Free function: it touches no worker state.
fn build_send<B: Buf>(
    id: u64,
    cmd_tx: mpsc::UnboundedSender<DriverCommand<B>>,
    send_accounting: Arc<SendAccounting>,
    retained: Option<SendEnd>,
) -> (SendHandoff<B>, Option<StreamSendState<B>>, bool) {
    let status = TerminalCell::new();
    let send_done = retained.is_some();
    if let Some(ref end) = retained {
        status.set(end.clone());
    }
    let handoff = SendHandoff {
        id,
        status: status.clone(),
        cmd_tx: cmd_tx.clone(),
        send_accounting,
        cleanup: HandoffCleanup::new(id, false, cmd_tx),
    };
    let state = if send_done {
        None
    } else {
        Some(StreamSendState {
            send_ops: VecDeque::new(),
            pending_reset: None,
            terminal: None,
            status,
            finished: false,
        })
    };
    (handoff, state, send_done)
}

impl<B: Buf + Send + 'static> ApplicationOverQuic for QuicheDriver<B> {
    fn on_conn_established(
        &mut self,
        _qconn: &mut QuicheConnection,
        _handshake_info: &HandshakeInfo,
    ) -> QuicResult<()> {
        self.acting = true;
        if let Some(tx) = self.established.take() {
            // Ignore send error: the front end may have stopped waiting.
            let _ = tx.send(Ok(()));
        }
        Ok(())
    }

    fn should_act(&self) -> bool {
        // Must stay false during the handshake so tokio-quiche drives its own
        // handshake callbacks (§5).
        self.acting
    }

    fn buffer(&mut self) -> &mut [u8] {
        // The outbound packet buffer (PKT_BUF_LEN), NOT the MAX_CHUNK recv buffer.
        &mut self.pkt_buf
    }

    // The explicit `impl Future + Send` return type (rather than `async fn`)
    // documents the `Send` bound the `ApplicationOverQuic` trait requires.
    #[allow(clippy::manual_async_fn)]
    fn wait_for_data(
        &mut self,
        _qconn: &mut QuicheConnection,
    ) -> impl Future<Output = QuicResult<()>> + Send {
        async move {
            match self.wait_decision() {
                WaitDecision::Pending => std::future::pending::<QuicResult<()>>().await,
                WaitDecision::Yield => {
                    // Fairness; the worker's biased select keeps timer priority.
                    tokio::task::yield_now().await;
                    Ok(())
                }
                WaitDecision::Recv => match self.cmd_rx.recv().await {
                    Some(cmd) => {
                        self.inbox.push_back(cmd);
                        Ok(())
                    }
                    None => {
                        // Channel EOF: one-shot last-handle teardown wake.
                        self.last_handle_teardown = true;
                        Ok(())
                    }
                },
            }
        }
    }

    fn process_reads(&mut self, qconn: &mut QuicheConnection) -> QuicResult<()> {
        self.do_process_reads(qconn);
        Ok(())
    }

    fn process_writes(&mut self, qconn: &mut QuicheConnection) -> QuicResult<()> {
        self.do_process_writes(qconn)
    }

    fn on_conn_close<M: tokio_quiche::metrics::Metrics>(
        &mut self,
        qconn: &mut QuicheConnection,
        _metrics: &M,
        _connection_result: &QuicResult<()>,
    ) {
        self.do_on_conn_close(qconn);
    }
}

impl<B: Buf> Drop for QuicheDriver<B> {
    fn drop(&mut self) {
        // A pre-handshake worker exit never reaches on_conn_close (should_act is
        // false), so resolve any in-flight `established` wait here (§7.1, §8.4).
        if let Some(tx) = self.established.take() {
            let _ = tx.send(Err(SetupFailure::PreHandshakeWorkerExit));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::WriteOutcome;

    fn driver() -> (QuicheDriver<Bytes>, DriverHandles<Bytes>) {
        // Default to the client convention (locally-opened bidi `0,4,8…`).
        QuicheDriver::<Bytes>::new(false, 4, 4)
    }

    #[test]
    fn wait_decision_idle_awaits_commands() {
        let (d, _h) = driver();
        assert_eq!(d.wait_decision(), WaitDecision::Recv);
    }

    #[test]
    fn wait_decision_yields_when_inbox_nonempty() {
        let (mut d, _h) = driver();
        d.inbox.push_back(DriverCommand::AcceptBidiResume);
        assert_eq!(d.wait_decision(), WaitDecision::Yield);
    }

    #[test]
    fn wait_decision_yields_when_needs_iteration() {
        let (mut d, _h) = driver();
        d.needs_iteration = true;
        assert_eq!(d.wait_decision(), WaitDecision::Yield);
    }

    #[test]
    fn wait_decision_pending_after_graceful_close() {
        let (mut d, _h) = driver();
        // Even with pending work, once closing we stay pending (no hot spin).
        d.needs_iteration = true;
        d.graceful_close_issued = true;
        assert_eq!(d.wait_decision(), WaitDecision::Pending);
    }

    #[test]
    fn should_act_false_until_established() {
        let (d, _h) = driver();
        assert!(!d.should_act());
    }

    #[test]
    fn buffer_is_packet_sized_not_chunk_sized() {
        let (mut d, _h) = driver();
        assert_eq!(d.buffer().len(), PKT_BUF_LEN);
        assert_ne!(PKT_BUF_LEN, MAX_CHUNK);
    }

    /// SF-4/SF-5 (SC-006): the default constructor preserves the historical
    /// buffer sizes exactly — recv channel depth == `BYTE_CHANNEL_DEPTH` and the
    /// packet buffer == `PKT_BUF_LEN`.
    #[test]
    fn sf4_sf5_default_buffer_sizes_unchanged() {
        let (mut d, _h) = QuicheDriver::<Bytes>::new(false, 4, 4);
        assert_eq!(d.recv_channel_depth, BYTE_CHANNEL_DEPTH);
        assert_eq!(d.buffer().len(), PKT_BUF_LEN);
    }

    /// SF-4/SF-5 (SC-006): `with_buffers` overrides take effect end-to-end — the
    /// packet buffer is sized to `packet_buffer_size`, and a freshly-built recv
    /// channel's max capacity equals `recv_channel_depth`.
    #[test]
    fn sf4_sf5_buffer_overrides_take_effect() {
        let (mut d, h) = QuicheDriver::<Bytes>::with_buffers(
            false,
            4,
            4,
            DriverBufferConfig {
                recv_channel_depth: 8,
                packet_buffer_size: 4096,
                max_buffered_send_bytes: None,
            },
        );
        assert_eq!(d.recv_channel_depth, 8);
        assert_eq!(d.buffer().len(), 4096);
        // The configured depth is applied to the per-stream byte channel.
        let cmd_tx = h.cmd_tx.clone();
        let (state, _handoff, _done) = d.build_recv(0, cmd_tx, None);
        let state = state.expect("live recv state");
        assert_eq!(state.bytes.max_capacity(), 8);
    }

    /// SF-4/SF-5: zero-valued overrides are clamped to at least 1 (no panic on
    /// channel/buffer construction).
    #[test]
    fn sf4_sf5_zero_sizes_clamped_to_one() {
        let (mut d, h) = QuicheDriver::<Bytes>::with_buffers(
            false,
            4,
            4,
            DriverBufferConfig {
                recv_channel_depth: 0,
                packet_buffer_size: 0,
                max_buffered_send_bytes: None,
            },
        );
        assert_eq!(d.buffer().len(), 1);
        let (state, _handoff, _done) = d.build_recv(0, h.cmd_tx.clone(), None);
        assert_eq!(state.expect("live recv state").bytes.max_capacity(), 1);
    }

    #[test]
    fn dropping_driver_before_handshake_reports_setup_failure() {
        let (d, mut h) = driver();
        drop(d);
        match h.established_rx.try_recv() {
            Ok(Err(SetupFailure::PreHandshakeWorkerExit)) => {}
            other => panic!("expected PreHandshakeWorkerExit, got {other:?}"),
        }
    }

    #[test]
    fn last_strong_sender_drop_closes_cmd_rx() {
        // finding 2 groundwork: the weak worker sender does not keep the channel
        // open, so dropping every strong front-end sender yields EOF.
        let (mut d, h) = driver();
        assert!(d.cmd_tx_weak.upgrade().is_some());
        drop(h); // drops the only strong cmd_tx
        assert!(d.cmd_tx_weak.upgrade().is_none());
        assert!(d.cmd_rx.try_recv().is_err());
    }

    // ===== Phase 3: read pump / discovery / admission (§11 matrix) =====

    use crate::conn::mock::{MockConn, RecvStep};

    fn data(bytes: &[u8], fin: bool) -> RecvStep {
        RecvStep::Data {
            bytes: bytes.to_vec(),
            fin,
        }
    }

    /// §11: buffered inbound bytes followed by FIN — all bytes delivered on the
    /// byte channel, then `RecvEnd::Fin` published (only after the last byte).
    #[test]
    fn buffered_bytes_then_fin_delivers_then_seals() {
        let (mut d, mut h) = driver();
        let mut c = MockConn::new();
        c.script_recv(0, [data(b"hello", true)]);
        c.queue_readable([0]);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);

        let mut ho = h.accept_bidi_rx.try_recv().expect("one bidi handoff");
        assert_eq!(ho.recv.id, 0);
        // The byte arrives before the terminal (sealing edge: Fin set after send).
        assert_eq!(
            ho.recv.bytes.try_recv().unwrap(),
            Bytes::from_static(b"hello")
        );
        assert!(matches!(ho.recv.terminal.get(), Some(RecvEnd::Fin)));
        // Nothing enqueued after the seal; no second admission.
        assert!(ho.recv.bytes.try_recv().is_err());
        assert!(h.accept_bidi_rx.try_recv().is_err());
    }

    /// SF-1 (SC-002): the delivered `Bytes` shares the reusable receive buffer's
    /// backing allocation — no per-chunk deep copy. The chunk is the front split
    /// of the buffer, so the retained `BytesMut` begins exactly `chunk.len()`
    /// bytes into the SAME allocation; a deep copy would place them in unrelated
    /// allocations, so this contiguity holds iff no copy occurred.
    #[test]
    fn sf1_delivered_chunk_shares_recv_buffer_backing() {
        let (mut d, mut h) = driver();
        let mut c = MockConn::new();
        // No FIN so the buffer retains a [len..] remainder to compare against.
        c.script_recv(0, [data(b"zerocopy-payload", false)]);
        c.queue_readable([0]);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);

        let mut ho = h.accept_bidi_rx.try_recv().expect("one bidi handoff");
        let chunk = ho.recv.bytes.try_recv().unwrap();
        assert_eq!(&chunk[..], b"zerocopy-payload");
        let buf = d
            .recv_buf
            .as_ref()
            .expect("recv buffer allocated on first read");
        assert_eq!(
            chunk.as_ptr() as usize + chunk.len(),
            buf.as_ptr() as usize,
            "delivered chunk must be contiguous with the retained buffer (shared backing)"
        );
    }

    /// SF-1 (SC-002): a multi-chunk drain produces distinct, byte-correct,
    /// non-aliasing payloads across the `split_to`/re-arm cycle — an earlier
    /// frozen chunk is never corrupted by a later read into the reused buffer.
    #[test]
    fn sf1_multi_chunk_drain_no_aliasing_corruption() {
        let (mut d, mut h) = driver();
        let mut c = MockConn::new();
        c.script_recv(0, [data(b"AAAA", false), data(b"BBBB", true)]);
        c.queue_readable([0]);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);

        let mut ho = h.accept_bidi_rx.try_recv().expect("one bidi handoff");
        let c1 = ho.recv.bytes.try_recv().unwrap();
        let c2 = ho.recv.bytes.try_recv().unwrap();
        // Both correct AND the first is intact after the second read reused the
        // buffer — proves the reuse cycle does not alias/overwrite live chunks.
        assert_eq!(&c1[..], b"AAAA");
        assert_eq!(&c2[..], b"BBBB");
        assert!(matches!(ho.recv.terminal.get(), Some(RecvEnd::Fin)));
    }

    /// SF-7 (SC-003): the per-stream sender lookup+clone is hoisted out of the
    /// chunk loop — draining K chunks in one `drain_stream` performs exactly one
    /// lookup/clone, not one per chunk.
    #[test]
    fn sf7_sender_lookup_hoisted_once_per_drain() {
        let (mut d, _h) = driver();
        let (tx, mut rx) = mpsc::channel::<Bytes>(BYTE_CHANNEL_DEPTH);
        d.recv.insert(
            0,
            StreamRecvState {
                bytes: tx,
                terminal: TerminalCell::new(),
                resume: Arc::new(AtomicBool::new(false)),
                blocked: Arc::new(AtomicBool::new(false)),
            },
        );
        let mut c = MockConn::new();
        c.script_recv(0, [data(b"a", false), data(b"b", false), data(b"c", false)]);
        d.read_budget = READ_BUDGET;
        d.drain_stream(&mut c, 0);

        // Three chunks pulled in one drain → exactly one hoisted lookup/clone.
        assert_eq!(d.recv_lookup_count, 1);
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"a"));
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"b"));
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"c"));
    }

    /// SF-5-scratch (SC-006): the receive buffer is lazily allocated — a driver
    /// that never receives data never allocates it; it materializes only on the
    /// first `stream_recv`.
    #[test]
    fn sf5_recv_buffer_lazily_allocated() {
        let (mut d, mut h) = driver();
        // Fresh driver, no receives yet → buffer not allocated.
        assert!(
            d.recv_buf.is_none(),
            "idle connection must not allocate the receive buffer"
        );

        let mut c = MockConn::new();
        c.script_recv(0, [data(b"x", true)]);
        c.queue_readable([0]);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);
        let _ho = h.accept_bidi_rx.try_recv().expect("one bidi handoff");

        // After the first receive it exists.
        assert!(
            d.recv_buf.is_some(),
            "receive buffer must materialize on first stream_recv"
        );
    }

    /// §11: queued bytes then `RESET_STREAM` — queued bytes delivered, then
    /// `RecvEnd::Reset`; no bytes enqueued after the seal.
    #[test]
    fn queued_bytes_then_reset_delivers_then_seals() {
        let (mut d, mut h) = driver();
        let mut c = MockConn::new();
        c.script_recv(
            0,
            [
                data(b"data", false),
                RecvStep::Err(crate::quiche::Error::StreamReset(7)),
            ],
        );
        c.queue_readable([0]);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);

        let mut ho = h.accept_bidi_rx.try_recv().expect("one bidi handoff");
        assert_eq!(
            ho.recv.bytes.try_recv().unwrap(),
            Bytes::from_static(b"data")
        );
        assert!(matches!(
            ho.recv.terminal.get(),
            Some(RecvEnd::Reset { error_code: 7 })
        ));
        assert!(ho.recv.bytes.try_recv().is_err());
    }

    /// §11: reserve-before-read — a full byte channel means `stream_recv` is
    /// NOT called (no bytes lost), the stream is marked `blocked`, then a
    /// `RecvResume` drains it.
    #[test]
    fn reserve_before_read_full_channel_then_resume() {
        let (mut d, _h) = driver();
        let (tx, mut rx) = mpsc::channel::<Bytes>(BYTE_CHANNEL_DEPTH);
        for _ in 0..BYTE_CHANNEL_DEPTH {
            tx.try_send(Bytes::from_static(b"x")).unwrap();
        }
        let terminal = TerminalCell::new();
        let resume = Arc::new(AtomicBool::new(false));
        d.recv.insert(
            0,
            StreamRecvState {
                bytes: tx,
                terminal: terminal.clone(),
                resume,
                blocked: Arc::new(AtomicBool::new(false)),
            },
        );
        d.admit.insert(
            0,
            AdmitState::Registered {
                send_done: false,
                recv_done: false,
            },
        );
        d.pending_readable.push_back(0);
        d.readable_set.insert(0);

        let mut c = MockConn::new();
        c.script_recv(0, [data(b"late", true)]);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);

        // Full channel: stream_recv never called, stream parked blocked.
        assert!(c.recv_calls.is_empty());
        assert!(d.recv.get(&0).unwrap().blocked.load(Ordering::Relaxed));

        // Free capacity, then RecvResume drains the pending read.
        for _ in 0..BYTE_CHANNEL_DEPTH {
            rx.try_recv().unwrap();
        }
        d.inbox.push_back(DriverCommand::RecvResume { id: 0 });
        d.apply_inbox(&mut c);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);

        assert_eq!(c.recv_calls, vec![0]);
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"late"));
        assert!(matches!(terminal.get(), Some(RecvEnd::Fin)));
    }

    /// SF-2 §5.1: the worker's capacity re-check under the lost-wakeup handshake
    /// must NOT park when a slot is already free. Here `blocked` is pre-set (as if
    /// a prior park), but the channel has room, so the drain proceeds and clears
    /// the park flag — never leaving the stream parked with a free slot.
    #[test]
    fn sf2_worker_recheck_does_not_park_with_free_slot() {
        let (mut d, _h) = driver();
        let (tx, mut rx) = mpsc::channel::<Bytes>(BYTE_CHANNEL_DEPTH);
        // Channel has capacity (empty). Pre-set blocked to model the race window
        // where the consumer's swap-clear has not yet been observed.
        let blocked = Arc::new(AtomicBool::new(true));
        let terminal = TerminalCell::new();
        d.recv.insert(
            0,
            StreamRecvState {
                bytes: tx,
                terminal: terminal.clone(),
                resume: Arc::new(AtomicBool::new(false)),
                blocked: Arc::clone(&blocked),
            },
        );
        d.admit.insert(
            0,
            AdmitState::Registered {
                send_done: false,
                recv_done: false,
            },
        );
        d.pending_readable.push_back(0);
        d.readable_set.insert(0);

        let mut c = MockConn::new();
        c.script_recv(0, [data(b"ok", true)]);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);

        // Slot was free → the read proceeded (bytes delivered), and the drain did
        // not leave the stream parked.
        assert_eq!(c.recv_calls, vec![0]);
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"ok"));
        assert!(matches!(terminal.get(), Some(RecvEnd::Fin)));
    }

    /// SF-2 §5.1: on a genuinely full channel the worker publishes `blocked=true`
    /// (Release) so a subsequent consumer free can observe it and emit a resume.
    /// This proves the park flag is set for the lost-wakeup handshake.
    #[test]
    fn sf2_worker_publishes_blocked_on_full_channel() {
        let (mut d, _h) = driver();
        let (tx, _rx) = mpsc::channel::<Bytes>(BYTE_CHANNEL_DEPTH);
        for _ in 0..BYTE_CHANNEL_DEPTH {
            tx.try_send(Bytes::from_static(b"x")).unwrap();
        }
        let blocked = Arc::new(AtomicBool::new(false));
        d.recv.insert(
            0,
            StreamRecvState {
                bytes: tx,
                terminal: TerminalCell::new(),
                resume: Arc::new(AtomicBool::new(false)),
                blocked: Arc::clone(&blocked),
            },
        );
        d.admit.insert(
            0,
            AdmitState::Registered {
                send_done: false,
                recv_done: false,
            },
        );
        d.pending_readable.push_back(0);
        d.readable_set.insert(0);

        let mut c = MockConn::new();
        c.script_recv(0, [data(b"late", true)]);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);

        // Full → no read, park flag published with Release for the consumer.
        assert!(c.recv_calls.is_empty());
        assert!(
            blocked.load(Ordering::Acquire),
            "park flag must be published"
        );
    }

    /// §11: destructive intake + admission — a new peer id is admitted exactly
    /// once; re-running the pump does not re-admit (membership).
    #[test]
    fn destructive_intake_admits_new_peer_once() {
        let (mut d, mut h) = driver();
        let mut c = MockConn::new();
        c.script_recv(0, [data(b"hi", false)]);
        c.queue_readable([0]);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);

        let mut ho = h.accept_bidi_rx.try_recv().expect("admitted once");
        assert_eq!(ho.recv.bytes.try_recv().unwrap(), Bytes::from_static(b"hi"));
        assert!(matches!(
            d.admit.get(&0),
            Some(AdmitState::Registered { .. })
        ));

        // Re-run: no rediscovery, so no second handoff.
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);
        assert!(h.accept_bidi_rx.try_recv().is_err());
    }

    /// §11: parked-stream single-admit — a full accept queue parks a peer stream
    /// exactly once; after `AcceptBidiResume` + capacity it is promoted once.
    #[test]
    fn parked_stream_single_admit_then_promote() {
        let (mut d, mut h) = QuicheDriver::<Bytes>::new(false, 1, 1);
        let mut c = MockConn::new();
        c.script_recv(0, [data(b"a", false)]);
        c.script_recv(4, [data(b"b", false)]);
        c.queue_readable([0, 4]);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);

        // Stream 0 fills the single accept slot; stream 4 is parked once.
        assert_eq!(d.parked_bidi.len(), 1);
        assert_eq!(*d.parked_bidi.front().unwrap(), 4);
        assert!(matches!(d.admit.get(&4), Some(AdmitState::Parked(_))));
        assert!(matches!(
            d.admit.get(&0),
            Some(AdmitState::Registered { .. })
        ));

        // Free capacity, signal AcceptBidiResume → promote 4 exactly once.
        let ho0 = h.accept_bidi_rx.try_recv().expect("stream 0 handoff");
        assert_eq!(ho0.recv.id, 0);
        d.inbox.push_back(DriverCommand::AcceptBidiResume);
        d.apply_inbox(&mut c);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);

        let ho4 = h.accept_bidi_rx.try_recv().expect("stream 4 promoted");
        assert_eq!(ho4.recv.id, 4);
        assert!(d.parked_bidi.is_empty());
        assert!(matches!(
            d.admit.get(&4),
            Some(AdmitState::Registered { .. })
        ));
        assert!(h.accept_bidi_rx.try_recv().is_err()); // no duplicate promotion
    }

    /// MF-1: a class that parks on a full accept channel must bound per-pump work
    /// to O(ADMIT_BUDGET) — the blocked-class backlog is left in place, never
    /// rescanned — and must NOT keep the worker runnable until its
    /// `Accept*Resume` arrives. FIFO order within the class is preserved across
    /// the parked queue + the pending backlog.
    #[test]
    fn mf1_parked_class_bounds_scan_and_quiesces_worker() {
        // Accept capacity 1: fill the single bidi slot, then leave it full.
        let (mut d, _h) = QuicheDriver::<Bytes>::new(false, 1, 1);
        let mut c = MockConn::new();
        d.pending_admit.insert(0, PeerStream::new(0));
        d.pending_admit_bidi.push_back(0);
        d.phase2_admission(&mut c);
        assert!(matches!(
            d.admit.get(&0),
            Some(AdmitState::Registered { .. })
        ));

        // Seed a large fresh backlog of the SAME class (bidi ids 4, 8, …).
        const N: u64 = 64;
        for k in 1..=N {
            let id = k * 4; // is_bidi(id) == true
            assert!(is_bidi(id));
            d.pending_admit.insert(id, PeerStream::new(id));
            d.pending_admit_bidi.push_back(id);
        }
        assert_eq!(d.pending_admit_bidi.len() as u64, N);

        // One admission pump: the first backlog id (4) attempts admission, hits
        // the full accept channel, and PARKS the class. Critically, only ONE id
        // is examined — the remaining backlog is NOT scanned (the old carry loop
        // drained/examined all N every pump → O(N²) across re-pumps).
        d.phase2_admission(&mut c);
        assert_eq!(
            d.phase2_pops, 1,
            "bounded scan: exactly one id examined, not the whole backlog (MF-1)"
        );
        assert_eq!(d.parked_bidi.len(), 1, "exactly one id parked");
        assert_eq!(
            *d.parked_bidi.front().unwrap(),
            4,
            "FIFO: id 4 parked first"
        );
        assert_eq!(
            d.pending_admit_bidi.len() as u64,
            N - 1,
            "blocked-class backlog left in place, not rescanned (MF-1)"
        );
        // FIFO preserved in the retained backlog (front is the next-oldest, 8).
        assert_eq!(*d.pending_admit_bidi.front().unwrap(), 8);

        // FR-002: an accept-blocked class must NOT self-reschedule the worker.
        assert!(
            !d.has_runnable_remainder(),
            "capacity-blocked class must not keep the worker runnable"
        );

        // A second pump likewise examines only O(1) ids (no unbounded self-scan),
        // and the class still does not report runnable → no re-pump spin.
        d.phase2_admission(&mut c);
        assert_eq!(
            d.phase2_pops, 1,
            "repeated pumps stay O(1) while the class is accept-blocked"
        );
        assert!(!d.has_runnable_remainder());

        // Only once the matching AcceptBidiResume arrives is the class runnable.
        d.accept_bidi_resume.store(true, Ordering::Relaxed);
        assert!(
            d.has_runnable_remainder(),
            "AcceptBidiResume re-arms the worker for the parked class"
        );
    }

    /// MF-1: after `AcceptBidiResume` + freed capacity, a parked class resumes in
    /// FIFO order (the earliest-parked id is admitted before later backlog ids).
    #[test]
    fn mf1_parked_class_resumes_in_fifo_order() {
        let (mut d, mut h) = QuicheDriver::<Bytes>::new(false, 1, 1);
        let mut c = MockConn::new();
        // Fill the single slot with id 0, then seed backlog 4, 8.
        for id in [0u64, 4, 8] {
            d.pending_admit.insert(id, PeerStream::new(id));
            d.pending_admit_bidi.push_back(id);
        }
        d.phase2_admission(&mut c);
        // id 0 admitted; id 4 parked; id 8 still pending.
        assert!(matches!(
            d.admit.get(&0),
            Some(AdmitState::Registered { .. })
        ));
        assert_eq!(d.parked_bidi.front().copied(), Some(4));
        assert_eq!(d.pending_admit_bidi.front().copied(), Some(8));

        // Free one slot (drain id 0), signal resume, pump promotion + admission.
        let ho0 = h.accept_bidi_rx.try_recv().expect("id 0 handoff");
        assert_eq!(ho0.recv.id, 0);
        d.accept_bidi_resume.store(true, Ordering::Relaxed);
        d.promote_parked(&mut c, true);
        // The FIFO-earliest parked id (4) is promoted before the newer id (8).
        let ho4 = h.accept_bidi_rx.try_recv().expect("id 4 promoted first");
        assert_eq!(ho4.recv.id, 4, "FIFO: earliest-parked id promoted first");
        assert!(matches!(
            d.admit.get(&4),
            Some(AdmitState::Registered { .. })
        ));
        // id 8 remains queued (slot re-filled by id 4) — still FIFO-next.
        assert_eq!(d.pending_admit_bidi.front().copied(), Some(8));
    }

    /// MF-1 / FR-003: a class blocked on its own accept capacity does not starve
    /// the other class — the unblocked class keeps admitting in the same pump.
    #[test]
    fn mf1_blocked_class_does_not_starve_other_class() {
        // bidi capacity 1 (fill it), uni capacity ample.
        let (mut d, _h) = QuicheDriver::<Bytes>::new(false, 1, 16);
        let mut c = MockConn::new();
        d.pending_admit.insert(0, PeerStream::new(0));
        d.pending_admit_bidi.push_back(0);
        d.phase2_admission(&mut c); // fills the single bidi slot
        assert!(matches!(
            d.admit.get(&0),
            Some(AdmitState::Registered { .. })
        ));

        // Flood bidi (all will be blocked) and enqueue one uni (id 2).
        for k in 1..=32u64 {
            let id = k * 4; // bidi
            d.pending_admit.insert(id, PeerStream::new(id));
            d.pending_admit_bidi.push_back(id);
        }
        d.pending_admit.insert(2, PeerStream::new(2)); // uni (is_bidi(2)==false)
        assert!(!is_bidi(2));
        d.pending_admit_uni.push_back(2);

        d.phase2_admission(&mut c);
        // The uni stream is admitted despite the bidi flood + bidi park.
        assert!(
            matches!(d.admit.get(&2), Some(AdmitState::Registered { .. })),
            "unblocked uni class admitted despite blocked bidi flood"
        );
        assert!(d.pending_admit_uni.is_empty());
        assert_eq!(d.parked_bidi.len(), 1, "bidi parked exactly once");
    }

    /// MF-1 / MF-A: a fresh, never-parked class with pending ids MUST report the
    /// worker runnable (the gate keys off `parked_*` non-emptiness, so it never
    /// deadlocks a class whose accept-resume bit is still the initial `false`).
    #[test]
    fn mf1_fresh_never_parked_class_reports_runnable() {
        let (mut d, _h) = driver();
        // No pending work at all → not runnable.
        assert!(!d.has_runnable_remainder());

        // A fresh bidi class with pending ids (parked_bidi empty, resume == false).
        d.pending_admit.insert(0, PeerStream::new(0));
        d.pending_admit_bidi.push_back(0);
        assert!(d.parked_bidi.is_empty());
        assert!(!d.accept_bidi_resume.load(Ordering::Relaxed));
        assert!(
            d.has_runnable_remainder(),
            "fresh never-parked class with pending ids must be runnable (MF-A)"
        );

        // Same for a fresh uni class.
        let (mut d2, _h2) = driver();
        d2.pending_admit.insert(2, PeerStream::new(2));
        d2.pending_admit_uni.push_back(2);
        assert!(d2.has_runnable_remainder());
    }

    /// MF-1 / FR-003: with both classes admissible and each holding a large
    /// backlog, a single pump makes fair progress on BOTH classes (round-robin),
    /// so neither is starved within `ADMIT_BUDGET`.
    #[test]
    fn mf1_cross_class_fair_servicing() {
        let (mut d, _h) = QuicheDriver::<Bytes>::new(false, 128, 128);
        let mut c = MockConn::new();
        for k in 0..40u64 {
            let bidi = k * 4; // bidi
            let uni = k * 4 + 2; // uni
            d.pending_admit.insert(bidi, PeerStream::new(bidi));
            d.pending_admit_bidi.push_back(bidi);
            d.pending_admit.insert(uni, PeerStream::new(uni));
            d.pending_admit_uni.push_back(uni);
        }
        d.phase2_admission(&mut c);
        // ADMIT_BUDGET (32) split evenly by round-robin → 16 admits per class.
        let admitted_bidi = 40 - d.pending_admit_bidi.len();
        let admitted_uni = 40 - d.pending_admit_uni.len();
        assert_eq!(admitted_bidi + admitted_uni, ADMIT_BUDGET);
        assert_eq!(admitted_bidi, ADMIT_BUDGET / 2, "bidi got a fair half");
        assert_eq!(admitted_uni, ADMIT_BUDGET / 2, "uni got a fair half");
    }

    /// §11 / §5.5 contract A: once a bidi stream's both directions are terminal,
    /// `admit[id]` (and the recv entry + cursor memberships) are dropped
    /// immediately. Per the §5.5 spike a collected id never reappears in
    /// discovery, so no tombstone is retained and nothing re-admits it.
    #[test]
    fn tombstone_contract_a_removes_bidi_at_both_terminal() {
        let (mut d, mut h) = driver();
        let mut c = MockConn::new();
        c.script_recv(0, [data(b"x", true)]);
        // Seed a peer bidi carrying a retained send terminal (as if a writable
        // STOP_SENDING was observed), so admission sets send_done = true; the
        // recv FIN then makes BOTH directions terminal.
        d.pending_admit.insert(
            0,
            PeerStream {
                id: 0,
                pending_send_terminal: Some(SendEnd::Stopped { error_code: 9 }),
                pending_recv_terminal: None,
            },
        );
        d.pending_admit_bidi.push_back(0);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);

        let mut ho = h.accept_bidi_rx.try_recv().expect("admitted");
        assert_eq!(ho.recv.bytes.try_recv().unwrap(), Bytes::from_static(b"x"));
        assert!(matches!(ho.recv.terminal.get(), Some(RecvEnd::Fin)));
        assert!(matches!(
            ho.send.status.get(),
            Some(SendEnd::Stopped { error_code: 9 })
        ));

        // Contract A: both terminal → admit[0] and recv[0] gone, memberships dropped.
        assert!(!d.admit.contains_key(&0));
        assert!(!d.recv.contains_key(&0));
        assert!(!d.pending_admit.contains_key(&0));
        assert!(!d.readable_set.contains(&0));

        // A subsequent pump (id not re-surfaced by quiche) does not re-admit it.
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);
        assert!(h.accept_bidi_rx.try_recv().is_err());
    }

    /// §5.5 / §14 Q4: a peer bidi discovered on the *writable* path with a
    /// pending `STOP_SENDING` is admitted with its send half already terminal
    /// (`send_done = true`, `status` cell = `SendEnd::Stopped`), so the retained
    /// send terminal is never lost at admission.
    #[test]
    fn writable_path_captures_peer_stop_sending() {
        let (mut d, mut h) = driver();
        let mut c = MockConn::new();
        // Peer bidi id 0 surfaces on the writable cursor; capacity probe reports
        // it stopped by the peer. Stage (d) captures it, then the read pump's
        // admission phase registers it on the following iteration.
        c.writable_next.push_back(0);
        c.capacity.insert(0, Err(quiche::Error::StreamStopped(66)));
        d.stage_writable(&mut c);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);

        let ho = h
            .accept_bidi_rx
            .try_recv()
            .expect("admitted via writable path");
        assert_eq!(ho.recv.id, 0);
        assert!(matches!(
            d.admit.get(&0),
            Some(AdmitState::Registered {
                send_done: true,
                ..
            })
        ));
        assert!(matches!(
            ho.send.status.get(),
            Some(SendEnd::Stopped { error_code: 66 })
        ));
        let _ = &mut h;
    }

    /// §11: READ_BUDGET boundary — a body larger than the shared budget drains
    /// only up to the budget, then requeues the id keeping its membership and
    /// sets `needs_iteration`.
    #[test]
    fn read_budget_boundary_requeues_with_membership() {
        let (mut d, _h) = driver();
        let (tx, mut rx) = mpsc::channel::<Bytes>(BYTE_CHANNEL_DEPTH);
        d.recv.insert(
            0,
            StreamRecvState {
                bytes: tx,
                terminal: TerminalCell::new(),
                resume: Arc::new(AtomicBool::new(false)),
                blocked: Arc::new(AtomicBool::new(false)),
            },
        );
        d.admit.insert(
            0,
            AdmitState::Registered {
                send_done: false,
                recv_done: false,
            },
        );
        d.pending_readable.push_back(0);
        d.readable_set.insert(0);

        let mut c = MockConn::new();
        c.script_recv(
            0,
            [
                data(b"a", false),
                data(b"b", false),
                data(b"c", false),
                data(b"d", false),
                data(b"e", false),
            ],
        );
        d.read_budget = 3;
        d.run_read_pump(&mut c);

        // Only 3 chunks drained; id requeued with membership; needs_iteration set.
        assert_eq!(c.recv_calls.len(), 3);
        assert!(d.needs_iteration);
        assert!(d.readable_set.contains(&0));
        assert_eq!(d.pending_readable.len(), 1);
        assert_eq!(*d.pending_readable.front().unwrap(), 0);
        for expected in [b"a", b"b", b"c"] {
            assert_eq!(rx.try_recv().unwrap(), Bytes::copy_from_slice(expected));
        }
        assert!(rx.try_recv().is_err());
    }

    // Regression: a deferral the read pump records in process_reads must survive
    // the following process_writes in the SAME packet iteration, so wait_for_data
    // forces another iteration instead of stranding the remainder (§5.1 finding
    // 5). A blanket `needs_iteration = false` in process_writes would break this.
    #[test]
    fn needs_iteration_survives_full_packet_iteration() {
        let (mut d, mut h) = driver();
        let (tx, _rx) = mpsc::channel(BYTE_CHANNEL_DEPTH);
        d.recv.insert(
            0,
            StreamRecvState {
                bytes: tx,
                terminal: TerminalCell::new(),
                resume: Arc::new(AtomicBool::new(false)),
                blocked: Arc::new(AtomicBool::new(false)),
            },
        );
        d.admit.insert(
            0,
            AdmitState::Registered {
                send_done: true,
                recv_done: false,
            },
        );
        d.pending_readable.push_back(0);
        d.readable_set.insert(0);

        let mut c = MockConn::new();
        c.script_recv(0, (0..40).map(|i| data(&[b'a' + (i % 26) as u8], false)));

        // A packet iteration: process_reads runs the pump (budget-limited, so it
        // defers and sets needs_iteration), then process_writes runs.
        d.do_process_reads(&mut c);
        assert!(d.needs_iteration, "pump should defer under READ_BUDGET");
        d.do_process_writes(&mut c).expect("writes ok");
        assert!(
            d.needs_iteration,
            "deferral must survive process_writes in the same iteration"
        );
        // Keep handles alive.
        let _ = &mut h;
    }

    // ===== Phase 4: per-stream SEND state machine (§11 send matrix) =====

    use h3::quic::WriteBuf;

    /// A `WriteBuf` carrying a DATA frame (header + payload); non-contiguous, so
    /// it exercises the multi-turn segment walk of `send_from_buf`.
    fn wbuf(payload: &'static [u8]) -> WriteBuf<Bytes> {
        WriteBuf::from(h3::proto::frame::Frame::Data(Bytes::from_static(payload)))
    }

    /// Total wire length of `wbuf(payload)` (frame header + payload).
    fn wbuf_len(payload: &'static [u8]) -> usize {
        wbuf(payload).remaining()
    }

    /// Sum of bytes `stream_send` recorded for `id`.
    fn sent_len(c: &MockConn, id: u64) -> usize {
        c.sent
            .iter()
            .filter(|(sid, _, _)| *sid == id)
            .map(|(_, b, _)| b.len())
            .sum()
    }

    fn sent_fin(c: &MockConn, id: u64) -> bool {
        c.sent.iter().any(|(sid, _, fin)| *sid == id && *fin)
    }

    /// Test-only view over a reusable [`WriteCompletion`](crate::buffer)
    /// generation (SF-3): mirrors the old per-write `oneshot` receiver so send
    /// tests can assert pending / `Ok` / `Err` / cancelled completion states.
    struct SendProbe {
        cell: crate::buffer::WriteCompletion<SendEnd>,
        generation: u64,
    }

    #[derive(Debug)]
    enum ProbeState {
        Pending,
        Ok,
        Err(SendEnd),
        Cancelled,
    }

    impl SendProbe {
        /// Non-consuming when pending; consumes the outcome once resolved.
        fn state(&self) -> ProbeState {
            match self.cell.try_take(self.generation) {
                None => ProbeState::Pending,
                Some(WriteOutcome::Done(Ok(()))) => ProbeState::Ok,
                Some(WriteOutcome::Done(Err(e))) => ProbeState::Err(e),
                Some(WriteOutcome::Cancelled) => ProbeState::Cancelled,
            }
        }
    }

    /// Enqueue a `Send` carrying a completer for `cell`'s next generation and
    /// return a [`SendProbe`] for it. When `cell` is shared across calls this
    /// proves the reusable-cell contract (SC-004): no per-chunk allocation.
    fn push_send_on(
        d: &mut QuicheDriver<Bytes>,
        cell: &crate::buffer::WriteCompletion<SendEnd>,
        id: u64,
        payload: &'static [u8],
    ) -> SendProbe {
        let generation = cell.begin();
        let buf = wbuf(payload);
        // Reserve the buffer's full wire size (frame header + payload) against the
        // driver's shared accounting, exactly as the front end does (SF-6); the
        // permit rides with the command and releases on the op's completion/drop.
        let permit = d.shared.send_accounting.try_reserve(buf.remaining());
        d.inbox.push_back(DriverCommand::Send {
            id,
            buf,
            done: cell.completer(generation),
            permit,
        });
        SendProbe {
            cell: cell.clone(),
            generation,
        }
    }

    /// The full wire size (DATA frame header + payload) a `wbuf(payload)` buffers,
    /// which is what SF-6 accounting reserves.
    fn wire_len(payload: &'static [u8]) -> usize {
        wbuf(payload).remaining()
    }

    fn push_send(d: &mut QuicheDriver<Bytes>, id: u64, payload: &'static [u8]) -> SendProbe {
        let cell = crate::buffer::WriteCompletion::new();
        push_send_on(d, &cell, id, payload)
    }

    fn push_finish(d: &mut QuicheDriver<Bytes>, id: u64) -> oneshot::Receiver<Result<(), SendEnd>> {
        let (tx, rx) = oneshot::channel();
        d.inbox.push_back(DriverCommand::Finish { id, done: tx });
        rx
    }

    /// §11: partial `stream_send` on a buffer larger than the available window,
    /// then re-armed low-water, then repeated partial progress as capacity opens.
    /// Exactly one `Ok(())` fires at full acceptance (§5.3 / §5.3a).
    #[test]
    fn partial_write_then_capacity_rearms_one_ok() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        let total = wbuf_len(b"hello world");
        // Accept at most 3 bytes per stream_send call.
        c.send_capacity.insert(0, 3);
        let done = push_send(&mut d, 0, b"hello world");
        d.apply_inbox(&mut c);
        d.stage_send(&mut c);

        // Not fully accepted yet: no completion, and the blocked write re-armed.
        assert!(matches!(done.state(), ProbeState::Pending));
        let rearms: Vec<usize> = c
            .rearms
            .iter()
            .filter(|(id, _)| *id == 0)
            .map(|(_, len)| *len)
            .collect();
        assert!(!rearms.is_empty(), "blocked write must low-water re-arm");
        assert_eq!(*rearms.last().unwrap(), REARM_THRESHOLD);
        let after_first = sent_len(&c, 0);
        assert!(after_first > 0 && after_first < total);

        // Capacity opens; the writable edge re-marks it runnable; it finishes.
        c.send_capacity.remove(&0);
        c.writable_next.push_back(0);
        loop {
            d.stage_writable(&mut c);
            d.stage_send(&mut c);
            if sent_len(&c, 0) == total {
                break;
            }
            // Keep re-arming the writable edge until drained.
            c.writable_next.push_back(0);
        }
        assert_eq!(sent_len(&c, 0), total, "all bytes eventually accepted");
        assert!(
            matches!(done.state(), ProbeState::Ok),
            "exactly one Ok at full acceptance"
        );
    }

    /// §11 / Q5: `Finish` acceptance completes once, even at **zero** send
    /// capacity (a FIN needs no window), and records the FIN on the wire.
    #[test]
    fn finish_accepted_at_zero_capacity_completes_once() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        c.send_capacity.insert(0, 0); // zero send capacity
        let mut done = push_finish(&mut d, 0);
        d.apply_inbox(&mut c);
        d.stage_send(&mut c);

        assert!(sent_fin(&c, 0), "zero-capacity FIN accepted (Q5)");
        assert!(matches!(done.try_recv(), Ok(Ok(()))));
        // Idempotent single completion: op popped, nothing more runnable.
        assert!(d.runnable_send.is_empty());
        d.stage_send(&mut c);
        assert!(matches!(
            done.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
    }

    /// §11: `Reset` preempts an in-flight/queued `Write` — the queued op is
    /// cancelled exactly once with the local-reset terminal, while a Write
    /// wholly accepted **before** reset keeps its recorded `Ok`. Stage (e) then
    /// emits `RESET_STREAM`.
    #[test]
    fn reset_preempts_queued_write_keeps_earlier_ok() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        // Write1 is small and fully accepted this turn.
        let done1 = push_send(&mut d, 0, b"a");
        d.apply_inbox(&mut c);
        d.stage_send(&mut c);
        assert!(
            matches!(done1.state(), ProbeState::Ok),
            "Write1 accepted before reset"
        );

        // Write2 queued, then Reset preempts it in the same stage (a).
        let done2 = push_send(&mut d, 0, b"bcde");
        d.inbox.push_back(DriverCommand::Reset { id: 0, code: 42 });
        d.apply_inbox(&mut c);
        match done2.state() {
            ProbeState::Err(SendEnd::Reset { error_code: 42 }) => {}
            other => panic!("Write2 must be cancelled once with local reset, got {other:?}"),
        }
        // Sticky status published for the front end.
        assert!(matches!(
            d.send.get(&0).unwrap().status.get(),
            Some(SendEnd::Reset { error_code: 42 })
        ));

        // Stage (e) services pending_reset before any eviction → RESET_STREAM.
        d.stage_send(&mut c);
        assert!(c.shutdowns.contains(&crate::conn::mock::ShutdownCall {
            id: 0,
            is_write: true,
            code: 42,
        }));
    }

    /// SF-6: a `Send` reserves its bytes against the shared accounting on
    /// admission and releases them exactly once when the write completes (default
    /// unlimited config keeps residency accurate without ever bounding admission).
    #[test]
    fn sf6_accounting_reserves_on_admission_releases_on_completion() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        assert_eq!(d.shared.send_accounting.resident(), 0);
        // push_send reserves the buffer's wire size against d.shared.send_accounting,
        // mirroring the front end.
        let done = push_send(&mut d, 0, b"hello");
        assert_eq!(
            d.shared.send_accounting.resident(),
            wire_len(b"hello"),
            "reserved when the command is admitted"
        );
        d.apply_inbox(&mut c);
        d.stage_send(&mut c);
        assert!(
            matches!(done.state(), ProbeState::Ok),
            "write completes once"
        );
        assert_eq!(
            d.shared.send_accounting.resident(),
            0,
            "released exactly once at the completion chokepoint"
        );
    }

    /// SF-6: a `Reset` that preempts a queued `Write` drains the op and thereby
    /// releases its byte reservation — the RAII permit ties release to the same
    /// exactly-once drain as the completion path.
    #[test]
    fn sf6_accounting_released_when_reset_drains_queued_write() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        let done = push_send(&mut d, 0, b"abcd");
        d.apply_inbox(&mut c); // move Write into send_ops (permit rides along)
        assert_eq!(d.shared.send_accounting.resident(), wire_len(b"abcd"));
        d.inbox.push_back(DriverCommand::Reset { id: 0, code: 7 });
        d.apply_inbox(&mut c); // apply_reset drains the op with the reset terminal
        assert!(matches!(
            done.state(),
            ProbeState::Err(SendEnd::Reset { error_code: 7 })
        ));
        assert_eq!(
            d.shared.send_accounting.resident(),
            0,
            "reset drain releases the reservation"
        );
    }

    /// SF-6: a finite worker-level cap bounds admitted send bytes; once the cap
    /// is full, a further admission via [`SendAccounting::try_reserve`] is
    /// refused (the front end would park) until an outstanding permit releases.
    #[test]
    fn sf6_worker_cap_bounds_admitted_bytes() {
        let cap = wire_len(b"abc");
        let (mut d, _h) = QuicheDriver::<Bytes>::with_buffers(
            false,
            4,
            4,
            DriverBufferConfig {
                recv_channel_depth: BYTE_CHANNEL_DEPTH,
                packet_buffer_size: PKT_BUF_LEN,
                max_buffered_send_bytes: Some(cap),
            },
        );
        let mut c = MockConn::new();
        assert_eq!(d.shared.send_accounting.cap(), Some(cap));
        let done = push_send(&mut d, 0, b"abc"); // fills the cap exactly
        assert_eq!(d.shared.send_accounting.resident(), cap);
        // Over the cap now → a fresh reservation is refused.
        assert!(d.shared.send_accounting.try_reserve(1).is_none());
        // Drain the write; the permit releases and capacity reopens.
        d.apply_inbox(&mut c);
        d.stage_send(&mut c);
        assert!(matches!(done.state(), ProbeState::Ok));
        assert_eq!(d.shared.send_accounting.resident(), 0);
        assert!(d.shared.send_accounting.try_reserve(cap).is_some());
    }

    /// §11 / Q3: a `Reset` at **zero** send capacity still emits `RESET_STREAM`
    /// (the reset call cannot sit behind a flow-control-blocked remainder).
    #[test]
    fn reset_emitted_at_zero_capacity() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        c.send_capacity.insert(0, 0);
        d.inbox.push_back(DriverCommand::Reset { id: 0, code: 7 });
        d.apply_inbox(&mut c);
        d.stage_send(&mut c);
        assert!(c.shutdowns.contains(&crate::conn::mock::ShutdownCall {
            id: 0,
            is_write: true,
            code: 7,
        }));
    }

    // Regression (review finding 1): a successfully sent FIN marks the send
    // direction done, so contract A reclaims an admitted bidi once its recv half
    // also ends. Before the fix, send_done stayed false and the stream leaked.
    #[test]
    fn accepted_fin_marks_send_done_and_enables_contract_a() {
        let (mut d, _h) = driver();
        d.admit.insert(
            0,
            AdmitState::Registered {
                send_done: false,
                recv_done: true,
            },
        );
        d.send.insert(0, StreamSendState::new());
        let mut c = MockConn::new();
        let mut fin = push_finish(&mut d, 0);
        d.apply_inbox(&mut c);
        d.stage_send(&mut c);
        assert!(matches!(fin.try_recv(), Ok(Ok(()))));
        // recv already done + send now done → contract A reclaimed admit + recv.
        assert!(
            !d.admit.contains_key(&0),
            "both directions terminal → admit dropped"
        );
        assert!(!d.recv.contains_key(&0));
    }

    // Regression (review finding 2): a second Reset command is idempotent — it
    // must not schedule a second RESET_STREAM. Before the fix, the guard only
    // checked pending_reset (cleared after servicing), so a later reset re-fired.
    #[test]
    fn duplicate_reset_is_idempotent() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        d.inbox.push_back(DriverCommand::Reset { id: 0, code: 7 });
        d.apply_inbox(&mut c);
        d.stage_send(&mut c); // services the reset → one RESET_STREAM
                              // A second reset with a different code must be a no-op.
        d.inbox.push_back(DriverCommand::Reset { id: 0, code: 9 });
        d.apply_inbox(&mut c);
        d.stage_send(&mut c);
        let resets: Vec<_> = c.shutdowns.iter().filter(|s| s.is_write).collect();
        assert_eq!(resets.len(), 1, "exactly one RESET_STREAM");
        assert_eq!(resets[0].code, 7, "first-effective reset code wins");
    }

    // Regression (review finding 3): a Send deferred in cmd_rx past the terminal
    // edge must complete with the retained sticky terminal, not recreate a fresh
    // non-terminal state. Contract A therefore retains self.send after reclaim.
    #[test]
    fn deferred_send_after_contract_a_completes_with_sticky_terminal() {
        let (mut d, _h) = driver();
        d.admit.insert(
            0,
            AdmitState::Registered {
                send_done: false,
                recv_done: true,
            },
        );
        let mut c = MockConn::new();
        // Peer STOP_SENDING on the send half → send terminal + contract A (recv
        // already done) reclaims admit/recv but retains self.send's terminal.
        let w1 = push_send(&mut d, 0, b"aa");
        c.send_errors
            .entry(0)
            .or_default()
            .push_back(quiche::Error::StreamStopped(55));
        d.apply_inbox(&mut c);
        d.stage_send(&mut c);
        assert!(matches!(
            w1.state(),
            ProbeState::Err(SendEnd::Stopped { error_code: 55 })
        ));
        assert!(!d.admit.contains_key(&0), "contract A reclaimed admit");
        assert!(d.send.contains_key(&0), "send retained for deferred ops");

        // A Send that was deferred past the terminal edge completes with the
        // sticky Stopped terminal (never a fabricated Internal / bare cancel).
        let late = push_send(&mut d, 0, b"bb");
        d.apply_inbox(&mut c);
        assert!(matches!(
            late.state(),
            ProbeState::Err(SendEnd::Stopped { error_code: 55 })
        ));
    }

    /// §11: peer `STOP_SENDING` observed on a `stream_send` call drains ALL
    /// remaining `send_ops` exactly once with `SendEnd::Stopped`, marks the send
    /// half done, and publishes the sticky terminal (invariant 13).
    #[test]
    fn stop_sending_on_send_drains_all_ops_once() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        let w1 = push_send(&mut d, 0, b"aa");
        let w2 = push_send(&mut d, 0, b"bb");
        let mut fin = push_finish(&mut d, 0);
        d.apply_inbox(&mut c);
        // The first stream_send call reports the peer stopped us.
        c.send_errors
            .entry(0)
            .or_default()
            .push_back(quiche::Error::StreamStopped(9));
        d.stage_send(&mut c);

        for (label, st) in [("w1", w1.state()), ("w2", w2.state())] {
            match st {
                ProbeState::Err(SendEnd::Stopped { error_code: 9 }) => {}
                other => panic!("{label} must complete once with Stopped, got {other:?}"),
            }
        }
        match fin.try_recv() {
            Ok(Err(SendEnd::Stopped { error_code: 9 })) => {}
            other => panic!("fin must complete once with Stopped, got {other:?}"),
        }
        assert!(matches!(
            d.send.get(&0).unwrap().status.get(),
            Some(SendEnd::Stopped { error_code: 9 })
        ));
        assert!(d.send.get(&0).unwrap().send_ops.is_empty());
        assert!(
            !d.runnable_send_set.contains(&0),
            "runnable membership released"
        );
    }

    /// §11 / invariant 13: peer `STOP_SENDING` surfaced on the **writable** path
    /// (stage (d) `stream_capacity` probe of a registered send id) resolves
    /// queued commands before runnable cleanup.
    #[test]
    fn stop_sending_via_writable_probe_drains_ops() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        let w1 = push_send(&mut d, 0, b"aa");
        let w2 = push_send(&mut d, 0, b"bb");
        d.apply_inbox(&mut c);
        // Stage (d) probes capacity and finds the stream stopped.
        c.writable_next.push_back(0);
        c.capacity.insert(0, Err(quiche::Error::StreamStopped(13)));
        d.stage_writable(&mut c);

        for (label, st) in [("w1", w1.state()), ("w2", w2.state())] {
            match st {
                ProbeState::Err(SendEnd::Stopped { error_code: 13 }) => {}
                other => panic!("{label} must complete once with Stopped, got {other:?}"),
            }
        }
        assert!(!d.runnable_send_set.contains(&0));
    }

    /// §11: round-robin fairness — a continuously-writable bulk stream yields
    /// turns to another runnable stream within one stage-(e) batch (invariant
    /// 12). The small stream completes while the bulk stream is still in flight.
    #[test]
    fn round_robin_bulk_yields_to_other_stream() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        // Bulk stream 0: more data than one whole stage-(e) batch can drain
        // (WRITE_BUDGET turns × MAX_WRITE_CHUNK = 512 KiB), so it cannot finish
        // within a single batch even if it takes every remaining turn.
        static BULK: [u8; 1024 * 1024] = [b'x'; 1024 * 1024];
        let bulk_cell = crate::buffer::WriteCompletion::<SendEnd>::new();
        let bulk_gen = bulk_cell.begin();
        d.inbox.push_back(DriverCommand::Send {
            id: 0,
            buf: WriteBuf::from(h3::proto::frame::Frame::Data(Bytes::from_static(&BULK))),
            done: bulk_cell.completer(bulk_gen),
            permit: None,
        });
        let bulk = SendProbe {
            cell: bulk_cell,
            generation: bulk_gen,
        };
        // Small stream 4 enqueued behind it.
        let small_done = push_send(&mut d, 4, b"z");
        d.apply_inbox(&mut c);
        d.stage_send(&mut c);

        // The small stream got its turn and completed despite the bulk backlog.
        assert!(
            matches!(small_done.state(), ProbeState::Ok),
            "small stream serviced"
        );
        // The bulk stream is still in flight (not completed, still runnable).
        assert!(matches!(bulk.state(), ProbeState::Pending));
        assert!(d.runnable_send_set.contains(&0) || d.needs_iteration);
    }

    /// §11: a `Send`/`Finish` received **after** the send half is terminal
    /// completes immediately once with the sticky terminal — never enqueued,
    /// never a bare cancel, never a fabricated `Ok` (§5.3a ops-after-terminal).
    #[test]
    fn send_after_terminal_completes_immediately() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        // Establish a local-reset terminal first.
        d.inbox.push_back(DriverCommand::Reset { id: 0, code: 5 });
        d.apply_inbox(&mut c);
        d.stage_send(&mut c); // service the reset shutdown

        // A late Send resolves immediately with the sticky reset terminal.
        let late = push_send(&mut d, 0, b"late");
        d.apply_inbox(&mut c);
        match late.state() {
            ProbeState::Err(SendEnd::Reset { error_code: 5 }) => {}
            other => panic!("late Send must complete once with sticky terminal, got {other:?}"),
        }
        assert!(
            d.send.get(&0).unwrap().send_ops.is_empty(),
            "late op not enqueued"
        );
        // No new transport send call was made for the late op.
        assert!(c.sent.iter().all(|(_, b, _)| b != b"late"));
    }

    /// §11: an admitted peer bidi retains a live `StreamSendState` sharing the
    /// handoff's `status` cell, so a later peer `STOP_SENDING` is visible to the
    /// front end (register_peer / send-registry integration).
    #[test]
    fn admitted_bidi_retains_send_state_sharing_status() {
        let (mut d, mut h) = driver();
        let mut c = MockConn::new();
        c.script_recv(0, [data(b"hi", false)]);
        c.queue_readable([0]);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);
        let ho = h.accept_bidi_rx.try_recv().expect("admitted");
        // The driver retains a live send half for the admitted bidi.
        assert!(d.send.contains_key(&0));
        assert!(ho.send.status.get().is_none());

        // A peer STOP_SENDING (via writable probe) publishes to the SHARED cell.
        c.writable_next.push_back(0);
        c.capacity.insert(0, Err(quiche::Error::StreamStopped(88)));
        d.stage_writable(&mut c);
        assert!(matches!(
            ho.send.status.get(),
            Some(SendEnd::Stopped { error_code: 88 })
        ));
    }

    // ===== Phase 5: connection CLOSE, teardown, finite close-cut (§11) =====

    fn conn_err(is_app: bool, code: u64, reason: &[u8]) -> quiche::ConnectionError {
        quiche::ConnectionError {
            is_app,
            error_code: code,
            reason: reason.to_vec(),
        }
    }

    /// §11: last-handle teardown issues the synthetic `H3_NO_ERROR` close, records
    /// it, and arms `graceful_close_issued` so `wait_for_data` stays pending.
    #[test]
    fn last_handle_teardown_issues_h3_no_error_close() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        d.last_handle_teardown = true;
        d.do_process_writes(&mut c).expect("clean teardown");
        assert_eq!(c.closed, Some((true, H3_NO_ERROR, b"".to_vec())));
        assert!(d.explicit_close_attempted);
        assert!(d.graceful_close_issued);
        let lc = d.local_close.as_ref().expect("recorded last-handle close");
        assert_eq!(lc.code, H3_NO_ERROR);
        assert!(lc.reason.is_empty());
    }

    /// §11: an explicit local `Close` crosses the mandatory barrier and is applied
    /// even after a saturated (WRITE_BUDGET) stream-write batch, recording the
    /// exact code/reason. A subsequent last-handle teardown issues NO synthetic
    /// `H3_NO_ERROR` (the attempt suppresses it).
    #[test]
    fn explicit_close_crosses_barrier_after_saturated_batch() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        // Saturate stage (e): WRITE_BUDGET+1 streams that only make partial
        // progress (capacity 1) so every turn requeues and the budget is spent.
        for id in 0..=(WRITE_BUDGET as u64) {
            let sid = id * 4; // client-bidi ids
            c.send_capacity.insert(sid, 1);
            let _rx = push_send(&mut d, sid, b"hello world");
        }
        // Stage the explicit close AFTER the sends (same inbox drain).
        d.inbox.push_back(DriverCommand::Close {
            code: 0x1234,
            reason: Bytes::from_static(b"bye"),
        });
        d.do_process_writes(&mut c).expect("close applied");

        // The write batch was saturated (needs another iteration)...
        assert!(d.needs_iteration, "write batch should be saturated");
        // ...but the close barrier still applied the explicit close.
        assert_eq!(c.closed, Some((true, 0x1234, b"bye".to_vec())));
        let lc = d.local_close.as_ref().expect("explicit close recorded");
        assert_eq!(lc.code, 0x1234);
        assert_eq!(&lc.reason[..], b"bye");
        assert!(d.graceful_close_issued);

        // A later last-handle teardown must NOT issue a second/synthetic close.
        d.last_handle_teardown = true;
        d.reads_ran_this_iter = false;
        d.do_process_writes(&mut c).expect("no second close");
        assert_eq!(
            c.closed,
            Some((true, 0x1234, b"bye".to_vec())),
            "synthetic H3_NO_ERROR must be suppressed"
        );
    }

    /// §11: first-close-wins — a second `Close` command is an idempotent no-op.
    #[test]
    fn first_close_wins_second_ignored() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        d.inbox.push_back(DriverCommand::Close {
            code: 0xaaa,
            reason: Bytes::from_static(b"first"),
        });
        d.inbox.push_back(DriverCommand::Close {
            code: 0xbbb,
            reason: Bytes::from_static(b"second"),
        });
        d.apply_inbox(&mut c);
        let pc = d.pending_close.as_ref().expect("first staged");
        assert_eq!(pc.code, 0xaaa);
        assert_eq!(&pc.reason[..], b"first");
        // Barrier applies the FIRST.
        d.apply_close_barrier(&mut c).expect("applied");
        assert_eq!(c.closed, Some((true, 0xaaa, b"first".to_vec())));
    }

    /// §8.3 precedence: a peer application close outranks a racing last-handle
    /// teardown — the synthetic `H3_NO_ERROR` is suppressed and the terminal is
    /// `AppClose { origin: Peer }`.
    #[test]
    fn peer_app_close_outranks_last_handle_teardown() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        c.peer_error = Some(conn_err(true, 0x99, b"peer-bye"));
        d.last_handle_teardown = true;
        // Barrier must NOT issue a synthetic close (a peer terminal exists).
        d.do_process_writes(&mut c).expect("no synthetic close");
        assert_eq!(
            c.closed, None,
            "synthetic close suppressed by peer terminal"
        );
        // Classification surfaces the peer app-close.
        d.do_on_conn_close(&mut c);
        match d.shared.conn_terminal.get().as_deref() {
            Some(ConnTerminal::AppClose {
                origin: CloseOrigin::Peer,
                error_code: 0x99,
                reason,
            }) => assert_eq!(&reason[..], b"peer-bye"),
            other => panic!("expected AppClose{{Peer}}, got {other:?}"),
        }
    }

    /// §8.3: idle timeout classifies as `Timeout`.
    #[test]
    fn timeout_classifies_as_timeout() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        c.timed_out = true;
        d.do_on_conn_close(&mut c);
        assert!(matches!(
            d.shared.conn_terminal.get().as_deref(),
            Some(ConnTerminal::Timeout)
        ));
    }

    /// §9/§8.3: `on_conn_close` publishes the terminal to a live recv cell
    /// (`RecvEnd::Conn`), a live send cell (`SendEnd::Conn`), and BOTH
    /// accept-terminal cells, plus the connection-level cell.
    #[test]
    fn on_conn_close_publishes_to_all_out_of_band_cells() {
        let (mut d, mut h) = driver();
        let mut c = MockConn::new();
        // Admit a live peer bidi (creates a live recv + send half).
        c.script_recv(0, [data(b"hi", false)]);
        c.queue_readable([0]);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);
        let ho = h.accept_bidi_rx.try_recv().expect("admitted");

        c.local_error = Some(conn_err(true, 0x100, b""));
        d.do_on_conn_close(&mut c);

        assert!(matches!(ho.recv.terminal.get(), Some(RecvEnd::Conn(_))));
        assert!(matches!(ho.send.status.get(), Some(SendEnd::Conn(_))));
        assert!(h.accept_terminal_bidi.get().is_some(), "bidi accept cell");
        assert!(h.accept_terminal_uni.get().is_some(), "uni accept cell");
        assert!(d.shared.conn_terminal.get().is_some(), "conn cell");
    }

    /// §5.3a/§14 invariant 14: a Send whose remainder is still in `send_ops`
    /// drains with `SendEnd::Conn` on close — never a bare oneshot cancel.
    #[test]
    fn pending_send_op_drains_with_conn_terminal() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        // Queue a Send op WITHOUT running stage (e), so it stays in send_ops.
        let done = push_send(&mut d, 0, b"payload");
        d.apply_inbox(&mut c);
        assert!(!d.send.get(&0).unwrap().send_ops.is_empty());

        c.peer_error = Some(conn_err(true, 0x7, b""));
        d.do_on_conn_close(&mut c);

        match done.state() {
            ProbeState::Err(SendEnd::Conn(_)) => {}
            other => panic!("expected SendEnd::Conn, got {other:?}"),
        }
    }

    /// SF-3 (gpt#7 lifecycle): a `Send` still **unapplied** in the inbox at close
    /// resolves its generation exactly once via `complete_command_on_close` — the
    /// reusable completer delivers the connection terminal, never a bare cancel.
    #[test]
    fn unapplied_send_at_close_completes_generation_once() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        // Queue a Send but do NOT apply_inbox: it never reaches send_ops, so the
        // on_conn_close command-drain loop must resolve it directly.
        let done = push_send(&mut d, 0, b"payload");
        assert!(
            !d.send.contains_key(&0),
            "precondition: Send not yet applied into send_ops"
        );
        c.peer_error = Some(conn_err(true, 0x9, b""));
        d.do_on_conn_close(&mut c);
        match done.state() {
            ProbeState::Err(SendEnd::Conn(_)) => {}
            other => panic!("expected SendEnd::Conn for unapplied Send, got {other:?}"),
        }
        // Exactly once: nothing left to consume.
        assert!(matches!(done.state(), ProbeState::Pending));
    }

    // Regression (review finding): a Send that hits `ConnGone` in the closing
    // window BEFORE on_conn_close classified the terminal must NOT be pinned with
    // a fabricated `Internal` (first-writer-wins status). It stays in send_ops
    // and on_conn_close drains it with the real terminal.
    #[test]
    fn send_conngone_in_closing_window_defers_to_on_conn_close() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        let done = push_send(&mut d, 0, b"x");
        c.send_errors
            .entry(0)
            .or_default()
            .push_back(quiche::Error::InvalidState);
        d.apply_inbox(&mut c);
        d.stage_send(&mut c);
        // Not completed, not pinned: op retained, status cell empty.
        assert!(matches!(done.state(), ProbeState::Pending));
        assert!(!d.send.get(&0).unwrap().send_ops.is_empty());
        assert!(d.send.get(&0).unwrap().status.get().is_none());
        // on_conn_close classifies and drains it with SendEnd::Conn.
        c.peer_error = Some(conn_err(true, 0x101, b""));
        d.do_on_conn_close(&mut c);
        match done.state() {
            ProbeState::Err(SendEnd::Conn(_)) => {}
            other => panic!("expected SendEnd::Conn after close, got {other:?}"),
        }
    }

    // Regression (review finding): a recv stream hitting `ConnGone` in the closing
    // window must NOT be removed before the terminal is published — otherwise
    // on_conn_close cannot publish `RecvEnd::Conn` and the front end hangs. The
    // recv entry is retained so on_conn_close seals it.
    #[test]
    fn recv_conngone_in_closing_window_defers_to_on_conn_close() {
        let (mut d, _h) = driver();
        let (tx, _rx) = mpsc::channel(BYTE_CHANNEL_DEPTH);
        let terminal_cell = TerminalCell::new();
        d.recv.insert(
            0,
            StreamRecvState {
                bytes: tx,
                terminal: terminal_cell.clone(),
                resume: Arc::new(AtomicBool::new(false)),
                blocked: Arc::new(AtomicBool::new(false)),
            },
        );
        d.pending_readable.push_back(0);
        d.readable_set.insert(0);
        let mut c = MockConn::new();
        c.readable_ids.insert(0);
        c.script_recv(0, [RecvStep::Err(quiche::Error::InvalidState)]);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);
        // Recv entry retained, terminal cell still empty (no fabricated seal).
        assert!(d.recv.contains_key(&0));
        assert!(terminal_cell.get().is_none());
        // on_conn_close publishes RecvEnd::Conn into the cell.
        c.peer_error = Some(conn_err(true, 0x102, b""));
        d.do_on_conn_close(&mut c);
        assert!(matches!(terminal_cell.get(), Some(RecvEnd::Conn(_))));
    }

    /// §5.2/M3 invariant 14: a `OpenBidi` deferred in `cmd_rx` resolves
    /// `Err(terminal)` after `cmd_rx.close()` — the finite drain completes it.
    #[test]
    fn deferred_open_bidi_resolves_err_after_close() {
        let (mut d, h) = driver();
        let mut c = MockConn::new();
        let (reply_tx, mut reply_rx) = oneshot::channel();
        h.cmd_tx
            .send(DriverCommand::OpenBidi { reply: reply_tx })
            .expect("enqueue open");

        c.peer_error = Some(conn_err(true, 0x2, b""));
        d.do_on_conn_close(&mut c);

        match reply_rx.try_recv() {
            Ok(Err(t)) => assert!(matches!(
                t.as_ref(),
                ConnTerminal::AppClose {
                    origin: CloseOrigin::Peer,
                    ..
                }
            )),
            Ok(Ok(_)) => panic!("expected Err(terminal), got Ok(handoff)"),
            Err(e) => panic!("expected Err(terminal), got {e:?}"),
        }
    }

    /// §8.3: a `qconn.close` returning `Done` defers to the pre-existing quiche
    /// terminal without fabricating acceptance (no `local_close` recorded) and
    /// suppresses a synthetic `H3_NO_ERROR`.
    #[test]
    fn done_close_result_defers_to_preexisting_terminal() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        // The staged explicit close will hit a Done result from quiche.
        c.close_result = Some(quiche::Error::Done);
        c.peer_error = Some(conn_err(true, 0x55, b"peer"));
        d.inbox.push_back(DriverCommand::Close {
            code: 0x1,
            reason: Bytes::from_static(b"local"),
        });
        d.apply_inbox(&mut c);
        d.apply_close_barrier(&mut c)
            .expect("done defers, not a bug");

        assert!(d.explicit_close_attempted);
        assert!(d.graceful_close_issued);
        assert!(d.local_close.is_none(), "Done must not record acceptance");

        d.do_on_conn_close(&mut c);
        match d.shared.conn_terminal.get().as_deref() {
            Some(ConnTerminal::AppClose {
                origin: CloseOrigin::Peer,
                error_code: 0x55,
                ..
            }) => {}
            other => panic!("expected pre-existing peer terminal, got {other:?}"),
        }
    }

    /// §8.3: an explicit local close's recorded code/reason outranks a later
    /// `local_error()` and is surfaced as `AppClose { origin: Local }`.
    #[test]
    fn recorded_explicit_close_outranks_local_error() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        d.inbox.push_back(DriverCommand::Close {
            code: 0x321,
            reason: Bytes::from_static(b"quit"),
        });
        d.apply_inbox(&mut c);
        d.apply_close_barrier(&mut c).expect("applied");
        // quiche also reports its own local_error; the recorded close wins.
        c.local_error = Some(conn_err(true, 0x999, b"other"));
        d.do_on_conn_close(&mut c);
        match d.shared.conn_terminal.get().as_deref() {
            Some(ConnTerminal::AppClose {
                origin: CloseOrigin::Local,
                error_code: 0x321,
                reason,
            }) => assert_eq!(&reason[..], b"quit"),
            other => panic!("expected recorded local close, got {other:?}"),
        }
    }

    /// §8.3: a `qconn.close` returning an unexpected error is an adapter bug —
    /// the barrier fails `process_writes` and the terminal classifies `Internal`.
    #[test]
    fn unexpected_close_error_is_internal_bug() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        c.close_result = Some(quiche::Error::TlsFail);
        d.last_handle_teardown = true;
        let err = d.do_process_writes(&mut c);
        assert!(
            err.is_err(),
            "unexpected close error must fail the callback"
        );
        assert!(d.close_bug.is_some());
        d.do_on_conn_close(&mut c);
        assert!(matches!(
            d.shared.conn_terminal.get().as_deref(),
            Some(ConnTerminal::Internal(_))
        ));
    }

    /// §5.2 finding 4: `ConnectionDropped` cleans parked/pending peer streams
    /// with a direction-aware `stream_shutdown` and drops their bookkeeping.
    #[test]
    fn connection_dropped_cleans_parked_peer_streams() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        // A parked peer bidi (id 0) and a pending-admit peer uni (id 2).
        d.admit.insert(0, AdmitState::Parked(PeerStream::new(0)));
        d.parked_bidi.push_back(0);
        d.pending_admit.insert(2, PeerStream::new(2));
        d.pending_admit_uni.push_back(2);

        d.inbox.push_back(DriverCommand::ConnectionDropped);
        d.apply_inbox(&mut c);

        assert!(!d.admit.contains_key(&0), "parked bidi dropped");
        assert!(d.parked_bidi.is_empty());
        assert!(d.pending_admit.is_empty());
        assert!(d.pending_admit_uni.is_empty());
        // Peer bidi shut down BOTH directions; peer uni only read.
        assert!(c.shutdowns.iter().any(|s| s.id == 0 && s.is_write));
        assert!(c.shutdowns.iter().any(|s| s.id == 0 && !s.is_write));
        assert!(c.shutdowns.iter().any(|s| s.id == 2 && !s.is_write));
        assert!(!c.shutdowns.iter().any(|s| s.id == 2 && s.is_write));
    }

    // ===== §6.1 worker-side open materialization =====

    fn push_open_bidi(
        d: &mut QuicheDriver<Bytes>,
    ) -> oneshot::Receiver<Result<BidiHandoff<Bytes>, Arc<ConnTerminal>>> {
        let (tx, rx) = oneshot::channel();
        d.inbox.push_back(DriverCommand::OpenBidi { reply: tx });
        rx
    }

    fn push_open_uni(
        d: &mut QuicheDriver<Bytes>,
    ) -> oneshot::Receiver<Result<SendHandoff<Bytes>, Arc<ConnTerminal>>> {
        let (tx, rx) = oneshot::channel();
        d.inbox.push_back(DriverCommand::OpenUni { reply: tx });
        rx
    }

    /// §6.1: a client bidi open materializes id `0` via `stream_priority(id,127,
    /// true)` **once**, advances the counter by 4, retains live registry halves,
    /// and delivers a `BidiHandoff` through the reply.
    #[test]
    fn stage_open_bidi_allocates_one_id_and_increments_counter() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        c.streams_left_bidi = 4;
        let mut reply = push_open_bidi(&mut d);
        d.apply_inbox(&mut c);
        assert_eq!(
            d.next_bidi_id, 0,
            "counter unchanged before materialization"
        );
        d.stage_open(&mut c);

        // Exactly one materialization call, at the h3 default priority.
        assert_eq!(c.priorities, vec![(0, 127, true)]);
        assert_eq!(d.next_bidi_id, 4, "counter advances by 4 after success");
        // Live registry halves retained on both directions.
        assert!(d.recv.contains_key(&0));
        assert!(d.send.contains_key(&0));
        // The reply carries both halves for id 0.
        match reply.try_recv() {
            Ok(Ok(handoff)) => {
                assert_eq!(handoff.send.id, 0);
                assert_eq!(handoff.recv.id, 0);
            }
            _ => panic!("expected BidiHandoff"),
        }
        assert!(d.open_bidi.is_empty());
    }

    /// §6.1: a uni open materializes the next uni id (`2` for a client) and
    /// hands back only a send half.
    #[test]
    fn stage_open_uni_allocates_send_only() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        c.streams_left_uni = 4;
        let mut reply = push_open_uni(&mut d);
        d.apply_inbox(&mut c);
        d.stage_open(&mut c);

        assert_eq!(c.priorities, vec![(2, 127, true)]);
        assert_eq!(d.next_uni_id, 6);
        assert!(d.send.contains_key(&2));
        assert!(!d.recv.contains_key(&2), "uni open has no recv half");
        match reply.try_recv() {
            Ok(Ok(handoff)) => assert_eq!(handoff.id, 2),
            _ => panic!("expected SendHandoff"),
        }
    }

    /// §6.1 step 1: a reply whose receiver was already dropped is skipped — no
    /// id is burned, no `stream_priority` call is made.
    #[test]
    fn stage_open_is_closed_skips_no_id_burned() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        c.streams_left_bidi = 4;
        let reply = push_open_bidi(&mut d);
        drop(reply); // poller cancelled before materialization.
        d.apply_inbox(&mut c);
        d.stage_open(&mut c);

        assert!(
            c.priorities.is_empty(),
            "no id materialized for a dead reply"
        );
        assert_eq!(d.next_bidi_id, 0, "counter not advanced");
        assert!(d.open_bidi.is_empty());
        assert!(!d.send.contains_key(&0));
        assert!(!d.recv.contains_key(&0));
    }

    /// §6.1 step 2: zero peer credit leaves the request queued; no id is
    /// allocated and the worker does NOT hot-spin (blocking on stream credit is
    /// re-driven by the peer's MAX_STREAMS packet, not a self-reschedule).
    #[test]
    fn stage_open_zero_credit_defers_request() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        c.streams_left_bidi = 0;
        let _reply = push_open_bidi(&mut d);
        d.apply_inbox(&mut c);
        d.needs_iteration = false;
        d.stage_open(&mut c);

        assert!(c.priorities.is_empty());
        assert_eq!(d.next_bidi_id, 0);
        assert_eq!(d.open_bidi.len(), 1, "request stays queued");
        assert!(
            !d.needs_iteration,
            "blocking on stream credit must not hot-spin (§5.2 progress bound)"
        );
    }

    /// §6.2: the direction-aware cleanup for a cancelled-after-materialize bidi
    /// shuts down BOTH directions with `H3_REQUEST_CANCELLED` and drops both
    /// retained halves; a uni cleanup touches only `Shutdown::Write`.
    #[test]
    fn cleanup_undeliverable_open_is_direction_aware() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        // Simulate retained halves for a materialized-but-undeliverable bidi.
        let cmd_tx = d.cmd_tx_weak.upgrade().unwrap();
        let (recv_state, _rh, _rd) = d.build_recv(0, cmd_tx.clone(), None);
        let (_sh, send_state, _sd) =
            build_send(0, cmd_tx, Arc::clone(&d.shared.send_accounting), None);
        d.recv.insert(0, recv_state.unwrap());
        d.send.insert(0, send_state.unwrap());

        d.cleanup_undeliverable_open(&mut c, 0, true);
        assert!(!d.recv.contains_key(&0));
        assert!(!d.send.contains_key(&0));
        let bidi_shuts: Vec<&crate::conn::mock::ShutdownCall> =
            c.shutdowns.iter().filter(|s| s.id == 0).collect();
        assert!(bidi_shuts
            .iter()
            .any(|s| s.is_write && s.code == H3_REQUEST_CANCELLED));
        assert!(bidi_shuts
            .iter()
            .any(|s| !s.is_write && s.code == H3_REQUEST_CANCELLED));

        // Uni: only Shutdown::Write.
        let (_sh, send_state, _sd) = build_send(
            2,
            d.cmd_tx_weak.upgrade().unwrap(),
            Arc::clone(&d.shared.send_accounting),
            None,
        );
        d.send.insert(2, send_state.unwrap());
        d.cleanup_undeliverable_open(&mut c, 2, false);
        assert!(!d.send.contains_key(&2));
        let uni_shuts: Vec<&crate::conn::mock::ShutdownCall> =
            c.shutdowns.iter().filter(|s| s.id == 2).collect();
        assert_eq!(uni_shuts.len(), 1);
        assert!(uni_shuts[0].is_write && uni_shuts[0].code == H3_REQUEST_CANCELLED);
    }

    /// §6.1: a server-role driver allocates the server bidi parity (`1,5,9…`).
    #[test]
    fn stage_open_bidi_uses_server_parity() {
        let (mut d, _h) = QuicheDriver::<Bytes>::new(true, 4, 4);
        let mut c = MockConn::new();
        c.streams_left_bidi = 4;
        let _reply = push_open_bidi(&mut d);
        d.apply_inbox(&mut c);
        d.stage_open(&mut c);
        assert_eq!(c.priorities, vec![(1, 127, true)]);
        assert_eq!(d.next_bidi_id, 5);
    }

    // Regression (review finding): >OPEN_BUDGET eligible opens with credit must
    // set needs_iteration after budget exhaustion so the backlog drains, instead
    // of stalling until an unrelated event.
    #[test]
    fn open_backlog_past_budget_sets_needs_iteration() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        c.streams_left_bidi = u64::MAX; // ample credit
                                        // Keep the reply receivers alive (a dropped reply is skipped as closed).
        let _replies: Vec<_> = (0..(OPEN_BUDGET + 4))
            .map(|_| push_open_bidi(&mut d))
            .collect();
        d.apply_inbox(&mut c);
        d.needs_iteration = false;
        d.stage_open(&mut c);
        // Exactly OPEN_BUDGET materialized this pass; the rest remain queued and
        // force another iteration (no stall).
        assert_eq!(c.priorities.len(), OPEN_BUDGET);
        assert_eq!(d.open_bidi.len(), 4);
        assert!(d.needs_iteration, "backlog must force another iteration");
    }

    // Regression (review finding): on_conn_close must resolve open requests
    // already staged into open_bidi/open_uni (not just those in cmd_rx), waking
    // pending openers with the classified terminal.
    #[test]
    fn on_conn_close_drains_staged_open_queues() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        // Move opens into the staged queues (apply_inbox), but do NOT run
        // stage_open, so they sit in open_bidi/open_uni.
        let mut bidi_reply = push_open_bidi(&mut d);
        let mut uni_reply = push_open_uni(&mut d);
        d.apply_inbox(&mut c);
        assert_eq!(d.open_bidi.len(), 1);
        assert_eq!(d.open_uni.len(), 1);

        c.peer_error = Some(conn_err(true, 0x3, b""));
        d.do_on_conn_close(&mut c);

        assert!(
            matches!(bidi_reply.try_recv(), Ok(Err(_))),
            "staged bidi open resolved"
        );
        assert!(
            matches!(uni_reply.try_recv(), Ok(Err(_))),
            "staged uni open resolved"
        );
        assert!(d.open_bidi.is_empty());
        assert!(d.open_uni.is_empty());
    }

    // Regression (final review, Opus + GPT): the send registry entry must be
    // reclaimed when the send half FINs AFTER the recv half already terminated
    // (the normal server request→response order), else one entry leaks per
    // handled request.
    #[test]
    fn send_entry_reclaimed_when_send_finishes_after_recv() {
        let (mut d, _h) = driver();
        d.admit.insert(
            0,
            AdmitState::Registered {
                send_done: false,
                recv_done: false,
            },
        );
        let (tx, _rx) = mpsc::channel(BYTE_CHANNEL_DEPTH);
        d.recv.insert(
            0,
            StreamRecvState {
                bytes: tx,
                terminal: TerminalCell::new(),
                resume: Arc::new(AtomicBool::new(false)),
                blocked: Arc::new(AtomicBool::new(false)),
            },
        );
        d.send.insert(0, StreamSendState::new());

        let mut c = MockConn::new();
        // 1. Server reads the request to FIN → recv terminal first.
        c.script_recv(0, [data(b"req", true)]);
        d.pending_readable.push_back(0);
        d.readable_set.insert(0);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);
        assert!(!d.recv.contains_key(&0), "recv reclaimed on FIN");
        assert!(d.send.contains_key(&0), "send retained while still open");

        // 2. Server sends the response + FIN → send terminal LAST.
        let mut fin = push_finish(&mut d, 0);
        d.apply_inbox(&mut c);
        d.stage_send(&mut c);
        assert!(matches!(fin.try_recv(), Ok(Ok(()))));
        assert!(
            !d.send.contains_key(&0),
            "send entry reclaimed on FIN-after-recv-done (no per-request leak)"
        );
    }

    // Regression (final review, GPT): a RecvResume applied during process_writes
    // on a PACKET iteration (the pump already ran in process_reads and does not
    // re-run) must still force another iteration via the needs_iteration
    // recompute — otherwise the resumed read strands under backpressure.
    #[test]
    fn packet_path_resume_forces_another_iteration() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        // Simulate a packet iteration: process_reads already ran.
        d.reads_ran_this_iter = true;
        d.needs_iteration = false;
        // A RecvResume arrives (front end freed capacity) and is applied here.
        d.inbox.push_back(DriverCommand::RecvResume { id: 0 });
        d.do_process_writes(&mut c).unwrap();
        assert!(
            d.needs_iteration,
            "a resume applied on a packet iteration must not strand"
        );
    }
}

/// Phase 2 loopback: a real handshake reaches `on_conn_established` on both
/// sides, driving the `established` oneshot to `Ok`. `#[ignore]`d (binds UDP +
/// runs a handshake); run with `--ignored`.
#[cfg(test)]
mod loopback_tests {
    use super::*;
    use tokio::net::UdpSocket;
    use tokio::time::{timeout, Duration};
    use tokio_quiche::metrics::DefaultMetrics;
    use tokio_quiche::quic::connect_with_config;
    use tokio_quiche::settings::{CertificateKind, Hooks, QuicSettings, TlsCertificatePaths};
    use tokio_quiche::socket::Socket;
    use tokio_quiche::ConnectionParams;

    use futures::StreamExt;

    struct TestCerts {
        cert_path: String,
        key_path: String,
    }

    impl TestCerts {
        fn generate() -> Self {
            let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
                .expect("self-signed cert");
            let dir = std::env::temp_dir();
            let uniq = format!(
                "quiche-h3-driver-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            let cert_path = dir.join(format!("{uniq}.crt"));
            let key_path = dir.join(format!("{uniq}.key"));
            std::fs::write(&cert_path, ck.cert.pem()).expect("write cert");
            std::fs::write(&key_path, ck.key_pair.serialize_pem()).expect("write key");
            Self {
                cert_path: cert_path.to_string_lossy().into_owned(),
                key_path: key_path.to_string_lossy().into_owned(),
            }
        }
    }

    impl Drop for TestCerts {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.cert_path);
            let _ = std::fs::remove_file(&self.key_path);
        }
    }

    fn client_params() -> ConnectionParams<'static> {
        let mut settings = QuicSettings::default();
        settings.verify_peer = false;
        settings.max_idle_timeout = Some(Duration::from_secs(10));
        ConnectionParams::new_client(settings, None, Hooks::default())
    }

    fn server_params(certs: &TestCerts) -> ConnectionParams<'_> {
        let mut settings = QuicSettings::default();
        settings.max_idle_timeout = Some(Duration::from_secs(10));
        ConnectionParams::new_server(
            settings,
            TlsCertificatePaths {
                cert: &certs.cert_path,
                private_key: &certs.key_path,
                kind: CertificateKind::X509,
            },
            Hooks::default(),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "loopback: binds UDP + runs a real handshake"]
    async fn handshake_reaches_on_conn_established() {
        let certs = TestCerts::generate();

        // --- server ---
        let server_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server_udp.local_addr().unwrap();
        let (server_driver, server_handles) = QuicheDriver::<Bytes>::new(true, 8, 8);
        let mut listeners =
            tokio_quiche::listen([server_udp], server_params(&certs), DefaultMetrics)
                .expect("listen");

        let server_task = tokio::spawn(async move {
            let stream = &mut listeners[0];
            if let Some(Ok(conn)) = stream.next().await {
                let _qconn = conn.start(server_driver);
                // Keep the listener + started handle alive while the worker runs.
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });

        // --- client ---
        let client_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client_udp.connect(server_addr).await.unwrap();
        let client_socket = Socket::try_from(client_udp).expect("socket");
        let (client_driver, client_handles) = QuicheDriver::<Bytes>::new(false, 8, 8);

        let params = client_params();
        let conn = connect_with_config(client_socket, Some("localhost"), &params, client_driver)
            .await
            .expect("client handshake");

        // Keep the control senders alive (no premature last-handle teardown)
        // while we consume the `established` receivers.
        let DriverHandles {
            cmd_tx: client_cmd_tx,
            accept_bidi_rx: _c_bidi,
            accept_uni_rx: _c_uni,
            established_rx: client_established_rx,
            shared: _c_shared,
            ..
        } = client_handles;
        let DriverHandles {
            cmd_tx: server_cmd_tx,
            accept_bidi_rx: _s_bidi,
            accept_uni_rx: _s_uni,
            established_rx: server_established_rx,
            shared: _s_shared,
            ..
        } = server_handles;

        // The `established` oneshot fires from on_conn_established in the worker.
        let client_est = timeout(Duration::from_secs(2), client_established_rx)
            .await
            .expect("client established within timeout")
            .expect("client established_rx not cancelled");
        assert!(client_est.is_ok(), "client establish should be Ok");

        let server_est = timeout(Duration::from_secs(2), server_established_rx)
            .await
            .expect("server established within timeout")
            .expect("server established_rx not cancelled");
        assert!(server_est.is_ok(), "server establish should be Ok");

        drop(client_cmd_tx);
        drop(server_cmd_tx);
        drop(conn);
        let _ = server_task.await;
    }
}
