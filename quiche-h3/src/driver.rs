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

use bytes::{Buf, Bytes};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};

use tokio_quiche::quic::HandshakeInfo;
use tokio_quiche::quic::QuicheConnection;
use tokio_quiche::{ApplicationOverQuic, QuicResult};

use crate::buffer::{send_from_buf, TerminalCell, MAX_CHUNK, PKT_BUF_LEN};
use crate::conn::QuicConn;
use crate::error::{
    classify_stream_recv_error, classify_stream_send_error, ConnTerminal, RecvEnd, SendEnd,
    StreamRecvClass, StreamSendClass, H3_NO_ERROR,
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
/// `BYTE_CHANNEL_DEPTH × MAX_CHUNK` (§5.1, provisional §12 S3).
const BYTE_CHANNEL_DEPTH: usize = 64;

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

/// Stream-class helper: a bidirectional stream has bit 0x2 clear (§5.5).
#[inline]
fn is_bidi(id: u64) -> bool {
    id & 0x2 == 0
}

/// Front-end → worker control commands, carried over the single unbounded
/// control channel (§5.2). Unbounded because the emitting trait methods cannot
/// exert backpressure or fail (`reset`/`stop_sending` return `()`), and the
/// resume signals are correctness-critical and must never be dropped.
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
    Send {
        id: u64,
        buf: h3::quic::WriteBuf<B>,
        done: oneshot::Sender<Result<(), SendEnd>>,
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

/// Cross-task shared state (§5). Holds the connection-level terminal cell the
/// close-admission gate publishes once and every submitter reads (§5.2, M3).
pub(crate) struct ConnShared {
    /// Published exactly once at the connection-terminal edge.
    pub(crate) conn_terminal: TerminalCell<Arc<ConnTerminal>>,
}

impl ConnShared {
    fn new() -> Arc<Self> {
        Arc::new(ConnShared {
            conn_terminal: TerminalCell::new(),
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
    /// For `stop_sending` and drop cleanup.
    pub(crate) cmd_tx: mpsc::UnboundedSender<DriverCommand<B>>,
}

/// Raw send-half state the worker hands to the front end at open. Phase 6 wraps
/// this into an `H3SendStream`.
pub(crate) struct SendHandoff<B: Buf> {
    pub(crate) id: u64,
    /// Out-of-band sticky end reason (§8.2), mirrors the worker's terminal.
    pub(crate) status: TerminalCell<SendEnd>,
    /// For `send_data`/`poll_finish`/`reset` and drop cleanup.
    pub(crate) cmd_tx: mpsc::UnboundedSender<DriverCommand<B>>,
}

/// A peer/opened bidi stream handed to the front end: both halves.
pub(crate) struct BidiHandoff<B: Buf> {
    pub(crate) send: SendHandoff<B>,
    pub(crate) recv: RecvHandoff<B>,
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
    pub(crate) blocked: bool,
}

/// One ordered send operation queued for a stream (§5.3a). `Write` carries the
/// caller's `WriteBuf` cursor (partial-consumed across turns) and its completion
/// oneshot; `Finish` carries only its completion oneshot.
pub(crate) enum SendOp<B: Buf> {
    Write {
        buf: h3::quic::WriteBuf<B>,
        done: oneshot::Sender<Result<(), SendEnd>>,
    },
    Finish {
        done: oneshot::Sender<Result<(), SendEnd>>,
    },
}

impl<B: Buf> SendOp<B> {
    /// Resolve this op's completion oneshot exactly once (§5.3a exactly-once).
    fn complete(self, result: Result<(), SendEnd>) {
        let done = match self {
            SendOp::Write { done, .. } => done,
            SendOp::Finish { done } => done,
        };
        // Ignore send error: the front end may have stopped polling (drop).
        let _ = done.send(result);
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
    /// `PeerStream`; `pending_admit_order` defines bounded admission order and
    /// its membership. Both are updated atomically on every exit (iter11 f6).
    pending_admit: HashMap<u64, PeerStream>,
    pending_admit_order: VecDeque<u64>,
    /// Per-class parked promotion queues (parking is independent per class, §5).
    parked_bidi: VecDeque<u64>,
    parked_uni: VecDeque<u64>,

    // ----- setup signalling (§7.1) -----
    established: Option<oneshot::Sender<Result<(), SetupFailure>>>,

    // ----- worker loop flags / buffers (§2.3, §5) -----
    /// `should_act()` result: true once established.
    acting: bool,
    /// Outbound packet buffer backing `buffer()` (§5, T3).
    pkt_buf: Vec<u8>,
    /// `stream_recv` target, capped at `MAX_CHUNK` (§5.1).
    scratch: Vec<u8>,
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
}

impl<B: Buf + Send + 'static> QuicheDriver<B> {
    /// Create a driver and its front-end handles. `accept_bidi_cap` /
    /// `accept_uni_cap` bound the respective accept queues (§5.2, provisional
    /// §12 S3).
    pub(crate) fn new(accept_bidi_cap: usize, accept_uni_cap: usize) -> (Self, DriverHandles<B>) {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let cmd_tx_weak = cmd_tx.downgrade();
        let (accept_bidi_tx, accept_bidi_rx) = mpsc::channel(accept_bidi_cap.max(1));
        let (accept_uni_tx, accept_uni_rx) = mpsc::channel(accept_uni_cap.max(1));
        let (est_tx, est_rx) = oneshot::channel();
        let shared = ConnShared::new();

        let driver = QuicheDriver {
            shared: Arc::clone(&shared),
            cmd_rx,
            cmd_tx_weak,
            inbox: VecDeque::new(),
            accept_bidi: accept_bidi_tx,
            accept_uni: accept_uni_tx,
            accept_bidi_resume: Arc::new(AtomicBool::new(false)),
            accept_uni_resume: Arc::new(AtomicBool::new(false)),
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
            pending_admit_order: VecDeque::new(),
            parked_bidi: VecDeque::new(),
            parked_uni: VecDeque::new(),
            established: Some(est_tx),
            acting: false,
            pkt_buf: vec![0u8; PKT_BUF_LEN],
            scratch: vec![0u8; MAX_CHUNK],
            needs_iteration: false,
            graceful_close_issued: false,
            last_handle_teardown: false,
            reads_ran_this_iter: false,
            read_budget: READ_BUDGET,
        };

        let handles = DriverHandles {
            cmd_tx,
            accept_bidi_rx,
            accept_uni_rx,
            established_rx: est_rx,
            shared,
        };

        (driver, handles)
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
    /// runnable-send drain. The Phase 5 close barrier will slot between (e) and
    /// teardown. Stages (d)/(e) run once per iteration on both paths, preserving
    /// the single-writable-scan-per-iteration contract (§5.3a).
    fn do_process_writes<C: QuicConn>(&mut self, qconn: &mut C) {
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
        self.stage_writable(qconn);
        self.stage_send(qconn);
        // Common per-iteration boundary: clear only the pump-selection flag.
        self.reads_ran_this_iter = false;
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
                self.pending_admit_order.push_back(id);
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
                            self.send_terminal_transition(id, SendEnd::Stopped { error_code: code });
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
                Err(quiche::Error::StreamStopped(code)) => Some(SendEnd::Stopped { error_code: code }),
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
                self.pending_admit_order.push_back(id);
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
                    state.blocked = false;
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
        for _ in 0..CHUNK_BUDGET {
            if self.read_budget == 0 {
                // Requeue keeping membership so the remainder drains next pump.
                self.requeue_readable(id);
                self.needs_iteration = true;
                return;
            }
            // Clone the sender so the reserved `Permit` does not borrow `self`,
            // freeing `&mut self.scratch` for `stream_recv`.
            let tx = match self.recv.get(&id) {
                Some(state) => state.bytes.clone(),
                None => return,
            };
            let permit = match tx.try_reserve() {
                Ok(permit) => permit,
                Err(TrySendError::Full(())) => {
                    // Full: leave bytes in quiche (flow control backpressures the
                    // peer); park until RecvResume. No stream_recv, no loss.
                    if let Some(state) = self.recv.get_mut(&id) {
                        state.blocked = true;
                    }
                    return;
                }
                Err(TrySendError::Closed(())) => {
                    // Dropped H3RecvStream: normal local abandonment (invariant 1).
                    self.abandon_recv(qconn, id);
                    return;
                }
            };
            match qconn.stream_recv(id, &mut self.scratch) {
                Ok((len, fin)) => {
                    if len > 0 {
                        let bytes = Bytes::copy_from_slice(&self.scratch[..len]);
                        permit.send(bytes);
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
                    if len == 0 || !qconn.stream_readable(id) {
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

    /// Phase 2: admit up to `ADMIT_BUDGET` ids from `pending_admit_order`
    /// (§5.1). A full accept queue parks **only that class**; the other class
    /// keeps admitting (parking is independent per class).
    fn phase2_admission<C: QuicConn>(&mut self, qconn: &mut C) {
        let mut budget = ADMIT_BUDGET;
        let mut bidi_blocked = false;
        let mut uni_blocked = false;
        // Ids of a blocked class are carried and restored to the front so their
        // relative order survives to the next pump.
        let mut carry: VecDeque<u64> = VecDeque::new();
        while budget > 0 {
            if bidi_blocked && uni_blocked {
                break;
            }
            let id = match self.pending_admit_order.pop_front() {
                Some(id) => id,
                None => break,
            };
            let bidi = is_bidi(id);
            if (bidi && bidi_blocked) || (!bidi && uni_blocked) {
                carry.push_back(id);
                continue;
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
        while let Some(id) = carry.pop_back() {
            self.pending_admit_order.push_front(id);
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
            let (send_handoff, send_state, send_done) =
                build_send(id, cmd_tx, peer.pending_send_terminal.take());
            if let Some(state) = recv_state {
                self.recv.insert(id, state);
            }
            // Retain the live send half so a later STOP_SENDING / Send / Reset
            // resolves through the SAME `status` cell the handle holds (§5.4
            // invariant 7). A send-terminal-at-admission bidi has no live half.
            if let Some(state) = send_state {
                self.send.insert(id, state);
            }
            self.admit
                .insert(id, AdmitState::Registered { send_done, recv_done });
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
        let (tx, rx) = mpsc::channel(BYTE_CHANNEL_DEPTH);
        let terminal = TerminalCell::new();
        let resume = Arc::new(AtomicBool::new(false));
        let recv_done = retained.is_some();
        if let Some(end) = retained {
            terminal.set(end);
        }
        let handoff = RecvHandoff {
            id,
            bytes: rx,
            terminal: terminal.clone(),
            resume: Arc::clone(&resume),
            cmd_tx,
        };
        let state = if recv_done {
            None
        } else {
            Some(StreamRecvState {
                bytes: tx,
                terminal,
                resume,
                blocked: false,
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
    /// no more bytes), drop cursor memberships, set `recv_done`, and run the
    /// terminal transition (§5.1 terminal transition).
    fn mark_recv_done(&mut self, id: u64) {
        self.recv.remove(&id);
        self.drop_recv_memberships(id);
        if let Some(AdmitState::Registered { recv_done, .. }) = self.admit.get_mut(&id) {
            *recv_done = true;
        }
        self.terminal_transition(id);
    }

    /// Normal local abandonment of a recv half (dropped `H3RecvStream`): issue an
    /// idempotent `stop_sending`, release the entry, never `InternalError`
    /// (invariant 1).
    fn abandon_recv<C: QuicConn>(&mut self, qconn: &mut C, id: u64) {
        let _ = qconn.stream_shutdown(id, Shutdown::Read, H3_NO_ERROR);
        self.mark_recv_done(id);
    }

    /// A stream-level `ConnGone` resolves via the connection terminal if one is
    /// already published, otherwise seals the recv half so it never spins
    /// (the connection close machine, Phases 4–5, owns the connection edge).
    fn resolve_recv_via_conn(&mut self, id: u64) {
        if let Some(terminal) = self.shared.conn_terminal.get() {
            self.publish_recv_terminal(id, RecvEnd::Conn(terminal));
        }
        self.mark_recv_done(id);
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
            Some(AdmitState::Registered { send_done, recv_done }) => {
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
                DriverCommand::Send { id, buf, done } => {
                    self.enqueue_send_op(id, SendOp::Write { buf, done });
                }
                DriverCommand::Finish { id, done } => {
                    self.enqueue_send_op(id, SendOp::Finish { done });
                }
                DriverCommand::Reset { id, code } => self.apply_reset(id, code),
                DriverCommand::StopSending { id, code } => {
                    let _ = qconn.stream_shutdown(id, Shutdown::Read, code);
                    self.mark_recv_done(id);
                }
                // Close / Open* / ConnectionDropped are driven by Phases 5–6.
                _ => {}
            }
        }
        if !self.inbox.is_empty() {
            // Budget-deferred commands remain in receipt order (§5.2).
            self.needs_iteration = true;
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

    /// Mark the send direction done (§5.3a): release runnable membership, set
    /// `send_done`, and run the contract-A terminal transition. The `send`
    /// registry entry is **retained** (with its sticky `terminal`) so an op that
    /// arrives after the terminal still completes immediately once; contract A
    /// removes it only when the recv direction is also terminal.
    fn mark_send_done(&mut self, id: u64) {
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
        let is_write = matches!(self.send.get(&id).and_then(|s| s.send_ops.front()), Some(SendOp::Write { .. }));
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
                // A reclaims an admitted bidi once its recv half also ends, and
                // a later Send/Finish completes via the retained sticky state
                // (§5.3a).
                self.mark_send_done(id);
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
                let end = self
                    .shared
                    .conn_terminal
                    .get()
                    .map(SendEnd::Conn)
                    .unwrap_or(SendEnd::Conn(Arc::new(ConnTerminal::Internal(
                        "stream_send after connection gone",
                    ))));
                self.send_terminal_transition(id, end);
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
        cmd_tx,
    };
    let state = if send_done {
        None
    } else {
        Some(StreamSendState {
            send_ops: VecDeque::new(),
            pending_reset: None,
            terminal: None,
            status,
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
        // The outbound packet buffer (PKT_BUF_LEN), NOT the MAX_CHUNK scratch.
        &mut self.pkt_buf
    }

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
        self.do_process_writes(qconn);
        Ok(())
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

    fn driver() -> (QuicheDriver<Bytes>, DriverHandles<Bytes>) {
        QuicheDriver::<Bytes>::new(4, 4)
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
        assert_eq!(ho.recv.bytes.try_recv().unwrap(), Bytes::from_static(b"hello"));
        assert!(matches!(ho.recv.terminal.get(), Some(RecvEnd::Fin)));
        // Nothing enqueued after the seal; no second admission.
        assert!(ho.recv.bytes.try_recv().is_err());
        assert!(h.accept_bidi_rx.try_recv().is_err());
    }

    /// §11: queued bytes then `RESET_STREAM` — queued bytes delivered, then
    /// `RecvEnd::Reset`; no bytes enqueued after the seal.
    #[test]
    fn queued_bytes_then_reset_delivers_then_seals() {
        let (mut d, mut h) = driver();
        let mut c = MockConn::new();
        c.script_recv(0, [data(b"data", false), RecvStep::Err(crate::quiche::Error::StreamReset(7))]);
        c.queue_readable([0]);
        d.read_budget = READ_BUDGET;
        d.run_read_pump(&mut c);

        let mut ho = h.accept_bidi_rx.try_recv().expect("one bidi handoff");
        assert_eq!(ho.recv.bytes.try_recv().unwrap(), Bytes::from_static(b"data"));
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
                blocked: false,
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
        assert!(d.recv.get(&0).unwrap().blocked);

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
        let (mut d, mut h) = QuicheDriver::<Bytes>::new(1, 1);
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
        d.pending_admit_order.push_back(0);
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

        let ho = h.accept_bidi_rx.try_recv().expect("admitted via writable path");
        assert_eq!(ho.recv.id, 0);
        assert!(matches!(
            d.admit.get(&0),
            Some(AdmitState::Registered { send_done: true, .. })
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
                blocked: false,
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
                blocked: false,
            },
        );
        d.admit
            .insert(0, AdmitState::Registered { send_done: true, recv_done: false });
        d.pending_readable.push_back(0);
        d.readable_set.insert(0);

        let mut c = MockConn::new();
        c.script_recv(0, (0..40).map(|i| data(&[b'a' + (i % 26) as u8], false)));

        // A packet iteration: process_reads runs the pump (budget-limited, so it
        // defers and sets needs_iteration), then process_writes runs.
        d.do_process_reads(&mut c);
        assert!(d.needs_iteration, "pump should defer under READ_BUDGET");
        d.do_process_writes(&mut c);
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

    fn push_send(
        d: &mut QuicheDriver<Bytes>,
        id: u64,
        payload: &'static [u8],
    ) -> oneshot::Receiver<Result<(), SendEnd>> {
        let (tx, rx) = oneshot::channel();
        d.inbox.push_back(DriverCommand::Send {
            id,
            buf: wbuf(payload),
            done: tx,
        });
        rx
    }

    fn push_finish(
        d: &mut QuicheDriver<Bytes>,
        id: u64,
    ) -> oneshot::Receiver<Result<(), SendEnd>> {
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
        let mut done = push_send(&mut d, 0, b"hello world");
        d.apply_inbox(&mut c);
        d.stage_send(&mut c);

        // Not fully accepted yet: no completion, and the blocked write re-armed.
        assert!(matches!(done.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
        let rearms: Vec<usize> = c.rearms.iter().filter(|(id, _)| *id == 0).map(|(_, len)| *len).collect();
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
        assert!(matches!(done.try_recv(), Ok(Ok(()))), "exactly one Ok at full acceptance");
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
        assert!(matches!(done.try_recv(), Err(oneshot::error::TryRecvError::Closed)));
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
        let mut done1 = push_send(&mut d, 0, b"a");
        d.apply_inbox(&mut c);
        d.stage_send(&mut c);
        assert!(matches!(done1.try_recv(), Ok(Ok(()))), "Write1 accepted before reset");

        // Write2 queued, then Reset preempts it in the same stage (a).
        let mut done2 = push_send(&mut d, 0, b"bcde");
        d.inbox.push_back(DriverCommand::Reset { id: 0, code: 42 });
        d.apply_inbox(&mut c);
        match done2.try_recv() {
            Ok(Err(SendEnd::Reset { error_code: 42 })) => {}
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
        d.admit
            .insert(0, AdmitState::Registered { send_done: false, recv_done: true });
        d.send.insert(0, StreamSendState::new());
        let mut c = MockConn::new();
        let mut fin = push_finish(&mut d, 0);
        d.apply_inbox(&mut c);
        d.stage_send(&mut c);
        assert!(matches!(fin.try_recv(), Ok(Ok(()))));
        // recv already done + send now done → contract A reclaimed admit + recv.
        assert!(!d.admit.contains_key(&0), "both directions terminal → admit dropped");
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
        d.admit
            .insert(0, AdmitState::Registered { send_done: false, recv_done: true });
        let mut c = MockConn::new();
        // Peer STOP_SENDING on the send half → send terminal + contract A (recv
        // already done) reclaims admit/recv but retains self.send's terminal.
        let mut w1 = push_send(&mut d, 0, b"aa");
        c.send_errors
            .entry(0)
            .or_default()
            .push_back(quiche::Error::StreamStopped(55));
        d.apply_inbox(&mut c);
        d.stage_send(&mut c);
        assert!(matches!(w1.try_recv(), Ok(Err(SendEnd::Stopped { error_code: 55 }))));
        assert!(!d.admit.contains_key(&0), "contract A reclaimed admit");
        assert!(d.send.contains_key(&0), "send retained for deferred ops");

        // A Send that was deferred past the terminal edge completes with the
        // sticky Stopped terminal (never a fabricated Internal / bare cancel).
        let mut late = push_send(&mut d, 0, b"bb");
        d.apply_inbox(&mut c);
        assert!(matches!(
            late.try_recv(),
            Ok(Err(SendEnd::Stopped { error_code: 55 }))
        ));
    }

    /// §11: peer `STOP_SENDING` observed on a `stream_send` call drains ALL
    /// remaining `send_ops` exactly once with `SendEnd::Stopped`, marks the send
    /// half done, and publishes the sticky terminal (invariant 13).
    #[test]
    fn stop_sending_on_send_drains_all_ops_once() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        let mut w1 = push_send(&mut d, 0, b"aa");
        let mut w2 = push_send(&mut d, 0, b"bb");
        let mut fin = push_finish(&mut d, 0);
        d.apply_inbox(&mut c);
        // The first stream_send call reports the peer stopped us.
        c.send_errors
            .entry(0)
            .or_default()
            .push_back(quiche::Error::StreamStopped(9));
        d.stage_send(&mut c);

        for (label, rx) in [("w1", &mut w1), ("w2", &mut w2), ("fin", &mut fin)] {
            match rx.try_recv() {
                Ok(Err(SendEnd::Stopped { error_code: 9 })) => {}
                other => panic!("{label} must complete once with Stopped, got {other:?}"),
            }
        }
        assert!(matches!(
            d.send.get(&0).unwrap().status.get(),
            Some(SendEnd::Stopped { error_code: 9 })
        ));
        assert!(d.send.get(&0).unwrap().send_ops.is_empty());
        assert!(!d.runnable_send_set.contains(&0), "runnable membership released");
    }

    /// §11 / invariant 13: peer `STOP_SENDING` surfaced on the **writable** path
    /// (stage (d) `stream_capacity` probe of a registered send id) resolves
    /// queued commands before runnable cleanup.
    #[test]
    fn stop_sending_via_writable_probe_drains_ops() {
        let (mut d, _h) = driver();
        let mut c = MockConn::new();
        let mut w1 = push_send(&mut d, 0, b"aa");
        let mut w2 = push_send(&mut d, 0, b"bb");
        d.apply_inbox(&mut c);
        // Stage (d) probes capacity and finds the stream stopped.
        c.writable_next.push_back(0);
        c.capacity.insert(0, Err(quiche::Error::StreamStopped(13)));
        d.stage_writable(&mut c);

        for (label, rx) in [("w1", &mut w1), ("w2", &mut w2)] {
            match rx.try_recv() {
                Ok(Err(SendEnd::Stopped { error_code: 13 })) => {}
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
        let (bulk_tx, mut bulk_done) = oneshot::channel();
        d.inbox.push_back(DriverCommand::Send {
            id: 0,
            buf: WriteBuf::from(h3::proto::frame::Frame::Data(Bytes::from_static(&BULK))),
            done: bulk_tx,
        });
        // Small stream 4 enqueued behind it.
        let mut small_done = push_send(&mut d, 4, b"z");
        d.apply_inbox(&mut c);
        d.stage_send(&mut c);

        // The small stream got its turn and completed despite the bulk backlog.
        assert!(matches!(small_done.try_recv(), Ok(Ok(()))), "small stream serviced");
        // The bulk stream is still in flight (not completed, still runnable).
        assert!(matches!(bulk_done.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
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
        let mut late = push_send(&mut d, 0, b"late");
        d.apply_inbox(&mut c);
        match late.try_recv() {
            Ok(Err(SendEnd::Reset { error_code: 5 })) => {}
            other => panic!("late Send must complete once with sticky terminal, got {other:?}"),
        }
        assert!(d.send.get(&0).unwrap().send_ops.is_empty(), "late op not enqueued");
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
        let (server_driver, server_handles) = QuicheDriver::<Bytes>::new(8, 8);
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
        let (client_driver, client_handles) = QuicheDriver::<Bytes>::new(8, 8);

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
        } = client_handles;
        let DriverHandles {
            cmd_tx: server_cmd_tx,
            accept_bidi_rx: _s_bidi,
            accept_uni_rx: _s_uni,
            established_rx: server_established_rx,
            shared: _s_shared,
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
