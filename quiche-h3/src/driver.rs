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

use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use tokio::sync::{mpsc, oneshot};

use tokio_quiche::quic::HandshakeInfo;
use tokio_quiche::quic::QuicheConnection;
use tokio_quiche::{ApplicationOverQuic, QuicResult};

use crate::buffer::{TerminalCell, MAX_CHUNK, PKT_BUF_LEN};
use crate::error::{ConnTerminal, RecvEnd, SendEnd};

/// Shared per-iteration chunk budget for the read pump (§5.1, provisional §12
/// S3). One `ReadBudget` is threaded through all receive draining in a single
/// helper invocation.
const READ_BUDGET: usize = 32;

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

    /// The shared read pump (§5.1). Stubbed in Phase 2; implemented in Phase 3.
    fn run_read_pump(&mut self, _qconn: &mut QuicheConnection) {
        // Phases 3–5: bounded destructive intake, registered-drain, admission,
        // parked-promotion — all sharing `self.read_budget`.
    }

    /// Apply queued control commands and run the send/close stages (§5.2/§5.3).
    /// Stubbed in Phase 2; implemented in Phases 3–5. For now, commands are not
    /// yet driven, so the inbox is simply cleared to keep the loop live.
    fn apply_inbox(&mut self, _qconn: &mut QuicheConnection) {
        // Phases 3–5: stage (a) command application, (e) runnable send, close
        // barrier, etc. No front-end handle drives commands until then.
        self.inbox.clear();
    }
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
        // Packet-driven acting iteration: reset the shared ReadBudget, claim the
        // single per-iteration pump invocation, and run it (§2.3, §5.1).
        self.read_budget = READ_BUDGET;
        self.reads_ran_this_iter = true;
        self.run_read_pump(qconn);
        Ok(())
    }

    fn process_writes(&mut self, qconn: &mut QuicheConnection) -> QuicResult<()> {
        // Stage (a)+: apply queued commands (stubbed in Phase 2).
        self.apply_inbox(qconn);

        // No-packet acting iteration: process_reads was skipped, so run the
        // pump here with a fresh budget (§2.3 finding, §5.1).
        if !self.reads_ran_this_iter {
            self.read_budget = READ_BUDGET;
            self.run_read_pump(qconn);
        }

        // Common per-iteration boundary: clear the pump flag. In Phase 2 no
        // stage defers work, so clear needs_iteration too (Phases 3–5 set it
        // per deferred stage and clear it once serviced).
        self.reads_ran_this_iter = false;
        self.needs_iteration = false;
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
