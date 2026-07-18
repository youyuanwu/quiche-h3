//! Phase 0 spike harness (design §14, §5.5).
//!
//! These tests stand up a real loopback QUIC client+server against the pinned
//! `tokio-quiche` 0.19.1 / `quiche` 0.29 build and observe load-bearing behavior
//! that the design depends on. They are `#[ignore]`d by default (they bind UDP
//! sockets and run handshakes); run with:
//!
//! ```text
//! cargo test -p quiche-h3 --test spike_harness -- --ignored --nocapture
//! ```
//!
//! This file starts with the harness foundation (a minimal `ApplicationOverQuic`
//! and a loopback connect). Individual spike probes (T1b, T2, T4, Q1–Q5, §5.5)
//! build on `MinimalApp` / `loopback`.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_quiche::metrics::{DefaultMetrics, Metrics};
use tokio_quiche::quic::connect_with_config;
use tokio_quiche::quic::QuicheConnection;
use tokio_quiche::QuicConnection;
use tokio_quiche::quiche;
use tokio_quiche::settings::{
    CertificateKind, Hooks, QuicSettings, TlsCertificatePaths,
};
use tokio_quiche::socket::Socket;
use tokio_quiche::{ApplicationOverQuic, ConnectionParams};

use futures::StreamExt;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};

// ---------------------------------------------------------------------------
// Cert fixtures
// ---------------------------------------------------------------------------

/// A self-signed cert + key written to temp PEM files (Phase 0 harness certs).
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
            "quiche-h3-spike-{}-{}",
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

// ---------------------------------------------------------------------------
// Minimal ApplicationOverQuic
// ---------------------------------------------------------------------------

/// The smallest driver that lets a handshake complete and then idles. Probes
/// override behavior by wrapping/extending this.
struct MinimalApp {
    pkt_buf: Vec<u8>,
    established: Arc<AtomicBool>,
}

impl MinimalApp {
    fn new(established: Arc<AtomicBool>) -> Self {
        Self {
            pkt_buf: vec![0u8; 1350],
            established,
        }
    }
}

impl ApplicationOverQuic for MinimalApp {
    fn on_conn_established(
        &mut self, _qconn: &mut QuicheConnection,
        _hs: &tokio_quiche::quic::HandshakeInfo,
    ) -> tokio_quiche::QuicResult<()> {
        self.established.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn should_act(&self) -> bool {
        self.established.load(Ordering::SeqCst)
    }

    fn buffer(&mut self) -> &mut [u8] {
        &mut self.pkt_buf
    }

    fn wait_for_data(
        &mut self, _qconn: &mut QuicheConnection,
    ) -> impl Future<Output = tokio_quiche::QuicResult<()>> + Send {
        // Idle: only packet/timer events drive the loop.
        std::future::pending()
    }

    fn process_reads(
        &mut self, _qconn: &mut QuicheConnection,
    ) -> tokio_quiche::QuicResult<()> {
        Ok(())
    }

    fn process_writes(
        &mut self, _qconn: &mut QuicheConnection,
    ) -> tokio_quiche::QuicResult<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Loopback wiring
// ---------------------------------------------------------------------------

fn client_params() -> ConnectionParams<'static> {
    let mut settings = QuicSettings::default();
    // Self-signed server cert on loopback: don't verify.
    settings.verify_peer = false;
    settings.max_idle_timeout = Some(std::time::Duration::from_secs(10));
    ConnectionParams::new_client(settings, None, Hooks::default())
}

fn server_params(certs: &TestCerts) -> ConnectionParams<'_> {
    let mut settings = QuicSettings::default();
    settings.max_idle_timeout = Some(std::time::Duration::from_secs(10));
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
#[ignore = "spike: binds UDP + runs a real handshake"]
async fn spike_foundation_handshake_completes() {
    let certs = TestCerts::generate();

    // --- server ---
    let server_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server_udp.local_addr().unwrap();
    let server_established = Arc::new(AtomicBool::new(false));

    let srv_est = server_established.clone();
    let mut listeners =
        tokio_quiche::listen([server_udp], server_params(&certs), DefaultMetrics)
            .expect("listen");

    let server_task = tokio::spawn(async move {
        let stream = &mut listeners[0];
        if let Some(Ok(conn)) = stream.next().await {
            conn.start(MinimalApp::new(srv_est));
            // Keep the listener task alive briefly so the worker can run.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    });

    // --- client ---
    let client_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client_udp.connect(server_addr).await.unwrap();
    let client_socket = Socket::try_from(client_udp).expect("socket");
    let client_established = Arc::new(AtomicBool::new(false));

    let params = client_params();
    let conn = connect_with_config(
        client_socket,
        Some("localhost"),
        &params,
        MinimalApp::new(client_established.clone()),
    )
    .await;

    assert!(
        conn.is_ok(),
        "client handshake future resolved Err: {:?}",
        conn.err()
    );
    // connect_with_config resolves AFTER the handshake, but on_conn_established
    // runs in the worker task, which may be scheduled slightly later (design §14
    // T2 nuance: the callback is not guaranteed to have run the instant the
    // future resolves). Poll both sides with a short timeout.
    assert!(
        wait_flag(&client_established, 1000).await,
        "client on_conn_established should run shortly after connect resolves"
    );
    assert!(
        wait_flag(&server_established, 1000).await,
        "server on_conn_established should have run"
    );

    drop(conn);
    let _ = server_task.await;
}

/// Poll an `AtomicBool` until true or `timeout_ms` elapses.
async fn wait_flag(flag: &Arc<AtomicBool>, timeout_ms: u64) -> bool {
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    while std::time::Instant::now() < deadline {
        if flag.load(Ordering::SeqCst) {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    flag.load(Ordering::SeqCst)
}

// ===========================================================================
// Probe driver infrastructure (shared by the §14 / §5.5 spike probes)
// ===========================================================================
//
// The worker task owns the `quiche::Connection`; the test can only touch it
// from inside `process_reads`/`process_writes`. `ProbeApp` therefore accepts
// closures (`Job`s) over an mpsc channel, runs them against `qconn` inside
// `process_writes`, and lets the test await their results over a oneshot. This
// gives each probe direct, synchronous access to the pinned quiche API on an
// established loopback connection.

type Job = Box<dyn FnOnce(&mut QuicheConnection) + Send>;

/// Observation captured from `on_conn_close` (peer/local `CONNECTION_CLOSE`).
#[allow(dead_code)] // `result_ok`/`local_error` are captured for observation/printing.
#[derive(Clone, Debug)]
struct CloseObs {
    result_ok: bool,
    peer_error: Option<(bool, u64, String)>,
    local_error: Option<(bool, u64, String)>,
}

fn conn_err_tuple(e: &quiche::ConnectionError) -> (bool, u64, String) {
    (
        e.is_app,
        e.error_code,
        String::from_utf8_lossy(&e.reason).into_owned(),
    )
}

/// A driver that runs test-submitted closures against the worker's `qconn`.
struct ProbeApp {
    pkt_buf: Vec<u8>,
    established: Arc<AtomicBool>,
    rx: mpsc::UnboundedReceiver<Job>,
    pending: VecDeque<Job>,
    close_obs: Arc<Mutex<Option<CloseObs>>>,
}

/// Test-side handle to a `ProbeApp` running in a worker task.
#[derive(Clone)]
struct ProbeHandle {
    tx: mpsc::UnboundedSender<Job>,
    established: Arc<AtomicBool>,
    close_obs: Arc<Mutex<Option<CloseObs>>>,
}

impl ProbeApp {
    fn pair() -> (ProbeApp, ProbeHandle) {
        let (tx, rx) = mpsc::unbounded_channel();
        let established = Arc::new(AtomicBool::new(false));
        let close_obs = Arc::new(Mutex::new(None));
        let app = ProbeApp {
            pkt_buf: vec![0u8; 1350],
            established: established.clone(),
            rx,
            pending: VecDeque::new(),
            close_obs: close_obs.clone(),
        };
        let handle = ProbeHandle {
            tx,
            established,
            close_obs,
        };
        (app, handle)
    }
}

impl ProbeHandle {
    /// Wait until the worker reports `on_conn_established`.
    async fn wait_established(&self, timeout_ms: u64) -> bool {
        wait_flag(&self.established, timeout_ms).await
    }

    /// Fire-and-forget a closure to run against `qconn` in the worker.
    fn submit(&self, f: impl FnOnce(&mut QuicheConnection) + Send + 'static) {
        let _ = self.tx.send(Box::new(f));
    }

    /// Run a closure against `qconn` in the worker and await its return value.
    async fn call<R: Send + 'static>(
        &self, f: impl FnOnce(&mut QuicheConnection) -> R + Send + 'static,
    ) -> R {
        let (otx, orx) = oneshot::channel();
        self.submit(move |q| {
            let _ = otx.send(f(q));
        });
        orx.await.expect("probe worker executed the submitted job")
    }

    /// Convenience: `call` with a timeout so a torn-down worker can't hang the
    /// test. Returns `None` on timeout / worker gone.
    async fn try_call<R: Send + 'static>(
        &self, timeout_ms: u64,
        f: impl FnOnce(&mut QuicheConnection) -> R + Send + 'static,
    ) -> Option<R> {
        tokio::time::timeout(Duration::from_millis(timeout_ms), self.call(f))
            .await
            .ok()
    }

    /// Poll for a captured close observation.
    async fn wait_close(&self, timeout_ms: u64) -> Option<CloseObs> {
        let deadline = std::time::Instant::now()
            + Duration::from_millis(timeout_ms);
        loop {
            if let Some(obs) = self.close_obs.lock().unwrap().clone() {
                return Some(obs);
            }
            if std::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

impl ApplicationOverQuic for ProbeApp {
    fn on_conn_established(
        &mut self, _qconn: &mut QuicheConnection,
        _hs: &tokio_quiche::quic::HandshakeInfo,
    ) -> tokio_quiche::QuicResult<()> {
        self.established.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn should_act(&self) -> bool {
        self.established.load(Ordering::SeqCst)
    }

    fn buffer(&mut self) -> &mut [u8] {
        &mut self.pkt_buf
    }

    fn wait_for_data(
        &mut self, _qconn: &mut QuicheConnection,
    ) -> impl Future<Output = tokio_quiche::QuicResult<()>> + Send {
        async move {
            // `mpsc::Receiver::recv` is cancel-safe: if a packet/timer wakes the
            // worker first and cancels this future, no queued job is lost.
            match self.rx.recv().await {
                Some(job) => {
                    self.pending.push_back(job);
                    Ok(())
                }
                // Sender dropped: idle forever (only packets/timers drive us).
                None => std::future::pending().await,
            }
        }
    }

    fn process_reads(
        &mut self, _qconn: &mut QuicheConnection,
    ) -> tokio_quiche::QuicResult<()> {
        Ok(())
    }

    fn process_writes(
        &mut self, qconn: &mut QuicheConnection,
    ) -> tokio_quiche::QuicResult<()> {
        while let Some(job) = self.pending.pop_front() {
            job(qconn);
        }
        Ok(())
    }

    fn on_conn_close<M: Metrics>(
        &mut self, qconn: &mut QuicheConnection, _metrics: &M,
        connection_result: &tokio_quiche::QuicResult<()>,
    ) {
        let obs = CloseObs {
            result_ok: connection_result.is_ok(),
            peer_error: qconn.peer_error().map(conn_err_tuple),
            local_error: qconn.local_error().map(conn_err_tuple),
        };
        *self.close_obs.lock().unwrap() = Some(obs);
    }
}

/// Server params that advertise `initial_max_data = 0`, i.e. grant the *client*
/// zero connection-level send capacity (client `tx_cap == 0` after handshake).
/// Stream-level limits stay at their defaults so the blocker is unambiguously
/// the *connection* capacity gate, not a stream-level one.
fn server_params_zero_grant(certs: &TestCerts) -> ConnectionParams<'_> {
    let mut settings = QuicSettings::default();
    settings.max_idle_timeout = Some(Duration::from_secs(10));
    settings.initial_max_data = 0;
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

/// Stand up a loopback client+server, both driven by `ProbeApp`s, and return
/// their test-side handles plus the client's `QuicConnection` metadata and the
/// server accept task. `certs` must outlive the returned values.
async fn probe_loopback(
    _certs: &TestCerts, client_params: &ConnectionParams<'_>,
    server_params: ConnectionParams<'_>,
) -> (ProbeHandle, ProbeHandle, QuicConnection, tokio::task::JoinHandle<()>) {
    let server_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server_udp.local_addr().unwrap();
    let (server_app, server_handle) = ProbeApp::pair();

    let mut listeners =
        tokio_quiche::listen([server_udp], server_params, DefaultMetrics)
            .expect("listen");

    let server_task = tokio::spawn(async move {
        let mut app = Some(server_app);
        let stream = &mut listeners[0];
        if let Some(Ok(conn)) = stream.next().await {
            if let Some(app) = app.take() {
                conn.start(app);
            }
            // Keep the listener/socket alive while the worker runs.
            tokio::time::sleep(Duration::from_secs(20)).await;
        }
    });

    let client_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client_udp.connect(server_addr).await.unwrap();
    let client_socket = Socket::try_from(client_udp).expect("socket");
    let (client_app, client_handle) = ProbeApp::pair();

    let conn =
        connect_with_config(client_socket, Some("localhost"), client_params, client_app)
            .await
            .expect("client handshake completed");

    (client_handle, server_handle, conn, server_task)
}

// ===========================================================================
// T2a — client rejects the server cert → connect future resolves Err
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spike: binds UDP + runs a real handshake"]
async fn spike_t2a_client_rejecting_cert_resolves_err() {
    let certs = TestCerts::generate();

    let server_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server_udp.local_addr().unwrap();
    let server_established = Arc::new(AtomicBool::new(false));
    let srv_est = server_established.clone();
    let mut listeners =
        tokio_quiche::listen([server_udp], server_params(&certs), DefaultMetrics)
            .expect("listen");
    let server_task = tokio::spawn(async move {
        if let Some(Ok(conn)) = listeners[0].next().await {
            conn.start(MinimalApp::new(srv_est));
            tokio::time::sleep(Duration::from_millis(800)).await;
        }
    });

    // Client that *verifies* the peer with no trust root → self-signed cert is
    // rejected during the TLS handshake.
    let client_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client_udp.connect(server_addr).await.unwrap();
    let client_socket = Socket::try_from(client_udp).expect("socket");
    let mut settings = QuicSettings::default();
    settings.verify_peer = true;
    settings.max_idle_timeout = Some(Duration::from_secs(3));
    let params = ConnectionParams::new_client(settings, None, Hooks::default());

    let established = Arc::new(AtomicBool::new(false));
    let conn = connect_with_config(
        client_socket,
        Some("localhost"),
        &params,
        MinimalApp::new(established.clone()),
    )
    .await;

    println!(
        "[T2a] connect_with_config(verify_peer=true, no root) resolved: {:?}",
        conn.as_ref().map(|_| "Ok").map_err(|e| format!("{e:?}"))
    );
    assert!(
        conn.is_err(),
        "verify_peer=true with no trust root must resolve Err, got Ok"
    );
    assert!(
        !established.load(Ordering::SeqCst),
        "client on_conn_established must not run on a rejected handshake (T2a gating)"
    );

    server_task.abort();
}

// ===========================================================================
// T2 — dropping the QuicConnection metadata handle does not tear down the worker
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spike: binds UDP + runs a real handshake"]
async fn spike_t2_handle_drop_keeps_worker_alive() {
    let certs = TestCerts::generate();
    let cparams = client_params();
    let sparams = server_params(&certs);
    let (client, server, conn, server_task) =
        probe_loopback(&certs, &cparams, sparams).await;

    assert!(client.wait_established(2000).await, "client established");
    assert!(server.wait_established(2000).await, "server established");

    // Drop the metadata handle. Per the design this is NOT the quiche::Connection.
    drop(conn);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The client worker must still be alive and processing jobs.
    let client_state = client
        .try_call(2000, |q| (q.is_established(), q.is_closed()))
        .await;
    println!(
        "[T2] client (is_established, is_closed) after handle drop = {:?}",
        client_state
    );
    assert_eq!(
        client_state,
        Some((true, false)),
        "client worker must survive QuicConnection handle drop"
    );

    // The server peer must still see the connection as established / not closed.
    let server_state = server
        .try_call(2000, |q| (q.is_established(), q.is_closed()))
        .await;
    println!(
        "[T2] server (is_established, is_closed) after client handle drop = {:?}",
        server_state
    );
    assert_eq!(
        server_state,
        Some((true, false)),
        "server peer must remain established shortly after client handle drop"
    );

    server_task.abort();
}

// ===========================================================================
// T1b — peer promptly observes an application CONNECTION_CLOSE
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spike: binds UDP + runs a real handshake"]
async fn spike_t1b_peer_observes_application_close() {
    let certs = TestCerts::generate();
    let cparams = client_params();
    let sparams = server_params(&certs);
    let (client, server, conn, server_task) =
        probe_loopback(&certs, &cparams, sparams).await;

    assert!(client.wait_established(2000).await, "client established");
    assert!(server.wait_established(2000).await, "server established");

    // Client stages an application close inside process_writes (the ordinary
    // post-process_writes flush should serialize the CONNECTION_CLOSE).
    client.submit(|q| {
        let _ = q.close(true, 0x1234, b"t1b-bye");
    });

    let obs = server.wait_close(3000).await;
    println!("[T1b] server on_conn_close observation = {:?}", obs);
    let obs = obs.expect("server must observe the peer close");
    let (is_app, code, reason) = obs
        .peer_error
        .expect("server must record a peer CONNECTION_CLOSE");
    assert!(is_app, "close was an application close");
    assert_eq!(code, 0x1234, "peer error code round-trips");
    assert_eq!(reason, "t1b-bye", "peer reason round-trips");

    drop(conn);
    server_task.abort();
}

// ===========================================================================
// T4 — garbage datagram before a real handshake; listener stays serving
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spike: binds UDP + runs a real handshake"]
async fn spike_t4_garbage_datagram_then_real_connection() {
    let certs = TestCerts::generate();

    let server_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server_udp.local_addr().unwrap();
    let (server_app, server_handle) = ProbeApp::pair();
    let mut listeners =
        tokio_quiche::listen([server_udp], server_params(&certs), DefaultMetrics)
            .expect("listen");

    // Fire garbage UDP at the server port BEFORE any real handshake.
    let attacker = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let garbage = vec![0xABu8; 1200];
    attacker.send_to(&garbage, server_addr).await.unwrap();
    attacker.send_to(&[0x00, 0x01, 0x02, 0x03], server_addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drive the accept stream and record item kinds (Ok vs Err variant).
    let (item_tx, mut item_rx) = mpsc::unbounded_channel::<Result<(), String>>();
    let server_task = tokio::spawn(async move {
        let mut app = Some(server_app);
        loop {
            match tokio::time::timeout(
                Duration::from_secs(4),
                listeners[0].next(),
            )
            .await
            {
                Ok(Some(Ok(conn))) => {
                    let _ = item_tx.send(Ok(()));
                    if let Some(app) = app.take() {
                        conn.start(app);
                    }
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    break;
                }
                Ok(Some(Err(e))) => {
                    let _ = item_tx.send(Err(format!("{e:?}")));
                }
                Ok(None) => break,
                Err(_) => break, // no item within timeout
            }
        }
    });

    // Now a real client connects.
    let client_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client_udp.connect(server_addr).await.unwrap();
    let client_socket = Socket::try_from(client_udp).expect("socket");
    let (client_app, client_handle) = ProbeApp::pair();
    let conn = connect_with_config(
        client_socket,
        Some("localhost"),
        &client_params(),
        client_app,
    )
    .await;
    println!(
        "[T4] real client connect after garbage: {:?}",
        conn.as_ref().map(|_| "Ok").map_err(|e| format!("{e:?}"))
    );
    assert!(
        conn.is_ok(),
        "real client must still connect after garbage datagrams: {:?}",
        conn.err()
    );
    assert!(
        client_handle.wait_established(2000).await
            && server_handle.wait_established(2000).await,
        "both sides establish after garbage"
    );

    // Collect any accept-stream items observed.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut errs = Vec::new();
    let mut oks = 0usize;
    while let Ok(item) = item_rx.try_recv() {
        match item {
            Ok(()) => oks += 1,
            Err(e) => errs.push(e),
        }
    }
    println!(
        "[T4] accept-stream items: Ok(conn)={} Err-item-variants={:?}",
        oks, errs
    );
    assert!(oks >= 1, "listener must yield the real connection after garbage");

    let _ = conn.map(drop);
    server_task.abort();
}

// ===========================================================================
// Q1 — destructive readable cursor + stream_priority materialization
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spike: binds UDP + runs a real handshake"]
async fn spike_q1_readable_destructive_and_priority_materialize() {
    let certs = TestCerts::generate();
    let cparams = client_params();
    let sparams = server_params(&certs);
    let (client, server, conn, server_task) =
        probe_loopback(&certs, &cparams, sparams).await;

    assert!(client.wait_established(2000).await, "client established");
    assert!(server.wait_established(2000).await, "server established");

    // (b) stream_priority materialization + one-credit / idempotency.
    let before_left = client.call(|q| q.peer_streams_left_bidi()).await;
    let mat = client
        .call(|q| {
            let exists_before = q.stream_capacity(0).is_ok();
            let p1 = format!("{:?}", q.stream_priority(0, 0, true));
            let cap1 = format!("{:?}", q.stream_capacity(0));
            // Idempotent second call with identical priority.
            let p2 = format!("{:?}", q.stream_priority(0, 0, true));
            let cap2 = format!("{:?}", q.stream_capacity(0));
            (exists_before, p1, cap1, p2, cap2)
        })
        .await;
    let after_left = client.call(|q| q.peer_streams_left_bidi()).await;
    println!(
        "[Q1b] exists_before={} priority1={} cap1={} priority2={} cap2={} peer_streams_left_bidi {}->{}",
        mat.0, mat.1, mat.2, mat.3, mat.4, before_left, after_left
    );
    assert!(!mat.0, "stream 0 must not exist before stream_priority");
    assert!(mat.1.starts_with("Ok"), "first stream_priority Ok");
    assert!(mat.2.starts_with("Ok"), "stream 0 known after priority");
    assert!(mat.3.starts_with("Ok"), "second stream_priority idempotent Ok");
    assert!(mat.4.starts_with("Ok"), "stream 0 still known after 2nd priority");

    // Positive control for the §5.5 blocker: under a normal grant a
    // materialized+written stream IS surfaced by writable discovery.
    let writable_pos = client
        .call(|q| {
            let sent = format!("{:?}", q.stream_send(0, b"x", false));
            let wn = q.stream_writable_next();
            (sent, wn)
        })
        .await;
    println!(
        "[Q1b] positive-control: stream_send={} stream_writable_next={:?}",
        writable_pos.0, writable_pos.1
    );
    assert_eq!(
        writable_pos.1,
        Some(0),
        "under normal capacity a writable stream is surfaced (blocker positive control)"
    );

    // (a) destructive stream_readable_next: server sends on server-initiated
    // unidirectional stream 3 → client sees it readable exactly once.
    server.submit(|q| {
        let _ = q.stream_send(3, b"hello-uni", true);
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let rd = client
        .call(|q| {
            let first = q.stream_readable_next();
            let second = q.stream_readable_next();
            (first, second)
        })
        .await;
    println!(
        "[Q1a] stream_readable_next first={:?} second={:?}",
        rd.0, rd.1
    );
    assert_eq!(rd.0, Some(3), "first readable_next returns the armed stream");
    assert_eq!(rd.1, None, "second readable_next is dearmed (destructive)");

    drop(conn);
    server_task.abort();
}

// ===========================================================================
// Q2 — Connection::close first Ok, repeat Done
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spike: binds UDP + runs a real handshake"]
async fn spike_q2_close_first_ok_repeat_done() {
    let certs = TestCerts::generate();
    let cparams = client_params();
    let sparams = server_params(&certs);
    let (client, _server, conn, server_task) =
        probe_loopback(&certs, &cparams, sparams).await;

    assert!(client.wait_established(2000).await, "client established");

    let res = client
        .call(|q| {
            let first = format!("{:?}", q.close(true, 0x100, b"bye"));
            let second = format!("{:?}", q.close(true, 0x100, b"bye"));
            (first, second)
        })
        .await;
    println!("[Q2] first close={} second close={}", res.0, res.1);
    assert!(res.0.starts_with("Ok"), "first close accepted (Ok)");
    assert!(res.1.contains("Done"), "repeated close returns Error::Done");

    drop(conn);
    server_task.abort();
}

// ===========================================================================
// Q3 — stream_shutdown(Write) resets a stream at zero send capacity
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spike: binds UDP + runs a real handshake"]
async fn spike_q3_stream_shutdown_write_resets_without_capacity() {
    let certs = TestCerts::generate();
    let cparams = client_params();
    let sparams = server_params_zero_grant(&certs);
    let (client, server, conn, server_task) =
        probe_loopback(&certs, &cparams, sparams).await;

    assert!(client.wait_established(2000).await, "client established");
    assert!(server.wait_established(2000).await, "server established");

    // Materialize stream 0 and confirm zero connection-level capacity.
    let cap = client
        .call(|q| {
            let _ = q.stream_priority(0, 0, true);
            let data = format!("{:?}", q.stream_send(0, b"payload", false));
            let cap = format!("{:?}", q.stream_capacity(0));
            (data, cap)
        })
        .await;
    println!(
        "[Q3] zero-grant: stream_send(data)={} stream_capacity(0)={}",
        cap.0, cap.1
    );
    assert!(
        cap.1.contains("Ok(0)"),
        "client stream 0 must have zero send capacity"
    );

    // Reset the send side at zero capacity.
    let sh = client
        .call(|q| format!("{:?}", q.stream_shutdown(0, quiche::Shutdown::Write, 0x1)))
        .await;
    println!("[Q3] stream_shutdown(Write, 0x1) at zero cap = {}", sh);
    assert!(sh.starts_with("Ok"), "shutdown(Write) accepted at zero cap");

    // Peer must observe RESET_STREAM without any MAX_DATA grant.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let peer = server
        .call(|q| {
            let mut buf = [0u8; 32];
            format!("{:?}", q.stream_recv(0, &mut buf))
        })
        .await;
    println!("[Q3] server stream_recv(0) after reset = {}", peer);
    assert!(
        peer.contains("StreamReset(1)"),
        "peer must observe RESET_STREAM with the supplied code, got {peer}"
    );

    drop(conn);
    server_task.abort();
}

// ===========================================================================
// Q4 — STOP_SENDING surfaces as StreamStopped(code) via stream_capacity
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spike: binds UDP + runs a real handshake"]
async fn spike_q4_stop_sending_surfaces_stream_stopped() {
    let certs = TestCerts::generate();
    let cparams = client_params();
    let sparams = server_params(&certs);
    let (client, server, conn, server_task) =
        probe_loopback(&certs, &cparams, sparams).await;

    assert!(client.wait_established(2000).await, "client established");
    assert!(server.wait_established(2000).await, "server established");

    // Client opens bidi stream 0 and sends data.
    client.submit(|q| {
        let _ = q.stream_priority(0, 0, true);
        let _ = q.stream_send(0, b"hello", false);
    });
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Server reads then STOP_SENDINGs the client's send side (Shutdown::Read).
    let srv = server
        .call(|q| {
            let mut buf = [0u8; 32];
            let recv = format!("{:?}", q.stream_recv(0, &mut buf));
            let sh = format!("{:?}", q.stream_shutdown(0, quiche::Shutdown::Read, 0x42));
            (recv, sh)
        })
        .await;
    println!("[Q4] server recv={} shutdown(Read,0x42)={}", srv.0, srv.1);
    assert!(srv.1.starts_with("Ok"), "server shutdown(Read) accepted");

    // Client's send side must now report StreamStopped(0x42) (66 decimal).
    tokio::time::sleep(Duration::from_millis(400)).await;
    let cap = client
        .call(|q| format!("{:?}", q.stream_capacity(0)))
        .await;
    println!("[Q4] client stream_capacity(0) after STOP_SENDING = {}", cap);
    assert!(
        cap.contains("StreamStopped(66)"),
        "client must see StreamStopped with the peer's code 0x42=66, got {cap}"
    );

    drop(conn);
    server_task.abort();
}

// ===========================================================================
// Q5 — pure FIN accepted and flushed at zero connection send capacity
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spike: binds UDP + runs a real handshake"]
async fn spike_q5_zero_capacity_fin_accepted_and_flushed() {
    let certs = TestCerts::generate();
    let cparams = client_params();
    let sparams = server_params_zero_grant(&certs);
    let (client, server, conn, server_task) =
        probe_loopback(&certs, &cparams, sparams).await;

    assert!(client.wait_established(2000).await, "client established");
    assert!(server.wait_established(2000).await, "server established");

    let res = client
        .call(|q| {
            let _ = q.stream_priority(0, 0, true);
            let cap = format!("{:?}", q.stream_capacity(0));
            // Data at zero capacity is refused...
            let data = format!("{:?}", q.stream_send(0, b"x", false));
            // ...but a pure FIN (len == 0, fin) is accepted.
            let fin = format!("{:?}", q.stream_send(0, &[], true));
            (cap, data, fin)
        })
        .await;
    println!(
        "[Q5] zero-grant: capacity={} data_send={} fin_send={}",
        res.0, res.1, res.2
    );
    assert!(res.0.contains("Ok(0)"), "zero connection send capacity");
    assert!(
        res.1.contains("Done"),
        "data write at zero cap returns Error::Done, got {}",
        res.1
    );
    assert!(
        res.2.starts_with("Ok"),
        "pure FIN accepted at zero cap, got {}",
        res.2
    );

    // Peer must observe the FIN without any MAX_DATA grant.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let peer = server
        .call(|q| {
            let mut buf = [0u8; 32];
            let recv = q.stream_recv(0, &mut buf);
            let fin_seen = matches!(recv, Ok((_, true)));
            (format!("{recv:?}"), fin_seen, q.stream_finished(0))
        })
        .await;
    println!(
        "[Q5] server stream_recv(0)={} fin_seen={} stream_finished={}",
        peer.0, peer.1, peer.2
    );
    assert!(
        peer.1 || peer.2,
        "server must observe the FIN on stream 0, got recv={} finished={}",
        peer.0, peer.2
    );

    drop(conn);
    server_task.abort();
}

// ===========================================================================
// §5.5 BLOCKER — zero tx_cap hides an otherwise-writable stream from discovery
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spike: binds UDP + runs a real handshake"]
async fn spike_5_5_blocker_zero_txcap_hides_writable_discovery() {
    let certs = TestCerts::generate();
    let cparams = client_params();
    let sparams = server_params_zero_grant(&certs);
    let (client, _server, conn, server_task) =
        probe_loopback(&certs, &cparams, sparams).await;

    assert!(client.wait_established(2000).await, "client established");

    let r = client
        .call(|q| {
            // Materialize + attempt a write so the stream is otherwise writable
            // (it has stream-level capacity); only the connection tx_cap is 0.
            let _ = q.stream_priority(0, 0, true);
            let attempt = format!("{:?}", q.stream_send(0, b"payload", false));
            let known = format!("{:?}", q.stream_capacity(0));
            let writable_next = q.stream_writable_next();
            let writable_count = q.writable().count();
            (attempt, known, writable_next, writable_count)
        })
        .await;
    println!(
        "[§5.5 BLOCKER] stream_send={} stream_capacity(0)={} stream_writable_next()={:?} writable().count()={}",
        r.0, r.1, r.2, r.3
    );
    assert!(
        r.1.contains("Ok(0)"),
        "stream 0 is materialized/known (Ok capacity) but at zero conn cap"
    );
    assert_eq!(
        r.2, None,
        "stream_writable_next() returns None at tx_cap==0 (blocker confirmed)"
    );
    assert_eq!(
        r.3, 0,
        "writable() is empty at tx_cap==0 (blocker confirmed)"
    );

    drop(conn);
    server_task.abort();
}

// ===========================================================================
// §5.5 tombstone — a fully-terminal stream id never reappears in discovery
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spike: binds UDP + runs a real handshake"]
async fn spike_5_5_tombstone_terminal_id_never_reappears() {
    let certs = TestCerts::generate();
    let cparams = client_params();
    let sparams = server_params(&certs);
    let (client, server, conn, server_task) =
        probe_loopback(&certs, &cparams, sparams).await;

    assert!(client.wait_established(2000).await, "client established");
    assert!(server.wait_established(2000).await, "server established");

    // Client sends ping+FIN on bidi stream 0.
    client.submit(|q| {
        let _ = q.stream_priority(0, 0, true);
        let _ = q.stream_send(0, b"ping", true);
    });
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Server reads ping+FIN and replies pong+FIN, completing both directions.
    server.submit(|q| {
        let mut buf = [0u8; 32];
        let _ = q.stream_recv(0, &mut buf);
        let _ = q.stream_send(0, b"pong", true);
    });
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Client reads pong+FIN → stream 0 fully terminal, then probe discovery.
    let r = client
        .call(|q| {
            let mut buf = [0u8; 32];
            let recv = format!("{:?}", q.stream_recv(0, &mut buf));
            let finished = q.stream_finished(0);
            let readable_next = q.stream_readable_next();
            let writable_next = q.stream_writable_next();
            let readable_has0 = q.readable().any(|id| id == 0);
            let writable_has0 = q.writable().any(|id| id == 0);
            (recv, finished, readable_next, writable_next, readable_has0, writable_has0)
        })
        .await;
    println!(
        "[§5.5 tombstone] recv={} finished={} readable_next={:?} writable_next={:?} readable_has0={} writable_has0={}",
        r.0, r.1, r.2, r.3, r.4, r.5
    );
    assert!(
        r.0.contains("true"),
        "client must observe pong+FIN (recv fin=true), got {}",
        r.0
    );
    assert!(!r.4, "completed stream 0 must not appear in readable()");
    assert!(!r.5, "completed stream 0 must not appear in writable()");
    assert_ne!(r.2, Some(0), "completed stream 0 must not re-arm readable");
    assert_ne!(r.3, Some(0), "completed stream 0 must not re-arm writable");

    // Second probe pass after a short idle: still absent.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let again = client
        .call(|q| {
            (
                q.readable().any(|id| id == 0),
                q.writable().any(|id| id == 0),
            )
        })
        .await;
    println!("[§5.5 tombstone] second pass readable_has0={} writable_has0={}", again.0, again.1);
    assert!(!again.0 && !again.1, "terminal id 0 stays absent");

    drop(conn);
    server_task.abort();
}
