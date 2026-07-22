//! Endpoint graceful-shutdown loopback tests (design
//! `docs/design/quiche-h3-endpoint-shutdown.md`, §5/§11). Stands up a real
//! `H3QuicheAcceptor` over loopback, establishes real handshakes through
//! hyperium `h3`, and exercises the [`H3QuicheEndpoint`] control surface
//! (`close` + `wait_idle`) end to end.
//!
//! `#[ignore]`d because they bind UDP and run real handshakes. Run with:
//!
//! ```text
//! cargo test -p quiche-h3 --test endpoint_shutdown -- --ignored --nocapture
//! ```
//!
//! The empirical shutdown spike tests (S1 rebind, S2 admission fence, S3
//! mid-handshake bound — design §5.6/§11) live at the bottom of this file; they
//! reuse the loopback helpers above. Their recorded outcomes are in
//! `SPIKE_OUTCOMES.md` (the Phase 0 harness spikes remain in `spike_harness.rs`).

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use quiche_h3::{H3QuicheAcceptor, H3QuicheClientConfig, H3QuicheConnector, H3QuicheServerConfig};
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;

/// Generous per-test deadline so a hang fails fast instead of blocking CI.
const DEADLINE: Duration = Duration::from_secs(10);

/// A self-signed cert + key written to temp PEM files (mirrors h3_e2e.rs).
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
            "quiche-h3-shutdown-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let cert_path = dir.join(format!("{uniq}.crt"));
        let key_path = dir.join(format!("{uniq}.key"));
        std::fs::write(&cert_path, ck.cert.pem()).expect("write cert");
        std::fs::write(&key_path, ck.signing_key.serialize_pem()).expect("write key");
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

fn server_config(certs: &TestCerts) -> H3QuicheServerConfig {
    let mut settings = tokio_quiche::settings::QuicSettings::default();
    settings.max_idle_timeout = Some(Duration::from_secs(10));
    H3QuicheServerConfig {
        cert_path: certs.cert_path.clone(),
        key_path: certs.key_path.clone(),
        settings,
        ..H3QuicheServerConfig::default()
    }
}

fn client_config() -> H3QuicheClientConfig {
    let mut settings = tokio_quiche::settings::QuicSettings::default();
    settings.max_idle_timeout = Some(Duration::from_secs(10));
    H3QuicheClientConfig {
        settings,
        verify_peer: false,
        ..H3QuicheClientConfig::default()
    }
}

/// Spawn an h3 client that connects, completes the h3 handshake, keeps the
/// connection open (holding `send_request`), and drives the connection until it
/// closes — returning the `poll_close` result so the caller can assert the
/// connection terminated after the server shutdown.
fn spawn_client(server_addr: SocketAddr) -> JoinHandle<h3::error::ConnectionError> {
    tokio::spawn(async move {
        let connector =
            H3QuicheConnector::new(server_addr, "localhost".to_string(), client_config())
                .expect("build connector");
        let conn = connector.connect().await.expect("client connect ok");
        let (mut driver, send_request) = h3::client::new(conn).await.expect("h3 client handshake");
        // Hold `send_request` so the client does not itself initiate close.
        let _keep_open = send_request;
        futures::future::poll_fn(|cx| driver.poll_close(cx)).await
    })
}

/// Spawn a server drive loop over an already-accepted front-end connection: run
/// the h3 server handshake and accept (and ignore) requests until the
/// connection ends.
fn spawn_server_drive(conn: quiche_h3::Connection<bytes::Bytes>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut h3_conn = match h3::server::Connection::new(conn).await {
            Ok(c) => c,
            Err(_) => return,
        };
        // Accept (and ignore) requests until the connection ends.
        while let Ok(Some(_resolver)) = h3_conn.accept().await {}
    })
}

/// Poll the endpoint's test-only registry snapshot until `live` reaches
/// `target`, or panic past the deadline.
async fn wait_for_live(endpoint: &quiche_h3::H3QuicheEndpoint, target: usize, deadline: Duration) {
    let start = Instant::now();
    loop {
        if endpoint.__test_registry_snapshot().1 == target {
            return;
        }
        if start.elapsed() > deadline {
            panic!(
                "live worker count never reached {target} (last = {})",
                endpoint.__test_registry_snapshot().1
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Sanity (§5.5): with a live connection, `close()` broadcasts to the worker and
/// `wait_idle()` resolves once it drains; the peer observes the connection close.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "binds UDP + runs a real handshake"]
async fn close_then_wait_idle_drains_the_connection() {
    let certs = TestCerts::generate();
    let server_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server_udp.local_addr().unwrap();
    let mut acceptors =
        H3QuicheAcceptor::bind([server_udp], &server_config(&certs)).expect("bind acceptor");
    let mut acceptor = acceptors.pop().unwrap();
    let endpoint = acceptor.endpoint();

    let client = spawn_client(server_addr);

    let conn = tokio::time::timeout(DEADLINE, acceptor.accept())
        .await
        .expect("accept did not hang")
        .expect("accept ok")
        .expect("a connection was accepted");
    let server_drive = spawn_server_drive(conn);

    // The worker is registered once established.
    wait_for_live(&endpoint, 1, DEADLINE).await;

    // Graceful shutdown: broadcast the close, then await full drain.
    endpoint.close(h3::error::Code::H3_NO_ERROR, b"server shutting down");
    tokio::time::timeout(DEADLINE, endpoint.wait_idle())
        .await
        .expect("wait_idle resolved after close()");
    assert_eq!(
        endpoint.__test_registry_snapshot().1,
        0,
        "no live workers remain after wait_idle"
    );

    // The peer's connection terminates as a result of the server close. `h3`
    // preserves the application close *code* (mapped to a remote
    // `ApplicationClose`), though not the reason bytes (error.rs to_h3, §8.4).
    let client_err = tokio::time::timeout(DEADLINE, client)
        .await
        .expect("client driver terminated after server close")
        .expect("client task did not panic");
    assert!(
        client_err.is_h3_no_error(),
        "peer should observe the H3_NO_ERROR application close broadcast by \
         close(); got: {client_err:?}"
    );

    server_drive.abort();
}

/// P2 acceptor-independent lifetime (§5.2): a cloned endpoint handle still
/// reaches the live worker and drains it after the acceptor has been dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "binds UDP + runs a real handshake"]
async fn endpoint_handle_outlives_the_acceptor() {
    let certs = TestCerts::generate();
    let server_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server_udp.local_addr().unwrap();
    let mut acceptors =
        H3QuicheAcceptor::bind([server_udp], &server_config(&certs)).expect("bind acceptor");
    let mut acceptor = acceptors.pop().unwrap();
    // Clone the handle to prove a clone (not the origin acceptor) drives shutdown.
    let endpoint = acceptor.endpoint();
    let endpoint_clone = endpoint.clone();

    let client = spawn_client(server_addr);

    let conn = tokio::time::timeout(DEADLINE, acceptor.accept())
        .await
        .expect("accept did not hang")
        .expect("accept ok")
        .expect("a connection was accepted");
    let server_drive = spawn_server_drive(conn);
    wait_for_live(&endpoint, 1, DEADLINE).await;

    // Drop the acceptor entirely; the established worker must remain live and
    // reachable through the handle.
    drop(acceptor);
    assert_eq!(
        endpoint.__test_registry_snapshot().1,
        1,
        "dropping the acceptor must not kill the established worker"
    );

    // Shutdown driven purely through the surviving clone.
    endpoint_clone.close(h3::error::Code::H3_NO_ERROR, b"bye");
    tokio::time::timeout(DEADLINE, endpoint_clone.wait_idle())
        .await
        .expect("wait_idle resolved through the clone after acceptor drop");

    tokio::time::timeout(DEADLINE, client)
        .await
        .expect("client terminated after close via clone")
        .expect("client task did not panic");

    server_drive.abort();
}

/// Multi-socket sharing (FR-002/§5.1): two acceptors from one `bind()` share a
/// single endpoint registry, so a connection established on the second socket is
/// visible through the endpoint obtained from the first, and one `close()`
/// drains it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "binds UDP + runs a real handshake"]
async fn endpoint_registry_is_shared_across_sockets() {
    let certs = TestCerts::generate();
    let udp0 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr1 = udp1.local_addr().unwrap();

    let mut acceptors =
        H3QuicheAcceptor::bind([udp0, udp1], &server_config(&certs)).expect("bind two acceptors");
    assert_eq!(acceptors.len(), 2, "two sockets → two acceptors");
    // `bind` order is preserved: acceptors[1] owns udp1.
    let mut acceptor1 = acceptors.pop().unwrap();
    let acceptor0 = acceptors.pop().unwrap();

    // The endpoint comes from the FIRST acceptor, but must observe the SECOND
    // acceptor's registrations (shared registry).
    let endpoint = acceptor0.endpoint();

    let client = spawn_client(addr1);
    let conn = tokio::time::timeout(DEADLINE, acceptor1.accept())
        .await
        .expect("accept did not hang")
        .expect("accept ok")
        .expect("a connection was accepted on socket 1");
    let server_drive = spawn_server_drive(conn);

    // Cross-socket visibility: the socket-1 connection shows up in socket-0's
    // endpoint view.
    wait_for_live(&endpoint, 1, DEADLINE).await;

    endpoint.close(h3::error::Code::H3_NO_ERROR, b"shared shutdown");
    tokio::time::timeout(DEADLINE, endpoint.wait_idle())
        .await
        .expect("wait_idle drained the cross-socket connection");

    tokio::time::timeout(DEADLINE, client)
        .await
        .expect("client terminated after shared close")
        .expect("client task did not panic");

    drop(acceptor1);
    server_drive.abort();
}

// ===========================================================================
// Empirical shutdown spikes (design §5.6 / §11). Recorded in SPIKE_OUTCOMES.md.
// ===========================================================================

/// Build a server config, letting the caller tweak the `QuicSettings` (used by
/// S3 to install a finite `handshake_timeout` and disable client-IP validation
/// so a single raw Initial spawns a worker immediately).
fn server_config_with(
    certs: &TestCerts,
    tweak: impl FnOnce(&mut tokio_quiche::settings::QuicSettings),
) -> H3QuicheServerConfig {
    let mut cfg = server_config(certs);
    tweak(&mut cfg.settings);
    cfg
}

/// Try to rebind `addr` with a bounded retry budget, returning the number of
/// attempts taken, the elapsed time, and the socket on success.
async fn rebind_same_port(
    addr: SocketAddr,
    max_attempts: usize,
    per_attempt_backoff: Duration,
) -> (usize, Duration, Option<UdpSocket>) {
    let start = Instant::now();
    for attempt in 1..=max_attempts {
        match UdpSocket::bind(addr).await {
            Ok(sock) => return (attempt, start.elapsed(), Some(sock)),
            Err(_) => tokio::time::sleep(per_attempt_backoff).await,
        }
    }
    (max_attempts, start.elapsed(), None)
}

/// **S1 — same-port rebind after graceful shutdown** (design §5.6, SC-001/SC-009).
///
/// The crux spike: with a live client connected, `close()` → drop the acceptor →
/// `wait_idle().await`, then measure whether the SAME UDP port can be rebound
/// immediately or whether a short bounded retry is required (the tokio-quiche
/// router task releases its `Arc<UdpSocket>` clones only when next polled after
/// its accept-sink closes). Runs many iterations and records the retry
/// frequency and worst-case attempts/latency, then proves the rebound port is
/// usable by completing a fresh handshake on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "S1 spike: binds UDP repeatedly + runs real handshakes"]
async fn s1_same_port_rebind_after_wait_idle() {
    const ITERS: usize = 50;
    // Generous SAFETY budget (distinct from the measured-typical verdict): the
    // measured worst case is 2 attempts / <7 ms, but a CPU-starved scheduler
    // (all spike tests run in parallel, each on its own 4-worker runtime) can
    // delay the tokio-quiche router-task poll that releases the FD. The budget
    // below (up to ~600 ms) absorbs that without weakening the recorded verdict.
    const MAX_ATTEMPTS: usize = 60;
    const BACKOFF: Duration = Duration::from_millis(10);

    let certs = TestCerts::generate();
    let config = server_config(&certs);

    let mut iters_needing_retry = 0usize;
    let mut worst_attempts = 1usize;
    let mut worst_latency = Duration::ZERO;

    for i in 0..ITERS {
        // Stand up a server on an ephemeral port and establish one live client.
        let server_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = server_udp.local_addr().unwrap();
        let mut acceptors = H3QuicheAcceptor::bind([server_udp], &config).expect("bind acceptor");
        let mut acceptor = acceptors.pop().unwrap();
        let endpoint = acceptor.endpoint();

        let client = spawn_client(addr);
        let conn = tokio::time::timeout(DEADLINE, acceptor.accept())
            .await
            .unwrap_or_else(|_| panic!("iter {i}: accept hung"))
            .expect("accept ok")
            .expect("a connection was accepted");
        let drive = spawn_server_drive(conn);
        wait_for_live(&endpoint, 1, DEADLINE).await;

        // Graceful shutdown, then release the listener FDs.
        endpoint.close(h3::error::Code::H3_NO_ERROR, b"rebind spike");
        drop(acceptor);
        tokio::time::timeout(DEADLINE, endpoint.wait_idle())
            .await
            .unwrap_or_else(|_| panic!("iter {i}: wait_idle hung"));

        // Measure the same-port rebind IMMEDIATELY after `wait_idle()` resolves —
        // before any client/drive cleanup — so the sample captures the tightest
        // residual window (the router task releasing the socket after its
        // accept_sink closed, §5.6). This is exactly the tonic-h3 reconnect race.
        let (attempts, latency, sock) = rebind_same_port(addr, MAX_ATTEMPTS, BACKOFF).await;
        drive.abort();
        let _ = tokio::time::timeout(DEADLINE, client).await;
        let sock = sock.unwrap_or_else(|| {
            panic!("iter {i}: same port never rebound within {MAX_ATTEMPTS} attempts")
        });
        if attempts > 1 {
            iters_needing_retry += 1;
        }
        worst_attempts = worst_attempts.max(attempts);
        worst_latency = worst_latency.max(latency);

        // Prove the rebound port is actually usable: fresh handshake on it.
        let mut acceptors2 = H3QuicheAcceptor::bind([sock], &config).expect("rebind acceptor");
        let mut acceptor2 = acceptors2.pop().unwrap();
        let endpoint2 = acceptor2.endpoint();
        let client2 = spawn_client(addr);
        let conn2 = tokio::time::timeout(DEADLINE, acceptor2.accept())
            .await
            .unwrap_or_else(|_| panic!("iter {i}: post-rebind accept hung"))
            .expect("post-rebind accept ok")
            .expect("a connection was accepted on the rebound port");
        let drive2 = spawn_server_drive(conn2);
        wait_for_live(&endpoint2, 1, DEADLINE).await;
        endpoint2.close(h3::error::Code::H3_NO_ERROR, b"rebind spike cleanup");
        drop(acceptor2);
        let _ = tokio::time::timeout(DEADLINE, endpoint2.wait_idle()).await;
        drive2.abort();
        let _ = tokio::time::timeout(DEADLINE, client2).await;
    }

    println!(
        "S1 rebind: iters={ITERS} needing_retry={iters_needing_retry} \
         worst_attempts={worst_attempts} worst_latency={worst_latency:?}"
    );

    // The port is always rebindable within the bounded budget. The printed
    // summary above records the honest measured-typical values (worst ~2 attempts
    // / <7 ms on an unloaded Linux host); the guard here only proves the rebind
    // is *bounded* (never hangs) and tolerates scheduler contention.
    assert!(
        worst_attempts < MAX_ATTEMPTS,
        "same-port rebind must succeed within the bounded retry budget (worst={worst_attempts})"
    );
    assert!(
        worst_latency <= Duration::from_secs(2),
        "same-port rebind must complete within a bounded window (worst={worst_latency:?})"
    );
}

/// Like [`spawn_client`], but tolerant of connection failure (the S2 fence
/// deliberately refuses clients that arrive after `close()`, so those tasks must
/// not panic). Returns once the connection ends or is refused.
fn spawn_client_lenient(server_addr: SocketAddr) -> JoinHandle<()> {
    tokio::spawn(async move {
        let connector =
            match H3QuicheConnector::new(server_addr, "localhost".to_string(), client_config()) {
                Ok(c) => c,
                Err(_) => return,
            };
        let conn = match connector.connect().await {
            Ok(c) => c,
            Err(_) => return,
        };
        let (mut driver, send_request) = match h3::client::new(conn).await {
            Ok(x) => x,
            Err(_) => return,
        };
        let _keep_open = send_request;
        let _ = futures::future::poll_fn(|cx| driver.poll_close(cx)).await;
    })
}

/// Poll the endpoint's registry snapshot until `live` reaches AT LEAST `target`.
async fn wait_for_live_at_least(
    endpoint: &quiche_h3::H3QuicheEndpoint,
    target: usize,
    deadline: Duration,
) {
    let start = Instant::now();
    loop {
        if endpoint.__test_registry_snapshot().1 >= target {
            return;
        }
        if start.elapsed() > deadline {
            panic!(
                "live worker count never reached at least {target} (last = {})",
                endpoint.__test_registry_snapshot().1
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// **S2 — admission fence under concurrent accept()/close()** (design §5.4/§5.5,
/// SC-002/SC-003).
///
/// With the acceptor loop running and live connections established, a `close()`
/// must (a) freeze the registration counter — no worker is started after
/// `close()`, even under a post-close burst of new clients — and (b) never yield
/// a freshly-established connection out of `accept()` once `closing` is observed.
/// "No worker started after close" is not observable through the public API, so
/// it is asserted via the `#[doc(hidden)]` test-only registry snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "S2 spike: binds UDP + runs concurrent handshakes"]
async fn s2_admission_fence_under_concurrent_close() {
    let certs = TestCerts::generate();
    let config = server_config(&certs);

    let server_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = server_udp.local_addr().unwrap();
    let mut acceptors = H3QuicheAcceptor::bind([server_udp], &config).expect("bind acceptor");
    let mut acceptor = acceptors.pop().unwrap();
    let endpoint = acceptor.endpoint();

    // Accept loop: it must NEVER yield a connection once `closing` has
    // linearized (FR-006/SC-003). The task checks the closing flag at the exact
    // moment it receives each `Some` and records a hard violation if one is
    // yielded post-`closing` — the deterministic half of the fence assertion.
    let task_endpoint = endpoint.clone();
    let accept_task = tokio::spawn(async move {
        let mut yielded = 0usize;
        let mut yielded_after_closing = 0usize;
        let mut drives = Vec::new();
        loop {
            match acceptor.accept().await {
                Ok(Some(conn)) => {
                    yielded += 1;
                    if task_endpoint.__test_is_closing() {
                        yielded_after_closing += 1;
                    }
                    drives.push(spawn_server_drive(conn));
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
        (yielded, yielded_after_closing, drives)
    });

    // Establish a handful of live connections before the fence.
    let mut clients: Vec<JoinHandle<()>> = (0..8).map(|_| spawn_client_lenient(addr)).collect();
    wait_for_live_at_least(&endpoint, 3, DEADLINE).await;

    // Fire the fence and capture the registration counter at the linearization
    // point.
    endpoint.close(h3::error::Code::H3_NO_ERROR, b"admission fence");
    let next_id_at_close = endpoint.__test_registry_snapshot().0;

    // Hammer the closed acceptor with a burst of new clients: none may register.
    clients.extend((0..24).map(|_| spawn_client_lenient(addr)));
    tokio::time::sleep(Duration::from_millis(400)).await;
    let next_id_after = endpoint.__test_registry_snapshot().0;
    assert_eq!(
        next_id_at_close, next_id_after,
        "no worker may be admitted after close() (fence breached: {next_id_at_close} -> {next_id_after})"
    );

    // Drain the acceptor to end-of-stream.
    let (yielded, yielded_after_closing, drives) = tokio::time::timeout(DEADLINE, accept_task)
        .await
        .expect("accept loop drained to Ok(None)")
        .expect("accept task did not panic");
    // (a) No connection is yielded once `closing` is observed (the strong,
    //     boundary-precise invariant), and (b) no connection is yielded beyond
    //     those registered before close (a coarser cross-check).
    assert_eq!(
        yielded_after_closing, 0,
        "accept() yielded {yielded_after_closing} connection(s) after `closing` linearized (FR-006/SC-003)"
    );
    assert!(
        (yielded as u64) <= next_id_at_close,
        "no connection may be yielded beyond those registered at close \
         (yielded={yielded}, registered_at_close={next_id_at_close})"
    );

    tokio::time::timeout(DEADLINE, endpoint.wait_idle())
        .await
        .expect("wait_idle drained after fenced close");

    for d in drives {
        d.abort();
    }
    for c in clients {
        c.abort();
    }
}

/// Send a single raw QUIC Initial flight to `server_addr` from a fresh ephemeral
/// socket, then stall (never advancing the handshake). Returns the socket so the
/// client port stays open. With `disable_client_ip_validation` set on the
/// server, this is enough to spawn and register a server-side worker that then
/// hangs mid-handshake.
fn send_initial_and_stall(server_addr: SocketAddr) -> std::net::UdpSocket {
    use tokio_quiche::quiche;

    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).expect("quiche config");
    config.set_application_protos(&[b"h3"]).expect("alpn");
    config.verify_peer(false);
    config.set_max_idle_timeout(5_000);
    config.set_initial_max_data(1_000_000);
    config.set_initial_max_stream_data_bidi_local(100_000);
    config.set_initial_max_stream_data_bidi_remote(100_000);
    config.set_initial_max_streams_bidi(10);
    config.set_initial_max_streams_uni(10);

    let udp = std::net::UdpSocket::bind("127.0.0.1:0").expect("client udp");
    udp.connect(server_addr).expect("connect client udp");
    let local = udp.local_addr().expect("client local addr");

    let mut scid = [0u8; 16];
    // Deterministic-enough unique SCID for a loopback probe.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    scid[..16].copy_from_slice(&nanos.to_le_bytes());
    let scid = quiche::ConnectionId::from_ref(&scid);

    let mut conn = quiche::connect(Some("localhost"), &scid, local, server_addr, &mut config)
        .expect("connect");

    // Emit the Initial flight, then STOP — never read the server's response, so
    // the handshake can never complete.
    let mut out = [0u8; 1350];
    loop {
        match conn.send(&mut out) {
            Ok((n, _)) => {
                udp.send(&out[..n]).expect("send initial");
            }
            Err(quiche::Error::Done) => break,
            Err(e) => panic!("raw client send failed: {e:?}"),
        }
    }
    udp
}

/// **S3 — mid-handshake bound** (design §5.5/§11, SC-004).
///
/// A stalled handshake must not pin `wait_idle()` open forever. With a finite
/// `handshake_timeout` configured, a raw client that sends only its Initial and
/// then stalls causes the server to start and register a worker (proven via the
/// registry snapshot) that then hangs mid-handshake. After `close()`,
/// `wait_idle()` must still complete within a bounded margin of the handshake
/// timeout (the worker self-terminates when the timeout expires — a
/// mid-handshake worker does not process the broadcast Close command).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "S3 spike: binds UDP + stalls a raw handshake"]
async fn s3_mid_handshake_bounded_by_timeout() {
    const HANDSHAKE_TIMEOUT: Duration = Duration::from_millis(800);

    let certs = TestCerts::generate();
    let config = server_config_with(&certs, |s| {
        s.handshake_timeout = Some(HANDSHAKE_TIMEOUT);
        // A single Initial should immediately spawn a worker (no stateless retry).
        s.disable_client_ip_validation = true;
    });

    let server_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = server_udp.local_addr().unwrap();
    let mut acceptors = H3QuicheAcceptor::bind([server_udp], &config).expect("bind acceptor");
    let mut acceptor = acceptors.pop().unwrap();
    let endpoint = acceptor.endpoint();

    // Drive the acceptor so the incoming stalled connection starts a worker.
    let accept_task = tokio::spawn(async move {
        // Loops until end-of-stream; the stalled handshake never yields.
        while let Ok(Some(conn)) = acceptor.accept().await {
            // Not expected for the stalled probe, but keep draining if it happens.
            drop(conn);
        }
    });

    // Fire the raw Initial and keep the client socket alive.
    let _client_sock = send_initial_and_stall(addr);

    // The worker must register BEFORE close() — i.e. mid-handshake.
    wait_for_live(&endpoint, 1, DEADLINE).await;
    assert_eq!(
        endpoint.__test_registry_snapshot().0,
        1,
        "exactly one worker registered from the stalled handshake"
    );

    // Broadcast close, then time how long wait_idle takes: it is bounded by the
    // handshake timeout, not by close responsiveness.
    let t0 = Instant::now();
    endpoint.close(h3::error::Code::H3_NO_ERROR, b"mid-handshake shutdown");
    tokio::time::timeout(HANDSHAKE_TIMEOUT * 4, endpoint.wait_idle())
        .await
        .expect("wait_idle completed within a bound of the handshake timeout");
    let elapsed = t0.elapsed();
    println!(
        "S3 mid-handshake: wait_idle elapsed={elapsed:?} (handshake_timeout={HANDSHAKE_TIMEOUT:?})"
    );

    // SC-004: bounded by ~the handshake timeout. The worker cannot self-terminate
    // before its timeout fires, so assert a lower sanity bound too (the resolve is
    // driven by the timeout, not by close() racing it away).
    assert!(
        elapsed >= HANDSHAKE_TIMEOUT / 2,
        "wait_idle should be gated by the handshake timeout, not resolve early (elapsed={elapsed:?})"
    );
    assert!(
        elapsed <= HANDSHAKE_TIMEOUT * 2,
        "wait_idle must complete within ~2x the handshake timeout (SC-004; elapsed={elapsed:?})"
    );
    assert_eq!(
        endpoint.__test_registry_snapshot().1,
        0,
        "the stalled worker deregistered on handshake-timeout exit"
    );

    // The accept loop must reach end-of-stream (it never yields the stalled probe).
    tokio::time::timeout(DEADLINE, accept_task)
        .await
        .expect("accept loop drained to end-of-stream")
        .expect("accept task did not panic");
}
