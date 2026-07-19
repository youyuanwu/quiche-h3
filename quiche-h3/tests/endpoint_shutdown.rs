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
//! The empirical spike tests (S1 rebind, S2 admission fence, S3 mid-handshake
//! bound) live in `spike_harness.rs`.

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

    // The peer's connection terminates as a result of the server close.
    tokio::time::timeout(DEADLINE, client)
        .await
        .expect("client driver terminated after server close")
        .expect("client task did not panic");

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
