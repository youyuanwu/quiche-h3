//! Phase 8 end-to-end HTTP/3 loopback (design §11). Stands up a real
//! `H3QuicheAcceptor` + `H3QuicheConnector` over loopback, then drives one
//! GET request/response through hyperium `h3` (client + server) *over the
//! bridge* and asserts the status, headers, and body round-trip exactly. This
//! is the crown-jewel validation that the whole stack works with real `h3`.
//!
//! Run with:
//!
//! ```text
//! cargo test -p quiche-h3 --test h3_e2e -- --nocapture
//! ```

use std::time::Duration;

use bytes::{Buf, Bytes};
use quiche_h3::{H3QuicheAcceptor, H3QuicheClientConfig, H3QuicheConnector, H3QuicheServerConfig};
use tokio::net::UdpSocket;

/// The known response body the server sends and the client asserts.
const BODY: &[u8] = b"hello h3 over quiche";
/// A custom response header the round-trip must preserve exactly.
const HDR_NAME: &str = "x-quiche-h3";
const HDR_VALUE: &str = "phase-8";
/// Generous per-test deadline so a bridge hang fails fast instead of hanging CI.
const DEADLINE: Duration = Duration::from_secs(10);

/// A self-signed cert + key written to temp PEM files (mirrors wiring.rs).
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
            "quiche-h3-e2e-{}-{}",
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
        // Self-signed server cert on loopback: don't verify.
        verify_peer: false,
        ..H3QuicheClientConfig::default()
    }
}

/// Drive one GET request/response through real hyperium `h3` over the bridge,
/// asserting the status, a custom header, and the full body round-trip exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_to_end_get_round_trips_status_headers_and_body() {
    let certs = TestCerts::generate();

    // --- server ---
    let server_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server_udp.local_addr().unwrap();
    let mut acceptors =
        H3QuicheAcceptor::bind([server_udp], &server_config(&certs)).expect("bind acceptor");
    let mut acceptor = acceptors.pop().unwrap();

    let server_task = tokio::spawn(async move {
        let conn: quiche_h3::Connection<Bytes> = acceptor
            .accept()
            .await
            .expect("accept ok")
            .expect("accepted a connection");

        let mut h3_conn = h3::server::Connection::new(conn)
            .await
            .expect("h3 server handshake");

        let resolver = h3_conn
            .accept()
            .await
            .expect("accept request")
            .expect("one request stream");
        let (req, mut stream) = resolver.resolve_request().await.expect("resolve request");

        assert_eq!(req.method(), http::Method::GET, "server sees a GET");

        // Read the (empty) request body to completion.
        while stream
            .recv_data()
            .await
            .expect("recv request body")
            .is_some()
        {}

        let response = http::Response::builder()
            .status(http::StatusCode::OK)
            .header(HDR_NAME, HDR_VALUE)
            .body(())
            .unwrap();
        stream.send_response(response).await.expect("send response");
        stream
            .send_data(Bytes::from_static(BODY))
            .await
            .expect("send body");
        stream.finish().await.expect("finish server stream");
    });

    // --- client ---
    let connector = H3QuicheConnector::new(server_addr, "localhost".to_string(), client_config())
        .expect("build connector");
    let conn = connector.connect().await.expect("client connect ok");

    let (mut driver, mut send_request) = h3::client::new(conn).await.expect("h3 client handshake");

    // Spawn the h3 client connection driver: it must be polled for the request
    // to make progress. It resolves when the connection closes.
    let drive = tokio::spawn(async move {
        let _ = futures::future::poll_fn(|cx| driver.poll_close(cx)).await;
    });

    let client_work = async {
        let req = http::Request::builder()
            .method(http::Method::GET)
            .uri("https://localhost/")
            .body(())
            .unwrap();

        let mut stream = send_request.send_request(req).await.expect("send_request");
        // GET: no request body.
        stream.finish().await.expect("finish request body");

        let resp = stream.recv_response().await.expect("recv response");
        assert_eq!(resp.status(), http::StatusCode::OK, "status is 200");
        assert_eq!(
            resp.headers()
                .get(HDR_NAME)
                .expect("custom header present")
                .to_str()
                .unwrap(),
            HDR_VALUE,
            "custom response header round-trips",
        );

        // Read the full response body.
        let mut body = Vec::new();
        while let Some(mut chunk) = stream.recv_data().await.expect("recv body chunk") {
            while chunk.has_remaining() {
                let n = chunk.chunk().len();
                body.extend_from_slice(chunk.chunk());
                chunk.advance(n);
            }
        }
        assert_eq!(body.as_slice(), BODY, "response body round-trips exactly");

        // No trailers expected.
        let trailers = stream.recv_trailers().await.expect("recv trailers");
        assert!(trailers.is_none(), "no trailers");
    };

    tokio::time::timeout(DEADLINE, client_work)
        .await
        .expect("client work completed before deadline");

    tokio::time::timeout(DEADLINE, server_task)
        .await
        .expect("server task joined before deadline")
        .expect("server task ok");

    drive.abort();
}

/// Issue #12: each side sends one DATA frame and then waits for its peer before
/// producing the next. A quiescent held write used to make this strict
/// bidirectional ping-pong deadlock on the first round.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bidirectional_ping_pong_streaming_never_deadlocks() {
    const ROUNDS: usize = 8;

    let certs = TestCerts::generate();

    // --- server ---
    let server_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server_udp.local_addr().unwrap();
    let mut acceptors =
        H3QuicheAcceptor::bind([server_udp], &server_config(&certs)).expect("bind acceptor");
    let mut acceptor = acceptors.pop().unwrap();

    let server_task = tokio::spawn(async move {
        let conn: quiche_h3::Connection<Bytes> = acceptor
            .accept()
            .await
            .expect("accept ok")
            .expect("accepted a connection");
        let mut h3_conn = h3::server::Connection::new(conn)
            .await
            .expect("h3 server handshake");
        let resolver = h3_conn
            .accept()
            .await
            .expect("accept request")
            .expect("one request stream");
        let (req, mut stream) = resolver.resolve_request().await.expect("resolve request");
        assert_eq!(req.method(), http::Method::POST);

        for round in 0..ROUNDS {
            let expected = format!("ping-{round}");
            let mut received = Vec::with_capacity(expected.len());
            while received.len() < expected.len() {
                let mut chunk = stream
                    .recv_data()
                    .await
                    .expect("recv ping")
                    .expect("request stream remains open");
                while chunk.has_remaining() {
                    let bytes = chunk.chunk();
                    received.extend_from_slice(bytes);
                    chunk.advance(bytes.len());
                }
            }
            assert_eq!(
                received,
                expected.as_bytes(),
                "server received round {round}"
            );

            if round == 0 {
                let response = http::Response::builder()
                    .status(http::StatusCode::OK)
                    .body(())
                    .unwrap();
                stream.send_response(response).await.expect("send response");
            }
            stream
                .send_data(Bytes::from(format!("pong-{round}")))
                .await
                .expect("send pong");
        }

        assert!(
            stream
                .recv_data()
                .await
                .expect("recv request finish")
                .is_none(),
            "request stream finishes after the final ping"
        );
        stream.finish().await.expect("finish response stream");
    });

    // --- client ---
    let connector = H3QuicheConnector::new(server_addr, "localhost".to_string(), client_config())
        .expect("build connector");
    let conn = connector.connect().await.expect("client connect ok");
    let (mut driver, mut send_request) = h3::client::new(conn).await.expect("h3 client handshake");
    let drive = tokio::spawn(async move {
        let _ = futures::future::poll_fn(|cx| driver.poll_close(cx)).await;
    });

    let client_work = async {
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri("https://localhost/stream")
            .body(())
            .unwrap();
        let mut stream = send_request.send_request(req).await.expect("send_request");

        for round in 0..ROUNDS {
            stream
                .send_data(Bytes::from(format!("ping-{round}")))
                .await
                .expect("send ping");

            if round == 0 {
                let response = stream.recv_response().await.expect("recv response");
                assert_eq!(response.status(), http::StatusCode::OK);
            }

            let expected = format!("pong-{round}");
            let mut received = Vec::with_capacity(expected.len());
            while received.len() < expected.len() {
                let mut chunk = stream
                    .recv_data()
                    .await
                    .expect("recv pong")
                    .expect("response stream remains open");
                while chunk.has_remaining() {
                    let bytes = chunk.chunk();
                    received.extend_from_slice(bytes);
                    chunk.advance(bytes.len());
                }
            }
            assert_eq!(
                received,
                expected.as_bytes(),
                "client received round {round}"
            );
        }

        stream.finish().await.expect("finish request stream");
        assert!(
            stream
                .recv_data()
                .await
                .expect("recv response finish")
                .is_none(),
            "response stream finishes after the final pong"
        );
    };

    tokio::time::timeout(DEADLINE, client_work)
        .await
        .expect("strict ping-pong completed before deadline");
    tokio::time::timeout(DEADLINE, server_task)
        .await
        .expect("server task joined before deadline")
        .expect("server task ok");

    drive.abort();
}
