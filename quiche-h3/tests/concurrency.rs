//! Concurrency stall repro for tonic-h3 issue #43
//! (<https://github.com/youyuanwu/tonic-h3/issues/43>).
//!
//! The reported symptom: the quiche HTTP/3 backend stalls when more than one
//! request is in flight concurrently over a single loopback connection. The bulk
//! of a run completes, but the final `concurrency - 1` in-flight requests hang
//! indefinitely. The two diagnostic tells from the issue are:
//!
//!   1. `failed ~= concurrency - 1` — exactly the workers left with an in-flight
//!      request when the request-issuing loop reaches the total count.
//!   2. `elapsed_s` pins to the per-request timeout, independent of real work —
//!      the trailing requests are not doing work, they are waiting out the
//!      timeout.
//!
//! This test reproduces the benchmark shape that triggers it: a single client
//! connection, `CONCURRENCY` worker tasks sharing one (cloned) `SendRequest`,
//! each pulling the next index off a shared counter and driving a full
//! request/response round-trip until `TOTAL` requests have been issued. If the
//! tail-drain bug is present, the last `CONCURRENCY - 1` requests never complete
//! and the whole run times out at [`DEADLINE`]; if the bridge drains the tail
//! correctly, every request completes well under the deadline.
//!
//! `#[ignore]`d because it binds UDP and runs a real handshake. Run with:
//!
//! ```text
//! cargo test -p quiche-h3 --test concurrency -- --ignored --nocapture
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, Bytes};
use quiche_h3::{H3QuicheAcceptor, H3QuicheClientConfig, H3QuicheConnector, H3QuicheServerConfig};
use tokio::net::UdpSocket;

/// Number of worker tasks issuing requests concurrently over one connection.
/// Must be > 1 to exercise the stall (at concurrency 1 the issue does not
/// reproduce).
const CONCURRENCY: usize = 8;
/// Total number of requests to issue across all workers.
const TOTAL: usize = 200;
/// Fixed response body size (mirrors the benchmark's `--payload-size 64`).
const PAYLOAD_SIZE: usize = 64;
/// Whole-run deadline. Generous enough that healthy loopback work (a few
/// hundred tiny requests) finishes with room to spare, but bounded so a
/// tail-drain stall fails fast instead of hanging the suite.
const DEADLINE: Duration = Duration::from_secs(30);

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
            "quiche-h3-concurrency-{}-{}",
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
    settings.max_idle_timeout = Some(Duration::from_secs(30));
    H3QuicheServerConfig {
        cert_path: certs.cert_path.clone(),
        key_path: certs.key_path.clone(),
        settings,
        ..H3QuicheServerConfig::default()
    }
}

fn client_config() -> H3QuicheClientConfig {
    let mut settings = tokio_quiche::settings::QuicSettings::default();
    settings.max_idle_timeout = Some(Duration::from_secs(30));
    H3QuicheClientConfig {
        settings,
        // Self-signed server cert on loopback: don't verify.
        verify_peer: false,
        ..H3QuicheClientConfig::default()
    }
}

/// Many concurrent requests over one connection must all complete; the trailing
/// `CONCURRENCY - 1` requests must be driven to completion after the issuing
/// loop stops (the tonic-h3 #43 tail-drain stall).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "binds UDP + runs a real handshake"]
async fn concurrent_requests_all_complete_no_tail_stall() {
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

        // Accept request streams until the client closes the connection, handling
        // each on its own task so responses for concurrent streams can be
        // produced in parallel.
        let body = Bytes::from(vec![0x5au8; PAYLOAD_SIZE]);
        loop {
            match h3_conn.accept().await {
                Ok(Some(resolver)) => {
                    let body = body.clone();
                    tokio::spawn(async move {
                        let (_req, mut stream) =
                            resolver.resolve_request().await.expect("resolve request");
                        // Drain the (empty) request body.
                        while stream
                            .recv_data()
                            .await
                            .expect("recv request body")
                            .is_some()
                        {}
                        let response = http::Response::builder()
                            .status(http::StatusCode::OK)
                            .body(())
                            .unwrap();
                        stream.send_response(response).await.expect("send response");
                        stream.send_data(body).await.expect("send body");
                        stream.finish().await.expect("finish server stream");
                    });
                }
                // Connection closed by client: normal end of run.
                Ok(None) => break,
                Err(_) => break,
            }
        }
    });

    // --- client ---
    let connector = H3QuicheConnector::new(server_addr, "localhost".to_string(), client_config())
        .expect("build connector");
    let conn = connector.connect().await.expect("client connect ok");

    let (mut driver, send_request) = h3::client::new(conn).await.expect("h3 client handshake");

    // The h3 client connection driver must be polled for requests to progress.
    let drive = tokio::spawn(async move {
        let _ = futures::future::poll_fn(|cx| driver.poll_close(cx)).await;
    });

    let next = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));

    let run = async {
        let mut workers = Vec::with_capacity(CONCURRENCY);
        for _ in 0..CONCURRENCY {
            let mut send_request = send_request.clone();
            let next = Arc::clone(&next);
            let completed = Arc::clone(&completed);
            workers.push(tokio::spawn(async move {
                // Each worker pulls indices off the shared counter until the run
                // is exhausted, exactly like the benchmark's concurrency workers.
                while next.fetch_add(1, Ordering::Relaxed) < TOTAL {
                    let req = http::Request::builder()
                        .method(http::Method::GET)
                        .uri("https://localhost/")
                        .body(())
                        .unwrap();

                    let mut stream = send_request.send_request(req).await.expect("send_request");
                    stream.finish().await.expect("finish request body");

                    let resp = stream.recv_response().await.expect("recv response");
                    assert_eq!(resp.status(), http::StatusCode::OK, "status is 200");

                    let mut len = 0usize;
                    while let Some(mut chunk) =
                        stream.recv_data().await.expect("recv body chunk")
                    {
                        len += chunk.remaining();
                        chunk.advance(chunk.remaining());
                    }
                    assert_eq!(len, PAYLOAD_SIZE, "response body fully received");

                    completed.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        for w in workers {
            w.await.expect("worker task ok");
        }
    };

    // The core assertion: the whole concurrent run drains within the deadline.
    // Under the #43 stall the trailing `CONCURRENCY - 1` requests never complete
    // and this times out.
    let result = tokio::time::timeout(DEADLINE, run).await;

    let done = completed.load(Ordering::Relaxed);
    assert!(
        result.is_ok(),
        "concurrent run stalled: only {done}/{TOTAL} requests completed before \
         the {DEADLINE:?} deadline (tonic-h3 #43 tail-drain stall)",
    );
    assert_eq!(done, TOTAL, "every issued request must complete");

    // Dropping the client (send_request handles + connection) closes the
    // connection, letting the server accept loop see `Ok(None)` and finish.
    drop(send_request);
    let _ = tokio::time::timeout(DEADLINE, server_task).await;
    drive.abort();
}
