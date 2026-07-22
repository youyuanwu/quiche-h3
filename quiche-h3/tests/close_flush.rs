//! Phase 8 T1b close-flush loopback (design §14.1 T1b, §5.2, §8.3, §9). After a
//! completed request/response, the client drops **all** its handles (the h3
//! driver + `SendRequest` + request stream). The bridge's worker observes the
//! last-handle EOF and issues a graceful `qconn.close(true, H3_NO_ERROR, b"")`;
//! this test asserts that close is actually **flushed to the peer** — the
//! server's `h3::server::Connection::accept()` resolves (returns `Ok(None)` or
//! an error) within a timeout instead of hanging.
//!
//! `#[ignore]`d because it binds UDP and runs a real handshake. Run with:
//!
//! ```text
//! cargo test -p quiche-h3 --test close_flush -- --ignored --nocapture
//! ```

use std::time::Duration;

use bytes::Bytes;
use quiche_h3::{H3QuicheAcceptor, H3QuicheClientConfig, H3QuicheConnector, H3QuicheServerConfig};
use tokio::net::UdpSocket;

const BODY: &[u8] = b"t1b close-flush body";
const DEADLINE: Duration = Duration::from_secs(10);

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
            "quiche-h3-closeflush-{}-{}",
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

/// After a normal GET, drop every client handle and assert the server observes
/// the graceful connection close (T1b flush reaches the peer).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "binds UDP + runs a real handshake"]
async fn client_last_handle_drop_closes_server_side() {
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

        // Serve exactly one request.
        let resolver = h3_conn
            .accept()
            .await
            .expect("accept request")
            .expect("one request stream");
        let (_req, mut stream) = resolver.resolve_request().await.expect("resolve request");
        while stream.recv_data().await.expect("recv body").is_some() {}
        let response = http::Response::builder()
            .status(http::StatusCode::OK)
            .body(())
            .unwrap();
        stream.send_response(response).await.expect("send response");
        stream
            .send_data(Bytes::from_static(BODY))
            .await
            .expect("send body");
        stream.finish().await.expect("finish server stream");

        // Now block on the next accept. When the client drops all its handles,
        // the bridge flushes a graceful H3_NO_ERROR CONNECTION_CLOSE; the server
        // must observe it here (Ok(None) = clean end, or Err = closed) rather
        // than hanging.
        match h3_conn.accept().await {
            Ok(None) => "Ok(None)".to_string(),
            Ok(Some(_)) => "Ok(Some) — unexpected second request".to_string(),
            Err(e) => format!("Err({e})"),
        }
    });

    // --- client ---
    let connector = H3QuicheConnector::new(server_addr, "localhost".to_string(), client_config())
        .expect("build connector");
    let conn = connector.connect().await.expect("client connect ok");
    let (mut driver, mut send_request) = h3::client::new(conn).await.expect("h3 client handshake");

    let drive = tokio::spawn(async move {
        let _ = futures::future::poll_fn(|cx| driver.poll_close(cx)).await;
    });

    // Complete one GET.
    let client_work = async {
        let req = http::Request::builder()
            .method(http::Method::GET)
            .uri("https://localhost/")
            .body(())
            .unwrap();
        let mut stream = send_request.send_request(req).await.expect("send_request");
        stream.finish().await.expect("finish request body");
        let resp = stream.recv_response().await.expect("recv response");
        assert_eq!(resp.status(), http::StatusCode::OK);
        while stream.recv_data().await.expect("recv body").is_some() {}
        // Drop the request stream explicitly before tearing down the rest.
        drop(stream);
    };
    tokio::time::timeout(DEADLINE, client_work)
        .await
        .expect("client work completed before deadline");

    // Last-handle teardown: drop `SendRequest` and the driver task (which owns
    // the driver handle). This drops every strong `cmd_tx`, so the worker
    // observes EOF and issues the graceful close.
    drop(send_request);
    drive.abort();
    let _ = drive.await;

    // The server must observe the close within the deadline.
    let observed = tokio::time::timeout(DEADLINE, server_task)
        .await
        .expect("server observed close before deadline (T1b flush reached peer)")
        .expect("server task ok");

    println!("T1b: server observed connection close as: {observed}");
    assert!(
        observed == "Ok(None)" || observed.starts_with("Err("),
        "server must observe a clean/closed connection, got: {observed}",
    );
}
