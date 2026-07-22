//! Phase 8 spike-outcome regression guard index + a loopback-level RESET guard
//! (design §14.1, §5.1, §8.4).
//!
//! # §14.1 recorded-outcome → guarding test index
//!
//! The Phase 0 spikes (`tests/spike_harness.rs`, recorded in
//! `tests/SPIKE_OUTCOMES.md`) observed load-bearing runtime behavior; Phases 3–6
//! then locked each behavior with deterministic **unit** tests in
//! `src/driver.rs` (mock-worker, no UDP). This index maps every §14.1 recorded
//! outcome to the test(s) that guard it so a regression is traceable. It does
//! **not** duplicate that coverage — it points at it.
//!
//! | §14.1 outcome | guarding test(s) |
//! |---|---|
//! | **T1b** successful last-handle `qconn.close` reaches the peer | `src/driver.rs::last_handle_teardown_issues_h3_no_error_close`, `explicit_close_crosses_barrier_after_saturated_batch`; **loopback**: `tests/close_flush.rs::client_last_handle_drop_closes_server_side` (server observes `ApplicationClose: H3_NO_ERROR`) + spike `spike_t1b_peer_observes_application_close` |
//! | **T2** peer app-close classified | `src/driver.rs::peer_app_close_outranks_last_handle_teardown`, `on_conn_close_publishes_to_all_out_of_band_cells` |
//! | **T2a** pre-handshake exit is cause-unclassified / log-only | connector/acceptor setup-error paths (`src/connector.rs`, `src/listener.rs`); spike `spike_t2a_client_rejecting_cert_resolves_err` |
//! | **T4** garbage initial packet is dropped, listener keeps serving | spike `spike_t4_garbage_datagram_then_real_connection` (listener-level; no per-item error) |
//! | **Q1** destructive recv cursor + one-credit open materialization | `src/driver.rs::stage_open_*`, `src/conn.rs` recv tests |
//! | **Q2** `qconn.close` `Done` defers to pre-existing terminal | `src/driver.rs::done_close_result_defers_to_preexisting_terminal`, `unexpected_close_error_is_internal_bug` |
//! | **Q3** RESET flushed at zero send capacity | `src/driver.rs::reset_emitted_at_zero_capacity`, `reset_preempts_queued_write_keeps_earlier_ok`, `duplicate_reset_is_idempotent`; **loopback**: `reset_stream_surfaces_as_remote_terminate` (this file) |
//! | **Q4** partial write then capacity re-arms exactly one `Ok` | `src/driver.rs::partial_write_then_capacity_rearms_one_ok` |
//! | **Q5** FIN accepted + flushed at zero send capacity | `src/driver.rs::finish_accepted_at_zero_capacity_completes_once`, `accepted_fin_marks_send_done_and_enables_contract_a`; **loopback**: `tests/h3_e2e.rs` (client sends empty FIN body → server drains to `Ok(None)`; server FIN → client body then `None`) |
//! | **§5.5 BLOCKER** (narrowed drop-in claim) | design-level; observed by spike `spike_5_5_blocker_zero_txcap_hides_writable_discovery` |
//! | **§5.5 tombstone** (contract A premise holds) | `src/driver.rs::tombstone_contract_a_removes_bidi_at_both_terminal`, `accepted_fin_marks_send_done_and_enables_contract_a` |
//!
//! Additional §5.1 sealing-edge / queued-then-terminal races are guarded by
//! `src/driver.rs::buffered_bytes_then_fin_delivers_then_seals`,
//! `queued_bytes_then_reset_delivers_then_seals`,
//! `send_conngone_in_closing_window_defers_to_on_conn_close`, and
//! `recv_conngone_in_closing_window_defers_to_on_conn_close`.
//!
//! # Loopback RESET guard
//!
//! The test below is the missing end-to-end complement to the Q3 unit tests: a
//! server-initiated stream reset must surface on the *client* as
//! `h3::error::StreamError::RemoteTerminate { code }` (the bridge maps
//! `RecvEnd::Reset { error_code }` → `StreamErrorIncoming::StreamTerminated`,
//! §8.4). `#[ignore]`d (binds UDP + handshake). Run with:
//!
//! ```text
//! cargo test -p quiche-h3 --test regression_index -- --ignored --nocapture
//! ```

use std::time::Duration;

use bytes::Bytes;
use quiche_h3::{H3QuicheAcceptor, H3QuicheClientConfig, H3QuicheConnector, H3QuicheServerConfig};
use tokio::net::UdpSocket;

/// Reset code the server sends and the client must observe.
const RESET_CODE: u64 = 0x10c; // H3_REQUEST_CANCELLED
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
            "quiche-h3-reset-{}-{}",
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

/// Assert a `StreamError` is a peer reset carrying `RESET_CODE`.
fn assert_remote_terminate(e: h3::error::StreamError) {
    match e {
        h3::error::StreamError::RemoteTerminate { code } => {
            assert_eq!(code.value(), RESET_CODE, "reset code round-trips");
        }
        other => panic!("expected RemoteTerminate, got: {other:?}"),
    }
}

/// A server-initiated `stop_stream` reset surfaces on the client as
/// `RemoteTerminate` with the same code (end-to-end Q3 / §8.4 guard).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "binds UDP + runs a real handshake"]
async fn reset_stream_surfaces_as_remote_terminate() {
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
        let (_req, mut stream) = resolver.resolve_request().await.expect("resolve request");
        while stream.recv_data().await.expect("recv body").is_some() {}

        // Send response headers + a body chunk, then RESET the send side.
        let response = http::Response::builder()
            .status(http::StatusCode::OK)
            .body(())
            .unwrap();
        stream.send_response(response).await.expect("send response");
        stream
            .send_data(Bytes::from_static(b"partial"))
            .await
            .expect("send partial body");
        // Give the worker a moment to flush the queued frames before the reset
        // so we exercise the "queued bytes then RESET_STREAM" path (§5.1) rather
        // than discarding the headers.
        tokio::time::sleep(Duration::from_millis(200)).await;
        stream.stop_stream(h3::error::Code::H3_REQUEST_CANCELLED);

        // Keep the connection alive long enough for the reset to flush.
        tokio::time::sleep(Duration::from_millis(500)).await;
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
            .method(http::Method::GET)
            .uri("https://localhost/")
            .body(())
            .unwrap();
        let mut stream = send_request.send_request(req).await.expect("send_request");
        stream.finish().await.expect("finish request body");

        // The reset must surface either on the response read or on a body read;
        // in both cases as `RemoteTerminate { code == RESET_CODE }`.
        match stream.recv_response().await {
            Ok(_resp) => loop {
                match stream.recv_data().await {
                    Ok(Some(_)) => continue,
                    Ok(None) => panic!("expected RESET, observed a clean FIN"),
                    Err(e) => {
                        assert_remote_terminate(e);
                        break;
                    }
                }
            },
            Err(e) => assert_remote_terminate(e),
        }
    };

    tokio::time::timeout(DEADLINE, client_work)
        .await
        .expect("client observed reset before deadline");

    let _ = tokio::time::timeout(DEADLINE, server_task).await;
    drive.abort();
}
