//! Phase 7 loopback wiring test: stand up a real `H3QuicheAcceptor` +
//! `H3QuicheConnector` over loopback and confirm both sides yield an
//! established `Connection<Bytes>` (design §7.1, §7.2). `#[ignore]`d because it
//! binds UDP and runs a real handshake (mirror of `spike_harness.rs`).
//!
//! Run with:
//!
//! ```text
//! cargo test -p quiche-h3 --test wiring -- --ignored --nocapture
//! ```

use std::time::Duration;

use bytes::Bytes;
use h3::quic::Connection as _;
use quiche_h3::{H3QuicheAcceptor, H3QuicheClientConfig, H3QuicheConnector, H3QuicheServerConfig};
use tokio::net::UdpSocket;

/// A self-signed cert + key written to temp PEM files (mirrors spike_harness).
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
            "quiche-h3-wiring-{}-{}",
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "binds UDP + runs a real handshake"]
async fn acceptor_and_connector_complete_handshake() {
    let certs = TestCerts::generate();

    // --- server ---
    let server_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server_udp.local_addr().unwrap();

    let mut settings = tokio_quiche::settings::QuicSettings::default();
    settings.max_idle_timeout = Some(Duration::from_secs(10));
    let server_config = H3QuicheServerConfig {
        cert_path: certs.cert_path.clone(),
        key_path: certs.key_path.clone(),
        settings,
        ..H3QuicheServerConfig::default()
    };

    let mut acceptors =
        H3QuicheAcceptor::bind([server_udp], &server_config).expect("bind acceptor");
    assert_eq!(acceptors.len(), 1, "one acceptor per socket");
    let mut acceptor = acceptors.pop().unwrap();

    let server_task = tokio::spawn(async move {
        let conn = acceptor.accept().await.expect("accept ok");
        // The accepted connection is `Some` and established: exercising the
        // `h3::quic::Connection` surface (obtaining an opener) proves usability.
        let conn: quiche_h3::Connection<Bytes> = conn.expect("accepted a connection");
        let _opener = conn.opener();
        // Hold the connection briefly so the client side stays up.
        tokio::time::sleep(Duration::from_millis(300)).await;
    });

    // --- client ---
    let mut client_settings = tokio_quiche::settings::QuicSettings::default();
    client_settings.max_idle_timeout = Some(Duration::from_secs(10));
    let client_config = H3QuicheClientConfig {
        settings: client_settings,
        // Self-signed server cert on loopback: don't verify.
        verify_peer: false,
        ..H3QuicheClientConfig::default()
    };
    let connector = H3QuicheConnector::new(server_addr, "localhost".to_string(), client_config)
        .expect("build connector");

    let conn = connector.connect().await.expect("client connect ok");
    // `connect_with_config` resolves only after the handshake completes, so a
    // returned connection proves the client-side handshake finished.
    // opener() borrows &self; no mut needed.
    let _opener = conn.opener();

    drop(conn);
    server_task.await.expect("server task joined");
}
