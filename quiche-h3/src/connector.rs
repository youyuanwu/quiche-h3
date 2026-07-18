//! Client wiring: [`H3QuicheConnector`] over
//! `tokio_quiche::quic::connect_with_config` (design §7.2).
//!
//! `H3Connector` requires `Clone + 'static`, but `ConnectionParams<'a>` is
//! neither `Clone` nor `'static` (it borrows TLS cert/key path strings). So the
//! connector holds a *named, owned* configuration behind an `Arc` and rebuilds a
//! borrowing `ConnectionParams<'_>` per `connect` (finding 4, §7.2). DNS/socket
//! work happens per-`connect`, never in `new`.
//!
//! On the client, `connect_with_config` resolves only *after* the handshake and
//! returns `Err` on handshake failure; that concrete raw error is the
//! authoritative setup-failure signal, mapped verbatim into [`crate::Error`]
//! without fabricating an application/transport code (§7.2, §8.4).

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use tokio::net::UdpSocket;
use tokio_quiche::quic::connect_with_config;
use tokio_quiche::settings::{CertificateKind, Hooks, QuicSettings, TlsCertificatePaths};
use tokio_quiche::socket::Socket;
use tokio_quiche::ConnectionParams;

use crate::driver::QuicheDriver;
use crate::stream::Connection;
use crate::Error;

/// Provisional accept-queue depth for peer-initiated bidi/uni streams (§5.2).
const DEFAULT_ACCEPT_BIDI_CAP: usize = 128;
const DEFAULT_ACCEPT_UNI_CAP: usize = 128;

/// Owned, cloneable client configuration. Holds cloneable `QuicSettings` and
/// `Hooks`, optional owned TLS cert/key **paths** for mTLS, a `verify_peer`
/// toggle applied per-`connect`, and an optional advisory SNI. A borrowing
/// `ConnectionParams<'_>` is rebuilt from these owned fields inside each
/// `connect` (§7.2).
#[derive(Clone)]
pub struct H3QuicheClientConfig {
    /// QUIC transport settings (ALPN defaults to `[b"h3"]`).
    pub settings: QuicSettings,
    /// Connection lifecycle hooks.
    pub hooks: Hooks,
    /// Optional client certificate path for mTLS.
    pub cert_path: Option<String>,
    /// Optional client private-key path for mTLS.
    pub key_path: Option<String>,
    /// Whether to verify the server's certificate chain. Applied onto
    /// `settings.verify_peer` when rebuilding `ConnectionParams` per connect.
    pub verify_peer: bool,
    /// Advisory default SNI / verification name. The explicit `server_name`
    /// passed to [`H3QuicheConnector::new`] takes precedence.
    pub server_name: Option<String>,
}

impl Default for H3QuicheClientConfig {
    fn default() -> Self {
        Self {
            settings: QuicSettings::default(),
            hooks: Hooks::default(),
            cert_path: None,
            key_path: None,
            verify_peer: true,
            server_name: None,
        }
    }
}

/// Immutable connector state shared behind an `Arc` so the connector is
/// `Clone + Send + Sync + 'static`.
struct Inner {
    server_addr: SocketAddr,
    server_name: String,
    config: H3QuicheClientConfig,
}

/// A standalone HTTP/3-over-quiche connector. `Clone + 'static` (state lives
/// behind an `Arc`). Its [`connect`](H3QuicheConnector::connect) yields the
/// crate's front-end [`Connection<Bytes>`], which implements
/// [`h3::quic::Connection<Bytes>`]. The `h3_util::H3Connector` conformance is
/// provided by the h3-util `quiche_h3` wrapper (Phase 9), keeping this crate
/// free of a circular h3-util dependency (design §10).
#[derive(Clone)]
pub struct H3QuicheConnector {
    inner: Arc<Inner>,
}

impl H3QuicheConnector {
    /// Validate the config (TLS material readable, if set) and store the owned
    /// config plus the target address and server name. No socket/DNS work
    /// happens here — that is per-`connect` (§7.2). Setup failures during
    /// `connect` surface there, not here.
    pub fn new(
        server_addr: SocketAddr,
        server_name: String,
        config: H3QuicheClientConfig,
    ) -> Result<Self, Error> {
        // mTLS material must be complete (both or neither): a lone cert or key
        // would silently disable client authentication at connect time.
        match (&config.cert_path, &config.key_path) {
            (Some(cert), Some(key)) => {
                crate::ensure_readable_file(cert, "client TLS certificate")?;
                crate::ensure_readable_file(key, "client TLS private key")?;
            }
            (Some(_), None) => {
                return Err("quiche-h3: client mTLS certificate set without a private key".into());
            }
            (None, Some(_)) => {
                return Err("quiche-h3: client mTLS private key set without a certificate".into());
            }
            (None, None) => {}
        }
        crate::ensure_nonempty_alpn(&config.settings, "client config")?;

        Ok(Self {
            inner: Arc::new(Inner {
                server_addr,
                server_name,
                config,
            }),
        })
    }

    /// Establish a new connection to the configured server (§7.2). Binds a local
    /// UDP socket, connects it to the target, rebuilds a borrowing
    /// `ConnectionParams<'_>` from the owned config, and drives the handshake
    /// through `connect_with_config` (which resolves only *after* the handshake).
    /// A handshake/setup failure surfaces as the mapped raw tokio-quiche error.
    pub async fn connect(&self) -> Result<Connection<Bytes>, Error> {
        let inner = &self.inner;

        // Bind a local endpoint matching the target's address family.
        let bind_addr = if inner.server_addr.is_ipv6() {
            "[::]:0"
        } else {
            "0.0.0.0:0"
        };
        let udp = UdpSocket::bind(bind_addr)
            .await
            .map_err(|e| -> Error { Box::new(e) })?;
        udp.connect(inner.server_addr)
            .await
            .map_err(|e| -> Error { Box::new(e) })?;
        let socket = Socket::try_from(udp).map_err(|e| -> Error { Box::new(e) })?;

        // Rebuild a borrowing ConnectionParams from the owned config.
        let mut settings = inner.config.settings.clone();
        settings.verify_peer = inner.config.verify_peer;
        let tls = match (&inner.config.cert_path, &inner.config.key_path) {
            (Some(cert), Some(key)) => Some(TlsCertificatePaths {
                cert,
                private_key: key,
                kind: CertificateKind::X509,
            }),
            _ => None,
        };
        let params = ConnectionParams::new_client(settings, tls, inner.config.hooks.clone());

        let (driver, handles) =
            QuicheDriver::<Bytes>::new(false, DEFAULT_ACCEPT_BIDI_CAP, DEFAULT_ACCEPT_UNI_CAP);

        match connect_with_config(socket, Some(&inner.server_name), &params, driver).await {
            Ok(_qconn) => {
                // The handshake already succeeded (that is exactly when this
                // future resolves `Ok`), so `on_conn_established` has already
                // resolved the establishment signal — this awaits immediately
                // (§7.2). The returned `QuicConnection` is metadata only (§2.3
                // T2) and dropped here.
                handles
                    .into_established_connection()
                    .await
                    .map_err(|e| -> Error { Box::new(e) })
            }
            // Before establishment `should_act()` is false, so tokio-quiche does
            // not call `on_conn_close`; this future's concrete error is the only
            // exact client failure available (§7.2, §8.4). It is already the boxed
            // `Error` shape, so return it directly without re-wrapping.
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr() -> SocketAddr {
        "127.0.0.1:4433".parse().unwrap()
    }

    #[test]
    fn new_rejects_unreadable_cert() {
        let config = H3QuicheClientConfig {
            cert_path: Some("/nonexistent/quiche-h3/missing.crt".to_string()),
            key_path: Some("/nonexistent/quiche-h3/missing.key".to_string()),
            ..H3QuicheClientConfig::default()
        };
        let err = match H3QuicheConnector::new(addr(), "localhost".to_string(), config) {
            Ok(_) => panic!("unreadable client cert must be rejected"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("certificate"));
    }

    #[test]
    fn new_accepts_config_without_mtls() {
        let config = H3QuicheClientConfig::default();
        let connector = H3QuicheConnector::new(addr(), "localhost".to_string(), config)
            .expect("no-mTLS config is valid");
        // `Clone` is part of the public contract (H3Connector: Clone + 'static).
        let _cloned = connector.clone();
    }

    // Regression (review finding): a lone cert or key silently disables mTLS.
    #[test]
    fn new_rejects_partial_mtls() {
        let cert_only = H3QuicheClientConfig {
            cert_path: Some("/tmp/some.crt".to_string()),
            key_path: None,
            ..H3QuicheClientConfig::default()
        };
        assert!(H3QuicheConnector::new(addr(), "localhost".to_string(), cert_only).is_err());
        let key_only = H3QuicheClientConfig {
            cert_path: None,
            key_path: Some("/tmp/some.key".to_string()),
            ..H3QuicheClientConfig::default()
        };
        assert!(H3QuicheConnector::new(addr(), "localhost".to_string(), key_only).is_err());
    }

    // Regression (review finding): a directory passes std::fs::metadata but is
    // not a readable file — the readable-file check must reject it.
    #[test]
    fn new_rejects_directory_as_cert() {
        let dir = std::env::temp_dir();
        let config = H3QuicheClientConfig {
            cert_path: Some(dir.to_string_lossy().into_owned()),
            key_path: Some(dir.to_string_lossy().into_owned()),
            ..H3QuicheClientConfig::default()
        };
        let err = match H3QuicheConnector::new(addr(), "localhost".to_string(), config) {
            Ok(_) => panic!("a directory is not a valid cert file"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("regular file"));
    }

    // Regression (review finding): an empty ALPN cannot negotiate HTTP/3.
    #[test]
    fn new_rejects_empty_alpn() {
        let mut settings = QuicSettings::default();
        settings.alpn = Vec::new();
        let config = H3QuicheClientConfig {
            settings,
            ..H3QuicheClientConfig::default()
        };
        let err = match H3QuicheConnector::new(addr(), "localhost".to_string(), config) {
            Ok(_) => panic!("empty ALPN must be rejected"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("ALPN"));
    }
}
