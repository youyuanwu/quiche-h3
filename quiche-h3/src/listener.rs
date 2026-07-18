//! Server wiring: [`H3QuicheAcceptor`] over `tokio_quiche::listen()` (design
//! §7.1). One acceptor wraps exactly one `QuicConnectionStream` (one socket).
//!
//! `accept()` must never serialize handshakes: awaiting establishment inline
//! would let one slow/stalled TLS handshake block acceptance of every other
//! pending connection (a head-of-line-blocking / DoS hazard). Instead the
//! acceptor drives a bounded number of handshakes concurrently through a
//! [`FuturesUnordered`] and returns whichever completes first. Per-connection
//! setup failures and per-packet stream item errors are surfaced **log-only**
//! and skipped; `accept()`'s `Err` is reserved for listener/socket-fatal
//! conditions (§7.1, §8.4).

use std::num::NonZeroUsize;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use tokio::net::UdpSocket;
use tokio_quiche::metrics::DefaultMetrics;
use tokio_quiche::settings::{CertificateKind, Hooks, QuicSettings, TlsCertificatePaths};
use tokio_quiche::{ConnectionParams, QuicConnectionStream};

use crate::driver::QuicheDriver;
use crate::stream::Connection;
use crate::Error;

/// Default per-listener cap on concurrently-progressing handshakes (§7.1). The
/// incoming branch of the accept loop is not polled once this many handshakes
/// are in flight, bounding the bridge-owned worker/future set under a flood.
pub const DEFAULT_MAX_IN_FLIGHT_HANDSHAKES: usize = 256;

/// Provisional accept-queue depth for peer-initiated bidi/uni streams (§5.2).
const DEFAULT_ACCEPT_BIDI_CAP: usize = 128;
const DEFAULT_ACCEPT_UNI_CAP: usize = 128;

/// Owned, cloneable server configuration. Owns everything `tokio_quiche::listen`
/// borrows (TLS cert/key **paths**, `QuicSettings`, `Hooks`) so the acceptor is
/// self-contained, plus the accept-queue depths and per-listener handshake cap
/// (§7.1).
#[derive(Clone)]
pub struct H3QuicheServerConfig {
    /// Path to the PEM X.509 certificate chain.
    pub cert_path: String,
    /// Path to the PEM private key.
    pub key_path: String,
    /// QUIC transport settings (ALPN defaults to `[b"h3"]`).
    pub settings: QuicSettings,
    /// Connection lifecycle hooks.
    pub hooks: Hooks,
    /// Bound on the peer-initiated bidi accept queue.
    pub accept_bidi_cap: usize,
    /// Bound on the peer-initiated uni accept queue.
    pub accept_uni_cap: usize,
    /// Cap on concurrently-progressing handshakes per listener (§7.1).
    pub max_in_flight_handshakes: NonZeroUsize,
}

impl Default for H3QuicheServerConfig {
    fn default() -> Self {
        Self {
            cert_path: String::new(),
            key_path: String::new(),
            settings: QuicSettings::default(),
            hooks: Hooks::default(),
            accept_bidi_cap: DEFAULT_ACCEPT_BIDI_CAP,
            accept_uni_cap: DEFAULT_ACCEPT_UNI_CAP,
            max_in_flight_handshakes: NonZeroUsize::new(DEFAULT_MAX_IN_FLIGHT_HANDSHAKES)
                .expect("DEFAULT_MAX_IN_FLIGHT_HANDSHAKES is non-zero"),
        }
    }
}

/// A standalone HTTP/3-over-quiche acceptor: one per bound socket. Its
/// [`accept`](H3QuicheAcceptor::accept) yields the crate's front-end
/// [`Connection<Bytes>`], which implements [`h3::quic::Connection<Bytes>`]. The
/// `h3_util::H3Acceptor` conformance is provided by the h3-util `quiche_h3`
/// wrapper (Phase 9), keeping this crate free of a circular h3-util dependency
/// (design §10).
pub struct H3QuicheAcceptor {
    stream: QuicConnectionStream<DefaultMetrics>,
    handshakes: FuturesUnordered<BoxFuture<'static, Result<Connection<Bytes>, Error>>>,
    max_in_flight_handshakes: NonZeroUsize,
    accept_bidi_cap: usize,
    accept_uni_cap: usize,
    incoming_done: bool,
}

impl H3QuicheAcceptor {
    /// Bind the given sockets and return one acceptor per socket
    /// (`tokio_quiche::listen` yields one `QuicConnectionStream` per socket).
    /// Validation failures — an empty socket set or unreadable/missing TLS
    /// cert/key files — surface as `Err(Error)` **here**, before any accept loop
    /// runs. Per-connection handshake failures never surface here; they are
    /// log-only (§8.4).
    pub fn bind(
        sockets: impl IntoIterator<Item = UdpSocket>,
        config: &H3QuicheServerConfig,
    ) -> Result<Vec<Self>, Error> {
        let sockets: Vec<UdpSocket> = sockets.into_iter().collect();
        if sockets.is_empty() {
            return Err("quiche-h3: H3QuicheAcceptor::bind requires at least one socket".into());
        }

        // Validate the TLS material is a readable file and the ALPN is set
        // before handing paths to quiche (which otherwise surfaces the failure
        // only asynchronously per-conn).
        crate::ensure_readable_file(&config.cert_path, "TLS certificate")?;
        crate::ensure_readable_file(&config.key_path, "TLS private key")?;
        crate::ensure_nonempty_alpn(&config.settings, "server config")?;

        // `ConnectionParams<'_>` borrows the owned cert/key paths; `listen` only
        // borrows `params` for the duration of the call, so this temporary is
        // sufficient.
        let params = ConnectionParams::new_server(
            config.settings.clone(),
            TlsCertificatePaths {
                cert: &config.cert_path,
                private_key: &config.key_path,
                kind: CertificateKind::X509,
            },
            config.hooks.clone(),
        );

        let streams = tokio_quiche::listen(sockets, params, DefaultMetrics)
            .map_err(|e| -> Error { Box::new(e) })?;

        Ok(streams
            .into_iter()
            .map(|stream| Self {
                stream,
                handshakes: FuturesUnordered::new(),
                max_in_flight_handshakes: config.max_in_flight_handshakes,
                accept_bidi_cap: config.accept_bidi_cap,
                accept_uni_cap: config.accept_uni_cap,
                incoming_done: false,
            })
            .collect())
    }

    /// Accept the next established connection (§7.1). Returns `Ok(Some(conn))`
    /// for a ready connection, `Ok(None)` once the listener has ended and all
    /// in-flight handshakes have drained, or `Err` only for a listener/socket
    /// fatal. Per-connection setup failures and per-packet stream item errors
    /// are logged and skipped — one peer can never fail `accept` for everyone.
    pub async fn accept(&mut self) -> Result<Option<Connection<Bytes>>, Error> {
        loop {
            if self.incoming_done && self.handshakes.is_empty() {
                return Ok(None);
            }

            tokio::select! {
                biased;

                // 1. A started handshake finished. Decoupled from how many other
                //    handshakes are still in flight, so one stalled peer cannot
                //    block delivery of a ready connection.
                Some(res) = self.handshakes.next(), if !self.handshakes.is_empty() => {
                    match res {
                        Ok(conn) => return Ok(Some(conn)),
                        Err(_e) => {
                            // Per-connection pre-handshake failure. `accept` has
                            // no per-connection error channel, so surface it
                            // log-only and keep serving others (§8.4).
                            #[cfg(feature = "tracing")]
                            tracing::debug!(
                                error = %_e,
                                "quiche-h3: connection setup failed before handshake"
                            );
                            continue;
                        }
                    }
                }

                // 2. A new incoming connection: START its handshake but DO NOT
                //    await it here. Disabled while the in-flight set is full, so
                //    backpressure is exerted by not polling the stream again
                //    until a slot frees.
                iqc = self.stream.next(),
                    if !self.incoming_done
                        && self.handshakes.len() < self.max_in_flight_handshakes.get() =>
                {
                    let Some(item) = iqc else {
                        self.incoming_done = true;
                        continue;
                    };
                    match item {
                        Ok(iqc) => {
                            let (driver, handles) = QuicheDriver::<Bytes>::new(
                                true,
                                self.accept_bidi_cap,
                                self.accept_uni_cap,
                            );
                            // `start` is synchronous and returns before the
                            // handshake runs; the returned `QuicConnection` is
                            // metadata only and safe to drop (§2.3 T2).
                            let _qconn = iqc.start(driver);
                            self.handshakes.push(
                                handles
                                    .into_established_connection()
                                    .map(|res| res.map_err(|e| -> Error { Box::new(e) }))
                                    .boxed(),
                            );
                        }
                        Err(_e) => {
                            // A stream item `Err` is a per-packet/per-attempt
                            // failure on the pinned 0.19.1 surface, not listener
                            // termination (§7.1 T4). Log and continue.
                            #[cfg(feature = "tracing")]
                            tracing::debug!(
                                error = %_e,
                                "quiche-h3: rejected initial connection packet; listener continues"
                            );
                            continue;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_rejects_empty_socket_set() {
        let config = H3QuicheServerConfig::default();
        // `H3QuicheAcceptor` is not `Debug`, so inspect the `Result` by hand
        // rather than via `expect_err`.
        match H3QuicheAcceptor::bind(Vec::<UdpSocket>::new(), &config) {
            Ok(_) => panic!("empty socket set must be rejected"),
            Err(e) => assert!(e.to_string().contains("at least one socket")),
        }
    }

    #[tokio::test]
    async fn bind_rejects_missing_cert() {
        // A real ephemeral socket is required to reach the TLS-material check
        // (the empty-set guard runs first); no handshake is performed.
        let sock = UdpSocket::bind("127.0.0.1:0").await.expect("bind udp");
        let config = H3QuicheServerConfig {
            cert_path: "/nonexistent/quiche-h3/missing.crt".to_string(),
            key_path: "/nonexistent/quiche-h3/missing.key".to_string(),
            ..H3QuicheServerConfig::default()
        };
        let err = match H3QuicheAcceptor::bind([sock], &config) {
            Ok(_) => panic!("missing cert path must be rejected"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("certificate"));
    }

    #[test]
    fn default_handshake_cap_is_256() {
        assert_eq!(DEFAULT_MAX_IN_FLIGHT_HANDSHAKES, 256);
        assert_eq!(
            H3QuicheServerConfig::default()
                .max_in_flight_handshakes
                .get(),
            256
        );
    }
}
