//! Phase 8 CI compatibility test (design §10, §11, §8.4).
//!
//! This test does **not** bind UDP or run a handshake; it is a *compile-time
//! contract guard* over the exact upstream API surface the bridge maps. It
//!
//!   1. constructs one value of every `h3` error variant the bridge depends on
//!      (§8.4), so a minor `h3` bump that renames or reshapes any of them fails
//!      this build rather than letting the bridge silently mismap; and
//!   2. names every load-bearing `quiche` / `tokio-quiche` item via the crate's
//!      re-exports (`quiche_h3::quiche`, `quiche_h3::tokio_quiche`), so a
//!      reshaped signature or renamed variant fails the build here.
//!
//! Because the bridge's own `error` / `ConnTerminal` mapping types are private,
//! this test constructs the `h3` variants directly from the `h3` crate and
//! reaches the transport APIs only through the public re-exports.
//!
//! Runs in the default (non-ignored) suite — it is pure compilation + trivial
//! asserts:
//!
//! ```text
//! cargo test -p quiche-h3 --test ci_compat
//! ```

use std::sync::Arc;

use h3::quic::{ConnectionErrorIncoming, StreamErrorIncoming};

use quiche_h3::quiche;
use quiche_h3::tokio_quiche;

/// A trivial error type for the `Undefined` / `Unknown` opaque variants.
#[derive(Debug)]
struct DummyErr;
impl std::fmt::Display for DummyErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("dummy")
    }
}
impl std::error::Error for DummyErr {}

/// Construct one value of every mapped `h3::quic::ConnectionErrorIncoming`
/// variant (§8.4). A renamed/reshaped variant fails to compile here.
#[test]
fn h3_connection_error_variants_are_stable() {
    let variants = [
        ConnectionErrorIncoming::ApplicationClose { error_code: 0x100 },
        ConnectionErrorIncoming::Timeout,
        ConnectionErrorIncoming::InternalError("internal".to_string()),
        ConnectionErrorIncoming::Undefined(Arc::new(DummyErr)),
    ];

    // Exhaustively match so an *added* variant also trips this guard.
    for v in variants {
        match v {
            ConnectionErrorIncoming::ApplicationClose { error_code } => {
                assert_eq!(error_code, 0x100);
            }
            ConnectionErrorIncoming::Timeout => {}
            ConnectionErrorIncoming::InternalError(msg) => assert_eq!(msg, "internal"),
            ConnectionErrorIncoming::Undefined(e) => {
                let _: Arc<dyn std::error::Error + Send + Sync> = e;
            }
        }
    }
}

/// Construct one value of every mapped `h3::quic::StreamErrorIncoming` variant
/// (§8.4), including the nested `ConnectionErrorIncoming` shape.
#[test]
fn h3_stream_error_variants_are_stable() {
    let variants = [
        StreamErrorIncoming::ConnectionErrorIncoming {
            connection_error: ConnectionErrorIncoming::Timeout,
        },
        StreamErrorIncoming::StreamTerminated { error_code: 7 },
        StreamErrorIncoming::Unknown(Box::new(DummyErr)),
    ];

    for v in variants {
        match v {
            StreamErrorIncoming::ConnectionErrorIncoming { connection_error } => {
                assert!(matches!(connection_error, ConnectionErrorIncoming::Timeout));
            }
            StreamErrorIncoming::StreamTerminated { error_code } => assert_eq!(error_code, 7),
            StreamErrorIncoming::Unknown(e) => {
                let _: Box<dyn std::error::Error + Send + Sync> = e;
            }
        }
    }
}

/// `h3::error::Code::H3_NO_ERROR` is the code the bridge uses for graceful
/// last-handle teardown (§5.2, §8.3). Pin its numeric value.
#[test]
fn h3_no_error_code_is_stable() {
    assert_eq!(h3::error::Code::H3_NO_ERROR.value(), 0x100);
}

/// Every load-bearing `quiche::Error` variant the bridge classifies (§8.3,
/// §8.4) must exist with the same shape (unit vs. code-carrying). A renamed or
/// reshaped variant fails to compile in this exhaustive-ish match.
#[test]
fn quiche_error_variants_are_stable() {
    // Name each load-bearing variant explicitly; the constructor form (unit vs.
    // tuple) is itself the contract.
    let load_bearing = [
        quiche::Error::Done,
        quiche::Error::StreamLimit,
        quiche::Error::InvalidState,
        quiche::Error::FinalSize,
        quiche::Error::FlowControl,
        quiche::Error::StreamStopped(9),
        quiche::Error::StreamReset(9),
    ];

    for e in load_bearing {
        match e {
            quiche::Error::Done => {}
            quiche::Error::StreamLimit => {}
            quiche::Error::InvalidState => {}
            quiche::Error::FinalSize => {}
            quiche::Error::FlowControl => {}
            quiche::Error::StreamStopped(code) => assert_eq!(code, 9),
            quiche::Error::StreamReset(code) => assert_eq!(code, 9),
            // Other variants are not load-bearing for the mapping; ignore.
            _ => {}
        }
    }
}

/// `quiche::ConnectionError` carries the `is_app` / `error_code` / `reason`
/// fields the bridge reads when classifying a `CONNECTION_CLOSE` (§8.2, §8.4).
#[test]
fn quiche_connection_error_fields_are_stable() {
    let ce = quiche::ConnectionError {
        is_app: true,
        error_code: 0x1234,
        reason: b"bye".to_vec(),
    };
    let is_app: bool = ce.is_app;
    let error_code: u64 = ce.error_code;
    let reason: Vec<u8> = ce.reason;
    assert!(is_app);
    assert_eq!(error_code, 0x1234);
    assert_eq!(reason, b"bye");
}

/// `quiche::h3::APPLICATION_PROTOCOL` is the ALPN the bridge negotiates. Pin its
/// shape and value.
#[test]
fn quiche_application_protocol_is_stable() {
    let alpn: &[&[u8]] = quiche::h3::APPLICATION_PROTOCOL;
    assert!(alpn.iter().any(|p| *p == b"h3"), "ALPN advertises h3");
}

/// Reference the load-bearing `tokio-quiche` items by path so a renamed/removed
/// export fails the build. The `use` statements themselves are the assertion;
/// the trait-bound helper and the `Noop` impl below additionally guard the
/// `ApplicationOverQuic` method surface.
#[test]
fn tokio_quiche_items_are_reachable() {
    // Path references: a renamed or removed export fails to resolve here.
    use tokio_quiche::settings::{
        CertificateKind as _CertificateKind, QuicSettings as _QuicSettings,
        TlsCertificatePaths as _TlsCertificatePaths,
    };
    use tokio_quiche::ApplicationOverQuic as _ApplicationOverQuic;
    use tokio_quiche::{ConnectionParams as _ConnectionParams, QuicConnection as _QuicConnection};

    // Bind the entrypoint-referencing helpers so their signature guards run at
    // type-check time (they are never actually called).
    let _ = connect_with_config_ref as fn();
    let _ = listen_ref as fn(_, _);

    // The bridge's front-end `Connection<Bytes>` must satisfy `h3`'s connection
    // trait; the acceptor/connector implement `ApplicationOverQuic` internally.
    fn _assert_app_trait<A: _ApplicationOverQuic>() {}
    let _ = _assert_app_trait::<Noop> as fn();

    fn _uses_types(
        _p: &_ConnectionParams,
        _s: &_QuicSettings,
        _c: &_TlsCertificatePaths,
        _k: _CertificateKind,
        _q: &_QuicConnection,
    ) {
    }
    let _ = _uses_types as fn(_, _, _, _, _);
}

/// Bind `connect_with_config` behind its generic bounds so a reshaped signature
/// fails the build. Never called.
fn connect_with_config_ref() {
    let _f = tokio_quiche::quic::connect_with_config::<
        tokio::net::UdpSocket,
        tokio::net::UdpSocket,
        Noop,
    >;
    let _ = _f;
}

/// Reference `listen` in call position with concrete types so a reshaped
/// signature fails the build. Never called.
fn listen_ref(socks: Vec<tokio::net::UdpSocket>, params: tokio_quiche::ConnectionParams<'_>) {
    let _ = tokio_quiche::listen(socks, params, tokio_quiche::metrics::DefaultMetrics);
}

/// A do-nothing `ApplicationOverQuic` used only to satisfy the trait-bound
/// references above; never driven. Implementing the full method set guards the
/// trait surface the bridge relies on.
struct Noop {
    buf: Vec<u8>,
}
impl tokio_quiche::ApplicationOverQuic for Noop {
    fn on_conn_established(
        &mut self,
        _qconn: &mut tokio_quiche::quic::QuicheConnection,
        _hs: &tokio_quiche::quic::HandshakeInfo,
    ) -> tokio_quiche::QuicResult<()> {
        Ok(())
    }
    fn should_act(&self) -> bool {
        false
    }
    fn buffer(&mut self) -> &mut [u8] {
        &mut self.buf
    }
    fn wait_for_data(
        &mut self,
        _qconn: &mut tokio_quiche::quic::QuicheConnection,
    ) -> impl std::future::Future<Output = tokio_quiche::QuicResult<()>> + Send {
        std::future::pending()
    }
    fn process_reads(
        &mut self,
        _qconn: &mut tokio_quiche::quic::QuicheConnection,
    ) -> tokio_quiche::QuicResult<()> {
        Ok(())
    }
    fn process_writes(
        &mut self,
        _qconn: &mut tokio_quiche::quic::QuicheConnection,
    ) -> tokio_quiche::QuicResult<()> {
        Ok(())
    }
}
