//! `quiche-h3` — an [`h3::quic`] bridge that runs hyperium [`h3`] over
//! Cloudflare [`quiche`], driven asynchronously by [`tokio_quiche`].
//!
//! See `docs/design/quiche-h3-bridge.md` for the full design.
//!
//! The public surface exposes a standalone [`H3QuicheAcceptor`] /
//! [`H3QuicheConnector`] whose `accept()` / `connect()` yield the crate's
//! front-end [`Connection<Bytes>`], which implements
//! [`h3::quic::Connection<Bytes>`]. The `h3_util::H3Acceptor` /
//! `h3_util::H3Connector` trait conformance lives in h3-util's `quiche_h3`
//! wrapper (Phase 9): because `h3-util` depends on `quiche-h3`, this crate must
//! **not** depend on `h3-util` (that would be circular, design §10). The
//! strongest checks available here are the compile-time assertions below that
//! [`Connection<Bytes>`] implements [`h3::quic::Connection<Bytes>`] and that
//! `accept()` / `connect()` return that type.
//!
//! [`h3::quic`]: h3::quic
//! [`quiche`]: tokio_quiche::quiche

// Re-export the transport crates (design §10) so downstreams can build configs
// and credentials without a separate dependency.
pub use tokio_quiche;
pub use tokio_quiche::quiche;

mod buffer;
mod conn;
mod connector;
mod driver;
mod error;
mod listener;
mod stream;

use bytes::Bytes;

/// The crate error type: a boxed, thread-safe error matching h3-util's boxed
/// `Error` shape (design §8.4). Defined here rather than imported so this crate
/// carries no `h3-util` dependency (design §10).
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Validate that `path` names an existing, readable **file** (not a directory)
/// by opening it (design §7 validation, S1). `std::fs::metadata` is
/// insufficient: it succeeds for directories and does not prove the contents
/// are readable, deferring the failure to the asynchronous per-connection path.
pub(crate) fn ensure_readable_file(path: &str, label: &str) -> Result<(), Error> {
    let meta = std::fs::metadata(path).map_err(|e| -> Error {
        format!("quiche-h3: {label} {path:?} is not accessible: {e}").into()
    })?;
    if !meta.is_file() {
        return Err(format!("quiche-h3: {label} {path:?} is not a regular file").into());
    }
    // Opening proves the contents are actually readable.
    std::fs::File::open(path).map_err(|e| -> Error {
        format!("quiche-h3: {label} {path:?} is not readable: {e}").into()
    })?;
    Ok(())
}

/// Reject an empty ALPN list (design §7 validation): an HTTP/3 endpoint with no
/// application protocol cannot negotiate and must not be exposed as HTTP/3.
pub(crate) fn ensure_nonempty_alpn(
    settings: &tokio_quiche::settings::QuicSettings,
    label: &str,
) -> Result<(), Error> {
    if settings.alpn.is_empty() {
        return Err(format!("quiche-h3: {label} has an empty ALPN list").into());
    }
    Ok(())
}

pub use connector::{H3QuicheClientConfig, H3QuicheConnector};
pub use listener::{H3QuicheAcceptor, H3QuicheServerConfig, DEFAULT_MAX_IN_FLIGHT_HANDSHAKES};

// Front-end `h3::quic` surface. These name the connection/stream types produced
// by `accept()` / `connect()`; the h3-util wrapper (Phase 9) drives `h3` over
// them.
pub use stream::{Connection, H3RecvStream, H3SendStream, H3Stream, StreamOpener};

// Compile-time conformance. Without an `h3-util` dependency (which would be
// circular, design §10) the strongest checks available are that the front-end
// `Connection<Bytes>` implements `h3::quic::Connection<Bytes>`, that the
// connector is `Clone + Send + 'static`, and that `accept()` / `connect()`
// return exactly `Connection<Bytes>`. The h3-util `H3Acceptor`/`H3Connector`
// conformance is verified in the h3-util `quiche_h3` wrapper (Phase 9).
const _: fn() = || {
    fn assert_h3_conn<C: h3::quic::Connection<Bytes>>() {}
    assert_h3_conn::<Connection<Bytes>>();

    fn assert_clone_send_static<T: Clone + Send + 'static>() {}
    assert_clone_send_static::<H3QuicheConnector>();

    // Pin the accept/connect return types to `Connection<Bytes>` (behind the
    // outer `Result`/`Option`). Never called; the coercion is the assertion.
    fn accept_yields_connection(a: &mut H3QuicheAcceptor) {
        fn is_accept_result(
            _: impl std::future::Future<Output = Result<Option<Connection<Bytes>>, Error>>,
        ) {
        }
        is_accept_result(a.accept());
    }
    fn connect_yields_connection(c: &H3QuicheConnector) {
        fn is_connect_result(
            _: impl std::future::Future<Output = Result<Connection<Bytes>, Error>>,
        ) {
        }
        is_connect_result(c.connect());
    }
    let _ = accept_yields_connection;
    let _ = connect_yields_connection;
};
