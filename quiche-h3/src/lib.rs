//! `quiche-h3` — an [`h3::quic`] bridge that runs hyperium [`h3`] over
//! Cloudflare [`quiche`], driven asynchronously by [`tokio_quiche`].
//!
//! See `docs/design/quiche-h3-bridge.md` for the full design. This crate is
//! under active implementation; the module skeleton below mirrors design §4.2.
//!
//! [`h3::quic`]: h3::quic
//! [`quiche`]: tokio_quiche::quiche

// Re-export the transport crates (design §10) so downstreams can build configs
// and credentials without a separate dependency.
pub use tokio_quiche;
pub use tokio_quiche::quiche;

mod buffer;
mod connector;
mod conn;
mod driver;
mod error;
mod listener;
mod stream;
