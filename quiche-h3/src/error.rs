//! Internal terminal-reason types and error mapping (design §8).
//!
//! The worker captures the *exact* terminal reason synchronously at the quiche
//! call site (§8.1) and carries it as typed data over the channels — channel
//! closure is never itself a semantic signal. This module defines those
//! crate-private reason types (§8.2), the quiche → reason classifiers (§8.3),
//! and the reason → [`h3::quic`] error mappings (§8.4).
#![allow(dead_code)] // wired up incrementally across Phases 2–8

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use h3::quic::{ConnectionErrorIncoming, StreamErrorIncoming};

use crate::quiche;

/// HTTP/3 `H3_NO_ERROR` application error code, used for graceful last-handle
/// teardown (§5.2, §8.3). Mirrors [`h3::error::Code::H3_NO_ERROR`].
pub(crate) const H3_NO_ERROR: u64 = 0x100;

/// HTTP/3 `H3_REQUEST_CANCELLED` application error code, used by the worker's
/// direction-aware cleanup when an open reply becomes undeliverable *after*
/// materialization (the poller cancelled in the window between the
/// `reply.is_closed()` check and `reply.send`) so the burned stream credit is
/// reclaimed (§6.2). Mirrors [`h3::error::Code::H3_REQUEST_CANCELLED`].
pub(crate) const H3_REQUEST_CANCELLED: u64 = 0x10c;

/// Who initiated the `CONNECTION_CLOSE` (§8.2). Retaining the origin is required
/// so a *local* close is never reported to `h3` as though the peer closed
/// (finding 5): `h3` treats `ApplicationClose` as a remote error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CloseOrigin {
    Peer,
    Local,
}

/// The connection terminal, computed **once** by the worker and published to
/// every live half and the accept-terminal cells (§8.2).
#[derive(Clone, Debug)]
pub(crate) enum ConnTerminal {
    /// `is_app == true` CONNECTION_CLOSE. `reason` is retained for diagnostics
    /// (h3 0.0.8 exposes only the code, but the reason must not be lost).
    AppClose {
        origin: CloseOrigin,
        error_code: u64,
        reason: Bytes,
    },
    /// `is_app == false`, or a transport-layer failure.
    Transport {
        origin: CloseOrigin,
        error_code: u64,
    },
    /// `conn.is_timed_out()`.
    Timeout,
    /// Our own contract violation / adapter bug — never a normal peer event.
    Internal(&'static str),
}

/// Terminal reason for a receive direction (§8.2).
#[derive(Clone, Debug)]
pub(crate) enum RecvEnd {
    /// Clean end (after draining buffered bytes).
    Fin,
    /// Peer `RESET_STREAM`, code verbatim.
    Reset { error_code: u64 },
    /// The connection ended underneath this stream.
    Conn(Arc<ConnTerminal>),
}

/// Terminal reason for a send direction (§8.2).
#[derive(Clone, Debug)]
pub(crate) enum SendEnd {
    /// Peer `STOP_SENDING`, code verbatim.
    Stopped { error_code: u64 },
    /// This adapter already reset its own send direction.
    Reset { error_code: u64 },
    /// The connection ended underneath this stream.
    Conn(Arc<ConnTerminal>),
}

// ===== §8.4: internal reason → h3::quic error =====

/// Wraps a *local* connection close so `h3` sees an opaque `Undefined` error
/// rather than a peer `ApplicationClose` (§8.4).
#[derive(Debug)]
struct LocalConnectionClose {
    error_code: u64,
    is_app: bool,
}

impl fmt::Display for LocalConnectionClose {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = if self.is_app {
            "application"
        } else {
            "transport"
        };
        write!(
            f,
            "connection closed locally ({kind} error code {:#x})",
            self.error_code
        )
    }
}

impl std::error::Error for LocalConnectionClose {}

/// Wraps a transport-layer terminal: `h3` has no transport-code variant, so we
/// surface a descriptive `Undefined` carrying the code (§8.4).
#[derive(Debug)]
struct TransportTerminal {
    error_code: u64,
    from_peer: bool,
}

impl fmt::Display for TransportTerminal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let who = if self.from_peer { "peer" } else { "local" };
        write!(
            f,
            "quic transport error (code {:#x}, {who}-initiated)",
            self.error_code
        )
    }
}

impl std::error::Error for TransportTerminal {}

impl ConnTerminal {
    /// Map to an `h3` [`ConnectionErrorIncoming`] (§8.4).
    pub(crate) fn to_h3(&self) -> ConnectionErrorIncoming {
        match self {
            ConnTerminal::AppClose {
                origin: CloseOrigin::Peer,
                error_code,
                ..
            } => ConnectionErrorIncoming::ApplicationClose {
                error_code: *error_code,
            },
            // A local application close is not a peer app-close: report opaque.
            ConnTerminal::AppClose {
                origin: CloseOrigin::Local,
                error_code,
                ..
            } => ConnectionErrorIncoming::Undefined(Arc::new(LocalConnectionClose {
                error_code: *error_code,
                is_app: true,
            })),
            ConnTerminal::Transport { origin, error_code } => {
                ConnectionErrorIncoming::Undefined(Arc::new(TransportTerminal {
                    error_code: *error_code,
                    from_peer: *origin == CloseOrigin::Peer,
                }))
            }
            ConnTerminal::Timeout => ConnectionErrorIncoming::Timeout,
            ConnTerminal::Internal(msg) => ConnectionErrorIncoming::InternalError(msg.to_string()),
        }
    }
}

impl RecvEnd {
    /// Map to the outcome of `RecvStream::poll_data` (§8.4):
    /// `None` == clean EOF (`Ok(None)`); `Some(err)` is the stream error.
    pub(crate) fn to_h3(&self) -> Option<StreamErrorIncoming> {
        match self {
            RecvEnd::Fin => None,
            RecvEnd::Reset { error_code } => Some(StreamErrorIncoming::StreamTerminated {
                error_code: *error_code,
            }),
            RecvEnd::Conn(t) => Some(StreamErrorIncoming::ConnectionErrorIncoming {
                connection_error: t.to_h3(),
            }),
        }
    }
}

impl SendEnd {
    /// Map to a `SendStream::poll_ready` / `poll_finish` error (§8.4).
    pub(crate) fn to_h3(&self) -> StreamErrorIncoming {
        match self {
            SendEnd::Stopped { error_code } | SendEnd::Reset { error_code } => {
                StreamErrorIncoming::StreamTerminated {
                    error_code: *error_code,
                }
            }
            SendEnd::Conn(t) => StreamErrorIncoming::ConnectionErrorIncoming {
                connection_error: t.to_h3(),
            },
        }
    }
}

/// Build an `h3` `InternalError` connection error for an adapter-contract
/// violation surfaced on a stream method (§8.4, e.g. double `send_data`).
pub(crate) fn internal_stream_error(msg: &'static str) -> StreamErrorIncoming {
    StreamErrorIncoming::ConnectionErrorIncoming {
        connection_error: ConnectionErrorIncoming::InternalError(msg.to_string()),
    }
}

// ===== §8.3: quiche → internal reason (captured on the worker) =====

/// Build a [`ConnTerminal`] from a quiche `CONNECTION_CLOSE` observation (§8.3).
pub(crate) fn conn_terminal_from_error(
    origin: CloseOrigin,
    err: &quiche::ConnectionError,
) -> ConnTerminal {
    if err.is_app {
        ConnTerminal::AppClose {
            origin,
            error_code: err.error_code,
            reason: Bytes::copy_from_slice(&err.reason),
        }
    } else {
        ConnTerminal::Transport {
            origin,
            error_code: err.error_code,
        }
    }
}

/// Classification of a `stream_recv` error (§8.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StreamRecvClass {
    /// `Err(StreamReset(code))` → [`RecvEnd::Reset`].
    Reset(u64),
    /// `Err(Done)` → nothing more readable right now (not terminal).
    Done,
    /// `InvalidState`/`InvalidStreamState`/`FinalSize`/`FlowControl` while
    /// closing → resolve via the connection terminal, not a bespoke error.
    ConnGone,
    /// Any other error is an unexpected invariant violation → `Internal`.
    Bug(&'static str),
}

/// Classify an error returned by `quiche::Connection::stream_recv` (§8.3).
pub(crate) fn classify_stream_recv_error(err: &quiche::Error) -> StreamRecvClass {
    match err {
        quiche::Error::StreamReset(code) => StreamRecvClass::Reset(*code),
        quiche::Error::Done => StreamRecvClass::Done,
        quiche::Error::InvalidState
        | quiche::Error::InvalidStreamState(_)
        | quiche::Error::FinalSize
        | quiche::Error::FlowControl => StreamRecvClass::ConnGone,
        _ => StreamRecvClass::Bug("unexpected stream_recv error"),
    }
}

/// Classification of a `stream_send` error (§8.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StreamSendClass {
    /// `Err(StreamStopped(code))` → [`SendEnd::Stopped`].
    Stopped(u64),
    /// `Err(Done)` → no send capacity right now (not terminal).
    Blocked,
    /// `Err(StreamLimit)` — at open, stream credit is exhausted and the open
    /// stays pending (§6.1); after a materialized open it is a `Bug` (the
    /// caller distinguishes by context).
    Limit,
    /// `InvalidState`/`InvalidStreamState`/`FinalSize`/`FlowControl` while
    /// closing → resolve via the connection terminal.
    ConnGone,
    /// Any other error is an unexpected invariant violation → `Internal`.
    Bug(&'static str),
}

/// Classify an error returned by `quiche::Connection::stream_send` (§8.3).
pub(crate) fn classify_stream_send_error(err: &quiche::Error) -> StreamSendClass {
    match err {
        quiche::Error::StreamStopped(code) => StreamSendClass::Stopped(*code),
        quiche::Error::Done => StreamSendClass::Blocked,
        quiche::Error::StreamLimit => StreamSendClass::Limit,
        quiche::Error::InvalidState
        | quiche::Error::InvalidStreamState(_)
        | quiche::Error::FinalSize
        | quiche::Error::FlowControl => StreamSendClass::ConnGone,
        _ => StreamSendClass::Bug("unexpected stream_send error"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app_close(origin: CloseOrigin, code: u64, reason: &[u8]) -> ConnTerminal {
        ConnTerminal::AppClose {
            origin,
            error_code: code,
            reason: Bytes::copy_from_slice(reason),
        }
    }

    // §8.4 — one value per mapped h3 ConnectionErrorIncoming variant.
    #[test]
    fn conn_terminal_maps_peer_app_close_to_application_close() {
        let t = app_close(CloseOrigin::Peer, 0x10a, b"boom");
        match t.to_h3() {
            ConnectionErrorIncoming::ApplicationClose { error_code } => {
                assert_eq!(error_code, 0x10a)
            }
            other => panic!("expected ApplicationClose, got {other:?}"),
        }
    }

    #[test]
    fn conn_terminal_maps_local_app_close_to_undefined() {
        let t = app_close(CloseOrigin::Local, 0x100, b"");
        match t.to_h3() {
            ConnectionErrorIncoming::Undefined(e) => {
                assert!(e.to_string().contains("locally"));
            }
            other => panic!("expected Undefined, got {other:?}"),
        }
    }

    #[test]
    fn conn_terminal_maps_transport_to_undefined_with_code() {
        let t = ConnTerminal::Transport {
            origin: CloseOrigin::Peer,
            error_code: 0x7,
        };
        match t.to_h3() {
            ConnectionErrorIncoming::Undefined(e) => {
                let s = e.to_string();
                assert!(s.contains("transport"), "{s}");
                assert!(s.contains("peer"), "{s}");
            }
            other => panic!("expected Undefined, got {other:?}"),
        }
    }

    #[test]
    fn conn_terminal_maps_timeout_and_internal() {
        assert!(matches!(
            ConnTerminal::Timeout.to_h3(),
            ConnectionErrorIncoming::Timeout
        ));
        match ConnTerminal::Internal("bug").to_h3() {
            ConnectionErrorIncoming::InternalError(m) => assert_eq!(m, "bug"),
            other => panic!("expected InternalError, got {other:?}"),
        }
    }

    // §8.4 — RecvEnd mapping (one value per h3 outcome).
    #[test]
    fn recv_end_fin_is_clean_eof() {
        assert!(RecvEnd::Fin.to_h3().is_none());
    }

    #[test]
    fn recv_end_reset_is_stream_terminated() {
        match (RecvEnd::Reset { error_code: 42 }).to_h3() {
            Some(StreamErrorIncoming::StreamTerminated { error_code }) => {
                assert_eq!(error_code, 42)
            }
            other => panic!("expected StreamTerminated, got {other:?}"),
        }
    }

    #[test]
    fn recv_end_conn_is_connection_error() {
        let t = Arc::new(ConnTerminal::Timeout);
        match RecvEnd::Conn(t).to_h3() {
            Some(StreamErrorIncoming::ConnectionErrorIncoming {
                connection_error: ConnectionErrorIncoming::Timeout,
            }) => {}
            other => panic!("expected ConnectionErrorIncoming::Timeout, got {other:?}"),
        }
    }

    // §8.4 — SendEnd mapping (one value per h3 outcome).
    #[test]
    fn send_end_stopped_and_reset_are_stream_terminated() {
        for (end, code) in [
            (SendEnd::Stopped { error_code: 5 }, 5u64),
            (SendEnd::Reset { error_code: 9 }, 9u64),
        ] {
            match end.to_h3() {
                StreamErrorIncoming::StreamTerminated { error_code } => {
                    assert_eq!(error_code, code)
                }
                other => panic!("expected StreamTerminated, got {other:?}"),
            }
        }
    }

    #[test]
    fn send_end_conn_is_connection_error() {
        let t = Arc::new(app_close(CloseOrigin::Peer, 1, b""));
        match SendEnd::Conn(t).to_h3() {
            StreamErrorIncoming::ConnectionErrorIncoming {
                connection_error: ConnectionErrorIncoming::ApplicationClose { error_code },
            } => assert_eq!(error_code, 1),
            other => panic!("expected ConnectionErrorIncoming::ApplicationClose, got {other:?}"),
        }
    }

    #[test]
    fn internal_stream_error_wraps_internal() {
        match internal_stream_error("double send_data") {
            StreamErrorIncoming::ConnectionErrorIncoming {
                connection_error: ConnectionErrorIncoming::InternalError(m),
            } => assert_eq!(m, "double send_data"),
            other => panic!("expected InternalError, got {other:?}"),
        }
    }

    // §8.3 — quiche → reason.
    #[test]
    fn conn_terminal_from_error_splits_app_and_transport() {
        let app = quiche::ConnectionError {
            is_app: true,
            error_code: 0x100,
            reason: b"bye".to_vec(),
        };
        match conn_terminal_from_error(CloseOrigin::Local, &app) {
            ConnTerminal::AppClose {
                origin,
                error_code,
                reason,
            } => {
                assert_eq!(origin, CloseOrigin::Local);
                assert_eq!(error_code, 0x100);
                assert_eq!(&reason[..], b"bye");
            }
            other => panic!("expected AppClose, got {other:?}"),
        }

        let transport = quiche::ConnectionError {
            is_app: false,
            error_code: 0x7,
            reason: Vec::new(),
        };
        match conn_terminal_from_error(CloseOrigin::Peer, &transport) {
            ConnTerminal::Transport { origin, error_code } => {
                assert_eq!(origin, CloseOrigin::Peer);
                assert_eq!(error_code, 0x7);
            }
            other => panic!("expected Transport, got {other:?}"),
        }
    }

    #[test]
    fn classify_recv_errors() {
        assert_eq!(
            classify_stream_recv_error(&quiche::Error::StreamReset(11)),
            StreamRecvClass::Reset(11)
        );
        assert_eq!(
            classify_stream_recv_error(&quiche::Error::Done),
            StreamRecvClass::Done
        );
        for e in [
            quiche::Error::InvalidState,
            quiche::Error::InvalidStreamState(3),
            quiche::Error::FinalSize,
            quiche::Error::FlowControl,
        ] {
            assert_eq!(classify_stream_recv_error(&e), StreamRecvClass::ConnGone);
        }
        assert!(matches!(
            classify_stream_recv_error(&quiche::Error::TlsFail),
            StreamRecvClass::Bug(_)
        ));
    }

    #[test]
    fn classify_send_errors() {
        assert_eq!(
            classify_stream_send_error(&quiche::Error::StreamStopped(66)),
            StreamSendClass::Stopped(66)
        );
        assert_eq!(
            classify_stream_send_error(&quiche::Error::Done),
            StreamSendClass::Blocked
        );
        assert_eq!(
            classify_stream_send_error(&quiche::Error::StreamLimit),
            StreamSendClass::Limit
        );
        for e in [
            quiche::Error::InvalidState,
            quiche::Error::InvalidStreamState(3),
            quiche::Error::FinalSize,
            quiche::Error::FlowControl,
        ] {
            assert_eq!(classify_stream_send_error(&e), StreamSendClass::ConnGone);
        }
        assert!(matches!(
            classify_stream_send_error(&quiche::Error::CryptoFail),
            StreamSendClass::Bug(_)
        ));
    }

    #[test]
    fn h3_no_error_matches_h3_crate() {
        assert_eq!(H3_NO_ERROR, h3::error::Code::H3_NO_ERROR.value());
    }

    #[test]
    fn h3_request_cancelled_matches_h3_crate() {
        assert_eq!(
            H3_REQUEST_CANCELLED,
            h3::error::Code::H3_REQUEST_CANCELLED.value()
        );
    }
}
