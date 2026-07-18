//! [`QuicConn`] — the crate-private abstraction over the `quiche::Connection`
//! methods the worker uses (design §5). The worker is the sole toucher of
//! quiche; routing its calls through this trait lets the §5 state machine be
//! unit-tested against a scriptable [`mock::MockConn`] without a live handshake
//! (design §11 "mock front end").
//!
//! The real implementation is a thin, zero-cost delegation to the inherent
//! `quiche::Connection` methods (inherent methods take precedence over trait
//! methods in `self.method()` resolution, so there is no recursion).
#![allow(dead_code)] // methods are consumed incrementally across Phases 3–5

use crate::quiche::{self, ConnectionError, Shutdown};

/// Result alias mirroring `quiche::Result`.
pub(crate) type QResult<T> = std::result::Result<T, quiche::Error>;

/// The subset of `quiche::Connection` the [`QuicheDriver`](crate::driver) worker
/// depends on (design §5.1–§5.3, §5.5, §8.3). Every method mirrors the quiche
/// signature verbatim so the real impl is a pure delegation.
pub(crate) trait QuicConn {
    /// Read contiguous stream data into `out`; returns `(len, fin)`.
    fn stream_recv(&mut self, id: u64, out: &mut [u8]) -> QResult<(usize, bool)>;
    /// Enqueue stream data; returns the number of bytes accepted.
    fn stream_send(&mut self, id: u64, buf: &[u8], fin: bool) -> QResult<usize>;
    /// Emit `RESET_STREAM` (`Write`) or `STOP_SENDING` (`Read`) with `err`.
    fn stream_shutdown(&mut self, id: u64, direction: Shutdown, err: u64) -> QResult<()>;
    /// Destructive readable cursor: returns the next readable id and dearms it.
    fn stream_readable_next(&mut self) -> Option<u64>;
    /// Destructive writable cursor: returns the next writable id and dearms it.
    fn stream_writable_next(&mut self) -> Option<u64>;
    /// Whether the stream currently has buffered readable data.
    fn stream_readable(&self, id: u64) -> bool;
    /// Whether the stream's receive side is finished (FIN read).
    fn stream_finished(&self, id: u64) -> bool;
    /// Remaining send capacity, or `Err(StreamStopped(code))` if stopped.
    fn stream_capacity(&mut self, id: u64) -> QResult<usize>;
    /// Materialize / prioritize a stream; consumes one unit of stream credit at
    /// open (§6.1). `Err(StreamLimit)` means credit is exhausted.
    fn stream_priority(&mut self, id: u64, urgency: u8, incremental: bool) -> QResult<()>;
    /// Remaining locally-initiable bidi stream credit.
    fn peer_streams_left_bidi(&self) -> u64;
    /// Remaining locally-initiable uni stream credit.
    fn peer_streams_left_uni(&self) -> u64;
    /// Close the connection (`app` = application vs transport).
    fn close(&mut self, app: bool, err: u64, reason: &[u8]) -> QResult<()>;
    /// The peer's `CONNECTION_CLOSE`, if received.
    fn peer_error(&self) -> Option<&ConnectionError>;
    /// The local `CONNECTION_CLOSE`, if sent.
    fn local_error(&self) -> Option<&ConnectionError>;
    /// Whether the connection has hit the idle timeout.
    fn is_timed_out(&self) -> bool;
}

impl QuicConn for tokio_quiche::quic::QuicheConnection {
    #[inline]
    fn stream_recv(&mut self, id: u64, out: &mut [u8]) -> QResult<(usize, bool)> {
        // Inherent method wins over the trait method here (no recursion).
        self.stream_recv(id, out)
    }
    #[inline]
    fn stream_send(&mut self, id: u64, buf: &[u8], fin: bool) -> QResult<usize> {
        self.stream_send(id, buf, fin)
    }
    #[inline]
    fn stream_shutdown(&mut self, id: u64, direction: Shutdown, err: u64) -> QResult<()> {
        self.stream_shutdown(id, direction, err)
    }
    #[inline]
    fn stream_readable_next(&mut self) -> Option<u64> {
        self.stream_readable_next()
    }
    #[inline]
    fn stream_writable_next(&mut self) -> Option<u64> {
        self.stream_writable_next()
    }
    #[inline]
    fn stream_readable(&self, id: u64) -> bool {
        self.stream_readable(id)
    }
    #[inline]
    fn stream_finished(&self, id: u64) -> bool {
        self.stream_finished(id)
    }
    #[inline]
    fn stream_capacity(&mut self, id: u64) -> QResult<usize> {
        self.stream_capacity(id)
    }
    #[inline]
    fn stream_priority(&mut self, id: u64, urgency: u8, incremental: bool) -> QResult<()> {
        self.stream_priority(id, urgency, incremental)
    }
    #[inline]
    fn peer_streams_left_bidi(&self) -> u64 {
        self.peer_streams_left_bidi()
    }
    #[inline]
    fn peer_streams_left_uni(&self) -> u64 {
        self.peer_streams_left_uni()
    }
    #[inline]
    fn close(&mut self, app: bool, err: u64, reason: &[u8]) -> QResult<()> {
        self.close(app, err, reason)
    }
    #[inline]
    fn peer_error(&self) -> Option<&ConnectionError> {
        self.peer_error()
    }
    #[inline]
    fn local_error(&self) -> Option<&ConnectionError> {
        self.local_error()
    }
    #[inline]
    fn is_timed_out(&self) -> bool {
        self.is_timed_out()
    }
}

#[cfg(test)]
pub(crate) mod mock {
    //! A scriptable [`QuicConn`] for unit-testing the §5 state machine.

    use std::collections::{HashMap, HashSet, VecDeque};

    use super::*;

    /// A scripted `stream_recv` outcome.
    #[derive(Clone, Debug)]
    pub(crate) enum RecvStep {
        /// Deliver `bytes` (copied into `out`, truncated to its length) with the
        /// FIN flag.
        Data { bytes: Vec<u8>, fin: bool },
        /// Return a quiche error (e.g. `Done`, `StreamReset(code)`).
        Err(quiche::Error),
    }

    /// A recorded `stream_shutdown` call (`is_write` distinguishes RESET_STREAM
    /// from STOP_SENDING, since `quiche::Shutdown` is not `Clone`/`Debug`).
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub(crate) struct ShutdownCall {
        pub id: u64,
        pub is_write: bool,
        pub code: u64,
    }

    /// A scriptable mock connection. Fields default to benign/empty; tests set
    /// only what they exercise.
    #[derive(Default)]
    pub(crate) struct MockConn {
        // --- readable/writable discovery cursors ---
        pub readable_next: VecDeque<u64>,
        pub writable_next: VecDeque<u64>,
        pub readable_ids: HashSet<u64>,
        pub finished_ids: HashSet<u64>,

        // --- stream_recv scripting (per id, consumed front-to-back) ---
        pub recv_script: HashMap<u64, VecDeque<RecvStep>>,
        /// Ids passed to `stream_recv`, in call order (reserve-before-read proof).
        pub recv_calls: Vec<u64>,

        // --- stream_send scripting ---
        /// Bytes `stream_send` accepts per call, per id (default: all offered).
        pub send_capacity: HashMap<u64, usize>,
        /// Force `stream_send` to return this error for an id (front-to-back).
        pub send_errors: HashMap<u64, VecDeque<quiche::Error>>,
        /// Recorded `stream_send` calls: (id, bytes, fin).
        pub sent: Vec<(u64, Vec<u8>, bool)>,

        // --- stream_capacity scripting ---
        pub capacity: HashMap<u64, QResult<usize>>,

        // --- stream_priority scripting ---
        pub priority_errors: HashMap<u64, VecDeque<quiche::Error>>,
        pub priorities: Vec<(u64, u8, bool)>,

        // --- stream_shutdown ---
        /// Force `stream_shutdown` to return this error for an id.
        pub shutdown_errors: HashMap<u64, VecDeque<quiche::Error>>,
        pub shutdowns: Vec<ShutdownCall>,

        // --- credit / terminals ---
        pub streams_left_bidi: u64,
        pub streams_left_uni: u64,
        pub closed: Option<(bool, u64, Vec<u8>)>,
        pub close_result: Option<quiche::Error>,
        pub peer_error: Option<ConnectionError>,
        pub local_error: Option<ConnectionError>,
        pub timed_out: bool,
    }

    impl MockConn {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        /// Queue scripted `stream_recv` steps for `id`.
        pub(crate) fn script_recv(&mut self, id: u64, steps: impl IntoIterator<Item = RecvStep>) {
            self.recv_script.entry(id).or_default().extend(steps);
            // A stream with a script is considered readable until drained.
            self.readable_ids.insert(id);
        }

        /// Mark ids to be returned by `stream_readable_next` (in order).
        pub(crate) fn queue_readable(&mut self, ids: impl IntoIterator<Item = u64>) {
            self.readable_next.extend(ids);
        }
    }

    impl QuicConn for MockConn {
        fn stream_recv(&mut self, id: u64, out: &mut [u8]) -> QResult<(usize, bool)> {
            self.recv_calls.push(id);
            let q = self
                .recv_script
                .get_mut(&id)
                .and_then(|q| q.pop_front())
                .unwrap_or(RecvStep::Err(quiche::Error::Done));
            match q {
                RecvStep::Data { bytes, fin } => {
                    let n = bytes.len().min(out.len());
                    out[..n].copy_from_slice(&bytes[..n]);
                    // If we couldn't deliver the whole chunk, push the remainder
                    // back so a subsequent call continues it.
                    if n < bytes.len() {
                        self.recv_script.entry(id).or_default().push_front(RecvStep::Data {
                            bytes: bytes[n..].to_vec(),
                            fin,
                        });
                        return Ok((n, false));
                    }
                    // Once drained (no more steps), the id is no longer readable.
                    if self.recv_script.get(&id).map(|q| q.is_empty()).unwrap_or(true) {
                        self.readable_ids.remove(&id);
                    }
                    Ok((n, fin))
                }
                RecvStep::Err(e) => {
                    self.readable_ids.remove(&id);
                    Err(e)
                }
            }
        }

        fn stream_send(&mut self, id: u64, buf: &[u8], fin: bool) -> QResult<usize> {
            if let Some(e) = self.send_errors.get_mut(&id).and_then(|q| q.pop_front()) {
                return Err(e);
            }
            let accept = self
                .send_capacity
                .get(&id)
                .copied()
                .unwrap_or(buf.len())
                .min(buf.len());
            self.sent.push((id, buf[..accept].to_vec(), fin && accept == buf.len()));
            Ok(accept)
        }

        fn stream_shutdown(&mut self, id: u64, direction: Shutdown, err: u64) -> QResult<()> {
            if let Some(e) = self.shutdown_errors.get_mut(&id).and_then(|q| q.pop_front()) {
                return Err(e);
            }
            self.shutdowns.push(ShutdownCall {
                id,
                is_write: direction == Shutdown::Write,
                code: err,
            });
            Ok(())
        }

        fn stream_readable_next(&mut self) -> Option<u64> {
            self.readable_next.pop_front()
        }

        fn stream_writable_next(&mut self) -> Option<u64> {
            self.writable_next.pop_front()
        }

        fn stream_readable(&self, id: u64) -> bool {
            self.readable_ids.contains(&id)
        }

        fn stream_finished(&self, id: u64) -> bool {
            self.finished_ids.contains(&id)
        }

        fn stream_capacity(&mut self, id: u64) -> QResult<usize> {
            match self.capacity.get(&id) {
                Some(Ok(v)) => Ok(*v),
                Some(Err(e)) => Err(*e),
                None => Ok(usize::MAX),
            }
        }

        fn stream_priority(&mut self, id: u64, urgency: u8, incremental: bool) -> QResult<()> {
            if let Some(e) = self.priority_errors.get_mut(&id).and_then(|q| q.pop_front()) {
                return Err(e);
            }
            self.priorities.push((id, urgency, incremental));
            Ok(())
        }

        fn peer_streams_left_bidi(&self) -> u64 {
            self.streams_left_bidi
        }

        fn peer_streams_left_uni(&self) -> u64 {
            self.streams_left_uni
        }

        fn close(&mut self, app: bool, err: u64, reason: &[u8]) -> QResult<()> {
            if let Some(e) = self.close_result {
                return Err(e);
            }
            self.closed = Some((app, err, reason.to_vec()));
            Ok(())
        }

        fn peer_error(&self) -> Option<&ConnectionError> {
            self.peer_error.as_ref()
        }

        fn local_error(&self) -> Option<&ConnectionError> {
            self.local_error.as_ref()
        }

        fn is_timed_out(&self) -> bool {
            self.timed_out
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn mock_recv_delivers_then_done() {
            let mut c = MockConn::new();
            c.script_recv(4, [RecvStep::Data { bytes: b"abc".to_vec(), fin: true }]);
            assert!(c.stream_readable(4));
            let mut out = [0u8; 16];
            assert_eq!(c.stream_recv(4, &mut out).unwrap(), (3, true));
            assert_eq!(&out[..3], b"abc");
            assert!(!c.stream_readable(4));
            assert!(matches!(c.stream_recv(4, &mut out), Err(quiche::Error::Done)));
        }

        #[test]
        fn mock_recv_truncates_to_out_len() {
            let mut c = MockConn::new();
            c.script_recv(0, [RecvStep::Data { bytes: b"abcdef".to_vec(), fin: true }]);
            let mut out = [0u8; 4];
            assert_eq!(c.stream_recv(0, &mut out).unwrap(), (4, false));
            assert_eq!(&out, b"abcd");
            // remainder continues
            let mut out2 = [0u8; 4];
            assert_eq!(c.stream_recv(0, &mut out2).unwrap(), (2, true));
            assert_eq!(&out2[..2], b"ef");
        }

        #[test]
        fn mock_send_partial_and_record() {
            let mut c = MockConn::new();
            c.send_capacity.insert(8, 2);
            assert_eq!(c.stream_send(8, b"hello", false).unwrap(), 2);
            assert_eq!(c.sent, vec![(8, b"he".to_vec(), false)]);
        }

        #[test]
        fn mock_readable_next_is_destructive() {
            let mut c = MockConn::new();
            c.queue_readable([4, 8]);
            assert_eq!(c.stream_readable_next(), Some(4));
            assert_eq!(c.stream_readable_next(), Some(8));
            assert_eq!(c.stream_readable_next(), None);
        }
    }
}
