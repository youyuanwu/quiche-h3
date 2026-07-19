//! Server-side endpoint control surface: graceful shutdown + wait-for-idle
//! ([`H3QuicheEndpoint`]) — the quiche analog of quinn's
//! `endpoint.close(code, reason)` / `endpoint.wait_idle().await` (design
//! `docs/design/quiche-h3-endpoint-shutdown.md`, §5).
//!
//! The state machine here is socket-free and fully unit-testable against plain
//! channels (mirroring the `MockConn` "no live handshake" philosophy, §9/§11):
//! the acceptor/driver wiring that feeds it lives in `listener.rs` /
//! `driver.rs`.
//!
//! A single [`std::sync::Mutex`] guards the whole registry so that registration
//! (`try_register`) and shutdown (`close`) linearize into one admission fence
//! (§5.1): once `close()` has been observed, no further worker is admitted, and
//! every worker registered before that point receives the close broadcast.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::sync::Notify;

use crate::driver::DriverCommand;

/// Per-endpoint mutable registry, guarded by the single [`EndpointShared`]
/// mutex (§5.1).
pub(crate) struct EndpointState {
    /// Live connection workers by id → *weak* control sender. Weak so the
    /// registry never keeps a worker alive (§5.1); a failed `upgrade()` means
    /// the worker already exited and the entry is stale.
    pub(crate) conns: HashMap<u64, mpsc::WeakUnboundedSender<DriverCommand<Bytes>>>,
    /// Count of registered (not-yet-deregistered) workers. `wait_idle()`
    /// resolves at the `live` 1→0 edge (§5.5).
    pub(crate) live: usize,
    /// Set once `close()` has been observed; gates admission (§5.4).
    pub(crate) closing: bool,
    /// The first close frame `(code, reason)`; first call wins (§5.3). Stored
    /// so late broadcasts (and workers that register-then-immediately-close)
    /// always carry the same frame.
    pub(crate) close_frame: Option<(u64, Bytes)>,
    /// Monotonic id source for registrations. Never reused; used by the S2/S3
    /// spike assertions to prove no worker was admitted after `close()`.
    pub(crate) next_id: u64,
}

impl EndpointState {
    fn new() -> Self {
        Self {
            conns: HashMap::new(),
            live: 0,
            closing: false,
            close_frame: None,
            next_id: 0,
        }
    }
}

/// Shared endpoint core behind the cloneable [`H3QuicheEndpoint`] and the
/// owning acceptor. One mutex serializes registration and close (the admission
/// fence, §5.1); two [`Notify`]s carry the wakeups.
pub(crate) struct EndpointShared {
    pub(crate) state: Mutex<EndpointState>,
    /// Fires at the `live` 1→0 edge to wake `wait_idle()` (§5.5).
    pub(crate) idle: Notify,
    /// Fires on `close()` to wake a parked `accept()` so it can observe
    /// `closing` and stop yielding new connections (§5.4).
    pub(crate) accept_wake: Notify,
}

impl EndpointShared {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(EndpointState::new()),
            idle: Notify::new(),
            accept_wake: Notify::new(),
        })
    }

    /// `true` once `close()` has linearized. Read by the acceptor before it
    /// starts a worker or yields an established connection (§5.4).
    pub(crate) fn is_closing(&self) -> bool {
        self.state.lock().unwrap().closing
    }
}

/// Attempt to register a new connection worker under the admission fence
/// (§5.1). Returns `None` if the endpoint is already `closing` — the caller
/// must then NOT start the worker (§5.4). On success the returned
/// [`ConnRegistration`] guard owns deregistration.
pub(crate) fn try_register(
    shared: &Arc<EndpointShared>,
    cmd_tx: &mpsc::UnboundedSender<DriverCommand<Bytes>>,
) -> Option<ConnRegistration> {
    let mut state = shared.state.lock().unwrap();
    if state.closing {
        return None;
    }
    let id = state.next_id;
    state.next_id += 1;
    state.conns.insert(id, cmd_tx.downgrade());
    state.live += 1;
    Some(ConnRegistration {
        shared: Arc::clone(shared),
        id,
    })
}

/// RAII deregistration guard. Moved into the `QuicheDriver` so it drops at
/// `QuicheDriver::drop` — the single worker-exit funnel that covers both pre-
/// and post-handshake exits (§5.5). Dropping removes the registry entry,
/// decrements `live`, and fires `idle` at the `live` 1→0 edge.
///
/// Deregistration deliberately lives here and NOT in `on_conn_close`: that
/// callback is skipped for pre-handshake exits and runs before the final flush
/// (§5.5).
pub(crate) struct ConnRegistration {
    shared: Arc<EndpointShared>,
    id: u64,
}

impl Drop for ConnRegistration {
    fn drop(&mut self) {
        // Compute the "became idle" edge while holding the lock, then release
        // the lock and notify OUTSIDE it — never hold the std mutex across
        // `notify_waiters()` (§5.5).
        let became_idle = {
            let mut state = self.shared.state.lock().unwrap();
            let removed = state.conns.remove(&self.id).is_some();
            // Every id is inserted exactly once at registration and this guard is
            // the sole, once-only remover, so the entry must be present. The
            // `saturating_sub` is a belt-and-braces guard against a hypothetical
            // double-decrement; assert the invariant in debug builds so a real
            // accounting bug is caught rather than silently masked.
            debug_assert!(
                removed,
                "ConnRegistration::drop for id {} that was not in the registry",
                self.id
            );
            state.live = state.live.saturating_sub(1);
            state.live == 0
        };
        if became_idle {
            self.shared.idle.notify_waiters();
        }
    }
}

/// A cloneable handle to a server endpoint's shutdown control surface — the
/// quiche analog of quinn's `Endpoint::close` / `Endpoint::wait_idle`.
///
/// Obtained from `H3QuicheAcceptor::endpoint()` (see [`H3QuicheAcceptor`](crate::H3QuicheAcceptor)).
/// All clones share one underlying registry, so a shutdown driven from any
/// clone is observed by the acceptor and every live connection worker.
///
/// # Example
///
/// Take the shutdown handle before serving, then drive a graceful shutdown.
/// The acceptor is *moved into* the serving task, so awaiting that task to
/// completion is what drops the acceptor; drive `wait_idle()` from the retained
/// `endpoint` handle afterwards (clone the endpoint → serve → `close()` → join
/// the acceptor task → `wait_idle()`), matching the ordering in design §6:
///
/// ```no_run
/// # use quiche_h3::{H3QuicheAcceptor, H3QuicheServerConfig};
/// # use tokio::net::UdpSocket;
/// # async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let socket = UdpSocket::bind("127.0.0.1:4433").await?;
/// let config = H3QuicheServerConfig {
///     cert_path: "cert.pem".into(),
///     key_path: "key.pem".into(),
///     ..H3QuicheServerConfig::default()
/// };
/// let mut acceptor = H3QuicheAcceptor::bind([socket], &config)?.pop().unwrap();
///
/// // Take a shutdown handle BEFORE serving; it clones and moves freely.
/// let endpoint = acceptor.endpoint();
///
/// // Serve on a task: `accept()` yields until the endpoint is closed and the
/// // pending handshakes drain, then returns `Ok(None)` and the task ends —
/// // dropping the acceptor.
/// let server = tokio::spawn(async move {
///     while let Ok(Some(_conn)) = acceptor.accept().await {
///         // ... spawn a task to drive each connection ...
///     }
/// });
///
/// // ... later, shut the server down gracefully:
/// endpoint.close(h3::error::Code::H3_NO_ERROR, b"server shutting down");
/// server.await?;              // accept loop ends → the acceptor is dropped
/// endpoint.wait_idle().await; // resolves once every live worker has ended
/// // The same UDP port can now be rebound (use the bounded retry documented on
/// // `H3QuicheAcceptor::endpoint()` — the socket may release slightly later).
/// # Ok(())
/// # }
/// ```
///
/// `H3_NO_ERROR` (`0x100`) is the conventional graceful-shutdown code; any
/// [`h3::error::Code`] may be supplied instead (a caller wiring through, e.g.,
/// `0u16.into()` selects application code `0`).
#[derive(Clone)]
pub struct H3QuicheEndpoint(pub(crate) Arc<EndpointShared>);

impl H3QuicheEndpoint {
    pub(crate) fn new(shared: Arc<EndpointShared>) -> Self {
        Self(shared)
    }

    /// Begin a graceful endpoint shutdown.
    ///
    /// Marks the endpoint `closing` — so the owning acceptor's `accept()` stops
    /// yielding new connections and refuses to start further workers (§5.4) —
    /// and broadcasts an [`h3`] connection close carrying `code`/`reason` to
    /// every connection worker registered at this point (§5.3).
    ///
    /// `close()` is **idempotent**: the *first* call's `(code, reason)` wins and
    /// is the frame delivered to every worker, including workers that register
    /// concurrently with (but before) the close. Later calls re-broadcast that
    /// same first frame; a re-broadcast to an already-closing worker is a no-op
    /// at the worker (first-close-wins, `driver.rs`).
    ///
    /// This does not block. Follow it with [`wait_idle`](Self::wait_idle) to
    /// await full drain.
    ///
    /// # Mid-handshake limitation
    ///
    /// A worker that has registered but is still **mid-handshake** is not yet
    /// established and does not process the broadcast close command; it is torn
    /// down only when its handshake completes or its `handshake_timeout` /
    /// `max_idle_timeout` expires. Configure a finite `handshake_timeout` in the
    /// server `QuicSettings` so a stalled peer cannot delay
    /// [`wait_idle`](Self::wait_idle) beyond that bound (spike S3).
    pub fn close(&self, code: h3::error::Code, reason: &[u8]) {
        // Under the lock: mark closing, record the first frame, and snapshot the
        // live (upgradable) recipients. Do the accept wake and the per-worker
        // sends OUTSIDE the lock (§5.3) — never hold the std mutex across a
        // channel send or a `notify_waiters()`.
        let (frame, recipients) = {
            let mut state = self.0.state.lock().unwrap();
            state.closing = true;
            if state.close_frame.is_none() {
                state.close_frame = Some((code.value(), Bytes::copy_from_slice(reason)));
            }
            // Always broadcast the stored *first* frame, never the raw
            // arguments of a later call (§5.3).
            let frame = state
                .close_frame
                .clone()
                .expect("close_frame was just set above");
            let recipients: Vec<mpsc::UnboundedSender<DriverCommand<Bytes>>> =
                state.conns.values().filter_map(|w| w.upgrade()).collect();
            (frame, recipients)
        };

        // Wake any `accept()` parked on `accept_wake` so it re-checks `closing`.
        self.0.accept_wake.notify_waiters();

        let (code, reason) = frame;
        for tx in recipients {
            // A closed/failed channel means the worker already exited; ignore.
            let _ = tx.send(DriverCommand::Close {
                code,
                reason: reason.clone(),
            });
        }
    }

    /// Resolve once every registered connection worker has ended (§5.5).
    ///
    /// Typically awaited after [`close`](Self::close) and after dropping the
    /// acceptor, to observe a fully idle endpoint. Without a prior `close()` it
    /// resolves after organic drain (immediately if the endpoint is already
    /// idle). Safe to call from multiple tasks and multiple times.
    ///
    /// Note: `wait_idle()` reflects this crate's per-connection worker
    /// lifetimes. The underlying UDP socket is owned by tokio-quiche's router
    /// task and may be released slightly later, so an immediate same-port rebind
    /// should use a short bounded retry; see
    /// [`H3QuicheAcceptor::endpoint()`](crate::H3QuicheAcceptor::endpoint) for
    /// the measured rebind guidance (spike S1: at most one retry — worst
    /// observed 2 attempts in the Linux loopback samples).
    pub async fn wait_idle(&self) {
        loop {
            // Register the waiter (via `enable()`) BEFORE observing `live`, so a
            // 1→0 transition that fires between the check and the await is not
            // lost (§5.5 lost-wakeup discipline).
            let notified = self.0.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if self.0.state.lock().unwrap().live == 0 {
                return;
            }

            notified.await;
        }
    }

    /// Test-only snapshot of `(next_id, live)` for the S2/S3 admission-fence
    /// spikes. Exposed as `#[doc(hidden)] pub` (not `pub(crate)`) so the
    /// `#[ignore]`d loopback spikes in `quiche-h3/tests/` — compiled as a
    /// separate crate that only sees the public surface — can assert that
    /// `next_id` does not advance after `close()`. NOT part of the stable API.
    #[doc(hidden)]
    pub fn __test_registry_snapshot(&self) -> (u64, usize) {
        let state = self.0.state.lock().unwrap();
        (state.next_id, state.live)
    }

    /// Test-only view of the `closing` flag for the S2 admission-fence spike, so
    /// the loopback accept loop can assert it never yields a connection once
    /// `closing` has linearized (FR-006/SC-003). Exposed as `#[doc(hidden)] pub`
    /// for the separate `tests/` crate; NOT part of the stable API.
    #[doc(hidden)]
    pub fn __test_is_closing(&self) -> bool {
        self.0.is_closing()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Frame helper: build the `(code, reason)` a worker should observe.
    fn frame_of(cmd: &DriverCommand<Bytes>) -> (u64, Bytes) {
        match cmd {
            DriverCommand::Close { code, reason } => (*code, reason.clone()),
            other => panic!("expected DriverCommand::Close, got {other:?}"),
        }
    }

    // --- close-broadcast (§5.3) --------------------------------------------

    #[test]
    fn close_broadcasts_to_live_workers_and_skips_dead_entries() {
        let shared = EndpointShared::new();
        let endpoint = H3QuicheEndpoint::new(Arc::clone(&shared));

        let (tx_a, mut rx_a) = mpsc::unbounded_channel::<DriverCommand<Bytes>>();
        let (tx_b, mut rx_b) = mpsc::unbounded_channel::<DriverCommand<Bytes>>();
        let (tx_dead, mut rx_dead) = mpsc::unbounded_channel::<DriverCommand<Bytes>>();

        let _reg_a = try_register(&shared, &tx_a).expect("a registers");
        let _reg_b = try_register(&shared, &tx_b).expect("b registers");
        // A registered-but-dead sender: guard kept, sender dropped → weak entry
        // fails to upgrade and must be skipped by the broadcast.
        let _reg_dead = try_register(&shared, &tx_dead).expect("dead registers");
        drop(tx_dead);

        endpoint.close(h3::error::Code::H3_NO_ERROR, b"bye");

        assert_eq!(
            frame_of(&rx_a.try_recv().expect("a receives close")),
            (
                h3::error::Code::H3_NO_ERROR.value(),
                Bytes::from_static(b"bye")
            ),
        );
        assert_eq!(
            frame_of(&rx_b.try_recv().expect("b receives close")),
            (
                h3::error::Code::H3_NO_ERROR.value(),
                Bytes::from_static(b"bye")
            ),
        );
        // The dead entry's receiver is closed; nothing was delivered.
        assert!(rx_dead.try_recv().is_err());
    }

    // --- deregistration + idle notify edge (§5.5) --------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_last_guard_wakes_parked_wait_idle() {
        let shared = EndpointShared::new();
        let endpoint = H3QuicheEndpoint::new(Arc::clone(&shared));

        let (tx, _rx) = mpsc::unbounded_channel::<DriverCommand<Bytes>>();
        let reg = try_register(&shared, &tx).expect("registers");
        assert_eq!(endpoint.__test_registry_snapshot(), (1, 1));

        let waiter = {
            let endpoint = endpoint.clone();
            tokio::spawn(async move { endpoint.wait_idle().await })
        };
        // Give the spawned task time to park on `wait_idle()`.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "wait_idle must block while live > 0");

        drop(reg); // 1→0 edge → idle.notify_waiters()

        tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("wait_idle wakes on the 1→0 edge")
            .expect("waiter task did not panic");
        assert_eq!(endpoint.__test_registry_snapshot(), (1, 0));
    }

    // --- lost-wakeup ordering (§5.5) ---------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_idle_created_before_drop_still_resolves() {
        let shared = EndpointShared::new();
        let endpoint = H3QuicheEndpoint::new(Arc::clone(&shared));

        let (tx, _rx) = mpsc::unbounded_channel::<DriverCommand<Bytes>>();
        let reg = try_register(&shared, &tx).expect("registers");

        // Poll the future ONCE while `live == 1`: this runs the `notified.enable()`
        // arming step and parks (Poll::Pending), reproducing the exact lost-wakeup
        // window. Only THEN do we drop the guard, firing `idle.notify_waiters()`
        // *after* the check but on an already-armed waiter. The `enable()`-before-
        // check discipline (§5.5) must deliver that wakeup on the next poll.
        let fut = endpoint.wait_idle();
        tokio::pin!(fut);
        assert!(
            matches!(futures::poll!(fut.as_mut()), std::task::Poll::Pending),
            "wait_idle must park while a worker is still live"
        );

        drop(reg); // 1→0 edge → idle.notify_waiters(), after the waiter armed

        tokio::time::timeout(Duration::from_secs(2), fut)
            .await
            .expect("no missed notification for a pre-armed wait_idle future");
    }

    #[tokio::test]
    async fn wait_idle_returns_immediately_when_already_idle() {
        let shared = EndpointShared::new();
        let endpoint = H3QuicheEndpoint::new(shared);
        // No registrations → live == 0 → fast return without awaiting a notify.
        tokio::time::timeout(Duration::from_secs(2), endpoint.wait_idle())
            .await
            .expect("wait_idle returns immediately at live == 0");
    }

    // --- admission fence (§5.1/§5.4) ---------------------------------------

    #[test]
    fn try_register_is_refused_after_close() {
        let shared = EndpointShared::new();
        let endpoint = H3QuicheEndpoint::new(Arc::clone(&shared));

        let (tx_before, mut rx_before) = mpsc::unbounded_channel::<DriverCommand<Bytes>>();
        let _reg_before = try_register(&shared, &tx_before).expect("registers before close");
        let (next_id_before, _) = endpoint.__test_registry_snapshot();

        endpoint.close(h3::error::Code::H3_NO_ERROR, b"bye");

        // The pre-close worker is in the snapshot and received the frame.
        assert!(
            rx_before.try_recv().is_ok(),
            "pre-close worker got the close"
        );

        // A registration attempt after close is refused, and next_id does not
        // advance (the admission fence, §5.1).
        let (tx_after, _rx_after) = mpsc::unbounded_channel::<DriverCommand<Bytes>>();
        assert!(
            try_register(&shared, &tx_after).is_none(),
            "no worker admitted after close()"
        );
        let (next_id_after, _) = endpoint.__test_registry_snapshot();
        assert_eq!(
            next_id_before, next_id_after,
            "next_id must not advance for a refused registration"
        );
    }

    // --- idempotent close: first frame wins (§5.3) -------------------------

    #[test]
    fn close_is_idempotent_first_frame_wins() {
        let shared = EndpointShared::new();
        let endpoint = H3QuicheEndpoint::new(Arc::clone(&shared));

        let (tx, mut rx) = mpsc::unbounded_channel::<DriverCommand<Bytes>>();
        let _reg = try_register(&shared, &tx).expect("registers");

        endpoint.close(h3::error::Code::H3_NO_ERROR, b"first");
        // A second call with a DIFFERENT frame must re-broadcast the FIRST one.
        endpoint.close(h3::error::Code::H3_REQUEST_CANCELLED, b"second");

        let first = frame_of(&rx.try_recv().expect("first broadcast"));
        assert_eq!(
            first,
            (
                h3::error::Code::H3_NO_ERROR.value(),
                Bytes::from_static(b"first")
            ),
        );
        let second = frame_of(&rx.try_recv().expect("second broadcast (same frame)"));
        assert_eq!(
            second,
            (
                h3::error::Code::H3_NO_ERROR.value(),
                Bytes::from_static(b"first")
            ),
            "the second close re-broadcasts the first frame, never its own args"
        );
        assert!(rx.try_recv().is_err(), "exactly two broadcasts delivered");
    }

    // --- wait_idle without a prior close (§5.5) ----------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_idle_without_close_blocks_then_wakes_on_organic_drain() {
        let shared = EndpointShared::new();
        let endpoint = H3QuicheEndpoint::new(Arc::clone(&shared));

        let (tx, _rx) = mpsc::unbounded_channel::<DriverCommand<Bytes>>();
        let reg = try_register(&shared, &tx).expect("registers");

        let waiter = {
            let endpoint = endpoint.clone();
            tokio::spawn(async move { endpoint.wait_idle().await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "blocks while a worker is live");

        // Organic drain (no close()): the worker simply exits and drops its
        // guard.
        drop(reg);

        tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("wait_idle wakes on organic drain")
            .expect("waiter task did not panic");
    }

    // --- shared-registry semantics across handles (§5.1/FR-002) ------------

    #[test]
    fn cloned_handles_share_one_registry() {
        // Two `H3QuicheEndpoint` handles over the same shared registry stand in
        // for endpoints obtained from two acceptors of the same `bind()` call
        // (FR-002): a registration seen through one is visible through the other
        // via the shared `live`/`next_id` state.
        let shared = EndpointShared::new();
        let a = H3QuicheEndpoint::new(Arc::clone(&shared));
        let b = a.clone();

        assert_eq!(a.__test_registry_snapshot(), (0, 0));
        let (tx, _rx) = mpsc::unbounded_channel::<DriverCommand<Bytes>>();
        let _reg = try_register(&shared, &tx).expect("registers");

        // The registration is observable through BOTH handles.
        assert_eq!(a.__test_registry_snapshot(), (1, 1));
        assert_eq!(b.__test_registry_snapshot(), (1, 1));
    }

    #[test]
    fn close_via_one_handle_fences_registration_seen_through_shared() {
        // `close()` from one handle latches `closing` on the shared registry, so
        // a subsequent `try_register` (as the acceptor would attempt) is refused
        // regardless of which handle drove the close.
        let shared = EndpointShared::new();
        let a = H3QuicheEndpoint::new(Arc::clone(&shared));
        let b = a.clone();

        b.close(h3::error::Code::H3_NO_ERROR, b"bye");

        let (tx, _rx) = mpsc::unbounded_channel::<DriverCommand<Bytes>>();
        assert!(
            try_register(&shared, &tx).is_none(),
            "close() through any handle fences admission on the shared registry"
        );
        // next_id did not advance for the refused registration.
        assert_eq!(a.__test_registry_snapshot(), (0, 0));
    }
}
