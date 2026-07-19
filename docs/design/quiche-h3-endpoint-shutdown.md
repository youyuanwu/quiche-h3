# Design: endpoint shutdown & wait-for-idle for `quiche-h3`

> **Status:** proposal / pre-implementation. Section numbers below are local to
> this document; references of the form "bridge §N" point at
> [`quiche-h3-bridge.md`](./quiche-h3-bridge.md).
>
> **Spike-driven, like the bridge design.** Several claims are marked
> **[SPIKE]** — they must be confirmed against the pinned build
> (`tokio-quiche 0.19.1` / `quiche 0.29.3`) before or during implementation, in
> the same spirit as bridge §14.

## 1. Goal

Give `quiche-h3` a first-class **endpoint shutdown** operation with a
**wait-for-idle** completion signal, matching the ergonomics quinn already
provides:

```rust
endpoint.close(0_u16.into(), b"svr shutdown");
endpoint.wait_idle().await;
```

Concretely, after a server's serve loop ends we want to be able to:

1. **Force every live connection closed** (send an application `CONNECTION_CLOSE`
   to each peer), instead of waiting for each peer to disconnect on its own, and
2. **Await true idle** — block until every connection worker has ended and the
   underlying UDP socket has been released — so the **same port can be rebound
   immediately**.

The motivating failure is the cross-backend reconnect test in `tonic-h3`
([`quiche-reconnect-socket-release.md`](https://github.com/youyuanwu/tonic-h3/blob/main/docs/quiche-reconnect-socket-release.md)):
it stops a server and rebinds a fresh one on the **same** UDP port. quinn, msquic
pass; the quiche backend fails with:

```
bind udp socket: Os { code: 98, kind: AddrInUse, message: "Address already in use" }
```

and is therefore `#[ignore]`d. This document explains **why**, answers **can the
`quiche-h3` API support this?** (yes), and specifies the API and implementation.

### 1.1 Non-goals

- No change to the steady-state data path (bridge §5/§6) — this is purely an
  additive teardown control surface.
- No dependency on `h3-util` (bridge §10 forbids it — circular). The `h3-util`
  wrapper and the `tonic-h3` test helper changes are **downstream** and only
  sketched here (§8).
- Not a graceful-drain / GOAWAY protocol negotiation. "Close" here is the
  transport-level `CONNECTION_CLOSE` broadcast, the analog of `quinn::Endpoint::close`.
  A cooperative HTTP/3 GOAWAY drain is a possible future layer (§11).

## 2. Background

### 2.1 The two release conditions (why retry+backoff cannot work)

`tokio-quiche::listen()` owns the UDP socket in a background I/O / routing task,
**not** in the acceptor handle we hold. Its `listen` docs are explicit
(`tokio-quiche-0.19.1/src/lib.rs:163-168`):

> Each socket starts a separate tokio task to process and route inbound packets.
> … **The task shuts down when the returned stream is closed (or dropped) and all
> previously-yielded connections are closed.**

So releasing the socket requires **both**:

1. the `QuicConnectionStream` (our acceptor) is dropped, **and**
2. every connection that stream already yielded is closed.

A retry-with-backoff on the rebind cannot help: the old socket is *deliberately*
held open while any old connection is still alive. In the reconnect test the
client stays connected across the rebind, so condition (2) is never met and the
bind fails **every** time, not intermittently. This is a lifecycle problem, not a
timing lag.

### 2.2 Confirmed socket ownership model **[SPIKE-CONFIRMED via source read]**

The listening socket is reference-counted (`Arc<UdpSocket>`), shared between the
router **task** and every per-connection worker:

- `listen` wraps the socket in an `Arc` and clones it for tx/rx:
  `socket_tx = Arc::new(socket.socket); socket_rx = Arc::clone(&socket_tx);`
  (`tokio-quiche-0.19.1/src/quic/mod.rs:301-302`).
- The **router** is a standalone future/task, `InboundPacketRouter`, that *itself*
  owns **both** `socket_tx: Arc<Tx>` and `socket_rx: Rx`
  (`.../src/quic/router/mod.rs:150-151`) and demuxes by DCID. It is tied to the
  `QuicConnectionStream` via its `accept_sink`.
- The **send** half is additionally cloned into each per-connection `IoWorker`
  (`.../src/quic/io/worker.rs:108` `socket: MaybeConnectedSocket<Tx>`).

The OS file descriptor is closed **only when the last `Arc<UdpSocket>` drops**.
There are **two distinct owners** of those `Arc`s:

1. every per-connection worker's send-socket clone — dropped when each worker task
   ends, **and**
2. the **router task's own** `socket_tx`/`socket_rx` — dropped only when the
   router **future returns**.

Crucially, the router future returns only after it is *polled* and observes both
`accept_sink.is_closed()` (the `QuicConnectionStream` was dropped) **and** all
connections have shut down (`.../src/quic/router/mod.rs:786-798`). So even after
every worker has ended and the stream is dropped, the FD is not released until the
**router task is scheduled and returns**. `quiche-h3` has no in-band handle to
await that router future (tokio-quiche spawns and owns it).

Because these are **UDP** sockets there is no `TIME_WAIT`; once the final `Arc`
truly drops, `close(2)` frees the port synchronously and it is immediately
rebindable. But "final `Arc`" includes the router task's two clones, whose drop we
**cannot** directly observe. This is the crux limitation `wait_idle` must be
honest about (§5.6, §7, **[SPIKE S1]**).

### 2.3 Current `quiche-h3` teardown model (what exists today)

- One `IoWorker` task per connection owns the `quiche::Connection`; the worker
  runs our `QuicheDriver` (`ApplicationOverQuic`). It ends when the connection
  closes, the app errors, or the **last front-end `cmd_tx` handle drops**
  (last-handle teardown, bridge §5.2).
- `QuicheDriver::drop` (`quiche-h3/src/driver.rs:2352`) is the **single funnel**
  that runs exactly when the worker task ends (pre- or post-handshake). This is
  the ideal in-band hook for a shutdown registration guard (§5).
- The front-end `Connection<Bytes>` already knows how to request a transport
  close: `quic::Connection::close` sends `DriverCommand::Close { code, reason }`
  to the worker (`quiche-h3/src/stream.rs:634`), which drives
  `qconn.close(...)` at the explicit-close barrier (bridge §5.2/§8.3).
- **Gap:** the acceptor (`H3QuicheAcceptor`, `quiche-h3/src/listener.rs`) holds
  only the `QuicConnectionStream`. Once it yields a `Connection<Bytes>`, ownership
  moves to the caller (in `tonic-h3`, into a detached `h3::server` task). The
  acceptor keeps **no handle to live connections**, so it can satisfy condition
  (1) but never force condition (2).

> The bridge design (§9) previously concluded *"We do not need the `wait_idle`
> machinery from `msquic-h3`."* That holds for **drop-driven** teardown of an
> owned connection, but **not** for **endpoint-initiated** shutdown where the
> connection handles are owned by detached tasks that outlive the serve loop.
> This document revises that conclusion for the endpoint-shutdown use case.

### 2.4 Reference: how quinn satisfies both conditions

quinn's `Endpoint` owns its socket and tracks its connections, so
`endpoint.close(code, reason)` force-closes **all** connections and
`endpoint.wait_idle().await` blocks until they have fully drained and the socket
is released — after which the port is free. `quiche-h3` needs the functional
equivalent, built from the pieces it already controls (the per-connection
`cmd_tx` and the `QuicheDriver::drop` funnel).

## 3. Feasibility

**Yes — `quiche-h3` can support endpoint shutdown + wait-for-idle**, without any
upstream `tokio-quiche` change and without touching the data path, **with one
honest caveat about the final FD-release instant** (§2.2). The design force-closes
every live connection and awaits all bridge workers ending; the residual gap is
the router task's own socket `Arc`s, which release a scheduler tick later and
which we cannot observe in-band (mitigation in §5.6 / **[SPIKE S1]**).

The two conditions map onto mechanisms the crate already owns:

| Release condition | Mechanism `quiche-h3` already has | What this design adds |
|---|---|---|
| (1) stream dropped | acceptor drop when serve loop ends | make `close()` also stop `accept()` so the loop ends promptly |
| (2) all conns closed | per-conn `cmd_tx` (`DriverCommand::Close`) + `QuicheDriver::drop` worker-exit funnel | a shared **connection registry** to broadcast Close to, and a **liveness guard** to await |
| final FD release (router task returns) | — (owned by tokio-quiche) | **not fully observable in-band**; see §5.6 mitigation / upstream ask (§11) |

The only genuinely new state is a small shared **endpoint registry** that (a)
records each live connection's `cmd_tx` (weakly) so `close()` can reach it, and
(b) counts live workers so `wait_idle()` can await zero — with registration and
close **serialized under one lock** so admission is fenced (§5).

> **Two capability tiers.** (i) *Bounded, deterministic force-close of every
> **established** connection* is fully achievable in-band and is what unblocks the
> reconnect scenario in practice. (ii) *A hard guarantee that the OS port is
> rebindable the instant `wait_idle` returns* additionally depends on the router
> task's scheduling; §5.6 gives a pragmatic close (a bounded post-idle rebind
> retry that absorbs a sub-tick delay — **not** the multi-second `AddrInUse` the
> issue describes, which was caused by connections that never closed at all).

## 4. Public API

A new cloneable control handle, obtained from the acceptor **before** the
acceptor is moved into a serve loop. Naming mirrors quinn (`close`/`wait_idle`)
and the crate's `H3Quiche*` prefix.

```rust
/// A cloneable handle to the shutdown/idle control surface shared by an
/// `H3QuicheAcceptor` and every connection it accepts. Cheap to clone; outlives
/// the acceptor (holds an `Arc` to shared endpoint state).
#[derive(Clone)]
pub struct H3QuicheEndpoint { /* Arc<EndpointShared> */ }

impl H3QuicheAcceptor {
    /// Obtain the endpoint control handle for this acceptor. All acceptors and
    /// endpoints derived from the same `bind` share one registry.
    pub fn endpoint(&self) -> H3QuicheEndpoint;
}

impl H3QuicheEndpoint {
    /// Begin endpoint shutdown (analog of `quinn::Endpoint::close`):
    ///  * mark the endpoint closing so `accept()` returns `Ok(None)` and no new
    ///    connection is admitted;
    ///  * broadcast `CONNECTION_CLOSE(code, reason)` to every currently-live
    ///    connection worker.
    /// Idempotent; the first call's (code, reason) win. Non-blocking.
    pub fn close(&self, code: h3::error::Code, reason: &[u8]);

    /// Resolve once every connection worker accounted for by this endpoint has
    /// ended (their per-worker socket `Arc`s dropped). Combined with the
    /// acceptor/stream having been dropped, the connection-owned FD references
    /// are gone; the **router task's** own socket `Arc`s then release a scheduler
    /// tick later (§2.2), so a same-port rebind should use a short bounded retry
    /// to absorb that sub-tick delay (§5.6). Does **not** itself initiate close —
    /// call `close()` first for a bounded wait (otherwise it waits for peers to
    /// leave on their own).
    pub async fn wait_idle(&self);
}
```

Notes:
- `close()` takes `h3::error::Code` for symmetry with
  `quic::Connection::close` (`quiche-h3/src/stream.rs:634`); a `u64` convenience
  overload can be added if desired. `H3_NO_ERROR` is the graceful default.
- `wait_idle()` is intentionally **decoupled** from `close()` (as in quinn), so a
  caller can `wait_idle()` for natural drain, or `close()` then `wait_idle()` for
  a bounded, deterministic shutdown. Callers should apply their own timeout
  (`tokio::time::timeout`) around `wait_idle()` if they need an upper bound.

### 4.1 Client symmetry (optional, thin)

The connector side (`H3QuicheConnector`, bridge §7.2) yields a single
`Connection<Bytes>`; its worker already ends via last-handle teardown when the
handle drops (bridge §9). For parity with `quinn::Endpoint::wait_idle` on the
client, `H3QuicheConnector` may expose the same `endpoint()` → `close/wait_idle`
pair backed by a one-entry registry. This is a small, optional add; the server
path is the load-bearing one and the rest of this document focuses on it.

## 5. Implementation

### 5.1 Shared endpoint state (single lock for linearizable admission)

Registration and `close()` **must be serialized** so a connection cannot register
after `close()` has scanned the registry (otherwise it would be neither
force-closed nor counted, and `wait_idle` could observe zero while a live worker
holds a socket `Arc`). Put the mutable set, the live count, and the closing flag
under **one** mutex; keep only the wakeup primitives outside it.

```rust
struct EndpointState {
    // Live connections: id -> weak command sender to that worker. Weak so the
    // registry never keeps a worker alive (would defeat last-handle teardown,
    // bridge §5.2). Upgrade fails ⇒ that worker is already ending.
    conns: HashMap<u64, mpsc::WeakUnboundedSender<DriverCommand<Bytes>>>,
    live: usize,                       // registered, not-yet-ended workers
    closing: bool,                     // set by close(); read by accept()/register
    close_frame: Option<(u64, Bytes)>, // (code, reason) captured on first close()
    next_id: u64,
}

struct EndpointShared {
    state: Mutex<EndpointState>,
    idle: Notify,        // notified when `live` reaches 0
    accept_wake: Notify, // wakes a blocked accept() so it observes `closing`
}
```

`H3QuicheEndpoint(Arc<EndpointShared>)`. `H3QuicheAcceptor` gains an
`Arc<EndpointShared>` field; `bind()` constructs one `EndpointShared` and shares
its `Arc` across the per-socket acceptors it returns (all sockets from one `bind`
share one endpoint, matching quinn's single-endpoint-many-sockets model). The
registry keys on the monotonic `next_id` assigned under the lock at start time
(the quiche connection id is not needed).

> A plain `std::sync::Mutex` is fine: every critical section is a few map/int ops
> with no `.await` held across the guard.

### 5.2 Registration at accept time (fenced against `close()`)

Where the acceptor starts a worker (`quiche-h3/src/listener.rs:189-203`,
`iqc.start(driver)`), it already holds the connection's handles (hence its
`cmd_tx`). Registration happens **under the lock**, and is the admission fence:

```rust
// returns None if the endpoint is already closing → do NOT start this conn
fn try_register(shared: &Arc<EndpointShared>, cmd_tx: &UnboundedSender<..>)
    -> Option<ConnRegistration>
{
    let mut st = shared.state.lock();
    if st.closing { return None; }          // fence: no new worker after close()
    let id = st.next_id; st.next_id += 1;
    st.conns.insert(id, cmd_tx.downgrade());
    st.live += 1;
    Some(ConnRegistration { shared: shared.clone(), id })
}
```

If `try_register` returns `None`, the acceptor drops the `InitialQuicConnection`
without calling `start` (no worker is spawned). Otherwise it calls
`iqc.start(driver)` with the `ConnRegistration` guard **moved into the driver**
(stored as a `QuicheDriver` field). Because the check-insert-increment is a single
locked section and `close()` snapshots under the same lock (§5.4), there is **no**
check-then-act window: either a connection is registered *before* `close()`'s
snapshot (and gets a `Close`), or it observes `closing` and is never started.

The `cmd_tx` is available from `DriverHandles` before
`into_established_connection()` consumes them; registration uses a **weak** clone
taken at that point.

### 5.3 Deregistration = worker-exit guard

```rust
struct ConnRegistration { shared: Arc<EndpointShared>, id: u64 }
impl Drop for ConnRegistration {
    fn drop(&mut self) {
        let mut st = self.shared.state.lock();
        st.conns.remove(&self.id);
        st.live -= 1;
        let now_zero = st.live == 0;
        drop(st);
        if now_zero { self.shared.idle.notify_waiters(); }
    }
}
```

Store the guard **inside `QuicheDriver`** so it drops precisely at
`QuicheDriver::drop` (`quiche-h3/src/driver.rs:2352`) — the single worker-exit
funnel — for **both** pre-handshake failures and normal post-`on_conn_close`
exit. `on_conn_close` would be wrong: it is skipped for pre-handshake exits
(bridge §8.4) and runs *before* the worker's final flush/return. The guard is a
field of the app the `IoWorker` owns, so it drops as the worker task ends, i.e. in
the same teardown that drops that worker's send-socket `Arc` (§2.2).

> **[SPIKE S1] `wait_idle` vs. actual FD release.** `live == 0` proves every
> **bridge worker** has ended (their per-worker socket `Arc`s dropped). It does
> **not** prove the OS FD is released: the **router task** still owns two socket
> `Arc`s and frees them only when it is polled and returns, after observing its
> `accept_sink` closed and all conns gone (`.../quic/router/mod.rs:786-798`,
> §2.2). We have no in-band handle to await the router future. Confirm empirically
> whether, after `wait_idle()` resolves *and* the acceptor/stream is dropped, an
> immediate same-port `UdpSocket::bind` succeeds on Linux and Windows. Expected
> outcome: it usually succeeds, but may transiently fail for a scheduler tick
> until the router task runs. **Mitigation (pragmatic close):** the caller's
> rebind should use a *short bounded* retry (e.g. a few attempts over tens of ms)
> to absorb that sub-tick delay. This is categorically different from the retry
> the issue doc rejects: there the socket was held **indefinitely** by connections
> that never closed; here all connections are provably closed and only the router
> task's scheduling remains. **Preferred real fix:** an upstream `tokio-quiche`
> signal to await listener/router completion (§11) — then `wait_idle` can be
> exact.
>
> **[RESOLVED — measured, Linux]** The S1 loopback test
> (`s1_same_port_rebind_after_wait_idle`, 6×50 iters) confirms the expected
> outcome: the immediate same-port rebind succeeds first-try in ~75–94% of
> shutdowns; when it does not, a single backoff retry always sufficed (worst
> observed across 300 shutdowns: **2 attempts**; retry latency ≈ one backoff
> interval, ~6–12 ms). Verdict: **bounded retry required** (not "effectively
> exact"). The shipped contract pairs `wait_idle()` with a short bounded rebind
> retry; see `tests/SPIKE_OUTCOMES.md` and the `close`/`wait_idle` rustdoc.

### 5.4 `close()`

```rust
pub fn close(&self, code: h3::error::Code, reason: &[u8]) {
    let senders: Vec<_> = {
        let mut st = self.0.state.lock();
        if st.close_frame.is_none() {
            st.close_frame = Some((code.value(), Bytes::copy_from_slice(reason)));
        }
        st.closing = true;
        // Snapshot live senders under the same lock that fences registration.
        st.conns.values().filter_map(|w| w.upgrade()).collect()
    };
    self.0.accept_wake.notify_waiters();   // unblock accept() → drain → Ok(None)
    let (c, r) = /* the stored close_frame */;
    for tx in senders {
        let _ = tx.send(DriverCommand::Close { code: c, reason: r.clone() });
    }
}
```

Snapshotting the senders **inside** the locked section (then sending outside it,
to avoid holding the lock across the sends) is what makes admission linearizable
with §5.2: any connection not in this snapshot never started (it will observe
`closing`). Each `DriverCommand::Close` drives the existing worker close path
(bridge §5.2/§8.3): `qconn.close(true, code, reason)` at the explicit-close
barrier → tokio-quiche flushes `CONNECTION_CLOSE` → closing/draining →
`on_conn_close` publishes the terminal → worker ends → `ConnRegistration` guard
drops (§5.3). Sending `Close` to a connection whose handles already dropped is a
no-op (weak upgrade already filtered, or the worker is mid-teardown) — idempotent.
`close()` is idempotent across repeated/concurrent calls (first frame wins).

> **Limitation — mid-handshake connections (§7).** A worker that has **not yet
> established** does not drain its command channel (`should_act()` is false until
> establishment, `driver.rs:2279-2295`), so a queued `Close` is **not acted on**
> until it establishes, and tokio-quiche's dedicated handshake timeout is disabled
> by default (`tokio-quiche-0.19.1/src/settings/quic.rs:238-243`). `close()` thus
> gives a bounded, deterministic force-close for **established** connections; a
> connection frozen mid-handshake is bounded only by the configured handshake/idle
> timeout. See §7 for the recommended `QuicSettings` and the admission fence that
> keeps this to at most the already-in-flight handshakes.

### 5.5 `accept()` stops on close

`H3QuicheAcceptor::accept()` (`quiche-h3/src/listener.rs:146`) gains a closing
check so the serve loop terminates and the acceptor/stream is dropped
(condition (1)):

- **Create the `accept_wake.notified()` future *before* reading `closing`** each
  iteration, then check `closing`: this avoids the lost-wakeup window where
  `close()` fires between the check and the await. (Same discipline as
  `wait_idle` in §5.6.)
- If `closing` is set, stop admitting: do not poll the incoming branch, and once
  in-flight handshakes have drained return `Ok(None)` (mirroring the existing
  `incoming_done && handshakes.is_empty()` exit at `listener.rs:148-150`).
- A handshake that **completes after** `closing` is observed must **not** be
  yielded to the caller (the caller's serve loop is winding down); drop it — its
  worker was registered (§5.2) and will be force-closed by the `close()` snapshot
  or has already received `Close`. Do not leak it out of `accept()`
  (`listener.rs:155-160` currently returns it unconditionally).

This preserves "already-established connections keep running until force-closed":
`close()` force-closes via §5.4; `accept()` merely stops yielding new ones and
lets the loop end.

### 5.6 `wait_idle()`

```rust
pub async fn wait_idle(&self) {
    loop {
        let waiter = self.0.idle.notified();          // register before the check
        if self.0.state.lock().live == 0 { return; }
        waiter.await;
    }
}
```

The `notified()`-before-check ordering avoids the lost-wakeup race. As §5.3
[SPIKE S1] notes, this returns when all **bridge workers** have ended; a strict
same-port rebind should pair it with a short bounded retry (or, ideally, an
upstream router-completion signal).

## 6. Shutdown ordering (end to end)

Mapping onto the quinn helper the reconnect test already uses (note the
**acceptor is dropped before `wait_idle`**, so the router can observe its
`accept_sink` closed):

```
h_sv.await …;                       // tonic serve future returns → acceptor dropped → stream/router accept_sink closed  (cond 1)
endpoint.close(H3_NO_ERROR, b"svr shutdown");   // force-close all live conns    (cond 2)
endpoint.wait_idle().await;         // all bridge workers ended (per-worker socket Arcs dropped)
// router task then releases its own socket Arcs when next polled → FD closed;
// same port is rebindable (use a short bounded retry to absorb the router tick — §5.6)
```

`endpoint` is cloned from the acceptor **before** the acceptor is moved into the
serve helper. Because `H3QuicheEndpoint` holds an `Arc<EndpointShared>` (not the
acceptor), it remains usable after the acceptor is dropped. `close()` reaches live
connections through the registry regardless of acceptor liveness. **Dropping the
acceptor before `wait_idle` matters**: the router future only exits (releasing its
socket `Arc`s) after `accept_sink.is_closed()` — i.e. after the
`QuicConnectionStream`/acceptor is dropped (§2.2). If the caller instead wants
`close()` to *drive* the loop exit, calling `close()` first (before awaiting the
serve future) also works via §5.5; the acceptor is still dropped when the loop
returns, before `wait_idle`.

## 7. Edge cases

- **Mid-handshake connection cannot be force-closed promptly (important).** A
  worker that has not yet established does **not** drain its command channel
  (`should_act()` false until establishment, `driver.rs:2279-2295`), so a queued
  `DriverCommand::Close` is not acted on until it establishes; and dropping the
  acceptor does **not** kill it, because `iqc.start(driver)` spawned it as an
  independent tokio-quiche task (the acceptor's `FuturesUnordered` only holds the
  *establishment oneshot*, not the worker). tokio-quiche's dedicated handshake
  timeout is **disabled by default**
  (`tokio-quiche-0.19.1/src/settings/quic.rs:238-243`). **Consequences &
  mitigations:** (a) the §5.2 admission fence bounds this to *at most the
  handshakes already in flight when `close()` is called* — no new ones start;
  (b) servers that need a hard `wait_idle` bound should set a finite
  `handshake_timeout` (and/or `max_idle_timeout`) in `QuicSettings` so a stalled
  handshake self-terminates, dropping its worker (and its `ConnRegistration`), so
  `wait_idle` completes; (c) once established (the common case), a connection is
  force-closed promptly by §5.4. This is a real limitation to document on
  `close()`/`wait_idle`, not a correctness bug: `live` stays accurate either way.
- **Pre-handshake worker exit.** The guard lives in `QuicheDriver`, so a
  connection that fails/times out before establishment still deregisters via
  `QuicheDriver::drop` (`driver.rs:2352`), keeping `live` accurate.
- **Connection closes normally before `close()`.** Its guard already dropped and
  it was removed from `conns`; `close()` simply doesn't see it. Correct.
- **`close()` races a connection ending.** `weak.upgrade()` returns `None`, or the
  `Close` lands on a worker already in teardown and is ignored. Idempotent.
- **Multiple `close()` calls / multiple endpoints.** First `close_frame` wins;
  `closing`/broadcast are idempotent. All clones share one `Arc<EndpointShared>`.
- **`wait_idle()` with no `close()`.** Legal; waits for organic drain. Callers
  wanting a bound must wrap in `tokio::time::timeout`.
- **Admission fence (no post-close registration).** Because registration
  (§5.2) and the `close()` snapshot (§5.4) run under the **same** lock, a
  connection is either registered before the snapshot (and receives `Close`) or
  observes `closing` and is never started. There is no window in which a worker
  registers after `wait_idle` has observed zero. **[SPIKE S2]** stress-test this
  with concurrent `accept()`/`close()` to confirm no worker is started after
  `close()` and no established connection is yielded out of `accept()` after
  `closing` (§5.5).

## 8. Downstream consumers (out of scope here, cross-repo)

- **`h3-util` `quiche_h3` wrapper** (`h3-util/src/quiche_h3/server.rs`): expose a
  passthrough `endpoint()` (or `close`/`wait_idle`) on its `H3QuicheAcceptor`
  wrapper so backend-specific shutdown is reachable, exactly as the quinn wrapper
  surfaces `quinn::Endpoint`. No change to the generic `H3Acceptor` trait is
  required — the reconnect helper calls backend-specific methods (as it already
  does for quinn's `endpoint.close()/wait_idle()`).
- **`tonic-h3` test helper** (`tonic-h3-tests/src/lib.rs`,
  `run_test_quiche_server`): capture `let endpoint = acceptor.endpoint();` before
  `run_test_server(acceptor, token)`, then after the serve future ends call
  `endpoint.close(0u16.into(), b"svr shutdown"); endpoint.wait_idle().await;` —
  structurally identical to `run_test_quinn_hello_server`. Then remove the
  `#[ignore]` from `reconnect::h3_quiche_test`.

## 9. Testing

- **Unit (in-crate, mockable per bridge §11):**
  - `close()` broadcasts `DriverCommand::Close{code,reason}` to every registered
    (live) `cmd_tx`; a deregistered/weak-dead entry is skipped.
  - `ConnRegistration::drop` decrements `live` and fires `idle` at the 1→0 edge.
  - `wait_idle()` returns immediately at `live==0`; blocks then wakes on the
    final guard drop (no lost wakeup: `notified()`-before-check).
  - `accept()` returns `Ok(None)` once `closing` is set and in-flight handshakes
    drain; a blocked `accept()` wakes on `close()`; a handshake completing after
    `closing` is dropped, not yielded.
  - **Admission fence:** a `try_register` attempted after `closing` returns `None`
    (no worker started); `close()`'s snapshot and registration are mutually
    exclusive (§5.2/§5.4).
  - Idempotent `close()` (first frame wins); `wait_idle()` with no `close()`.
- **Loopback integration (`#[ignore]`d, bridge §11 convention):**
  - **S1 — rebind (primary acceptance for [SPIKE S1]):** bind a server, establish
    a client connection, then (in this order) `close()`, **drop the acceptor**,
    `wait_idle().await`, and **rebind the same port** (with a short bounded retry
    per §5.6) and complete a fresh request — all while the original client is
    still connected. Measure how often/long the post-`wait_idle` rebind retry is
    needed; that quantifies the router-task residual (§2.2). If rebind *never*
    needs a retry across many runs, `wait_idle` can be documented as effectively
    exact for this build; if it sometimes does, the bounded retry is the
    documented contract (and motivates the upstream ask, §11).
    **[RESOLVED]** implemented as `s1_same_port_rebind_after_wait_idle`; verdict
    **bounded retry required** (~5–25% of shutdowns needed one retry;
    worst 2 attempts) — see §5.6 [RESOLVED] note and `SPIKE_OUTCOMES.md`.
  - **S2 — admission fence:** hammer `accept()` while calling `close()`
    concurrently; assert no worker is started after `close()` and no connection is
    yielded after idle is observed.
    **[RESOLVED]** implemented as `s2_admission_fence_under_concurrent_close`
    (asserts `next_id` frozen post-`close()` via the test accessor, and nothing
    yielded after `closing`) — **Confirmed**.
  - **S3 — mid-handshake bound:** with a finite `handshake_timeout`, start a
    handshake that never completes, `close()` + `wait_idle()`; assert `wait_idle`
    completes within ~the timeout (validates the §7 mitigation).
    **[RESOLVED]** implemented as `s3_mid_handshake_bounded_by_timeout` (raw
    Initial-only client + `handshake_timeout = 800 ms`; `wait_idle` ≈ 801 ms) —
    **Confirmed**.
  - peers receive a `CONNECTION_CLOSE` with the supplied code/reason after
    `close()` (front-end poll resolves to the classified terminal, bridge §8).
- **Downstream acceptance:** the un-`#[ignore]`d `tonic-h3`
  `reconnect::h3_quiche_test` passing is the end-to-end proof (tracked in the
  `tonic-h3` repo, §8).

## 10. Alternatives considered

1. **Track & abort the detached connection tasks in `axum-h3` / the serve loop**
   (issue doc "option 2"). Replace fire-and-forget `executor.execute(...)` with a
   tracked `JoinSet` aborted on shutdown. Rejected as the *primary* fix here: it
   is a **cross-backend** change to shared serve-loop code, risks regressing
   graceful shutdown for quinn/msquic/s2n, and aborting the h3 task drops the
   connection **abruptly** (no clean `CONNECTION_CLOSE`) versus this design's
   explicit application close. It also lives in the wrong repo (`axum-h3`), not in
   `quiche-h3` where the capability belongs. Our design is the issue doc's
   preferred "option 1" (give the acceptor a `close()` that shuts down the
   listener and its live connections).
2. **`SO_REUSEPORT` on the bind.** Rejected (as in the issue doc): for UDP the
   kernel load-balances datagrams across both sockets, so client packets could be
   delivered to the dead old socket — a correctness bug, not a fix.
3. **Retry-with-backoff on rebind as the *primary* fix.** Cannot succeed while an
   old client connection is open (§2.1): it turns a fast, clearly-labelled skip
   into a slow, confusing multi-second timeout. This design instead *closes* the
   connections, so the only thing a bounded post-`wait_idle` retry absorbs is the
   **router task's scheduling tick** (§2.2/§5.6) — sub-millisecond, not the
   indefinite hold the issue rejects. That narrow, bounded retry is acceptable (or
   removable once the upstream signal below lands).
4. **Strong `cmd_tx` in the registry.** Rejected: a strong sender would keep the
   worker alive and defeat last-handle teardown (bridge §5.2). Weak is required.
5. **Deregister in `on_conn_close` instead of a Drop guard.** Rejected:
   `on_conn_close` runs *before* the worker's final flush/return and is **skipped
   for pre-handshake exits** (bridge §8.4). The `QuicheDriver::drop` guard is the
   only funnel that covers every exit at the correct (latest) instant.
6. **Separate atomics (`AtomicBool closing` + `AtomicUsize live`) instead of one
   mutex.** Rejected: check-then-act across two atomics reintroduces the
   admission race the reviewer flagged — a connection could register between
   `close()` reading the set and setting `closing`. One lock around
   register/close/snapshot makes admission linearizable (§5.1).

## 11. Follow-ups / risks

- **[SPIKE S1]** FD-release timing vs. `wait_idle` — the router task owns socket
  `Arc`s and releases them only when polled after stream-closed + conns-gone
  (§2.2/§5.3). Quantify the residual with the S1 rebind test.
  **[RESOLVED — Linux]** measured: bounded retry required (~5–25% of shutdowns
  need one retry; worst 2 attempts). The bounded rebind retry is
  the shipped contract.
- **Upstream ask (preferred real fix for S1):** request a `tokio-quiche` API to
  **await listener/router-task completion** (or a socket-released signal). With
  it, `wait_idle` becomes exact and the bounded rebind retry can be dropped. This
  is the clean analog of quinn owning its socket. Track as an upstream issue.
  (Still open — the measured residual is small but non-zero, so this remains the
  preferred exact fix.)
- **[SPIKE S2]** admission-fence completeness under concurrent `close()`/`accept()`
  (§7) — **[RESOLVED]** Confirmed by `s2_admission_fence_under_concurrent_close`.
  **[SPIKE S3]** mid-handshake `wait_idle` bound with a finite
  `handshake_timeout` (§7) — **[RESOLVED]** Confirmed by
  `s3_mid_handshake_bounded_by_timeout` (`wait_idle` ≈ the timeout).
- **Mid-handshake force-close** is bounded only by the configured handshake/idle
  timeout (§5.4/§7). Document on `close()`; recommend a finite `handshake_timeout`
  in server `QuicSettings`.
- Consider a **graceful HTTP/3 GOAWAY** drain layer above the transport close, so
  in-flight requests complete before `CONNECTION_CLOSE` (out of scope; builds on
  this surface).
- Consider per-connection **close-complete** signals (a `closed()` future) if a
  caller needs finer-grained than endpoint-wide `wait_idle`.
- Update bridge §9 wording once implemented: **endpoint-initiated** shutdown *does*
  use a `wait_idle` mechanism (drop-driven teardown still does not).

## 12. Open questions

1. Should `wait_idle()` implicitly drop the acceptor stream if the caller still
   holds it? It **cannot** be exact otherwise (the router needs `accept_sink`
   closed to exit, §2.2/§6). Options: (a) strict — require the caller to drop the
   acceptor first (matches quinn's separate `Endpoint`/accept split; current
   assumption); (b) have `H3QuicheEndpoint` hold a way to signal/close the
   listener directly. Leaning (a), documented loudly.
2. Do we need a `close()` variant that **aborts** (immediate `CONNECTION_CLOSE`,
   no draining) vs. graceful? quinn's `close` is effectively immediate-app-close;
   matching that is likely sufficient. A separate graceful-drain is §11's GOAWAY
   follow-up.
3. Is the bounded post-`wait_idle` rebind retry acceptable as the shipped contract
   until the upstream router-completion signal lands, or should we block on the
   upstream change? Leaning: ship the bounded retry (unblocks the reconnect test
   now), pursue upstream in parallel.
4. Client-side `endpoint()` (§4.1): ship now for symmetry, or defer? Leaning:
   defer; the server path unblocks the reconnect test.
