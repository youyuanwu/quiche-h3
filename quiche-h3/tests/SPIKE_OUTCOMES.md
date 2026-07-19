# Phase 0 spike outcomes — pinned `tokio-quiche 0.19.1` / `quiche 0.29.3`

Observed by the loopback QUIC probes in `quiche-h3/tests/spike_harness.rs`.
Run them with:

```text
cargo test -p quiche-h3 --test spike_harness -- --ignored --nocapture
```

All 12 probes pass. Each gate below records the design **Assumption** (from
`docs/design/quiche-h3-bridge.md` §14 / §5.5), the **Observed** runtime
behavior (concrete return values), and a **Verdict**. Line references point at
the pinned crate sources under
`~/.cargo/registry/.../{tokio-quiche-0.19.1,quiche-0.29.3}`.

> These outcomes are recorded here only. Per the task, `docs/design/quiche-h3-bridge.md`
> is intentionally **not** edited — the maintainer folds these into §14/§5.5.

| gate | verdict |
|---|---|
| T1b | Confirmed (peer-observed close) |
| T2  | Confirmed |
| T2a | Confirmed (raw err = `TimedOut`, see nuance) |
| T4  | Confirmed (garbage silently dropped; no item error; listener keeps serving) |
| Q1  | Confirmed (destructive cursor + one-credit materialization) |
| Q2  | Confirmed |
| Q3  | Confirmed (reset flushed at zero capacity) |
| Q4  | Confirmed |
| Q5  | Confirmed (zero-capacity FIN accepted + flushed) |
| §5.5 BLOCKER | Confirmed premise → **outcome 3** (narrow the drop-in claim) |
| §5.5 tombstone | Confirmed (contract A premise holds) |

---

## T1b — successful last-handle `qconn.close` reaches the peer

**Test:** `spike_t1b_peer_observes_application_close`

**Assumption (§14 T1b):** a successful `qconn.close` is serialized in the
ordinary post-`process_writes` flush and the peer promptly receives the selected
application `CONNECTION_CLOSE`.

**Observed:** the client calls `qconn.close(true, 0x1234, b"t1b-bye")` inside
`process_writes`. The server's `on_conn_close` (captured via `CloseObs`) reports:

```text
CloseObs { result_ok: true,
           peer_error: Some((true, 4660, "t1b-bye")),
           local_error: None }
```

`4660 == 0x1234`; `is_app == true`; the reason string round-trips. The peer
observed the application close within a few hundred ms, driven purely by the
worker's ordinary flush (the test never forces a flush).

**Verdict:** **Confirmed** at the peer-observed-close level (as scoped by the
task; raw single-packet `CONNECTION_CLOSE` capture not attempted). The
saturated-write / pending-explicit-close permutation is left to the Phase 8
close-flush loopback test.

---

## T2 — `QuicConnection` handle drop does not tear down the worker

**Test:** `spike_t2_handle_drop_keeps_worker_alive`

**Assumption (§14 T2):** `QuicConnection` is metadata and does not represent the
`quiche::Connection`; dropping it does not tear down the worker.

**Observed:** after both sides establish, the test `drop`s the `QuicConnection`
returned by `connect_with_config`, waits 300 ms, then pokes the worker:

```text
client (is_established, is_closed) after handle drop = Some((true, false))
server (is_established, is_closed) after client handle drop = Some((true, false))
```

The client worker still runs submitted jobs (so it is alive) and reports
`is_established == true, is_closed == false`; the server peer is likewise still
established and not closed.

**Verdict:** **Confirmed.** The metadata handle drop is independent of worker
lifetime.

---

## T2a — client rejecting the server cert resolves `Err`

**Test:** `spike_t2a_client_rejecting_cert_resolves_err`

**Assumption (§14 T2 / T2a):** with a failed handshake the `connect_with_config`
future resolves `Err` (client setup failures map that future's raw error), and
`on_conn_established` never runs so no `ConnTerminal` is published pre-handshake.

**Observed:** a client with `verify_peer = true` and no trust root (against the
self-signed server) resolves:

```text
connect_with_config(...) resolved: Err("Custom { kind: TimedOut,
    error: \"connection <cid> timed out\" }")
```

`on_conn_established` did **not** run (asserted false). The connect future
resolves `Err`.

**Nuance / Phase 7 note:** the surfaced raw error is a **`std::io::Error` of kind
`TimedOut`**, not a distinct TLS/verification variant. The cert rejection aborts
the handshake, and the future resolves via the idle-timeout path rather than a
typed TLS error. §7/§8.4's client-error mapping should treat the raw
`connect_with_config` error as opaque/unclassified (as the design already says —
"map the client's raw error") and must not assume a TLS-specific variant is
surfaced here.

**Verdict:** **Confirmed** (future resolves `Err`; no pre-handshake
`on_conn_established`), with the recorded nuance that the raw error is a
`TimedOut` `io::Error`.

---

## T4 — `QuicConnectionStream` item-error taxonomy vs listener-fatal

**Test:** `spike_t4_garbage_datagram_then_real_connection`

**Assumption (§14 T4):** an error processing an individual initial packet is a
stream item (`Some(Err)`), not listener termination; `next()` keeps yielding
subsequent connections. The exact item-error set is undocumented.

**Observed:** two garbage UDP datagrams (a 1200-byte `0xAB` run and a 4-byte
runt) are sent to the server port **before** any real handshake. A real client
then connects successfully:

```text
real client connect after garbage: Ok
accept-stream items: Ok(conn)=1  Err-item-variants=[]
```

The garbage is **silently dropped by the router** — it never surfaces as a
`Some(Err(..))` accept-stream item. The stream went on to yield the real
connection as `Ok`, i.e. the listener kept serving.

**Verdict:** **Confirmed** for the load-bearing property (garbage does not kill
the listener; it keeps yielding). Refinement of the design's phrasing:
undecryptable/garbage datagrams do **not** produce a per-item `Err` at all on
this build — they are dropped below the accept stream. No distinct item-`Err`
variant was reproducible via raw garbage, and no fatal signal appeared. §7.1's
continue-on-item-error policy is safe; the recorded reality is that ordinary
malformed traffic is invisible to the accept loop (so item errors, if any, come
from other conditions, and a socket-fatal condition would surface as stream
end / `None`).

---

## Q1 — destructive `*_next` cursors + `stream_priority` materialization

**Test:** `spike_q1_readable_destructive_and_priority_materialize`

**Assumption (§14 Q1 / §5.5 third spike):** both `*_next` iterators are
destructive (dearm until re-armed); `stream_priority(id, urgency, incremental)`
creates the stream on first call and consumes exactly one unit of stream credit
(idempotent on repeat).

**Observed:**

- **Destructive readable cursor.** After the server sends on its
  server-initiated unidirectional stream 3, the client observes:
  ```text
  stream_readable_next first=Some(3)  second=None
  ```
  The id is returned once and then dearmed until new data arrives.

- **`stream_priority` materialization + one credit.** On a fresh client bidi id 0:
  ```text
  exists_before=false  priority1=Ok(())  cap1=Ok(13500)
                       priority2=Ok(())  cap2=Ok(13500)
  peer_streams_left_bidi 100 -> 99
  ```
  Before prioritization the stream does not exist (`stream_capacity` errors);
  the first `stream_priority` materializes it (subsequent `stream_capacity` is
  `Ok`), and `peer_streams_left_bidi` drops by **exactly one** (100→99). The
  second identical `stream_priority` is idempotent — `Ok(())`, no further credit
  consumed, capacity unchanged.

- **Positive control for the §5.5 blocker.** Under normal capacity, after
  `stream_send(0, b"x", false) = Ok(1)`, `stream_writable_next() = Some(0)` — the
  writable path surfaces a materialized/written stream when `tx_cap > 0`.

  (Note: `cap1 = Ok(13500)` reflects `min(tx_cap, stream_cap)` where the fresh
  connection's `tx_cap` is bounded by the initial ~10-packet congestion window,
  not the 1 MB stream flow-control limit.)

**Verdict:** **Confirmed** — destructive dearm on both `*_next`, first-call
materialization, exactly-one-credit consumption, idempotent repeat.

---

## Q2 — `Connection::close` result semantics

**Test:** `spike_q2_close_first_ok_repeat_done`

**Assumption (§14 Q2):** `Ok(())` means the call accepted the supplied close
code/reason; a repeated close returns `Error::Done` (a close was already in
progress; no new cause accepted).

**Observed:**

```text
first close=Ok(())  second close=Err(Done)
```

Matches quiche source: `close()` returns `Err(Done)` once `local_error` is set
(`quiche-0.29.3/src/lib.rs:7526`).

**Verdict:** **Confirmed.** (Peer-close race / invalid-state permutations are
left to Phase 5 regression tests; the load-bearing first-Ok / repeat-Done
contract is confirmed.)

---

## Q3 — `stream_shutdown(Write)` at zero connection send capacity

**Test:** `spike_q3_stream_shutdown_write_resets_without_capacity`

**Assumption (§14 Q3):** after a stream is materialized and blocked by credit,
`stream_shutdown(Write, code)` accepts the reset independently of capacity and
the ordinary flush emits `RESET_STREAM` without waiting for `MAX_DATA` /
`MAX_STREAM_DATA`.

**Observed:** with a server that advertises `initial_max_data = 0` (client
`tx_cap == 0`):

```text
zero-grant: stream_send(data)=Err(Done)  stream_capacity(0)=Ok(0)
stream_shutdown(Write, 0x1) at zero cap = Ok(())
server stream_recv(0) after reset = Err(StreamReset(1))
```

A data write is refused (`Err(Done)`) at zero capacity, yet
`stream_shutdown(Write, 0x1)` returns `Ok(())` and the peer observes
`StreamReset(1)` — the supplied code round-trips and the `RESET_STREAM` is
flushed with **no** `MAX_DATA` grant.

**Verdict:** **Confirmed** — the zero-connection-capacity dimension is exercised
directly (not merely the non-blocked path). §5.3a's "call shutdown ahead of the
remainder" is safe on this build.

---

## Q4 — registered-stream STOP_SENDING → `StreamStopped(code)`

**Test:** `spike_q4_stop_sending_surfaces_stream_stopped`

**Assumption (§14 Q4):** after a peer STOP_SENDING for a registered stream, an
immediate `stream_capacity(id)` returns `Error::StreamStopped(code)` with the
peer's code.

**Observed:** the client opens bidi stream 0 and sends data; the server reads it
and calls `stream_shutdown(0, Shutdown::Read, 0x42)` (which emits STOP_SENDING to
the client's send side):

```text
server recv=Ok((5, false))  shutdown(Read, 0x42)=Ok(())
client stream_capacity(0) after STOP_SENDING = Err(StreamStopped(66))
```

`0x42 == 66`; the peer's stop code round-trips into
`Error::StreamStopped(66)` on the stopped (client) send side.

**Verdict:** **Confirmed.** (The idle / queued-write / queued-FIN /
partial-write / repeated-probe permutations are left to Phase 4 regression
tests; the load-bearing visibility+code behavior is confirmed.)

---

## Q5 — pure FIN at zero connection send capacity

**Test:** `spike_q5_zero_capacity_fin_accepted_and_flushed`

**Assumption (§14 Q5):** a pure FIN (`stream_send(id, &[], true)`) carries no
data, so it is accepted and flushed even at zero stream/connection send
capacity, without waiting for a `MAX_DATA` grant.

**Observed:** with a server advertising `initial_max_data = 0` (client
`tx_cap == 0`):

```text
zero-grant: capacity=Ok(0)  data_send=Err(Done)  fin_send=Ok(0)
server stream_recv(0)=Ok((0, true))  fin_seen=true  stream_finished=true
```

A data write returns `Err(Done)` at zero capacity, but the pure FIN returns
`Ok(0)` and the peer observes `Ok((0, true))` — the FIN is flushed with no
`MAX_DATA` grant. Confirmed by quiche source: `stream_send` early-returns
`Err(Done)` only for `cap == 0 && len > 0`, while an `empty_fin` (`len == 0 &&
fin`) is pushed onto the flushable queue (`quiche-0.29.3/src/lib.rs:5910`+).

**Verdict:** **Confirmed** — zero-capacity FIN is accepted and flushed. §5.3a can
complete a `Finish` at the transport-acceptance boundary; connection close is not
needed as the FIN mechanism.

---

## §5.5 BLOCKER — zero `tx_cap` hides writable-only peer bidi discovery

**Test:** `spike_5_5_blocker_zero_txcap_hides_writable_discovery`

**Assumption / question (§5.5):** does `stream_writable_next()` / `writable()`
surface a stream at zero connection-level send capacity? Outcome **1** (yes,
even stopped streams are surfaced) would fully handle the case via §5's writable
path; otherwise outcome **2** (upstream API) or **3** (narrow the claim).

**Observed:** with a server advertising `initial_max_data = 0` (client
`tx_cap == 0`), a stream is materialized and has stream-level capacity, yet:

```text
stream_send=Err(Done)  stream_capacity(0)=Ok(0)
stream_writable_next()=None  writable().count()=0
```

The stream is **known** (`stream_capacity(0) = Ok(0)`) but is **invisible** to
both `stream_writable_next()` and `writable()`. This matches the quiche source:
both methods early-return empty when `tx_cap == 0` — the guard runs **before**
the stopped-stream branch, so even a STOP_SENDING-stopped stream is not surfaced
at zero connection capacity (`quiche-0.29.3/src/lib.rs:6403` and `:6643`).

**Implied outcome:** **Outcome 3.** Outcome 1 is **refuted** — no public quiche
0.29 API enumerates a writable-only (or STOP_SENDING-stopped) peer bidi stream
while `tx_cap == 0`. Absent an upstream API (outcome 2), the adapter's drop-in
correctness claim must be **explicitly narrowed** for this pathological case
(peer opens a bidi stream while our connection-level send capacity is exhausted).
The `stream_capacity(id)` / `stream_writable(id)` probes cannot substitute — they
require a **known** id and do not enumerate the unknown one.

**Verdict:** **Confirmed premise → outcome 3** (narrow the drop-in claim), unless
an upstream enumeration API is adopted (outcome 2). This is an
external-feasibility/scope decision per the Phase 0 "outcome branching".

---

## §5.5 admission-tombstone — a fully-terminal id never reappears

**Test:** `spike_5_5_tombstone_terminal_id_never_reappears`

**Assumption (§5.5 committed contract A):** once a stream is fully terminal
(send+FIN both directions, both sides finished/collected), its id never
reappears in `readable()` / `writable()` / `stream_*_next()` discovery, so the
adapter may drop `admit[id]` at the terminal edge with no reclaim subsystem.

**Observed:** client sends `ping`+FIN on bidi stream 0; the server reads it and
replies `pong`+FIN; the client reads `pong`+FIN, then probes discovery:

```text
recv=Ok((4, true))  finished=true
readable_next=None   writable_next=None
readable_has0=false  writable_has0=false
(second pass after idle) readable_has0=false  writable_has0=false
```

After full bidirectional completion the id is collected and does **not** reappear
in any discovery surface, on the immediate probe or a later idle re-probe.

**Verdict:** **Confirmed.** Contract A's premise holds for the normal
full-completion path. The only doubtful path called out in §5.5 — a **late
STOP_SENDING re-marking an already-finished stream writable** — is not triggered
by this scenario and is not reproducible on loopback with the standard
completion order; contract A ships as-is, with the upstream
`stream_collected(id)` fallback (A-plus-upstream-API) reserved only if that
adversarial path is later shown to resurrect an id.

---

# Endpoint graceful-shutdown spike outcomes (design `quiche-h3-endpoint-shutdown.md` §5.6/§11)

Observed by the loopback probes in `quiche-h3/tests/endpoint_shutdown.rs`. Run:

```text
cargo test -p quiche-h3 --test endpoint_shutdown -- --ignored --nocapture
```

These resolve the three spikes the endpoint-shutdown design intentionally left
empirical. Measurements are Linux-local (the CI matrix additionally builds/tests
on Windows, but the S1 timing verdict below is Linux-measured — see the platform
note in S1).

| gate | verdict |
|---|---|
| S1 | **Bounded retry required** (same-port rebind: ~5–25% of shutdowns need exactly one retry; worst observed **2 attempts** across 300 shutdowns; latency ≈ one backoff interval) |
| S2 | Confirmed (admission fence holds: `next_id` frozen after `close()`; nothing yielded post-`closing`) |
| S3 | Confirmed (stalled mid-handshake worker bounded by `handshake_timeout`; `wait_idle` ≈ timeout) |

## S1 — same-port rebind after `close()` → drop acceptor → `wait_idle()`

**Test:** `s1_same_port_rebind_after_wait_idle` (50 iterations/run)

**Assumption (§5.6):** after graceful shutdown, the SAME UDP port may not be
*instantly* rebindable because tokio-quiche's router task owns its own
`Arc<UdpSocket>` clones and releases them only when next polled after its
accept-sink closes; a short bounded rebind retry may be needed. This mirrors the
`tonic-h3` reconnect scenario.

**Observed — two measurement points (Linux loopback, `#[ignore]`d test, 50
iters/invocation):**

The residual is sensitive to *when* the rebind is attempted relative to
`wait_idle()`:

```text
# (A) rebind measured IMMEDIATELY after wait_idle() (the tightest window, and
#     what the shipped test now measures — matches the tonic-h3 reconnect race):
S1 rebind: iters=50 needing_retry=50 worst_attempts=2 worst_latency=11.979026ms
S1 rebind: iters=50 needing_retry=50 worst_attempts=2 worst_latency=12.298068ms
S1 rebind: iters=50 needing_retry=50 worst_attempts=2 worst_latency=12.598355ms

# (B) earlier config, with incidental client/drive cleanup between wait_idle()
#     and the rebind (gives the router task a scheduler tick to be polled):
S1 rebind: iters=50 needing_retry=6  worst_attempts=2 worst_latency=6.723448ms
S1 rebind: iters=50 needing_retry=3  worst_attempts=2 worst_latency=6.468537ms
S1 rebind: iters=50 needing_retry=12 worst_attempts=2 worst_latency=11.686635ms
```

The decisive, backoff-independent invariant is stable across BOTH measurement
points: the immediate same-port rebind may fail its first attempt, and **exactly
one** backoff retry always sufficed — the worst case observed anywhere was **2
attempts** (one retry), with retry latency ≈ one backoff interval. The *rate* of
first-attempt failure depends on timing: at the tightest window (A) it is
effectively 100% (the router task has not yet been polled to release its socket
clone), whereas even a few hundred microseconds of unrelated work (B) drops it to
~5–25%. Either way, one bounded retry closes the window. The rebound port was
proven usable each iteration by completing a fresh handshake on it.

**Verdict:** **Bounded retry required (nuanced, not "effectively exact").** The
port is reliably rebindable after at most one retry; a caller that must rebind
the exact port immediately after `wait_idle()` should use a bounded retry loop.
The shipped test budgets a generous safety margin (≤ 60 attempts / 10 ms backoff)
to stay robust under CI scheduler contention — distinct from the measured-typical
worst case above. This is encoded in the `close`/`wait_idle`/`endpoint()` rustdoc.

**Platform note:** measured on Linux only. The documented contract for
un-measured platforms (incl. Windows, which the CI matrix builds) stays the
conservative "bounded retry may be required" wording.

## S2 — admission fence under concurrent `accept()`/`close()`

**Test:** `s2_admission_fence_under_concurrent_close`

**Assumption (§5.4/§5.5):** registration and `close()` serialize under the single
endpoint lock, giving a linearizable admission fence: no worker is started after
`close()`, and no established connection is yielded out of `accept()` once
`closing` is observed (post-`closing` handshakes are dropped, not yielded).

**Observed:** with ≥ 3 live connections established, `close()` is fired and the
registration counter (`next_id`, via the `#[doc(hidden)]` test accessor) is
snapshotted at the linearization point. A post-close burst of 24 new clients is
launched; after 400 ms the counter is unchanged, and the acceptor drains to
`Ok(None)` having yielded no more connections than were registered before close.

**Verdict:** **Confirmed.** The single-lock registration/close serialization
holds the fence exactly as designed.

## S3 — mid-handshake worker bounded by `handshake_timeout`

**Test:** `s3_mid_handshake_bounded_by_timeout`

**Assumption (§5.5/§11):** a connection that registers a worker but stalls
mid-handshake must not pin `wait_idle()` open forever. With a finite
`handshake_timeout` configured, the stalled worker self-terminates at the
timeout (a mid-handshake worker is not yet established, so it does not process
the broadcast `Close` command), and `wait_idle()` completes within a bounded
margin of the timeout.

**Observed:** with `handshake_timeout = 800 ms` and
`disable_client_ip_validation = true`, a raw quiche client that sends only its
Initial flight then stalls causes the server to start and register exactly one
worker (proven via the registry snapshot — registration precedes `close()`).
After `close()`, `wait_idle()` resolved in **~801 ms** (≈ the handshake timeout),
and the worker deregistered on its timeout exit.

**Verdict:** **Confirmed.** The mid-handshake case is bounded by the configured
`handshake_timeout`, not by `close()` responsiveness.
