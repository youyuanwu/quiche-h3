# Design: issue #43 — concurrency tail-drain stall & FIN coalescing

> **Status: Fixed for unary request/response; streaming caveat outstanding.**
> The tail-drain stall is resolved in `quiche-h3/src/driver.rs` (held-write FIN
> coalescing) and covered by a real-I/O loopback repro
> (`quiche-h3/tests/concurrency.rs::concurrent_requests_all_complete_no_tail_stall`)
> plus a deterministic `MockConn` unit test
> (`coalesced_write_then_finish_emits_single_fin_frame`). The fix's remaining
> streaming limitation (§5) is **not yet addressed** and is tracked as follow-up.
>
> Section numbers below are local to this document; references of the form
> "bridge §N" point at [`quiche-h3-bridge.md`](./quiche-h3-bridge.md). Inline
> `file.rs:NN` references are approximate pointers against the pinned build
> (`tokio-quiche 0.19.1` / `quiche 0.29.3`) and the fix commit, not exact lines.

## 1. Summary

A downstream consumer ([tonic-h3 issue
#43](https://github.com/youyuanwu/tonic-h3/issues/43)) reported that the
`quiche-h3` backend **stalls above concurrency 1** on loopback: with `N`
requests in flight concurrently over a single QUIC connection, the bulk of a run
completes but the final `N − 1` in-flight requests **hang indefinitely**.

The root cause is a transport-level interaction with the pinned **quiche
0.29.3**: the bridge emitted each HTTP/3 response **body** and its terminating
**FIN** as two *separate* `stream_send` calls, and quiche discards the resulting
**standalone empty-FIN** frame once the stream's body is fully ACKed. The fix
**coalesces the FIN onto the final data frame** so quiche never produces a
discardable standalone empty-FIN.

The fix fully resolves the reported (unary request/response) stall. It carries a
**streaming tradeoff** — deferring the last written message until the next
write/finish — documented in §5.

## 2. The bug

### 2.1 Symptoms & diagnostics (from the issue)

- `failed ≈ concurrency − 1` — exactly the number of workers left holding an
  in-flight request when the request-issuing loop reaches the total count.
- Elapsed time **pins to the per-request timeout** (3s → 3.07, 10s → ~10.25),
  independent of the real work — the trailing requests are not doing work, they
  are waiting out a timeout.
- **No stall at concurrency 1** (0 failed; elapsed reflects real work).
- Masked over real WAN networking (~67 ms RTT), because request pacing keeps the
  connection continuously driven; on loopback the tight loop finishes instantly
  and the tail requests are stranded.

### 2.2 Reproduction

`quiche-h3/tests/concurrency.rs::concurrent_requests_all_complete_no_tail_stall`
mirrors the benchmark shape: one client connection, `CONCURRENCY = 8` worker
tasks sharing a cloned `h3::client::SendRequest`, each pulling the next index off
a shared atomic counter and driving a full GET request/response round-trip until
`TOTAL = 200` requests are issued; it asserts all 200 complete within a 30 s
deadline.

- **Before the fix:** 193/200 completed, pinned to the 30 s deadline. `193 = 200
  − (8 − 1)`, i.e. exactly `concurrency − 1 = 7` streams stranded. Confirmed
  deterministic scaling: C=1 → 0 stranded; C=2 → 1; C=4 → 3; C=8 → 7.
- **After the fix:** 200/200 in ~0.09 s.

The test is `#[ignore]`d (binds UDP + runs a real handshake). Run with:

```text
cargo test -p quiche-h3 --test concurrency -- --ignored --nocapture
```

## 3. Root cause

### 3.1 The bridge emitted body and FIN as two separate transport calls

For a response the front end issues `send_response` + `send_data` + `finish`,
which the bridge turns into ordered `SendOp`s on the per-stream `send_ops` queue,
driven one op per round-robin turn (bridge §5.3a):

- `service_write_turn` issued `qconn.stream_send(id, &chunk[..n], false)` — the
  FIN bit hardcoded `false`.
- `service_finish_turn`, in a **later** round-robin turn, issued a standalone
  `qconn.stream_send(id, &[], true)`.

So the terminating FIN was always a **separate, 0-length STREAM frame** emitted
in a different turn from the body. Under concurrency the round-robin separates a
stream's `Finish` turn from its `Write` turn in time; the body is sent and ACKed
before the standalone empty-FIN turn.

Note the two ops are effectively **never adjacent** in the queue: hyperium `h3`
completes a `send_data` write (via its `poll_ready` round-trip through the
front↔back channel) *before* it issues `finish()`. So a naive "peek the next op
and coalesce if it's a `Finish`" cannot fire — the `Finish` op has not been
enqueued yet when the `Write` is serviced. (Empirically, 615/615 write turns in a
scratch run saw only the `Write`.)

### 3.2 Why quiche discards the standalone empty-FIN

Verified directly against `quiche 0.29.3` source:

1. A standalone empty-FIN **is** inserted into quiche's `flushable` queue — the
   send path special-cases `empty_fin = len == 0 && fin` and inserts the stream
   when `(flushable || empty_fin) && !was_flushable`
   (`quiche/src/lib.rs:6076`, `:6095`). So quiche *intends* to send it, and the
   flushable emit loop would encode a 0-length STREAM frame with the FIN bit
   (`lib.rs:5180`–`5266`, `send_buf::emit` at `send_buf.rs:298`).

2. **But** `Stream::is_complete()` for a bidi stream is `recv.is_fin() &&
   send.is_complete()` (`lib.rs:863`), and `SendBuf::is_complete()` is simply
   `acked == (0..fin_off)` (`send_buf.rs:525`). A 0-byte FIN sets
   `fin_off = off_back`, which for an already-fully-sent body **equals the
   already-ACKed offset**. So `is_complete()` flips **true the instant the FIN is
   queued** — even though the FIN bit has never reached the peer (quiche treats
   the send side "complete" when all *bytes* up to `fin_off` are ACKed,
   irrespective of whether the FIN flag was transmitted).

3. On the next ACK-processing pass, the reap check
   `if is_complete && !is_readable && !is_writable { streams.collect(...) }`
   (`lib.rs:3655`) fires, and `StreamMap::collect` **removes the stream from the
   flushable queue** (`stream/mod.rs:606`). The queued empty-FIN is discarded and
   **never packetized**.

Frame-level qlog on both endpoints confirmed this: for the stranded stream the
server logged `data_moved app→transport … fin_set` (the FIN queued) but **no**
`packet_sent` ever carried a STREAM frame at that offset with the FIN bit; the
client received the body bytes but never observed end-of-stream, so its
`recv_data()` loop blocked forever. No STOP_SENDING / RESET_STREAM was involved.

### 3.3 Why concurrency 1 never stalls

At concurrency 1 there is only ever one in-flight stream and the issuing loop
immediately follows each completion with the next request, so the connection is
continuously driven and the empty-FIN is flushed in the same active send cycle as
(or immediately after) its body — before the reap race can occur. Under `N > 1`
the round-robin separates `Write` from `Finish`; once the connection quiesces
(issuing loop done, `cmd_rx` idle) the queued empty-FIN for the last such stream
is reaped before a `send()` emits it — stranding exactly one per quiescence edge,
i.e. `concurrency − 1` in aggregate.

### 3.4 Ruled out

Recv backpressure, flow control (defaults are ~10 MB conn / 1 MB stream / 100
streams vs. ~72 bytes/stream), loss/congestion (`lost = 0`, `retrans = 0`),
client-side lost wakeup / `TerminalCell` (the stranded stream's FIN never reaches
the client at all — a server-side never-sent-FIN), and STOP_SENDING-on-drop
(suppressing it changed nothing). Forcing the worker to never park (a
`wait_decision` → `Yield` experiment) did **not** deliver the FIN, because quiche
had already reaped the stream — distinguishing this from a driver-side lost
wakeup.

## 4. The fix — held-write FIN coalescing

The cure is to **never emit a standalone empty-FIN**: carry the FIN bit on the
final DATA frame. Because the `Write` and `Finish` ops are not adjacent (§3.1),
the driver **holds** a fully-sendable write across the `write-completion →
finish` round-trip and flushes it carrying the FIN when the `Finish` arrives.

Implemented in `quiche-h3/src/driver.rs` (driver-confined):

- **`HeldWrite<B>`** and a transient per-stream `held: Option<HeldWrite<B>>` slot
  on `StreamSendState` (`driver.rs:394`, `:451`) — at most one held write per
  stream (FIFO single-slot, bridge §2.1).
- **`service_write_turn`**: when the whole buffer fits `stream_capacity(id)`,
  **defer** the transport `stream_send` — move the buffer into `held`, resolve
  the write completion `Ok` and release its SF-6 send permit *now* (preserving
  pre-#43 completion timing), and pop the op. Capacity-limited or 0-byte writes
  keep the unchanged partial-send + low-water re-arm backpressure path
  (bridge §5.1).
- **`flush_held_once`** (`driver.rs:2332`) + **`service_finish_turn`**: flush the
  held bytes carrying the FIN as a single `stream_send(id, last_chunk, true)` —
  **coalesced**. A lone `Finish` / empty-body response still emits
  `stream_send(id, &[], true)` (the concurrency-1-safe path is preserved for the
  legitimately-empty case).
- `held` is cleared at every drain site (reset, send-terminal, connection close;
  e.g. `driver.rs:2200`, `:2223`); exactly-once completion (bridge §5.3a) is
  preserved for both the `Write` and `Finish` ops.

Because the coalesced FIN rides a DATA frame carrying real bytes, `fin_off >
acked` until that frame is ACKed, so `is_complete()` stays false and the reap
race of §3.2 cannot discard the FIN.

### 4.1 Tests

- **Loopback repro** (§2.2): 200/200, run 3× per toolchain.
- **`coalesced_write_then_finish_emits_single_fin_frame`** (non-ignored,
  `MockConn`): asserts a `Write(body)` then `Finish` produces a **single**
  `mock.sent` entry `(id, body, true)` (not the pre-fix `(id, body, false)` +
  `(id, [], true)` pair), that both ops' completions resolve `Ok` exactly once,
  and that a lone `Finish` still records `(id, [], true)`. A `MockConn` test
  cannot reproduce the *live* stall (it does not model quiche's packetization),
  but it does lock in the coalescing invariant in normal CI.

### 4.2 Validation

Green on both CI toolchains (Rust 1.90.0 and 1.97.0): `fmt --check`, `clippy
--all-targets -D warnings`, the unit suite (164 tests), and all `#[ignore]`d
loopback tests including the repro.

## 5. Remaining problem — the streaming tradeoff (NOT yet addressed)

FIN coalescing fundamentally requires **holding the last written message until
the FIN is known**. For unary request/response this hold is transient (the
`finish` follows the final `send_data` within one front↔back round-trip). But the
hold is problematic for streaming:

- **Server-streaming across producer pauses:** a message written and then *not*
  immediately followed by another write or a `finish` sits in `held` and is not
  put on the wire until the next write/finish. This **delays** delivery of the
  last message before any producer pause.
- **Strict ping-pong bidi streaming (deadlock):** if the server writes a message
  and then *awaits a client response before producing the next one*, the held
  message is never flushed (the worker parks on `cmd_rx` with data still held),
  the client never receives it, so it never responds — a **deadlock**. This
  pattern is common in gRPC bidi streaming, which **tonic-h3 supports**.

This tension is inherent to *any* FIN-coalescing scheme: the final data must be
held speculatively until the FIN is known, which conflicts with sending data
eagerly for streaming liveness. It cannot be cheaply removed without either
reverting to immediate sends (which re-opens #43) or adding a flush trigger.

### 5.1 Candidate remedies (for a follow-up)

1. **Bounded flush-on-park timer.** Hold the fully-sendable write, but arm a
   short timer; if a `Finish` arrives first, coalesce (the unary fast path — the
   `finish` is a channel round-trip, not a network RTT, so the window can be
   very small); otherwise flush the held bytes without the FIN when the timer
   fires or the worker would park with data held. This bounds streaming latency
   to the timer and removes the deadlock. **Caveat:** it does *not* fully protect
   a streaming response whose *final* message + `finish` still race the §3.2 reap
   after a flush-without-FIN; that residual case would still rely on the
   connection staying driven (as today for streaming) or on an upstream fix.
2. **Gate/scope streaming use** and document the limitation until (1) lands.
3. **Upstream fix in quiche** so a queued standalone empty-FIN is not reaped
   before it is emitted (e.g. keep the stream flushable until the FIN bit is
   actually transmitted, not merely until all bytes are ACKed). This is the most
   correct long-term fix but is outside the `quiche-h3` bridge.

Until a remedy lands, `quiche-h3` is safe for **unary** request/response (the
#43 scenario) and remains **experimental for streaming** workloads.

## 6. References

- Issue: <https://github.com/youyuanwu/tonic-h3/issues/43>
- Fix commit: `4620d11` (`fix(driver): coalesce FIN onto final data write …`).
- Repro: `quiche-h3/tests/concurrency.rs`.
- Bridge send model: [`quiche-h3-bridge.md`](./quiche-h3-bridge.md) §5.3a
  (per-stream send ordering & completion), §5.1 (backpressure), §12 (follow-ups).
- quiche 0.29.3 evidence: `src/lib.rs:6076`/`:6095` (empty-FIN insert-flushable),
  `:5180`–`:5266` (flushable emit loop), `:3655` (ACK-path reap),
  `src/stream/mod.rs:606` (`collect` removes flushable), `:863`
  (`Stream::is_complete`), `src/stream/send_buf.rs:298` (`emit` FIN),
  `:525` (`SendBuf::is_complete`).
