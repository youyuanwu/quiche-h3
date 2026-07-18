# Implementation Plan: `quiche-h3`

Derived from `docs/design/quiche-h3-bridge.md` (the approved design). Section
references (`§N`) point into that document. This plan sequences the work into
phases with explicit gates, deliverables, and success criteria. It does **not**
restate the design — read the design section named in each phase before coding
it.

## Readiness snapshot

| Prerequisite | Status |
|---|---|
| Rust toolchain (`rustc`/`cargo` 1.95) | ✅ present |
| `cmake` + `cc` (quiche → BoringSSL build) | ✅ present |
| crates.io reachable (pinned `quiche 0.29`, `tokio-quiche 0.19`, `h3 0.0.8`) | ✅ reachable |
| §14 spike outcomes recorded | ✅ **done — §14.1 (pinned 0.19.1/0.29.3)** |
| §5.5 blocker + tombstone spikes recorded | ✅ **done — §5.5 (blocker→outcome 3; tombstone→contract A)** |
| Loopback TLS test certificates | ✅ `rcgen` fixtures in `tests/spike_harness.rs` |
| `h3-util` / `tonic-h3` repos for cross-repo integration | ❌ external — Phase 9 (out of scope) |

**Gate rule (from the design banner and §14 "Gating"):** ✅ **CLEARED.** Every
gating spike outcome (T1b, T2, T4, Q1, Q2, Q3, Q4, Q5, and the §5.5
discovery/tombstone spikes) is recorded in the design doc (§14.1, §5.5) and the
BLOCKED banner is lifted. **§5.5 BLOCKER resolved to outcome 3** — the drop-in
claim is narrowed for the zero-connection-capacity writable-only peer-bidi case;
this constrains Phase 3 discovery (do not assume full coverage of that path).
Phases 1–8 may now proceed.

---

## Phase 0 — Workspace bootstrap + spike harness  *(GATE)*

**Goal:** stand up the crate and *resolve every §14/§5.5 gate* against the pinned
build, recording outcomes back into the design doc.

Deliverables:
- Workspace + crate skeleton per §10:
  ```
  Cargo.toml (workspace)
  quiche-h3/Cargo.toml
  quiche-h3/src/{lib.rs, driver.rs, stream.rs, listener.rs, connector.rs, buffer.rs}
  ```
  Dependencies exactly per §10: `tokio-quiche ~0.19`, `h3 0.0.8`, `bytes`,
  `futures`, `tokio` (mpsc/oneshot), optional `tracing`. **No `tokio-util`.**
  Re-export `pub use tokio_quiche;` and `pub use tokio_quiche::quiche;`. Commit
  `Cargo.lock`.
- A **spike harness** — an ignored-by-default integration test module or a
  `spikes/` example binary that exercises the pinned APIs and prints observed
  behavior. It must answer, with a loopback QUIC connection + packet capture
  where noted:
  - **T1b** — a successful last-handle `qconn.close` is serialized in the ordinary
    post-`process_writes` flush; peer promptly receives the selected
    `CONNECTION_CLOSE` (incl. saturated-write / pending-explicit-close case).
  - **T2 / T2a** — `start` (sync, server) vs `connect_with_config` (async, client)
    handshake timing; `QuicConnection` handle-drop does not tear down the worker;
    pre-handshake `on_conn_close` gating.
  - **T4** — enumerate `QuicConnectionStream` item-error variants; whether
    `next()` keeps yielding after `Some(Err)`; whether a fatal socket condition is
    a distinct signal or just `None`.
  - **Q1** — `stream_readable_next`/`stream_writable_next`/`stream_priority`
    behavior; **confirm the documented destructive cursor dearm** (both `*_next`
    dearm until re-armed) and **exactly-one-credit** consumption by
    `stream_priority` (§5.5 "third spike", design §5.5) — these gate Phase 3's
    destructive readable intake and Phase 6 materialization, not mere API existence.
  - **Q2** — `Connection::close` result semantics (first/repeat/peer-race/invalid).
  - **Q3** — `stream_shutdown(Write, code)` under zero send capacity (accept +
    flush RESET_STREAM without credit; duplicate; race with `StreamStopped`).
  - **Q4** — registered-stream STOP_SENDING visibility via `stream_writable_next`
    → `stream_capacity` == `Error::StreamStopped(code)` across the listed cases.
  - **Q5** — `stream_send(id, &[], fin=true)` acceptance/flush at zero capacity.
  - **§5.5 BLOCKER** — zero-send-capacity writable-only peer bidi discovery
    (choose spike outcome 1/2, or narrow the claim per outcome 3).
  - **§5.5 admission-tombstone** — a fully-terminal id can never reappear in
    `readable()`/`writable()` discovery (committed contract A premise).
- **Record outcomes** in `docs/design/quiche-h3-bridge.md` §14 table + §5.5, and
  flip the status banner from BLOCKED once all gates are closed.
- Loopback TLS cert fixtures (self-signed via `rcgen`) for later phases.

**Success criteria:** crate compiles and links (BoringSSL builds); every gating
spike has a recorded outcome in the design doc; `cargo build` and
`cargo test -- --ignored` (spikes) run clean.

**Outcome branching (not all failures are equal):**
- An outcome that merely differs in detail from the design → **revise the design
  doc §14/§5.5 before Phase 2** (in-repo edit).
- An outcome that requires an *upstream* API to exist — the §5.5 BLOCKER outcome 2
  (a writable-discovery credit API) or a disproved admission-tombstone premise
  needing an upstream `stream_collected(id)`-style API (design §5.5) — is an
  **external feasibility/scope decision, not an in-repo edit**, and can block all
  of Phase 3. If the needed upstream API is unavailable on the pinned build,
  **escalate and adopt §5.5 outcome 3 (explicitly narrow the drop-in correctness
  claim)** rather than entering Phase 3 assuming full coverage.

---

## Phase 1 — Primitives: buffers, reason types, error mapping  *(no external gate)*

Design: §8 (all), §5 `TerminalCell`, §10 buffer sizing (`PKT_BUF_LEN`, `MAX_CHUNK`).

Deliverables (`buffer.rs`, plus a small internal `error`/`reason` module):
- `WriteBuf<Bytes>` cursor + the outbound `pkt_buf` / recv `stream_recv` scratch
  split (§5, T3).
- `TerminalCell` (Mutex + `futures::task::AtomicWaker`) with the race-free
  check/register/recheck ordering (§5, §5.4 invariant 2).
- Crate-private internal reason types (§8.2) and the two mapping functions:
  quiche → internal reason (§8.3) and internal reason → `h3::quic` error (§8.4).

**Success criteria:** unit tests for error mapping (one value per mapped `h3`
variant), `WriteBuf` partial-consume, and `TerminalCell` set-vs-register race
(§11 unit + `TerminalCell` race test).

---

## Phase 2 — Back-end skeleton: `QuicheDriver` worker loop  *(depends: 0,1)*

Design: §5 intro, §2.3, §5.4 (esp. **invariant 4** — the worker never `await`s
inside `process_reads`/`process_writes`; synchronous `try_send`/`try_reserve`,
which is the load-bearing rationale for "No `tokio-util`"). Design refs T1/T3
(verified); **gating spike T1b** must be recorded before this phase is trusted.

Deliverables (`driver.rs`): `impl ApplicationOverQuic for QuicheDriver` with the
structural callbacks wired but stages stubbed:
- `on_conn_established`, `should_act` (true once established), `buffer` (returns
  `pkt_buf`), `wait_for_data` pending-work fast path (finding 2),
  `process_reads`/`process_writes` skeletons honoring the §2.3 invocation contract
  (per-iteration receive quotas; writes every acting iteration).
- Command channel plumbing: unbounded control `mpsc` + weak worker sender; bounded
  byte/accept channels created but not yet driven.

**Success criteria:** driver compiles against pinned `ApplicationOverQuic`; a
loopback handshake reaches `on_conn_established`; `wait_for_data` fast path and
budget-reset points unit-tested (§11 "worker loop" cases).

---

> **Verification strategy for the back-end core (Phases 3–5).** The §11 scenarios
> these phases implement are ultimately *driven* through front-end APIs
> (`poll_data`, `poll_ready`, `poll_finish`, `poll_accept_*`) that first exist in
> Phase 6. Therefore each of Phases 3–5 closes on **unit tests against a mock/harness
> front end** (design §11 "mock worker") that pokes the worker's channels directly;
> the **full front-end-driven loopback versions** of the same scenarios are part of
> **Phase 6/8 acceptance**, not the closing criteria of 3–5.

## Phase 3 — Read pump, admission, peer-stream discovery  *(depends: 2)*

Design: §5.1, §5.5, §5.4 invariants 1,5,6,7,9; spikes Q1/Q4 and §5.5 spikes.

Deliverables: the shared read pump (bounded byte channels, **reserve-before-recv**,
chunk/readable/admit/discovery budgets), the sealing-edge terminal publication +
one-shot consumer recheck, destructive readable intake, `pending_admit` /
tombstone admission state machine, writable-path peer-bidi admission.

**Success criteria (against the mock front end + unit tests; full loopback in
Phase 6/8):** the §11 read/discovery matrix — sealing-edge interleaving (byte
inserted between first `poll_recv` and terminal), budget boundaries
(`READABLE_/ADMIT_/DISCOVERY_/RECV_RESUME_BUDGET`), tombstone non-reappearance,
parked-stream single-admit.

---

## Phase 4 — Send state machine + writable re-arm  *(depends: 3)*

Design: §5.3, §5.3a, §5.4 invariants 3,11,12,13; spikes Q3/Q5.

Deliverables: per-stream `StreamSendState` (ordered `Write`/`Finish` queue),
completion oneshots fired exactly once at the transport-acceptance boundary
(`Ok`/`Stopped`/`Conn`), `Reset` preemption of unaccepted remainder, round-robin
`runnable_send` scheduling, low-water writable re-arm, STOP_SENDING resolving
accepted commands before runnable cleanup.

**Success criteria (against the mock front end + unit tests; full loopback in
Phase 6/8):** the §11 send matrix — partial-write-then-close/capacity, reset
preemption of an in-flight write, STOP_SENDING-after-intake-before-stage-(e),
round-robin fairness, per-`Finish` idempotent completion, zero-capacity FIN (Q5
regression) and zero-capacity RESET (Q3 regression).

---

## Phase 5 — Close, teardown, finite close-admission cut  *(depends: 2,4)*

Design: §5.2, §5.4 invariants 10,14; §9. Gating spikes **T1b/Q2** (T2a is
verified from published source, not a recordable gate).

Deliverables: the close-admission gate (`shared.conn_terminal` + `cmd_rx.close()`
at the terminal edge = finite cut), the 4-step `on_conn_close` protocol, per-command
rejection behavior, graceful `H3_NO_ERROR` last-handle teardown staged inside
`process_writes`, explicit-close barrier precedence, `ConnectionDropped` cleanup.

**Success criteria (against the mock front end + unit tests; full loopback in
Phase 6/8):** the §11 teardown matrix — submit-across-the-gate race resolves
with the classified terminal (never a bare cancel), last-handle teardown observed
by peer as `H3_NO_ERROR`, explicit local close precedence over graceful,
`close`-staged-behind-saturated-batch, peer-close-vs-teardown race.

---

## Phase 6 — Front end: streams, connection, opener  *(depends: 3,4,5)*

Design: §6, §6.1, §6.2, §4 (trait mapping); §5.4 **invariant 8** (every
materialized stream is eventually reclaimed — the drop-cleanup obligation).

Deliverables (`stream.rs` + `lib.rs`): `H3SendStream` (single-slot send contract,
`poll_ready`/`poll_finish`/`reset`/`send_id`), `H3RecvStream` (`poll_data` with the
post-terminal recheck, `stop_sending`, `recv_id`), `H3Stream`+`split`, `Connection`
(bounded accept receivers, `poll_accept_bidi`/`poll_accept_recv` with recheck),
`StreamOpener` (worker-owned id allocation, `poll_open_bidi`/`poll_open_send`,
`close`), and all `h3::quic` trait impls. Drop cleanup for both halves (§6.2).

**Success criteria:** all `h3::quic` traits implemented (compile-time trait
assertions); §11 front-end matrix — split-drop-one-half, drop-half-before-packet,
open-reply cancellation at each materialization boundary, no-runtime-drop lifecycle
test.

---

## Phase 7 — Wiring: acceptor + connector + public config  *(depends: 6)*

Design: §7.1, §7.2, §7 construction contract (S1), §7.1 item-error classification
(M2); spikes T2/T4.

Deliverables (`listener.rs`, `connector.rs`): `H3QuicheServerConfig` +
`H3QuicheAcceptor::bind(...) -> Result<Vec<Self>, Error>` (one acceptor per socket),
accept loop over `QuicConnectionStream` with `FuturesUnordered` concurrency cap and
**continue-on-item-error** (log/metric; `accept()` `Err` reserved for listener-fatal),
`H3QuicheClientConfig` + `H3QuicheConnector::new(uri, server_name, config)`,
compile-time `H3Acceptor`/`H3Connector` assertions, validation-at-`bind`/`new`.

**Success criteria:** §11 wiring matrix — recoverable item error keeps acceptor
serving, handshake flood bounded by the cap, slow handshake doesn't block accept,
client/server handshake-failure mapping (raw client err / typed-but-unclassified
server), public-construction compile/conformance test.

---

## Phase 8 — Test suite hardening + CI compatibility  *(depends: 7)*

Design: §11 (full), §10 CI compatibility test, §14 H1.

Deliverables: complete the §11 unit + loopback integration matrix; the CI
compatibility test that constructs one value of every mapped `h3` error variant
and calls each load-bearing `quiche`/`tokio-quiche` API (T1–T3, Q1, H1); the T1b
close-flush loopback observation; regression tests guarding each recorded spike
outcome (Q3, Q4, Q5, tombstone).

**Success criteria:** `cargo test` (all, including loopback) green; CI
compatibility test fails loudly on a reshaped upstream API; coverage present for
every §11 scenario bullet.

---

## Phase 9 — Cross-repo integration  *(OUT OF SCOPE for this repo)*

> **Scope decision:** this repo builds only the `quiche-h3` crate. The `h3-util`
> and `tonic-h3` integration below is documented for completeness but is **not**
> part of this repo's work; it happens in those separate repos once `quiche-h3`
> is published/available.

Design: §10 `h3-util` integration, §11 `tonic-h3` end-to-end, §12 follow-ups.

Deliverables (only when `h3-util` / `tonic-h3` are available): replace the
`h3-util/src/quiche_h3/mod.rs` stub with `client.rs`/`server.rs` behind the
`quiche` feature (manifest change: `quiche` feature enables `quiche-h3`, which
re-exports `tokio_quiche`); wire `H3QuicheConnector`/`H3QuicheAcceptor` into the
`tonic-h3` e2e suite.

**Success criteria:** `h3-util` builds against `quiche-h3`; `tonic-h3` e2e passes.

---

## Dependencies / sequencing

```
       ┌─ 1 ─────────────┐            (1 branches off the skeleton, not the gate)
0 (gate) ┤                 ├→ 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 (out of scope)
       └─ workspace ──────┘          └────┴────┘  back-end core: 4,5 depend on 3
```

The workspace/crate **skeleton** is created first inside Phase 0; **Phase 1** (pure
logic) depends only on that skeleton and runs in parallel with the Phase 0 spike
harness. Everything from **Phase 2** onward is gated on recorded spike outcomes.
Phases 3–5 (back-end core) are *implemented* in order (4,5 depend on 3) but their
full front-end-driven acceptance is completed in Phase 6/8 (see the verification
note above Phase 3).

## Open items to confirm before Phase 0

1. **~~`h3-util` / `tonic-h3` scope~~ — SETTLED:** out of scope for this repo; this
   repo builds only the `quiche-h3` crate (Phase 9 is documentation-only).
2. **Spike environment**: loopback UDP + a packet-capture path (or an in-process
   observation shim) for T1b/Q3 — confirm we can capture emitted frames, else use
   peer-side observation of `CONNECTION_CLOSE`/`RESET_STREAM`.
3. **Concrete numeric knobs** are left to implementation/benchmarking per §12 (S3,
   C1); each has a recording home: **Phase 2** — bounded byte/accept channel sizing
   + per-iteration receive quotas; **Phase 3/4** — pump/send budgets
   (`READABLE_/ADMIT_/DISCOVERY_/RECV_RESUME_/WRITABLE_BUDGET`, `MAX_CHUNK`,
   `CMD_BUDGET`); **Phase 7** — handshake concurrency cap. Pick provisional values
   and record them where they first appear.
