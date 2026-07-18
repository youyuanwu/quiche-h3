# quiche-h3

An [`h3::quic`](https://docs.rs/h3) transport-adapter bridge that runs hyperium
[`h3`](https://github.com/hyperium/h3) (HTTP/3) over Cloudflare
[`quiche`](https://github.com/cloudflare/quiche), driven asynchronously by
[`tokio-quiche`](https://github.com/cloudflare/quiche/tree/master/tokio-quiche).

It exposes a standalone acceptor/connector whose `accept()` / `connect()` yield a
`Connection` that implements `h3::quic::Connection<Bytes>`, so it plugs directly
into `h3::client` / `h3::server`.

## Design

The full design lives in
[`docs/design/quiche-h3-bridge.md`](https://github.com/youyuanwu/quiche-h3/blob/main/docs/design/quiche-h3-bridge.md). In short:

- A single-task **`QuicheDriver`** worker (`tokio_quiche::ApplicationOverQuic`) is
  the sole toucher of `quiche::Connection`. It owns all cross-task state and never
  `await`s inside its synchronous read/write callbacks (`try_send`/`try_reserve`).
- The **front end** implements the `h3::quic` traits over bounded byte/accept
  channels, an unbounded control channel, and race-free sticky **terminal cells**,
  so a full data queue can never hide a stream/connection terminal, and every
  terminal reason is typed data carried over the channel — channel closure is
  never itself a semantic signal.
- Error mapping is designed from quiche's synchronous error surface (not inherited
  from a lossy adapter): peer `RESET_STREAM` / `STOP_SENDING` codes, timeouts,
  and local vs. peer connection closes are all distinguished.

## Pinned build

Depends on semver ranges (`tokio-quiche 0.19`, which pulls `quiche 0.29`; `h3 0.0.8`)
and pins exact builds via the committed `Cargo.lock`. A CI compatibility test
(`tests/ci_compat.rs`) constructs one value of every mapped `h3` error variant and
names every load-bearing `quiche`/`tokio-quiche` API, so a minor upstream bump that
reshapes the surface fails the build rather than silently mismapping.

## Usage

Server:

```rust,no_run
use quiche_h3::{H3QuicheAcceptor, H3QuicheServerConfig};

# async fn run() -> Result<(), quiche_h3::Error> {
let config = H3QuicheServerConfig {
    cert_path: "cert.pem".into(),
    key_path: "key.pem".into(),
    ..Default::default()
};
let socket = tokio::net::UdpSocket::bind("0.0.0.0:4433").await?;
let mut acceptor = H3QuicheAcceptor::bind([socket], &config)?.pop().unwrap();

while let Some(conn) = acceptor.accept().await? {
    tokio::spawn(async move {
        let mut h3 = h3::server::Connection::new(conn).await.unwrap();
        // ... accept and serve requests over `h3` ...
    });
}
# Ok(()) }
```

Client:

```rust,no_run
use quiche_h3::{H3QuicheConnector, H3QuicheClientConfig};

# async fn run(addr: std::net::SocketAddr) -> Result<(), quiche_h3::Error> {
let config = H3QuicheClientConfig::default();
let connector = H3QuicheConnector::new(addr, "example.com".into(), config)?;
let conn = connector.connect().await?;
let (mut driver, mut send_request) = h3::client::new(conn).await.unwrap();
// ... drive `driver` and issue requests via `send_request` ...
# Ok(()) }
```

`tokio_quiche` and `quiche` are re-exported (`quiche_h3::tokio_quiche`,
`quiche_h3::quiche`) so downstreams can build `QuicSettings`, TLS material, and
`Hooks` without a separate dependency.

## Known limitation (§5.5)

At **zero connection-level send capacity**, a peer that opens a *writable-only*
bidi stream (e.g. via `STOP_SENDING`) is undiscoverable through any public
`quiche 0.29` API — quiche's `tx_cap == 0` guard precedes the stopped-stream
branch. This pathological case is an explicitly documented gap in the otherwise
drop-in adapter contract, pending an upstream stream-enumeration API.

## Features

- `tracing` — structured instrumentation.
- `gcongestion` / `rpk` — pass-through toggles for `tokio-quiche`.

## Testing

```sh
cargo test -p quiche-h3                       # unit + CI compatibility tests
cargo test -p quiche-h3 -- --include-ignored  # + loopback/e2e integration tests
```

The ignored suite includes a real end-to-end HTTP/3 GET round-trip
(`tests/h3_e2e.rs`) through `h3::client` ↔ `h3::server` over the bridge.

## License

MIT.
