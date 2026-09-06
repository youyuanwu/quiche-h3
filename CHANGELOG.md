# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.0.4

### Fixed

- Fixed the concurrent-request tail stall from issue
  [#10](https://github.com/youyuanwu/quiche-h3/issues/10). Final response data
  and its FIN are now coalesced so quiche cannot discard a standalone FIN and
  strand the last in-flight requests.
- Fixed a streaming deadlock where a fully sendable write held for FIN
  coalescing could remain buffered after the command stream became quiescent.
  Quiescent held writes are now flushed without FIN, allowing bidirectional
  streaming to make progress while keeping the stream open.