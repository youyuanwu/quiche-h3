//! Buffers and shared primitives (design §5, §8, §10).
//!
//! Home of `WriteBuf`, the outbound `pkt_buf` / recv scratch split, the
//! `TerminalCell` out-of-band terminal primitive, and the internal reason
//! types + error mapping. Implemented in Phase 1.
#![allow(dead_code)]
