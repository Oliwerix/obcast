# OBCast

Resilient HLS audio streaming for radio outside broadcast. See `docs/protocol.md`
for the control plane and the encoder↔server feedback loop, and
`crates/obcast-proto/src/scheduler.rs` for the closed-loop upload scheduler.

## Workspace
- `obcast-proto` — shared wire types + upload scheduler (built + unit-tested).
- `obcast-server` — ingest + DVR + HLS origin + playout + control API (skeleton).
- `obcast-client` — encoder: capture + ABR encode + adaptive uploader (skeleton).

## Dev
```
cargo test           # runs the scheduler test suite
cargo build
```
