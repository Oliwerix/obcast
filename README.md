# OBCast

Resilient audio streaming for radio outside broadcast (OB). A flaky uplink from the
broadcast site causes *delay*, not *loss*: audio is chopped into short segments that
upload independently, retry on failure, and buffer on local disk, instead of relying
on one continuous connection like Icecast.

The server continuously tells the encoder where playout is and where its buffer is
thin; the encoder spends its bandwidth on exactly the segments that will actually be
played — low quality first to avoid dropout, higher quality as the link allows.

## Workspace

- **`obcast-proto`** — shared wire types + the closed-loop upload scheduler
  (`plan_uploads`), pure and unit-tested.
- **`obcast-server`** — ingest, DVR buffer, HLS origin, hardware playout (`cpal`),
  and the control API (REST + WebSocket).
- **`obcast-client`** — encoder: `ffmpeg` ABR capture/encode, disk ring buffer, and
  the adaptive uploader that drives the scheduler. CLI only; no GUI yet.
- **`web/remote`** — a small static control page (start/stop/seek the server's
  hardware output, watch health/meters, listen along via `hls.js`).

See **[`docs/getting-started.md`](docs/getting-started.md)** to run it end to end,
and **[`docs/protocol.md`](docs/protocol.md)** for the full wire protocol.

## Quick start

```
cargo run -p obcast-server &
cargo run -p obcast-client -- --server http://127.0.0.1:8080 --stream myshow
```

The client defaults to a synthetic test tone, so this works without a microphone.
Then open `http://127.0.0.1:8080/remote/?stream=myshow` to control playout and
listen, or point a player at `http://127.0.0.1:8080/hls/myshow/master.m3u8`.

## Dev

```
cargo test                                    # scheduler + server unit tests
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt
```
