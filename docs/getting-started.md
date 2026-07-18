# Getting started

This walks through running the server and encoder client locally, and driving
playout. For the wire protocol (every route, header, and message shape), see
[`protocol.md`](protocol.md).

## Prerequisites

- Rust (`cargo build`/`cargo test` need to work — see the repo root `Cargo.toml`)
- `ffmpeg` on `PATH` — used for both encoding (client) and decoding (server
  playout); there's no way around this dependency, it's load-bearing
- An audio output device for playout (optional — the server logs an error and
  disables playout if none is found, everything else still works)

## Run the server

```
cargo run -p obcast-server
```

Config is via environment variables, all optional:

| Variable                | Default        | Meaning                                   |
|--------------------------|----------------|--------------------------------------------|
| `OBCAST_DATA_DIR`        | `./data`       | where segments and DVR state are stored     |
| `OBCAST_LISTEN_ADDR`     | `0.0.0.0:8080` | HTTP listen address                         |
| `OBCAST_INGEST_TOKEN`    | unset          | if set, `POST /ingest/*` requires `X-Auth`  |
| `OBCAST_WEB_REMOTE_DIR`  | `web/remote`   | static files served at `/remote`            |

Each `{stream}` name is created lazily on first contact (first ingest, first
status request, etc.) — there's no separate "create a stream" step.

## Run the encoder client

```
cargo run -p obcast-client -- --server http://127.0.0.1:8080 --stream myshow
```

Useful flags:

```
--device <pulse-source-name>   # capture from a real mic instead of the test tone
--segment-ms 2000               # segment length; must match across restarts of a stream
--ingest-token <token>          # required if the server has OBCAST_INGEST_TOKEN set
--out-dir ./client-buffer        # local disk ring buffer
```

Find a real capture device with `pactl list sources short`. Without `--device`,
the client synthesizes a 440Hz test tone (paced in real time), which is enough
to exercise the whole pipeline without any audio hardware.

## Listen

```
http://127.0.0.1:8080/hls/myshow/master.m3u8
```

Any HLS-capable player works (`ffplay`, Safari, `hls.js`). This is independent
listener playback — it has nothing to do with the server's hardware output
below.

## Control the server's hardware output

This is the real playhead: its position feeds back into `ServerState` and
reshapes what the encoder uploads next (see `protocol.md` §5). Two ways to
drive it:

**Web remote** — open `http://127.0.0.1:8080/remote/?stream=myshow` for
start/stop/seek buttons, live health, VU meters, and a waveform (BBC
peaks.js) you can click or drag to seek — colored by ABR rung, so you can
see where quality is low at a glance.

**REST**, for scripting:

```
# start playback at the live edge
curl -X POST http://127.0.0.1:8080/api/myshow/playout \
  -H 'Content-Type: application/json' \
  -d '{"cmd":"start","position":{"kind":"live"}}'

# seek to 30s behind live
curl -X POST http://127.0.0.1:8080/api/myshow/playout \
  -H 'Content-Type: application/json' \
  -d '{"cmd":"seek","position":{"kind":"seconds_behind_live","value":30}}'

# check status
curl http://127.0.0.1:8080/api/myshow/status | python3 -m json.tool
```

## Simulating a bad link

To see the scheduler actually defend against dropout, throttle the client's
upload path (e.g. `tc` / a proxy that rate-limits) and watch
`crates/obcast-client` logs: uploads shift to `reason=Continuity` at the low
rung near the playout head, and `reason=Upgrade` only resumes once
`lead_ms` climbs back above the `high_ms` water level. `docs/protocol.md` §5–6
explains why.

## Troubleshooting

- **No sound from playout, no errors**: check the server logs for "no default
  audio output device" — playout silently no-ops without one.
- **`ffmpeg: command not found`**: both crates shell out to it; it must be on
  the server's and the client's `PATH`.
- **Web remote shows "link: down"**: the server marks a stream's link down
  after 5s without a successful ingest — check the client is actually running
  and pointed at the right `--stream` name.
