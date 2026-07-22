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
| `OBCAST_CONTROL_TOKEN`   | unset          | if set, `POST /api/{stream}/playout` requires `X-Auth` (separate credential from ingest's) |
| `OBCAST_WEB_REMOTE_DIR`  | `web/remote`   | static files served at `/remote`            |
| `OBCAST_DVR_WINDOW_MS`   | `300000` (5 min) | how much history the DVR keeps before evicting; `0` disables eviction (unbounded — retains every segment for the life of the stream, unbounded disk use) |

Each `{stream}` name is created lazily on first ingest — there's no separate
"create a stream" step. Read-only routes (`status`/`waveform`/`ws`/HLS
listen) never create a stream themselves; they 404 for a name that's never
been ingested into.

### Picking the playout audio subsystem/device

The rest of the server's config is environment variables, but the hardware
playout device is a TOML file (`OBCAST_CONFIG_FILE`, default
`obcast-server.toml` in the working directory) since it's a per-machine
setting rather than a per-run one:

```toml
# obcast-server.toml
[audio]
host = "PipeWire"      # cpal host / "audio subsystem": e.g. ALSA, JACK,
                       # PulseAudio, PipeWire (Linux/BSD); WASAPI, ASIO
                       # (Windows); CoreAudio, JACK (macOS). Empty =
                       # platform default.
device = ""            # output device name within that host. Empty = its default.
```

The file (and each field) is optional — a missing file, or one that only sets
`host`, just leaves the rest at the platform default.

`ALSA` (Linux/BSD), `WASAPI` (Windows) and `CoreAudio` (macOS) are cpal's
mandatory native hosts and are always available — no cargo feature needed,
they're what you get from a plain `cargo build` on that platform. Everything
else is an opt-in cargo feature (off by default so a plain `cargo build`
never needs extra dev headers or a proprietary SDK):

```
cargo build -p obcast-server --features jack,pulseaudio,pipewire   # Linux/BSD
cargo build -p obcast-server --features jack,asio                  # Windows
cargo build -p obcast-server --features jack                       # macOS
```

**Which extra ones to enable** depends on what's actually running on the OB
machine:

- **PipeWire** (Linux/BSD) is the sound server on most current Linux distros
  (Fedora, current Ubuntu/Debian, Arch). Enable the `pipewire` feature — cpal
  talks to `libpipewire` directly rather than through a compatibility shim,
  and when it's compiled in, cpal's own platform-default host already
  prefers it over PulseAudio/ALSA, so leaving `host` empty does the right
  thing.
- **JACK** (Linux/BSD/macOS/Windows) — enable *in addition* to whatever
  native host is in use if the operator wants JACK's port-graph semantics —
  patching OBCast into other pro-audio JACK clients by name — rather than a
  plain sink/source. On a PipeWire machine this talks to PipeWire's own
  JACK-compatible interface (`pipewire-jack`); on a machine running real
  `jackd` (any of the three platforms) it talks to that instead. It doesn't
  replace the native `pipewire`/`WASAPI`/`CoreAudio` feature — those don't
  expose graph routing, `jack` is the one to reach for when that matters.
- **PulseAudio** (Linux/BSD) is for a machine that's genuinely PulseAudio
  without PipeWire (older distros, minimal installs). It's redundant on a
  modern PipeWire desktop, since `pipewire-pulse` is just a compatibility
  shim in front of the same server the native `pipewire` feature already
  reaches directly.
- **ASIO** (Windows only) is Steinberg's exclusive-mode low-latency backend —
  what most pro-audio interfaces on Windows actually want for playout over
  WASAPI's shared mode. Unlike the other features, building it needs the
  Steinberg ASIO SDK downloaded separately (its license forbids
  redistributing it) with `CPAL_ASIO_DIR` pointed at it — see
  [cpal's README](https://github.com/RustAudio/cpal#asio-on-windows). Skip it
  if WASAPI's latency is already good enough for the interface in use.
- They're independent cpal hosts and can all be compiled in at once — the
  only cost is needing the matching dev headers (`libpipewire-0.3-dev`,
  `libjack-dev`/`libjack-jackd2-dev`, `libpulse-dev`, or the ASIO SDK) at
  build time on that machine, not at runtime. When in doubt, build with
  everything available on the target platform and pick per-machine via
  `obcast-server.toml`'s `host`.

If `host`/`device` don't match anything available at startup, playout logs an
error and disables itself — same as the old "no default output device"
behavior, just resolved against the configured subsystem/device instead of
always the platform default.

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

The GUI (the default, non-`--headless` path) has an **Audio Subsystem**
picker above the device list — pick a cpal host (ALSA/JACK/PulseAudio/
PipeWire/WASAPI/ASIO/CoreAudio, whatever's available) before picking a
device from it. Like the device and channel map, the choice is persisted to
the client's TOML config across restarts. `JACK`/`PulseAudio`/`PipeWire`/
`ASIO` need the same opt-in cargo features as the server, e.g.
`cargo build -p obcast-client --features jack,pulseaudio,pipewire` on
Linux/BSD or `--features jack,asio` on Windows (see the server section above
for which to pick).

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

**Web remote** — open `http://127.0.0.1:8080/remote/` for the shows overview
(every stream the server has received audio for, live or past), or jump
straight to one with `http://127.0.0.1:8080/remote/stream.html?stream=myshow`
for start/stop/seek buttons, live health, VU meters, and a waveform (BBC
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

- **No sound from playout, no errors**: check the server logs for "no matching
  audio output device" — playout silently no-ops without one. If you set
  `[audio]` in `obcast-server.toml`, double check `host`/`device` actually
  match something
  `cargo run -p obcast-server --features jack,pulseaudio,pipewire` (if used)
  has compiled in on this machine.
- **`ffmpeg: command not found`**: both crates shell out to it; it must be on
  the server's and the client's `PATH`.
- **Web remote shows "link: down"**: the server marks a stream's link down
  after 5s without a successful ingest — check the client is actually running
  and pointed at the right `--stream` name.
