# OBCast protocol — control plane & encoder↔server feedback loop

The Rust types in `crates/obcast-proto` are the source of truth for every schema
here. This document describes how they move on the wire. JSON is the encoding for
the control and link planes; media segments are raw bytes.

There are three planes:

| Plane   | Who               | Transport                                  |
|---------|-------------------|--------------------------------------------|
| Media   | encoder → server  | HTTP/2 segment uploads; HLS to listeners   |
| Link    | encoder ↔ server  | segment-response piggyback + SSE state feed |
| Control | web ↔ server      | REST + WebSocket                           |

The **link plane is the new heart of the system**: the server continuously tells
the encoder where playout is and where its buffer is thin, so the encoder can aim
bandwidth at the segments that will actually be played.

---

## 1. Why both ends share state

On a flaky uplink with the server draining its buffer, a naive "prefer high
quality" encoder will keep the pipe full of HD segments and starve the one
segment the server is about to play — causing a dropout. OBCast closes the loop:

- The server publishes [`ServerState`](../crates/obcast-proto/src/state.rs):
  playout head, the contiguous playable **frontier**, the **lead** (ms of audio
  ahead of the head), buffer **water levels**, and per-segment **coverage**
  (which rung it holds where).
- The encoder feeds that into [`plan_uploads`](../crates/obcast-proto/src/scheduler.rs),
  which decides, each tick, what to send. Priority is absolute:
  **continuity → live-edge → upgrade**, and upgrades are *only* scheduled ahead
  of the playout head so no bandwidth is wasted on segments already played.

If the feedback goes stale (link down), the encoder falls back to
`ServerState::unknown()` and behaves conservatively (low rung, protect the live
edge).

---

## 2. Media plane (encoder → server)

### Upload a segment
```
POST /ingest/{stream}/segment
Headers:
  X-Auth: <ingest token>
  X-Rendition: <rung id>          # 0 = survival rung
  X-Seq: <u64>                    # canonical clock; idempotent on (rung, seq)
  Content-Type: audio/mp2t
Body: <segment bytes>
```
Idempotent: re-POSTing the same `(rung, seq)` is safe (overwrite / no-op), which
makes retries and reconnect-resume trivial.

**Response body is a `ServerState` JSON** — every successful upload piggybacks
fresh feedback, so when the link works the encoder needs no extra round trips.

### Abandon segments
```
POST /ingest/{stream}/abandon      Body: { "seqs": [u64, ...] }
```
Tells the server to stop waiting on permanent gaps so playout can skip them.

---

## 3. Link plane (server → encoder)

### State feed (survives upload stalls)
```
GET /ingest/{stream}/state         # text/event-stream (SSE)
event: state
data: <ServerState JSON>
```
Pushed on every significant change and at least every ~1 s. Small enough to
survive a thin link even when segment uploads are failing. The encoder always
uses the **highest `rev`** it has seen and ignores older ones.

### Encoder telemetry (for operator dashboards)
Piggybacked on uploads, or `POST /ingest/{stream}/heartbeat` with an
[`EncoderState`](../crates/obcast-proto/src/state.rs) body when uploads are idle.

### `ServerState` fields that drive the scheduler
- `playout.state` + `playout.position_seq` — the anchor for all urgency.
- `frontier_seq` — highest seq contiguously playable from the anchor.
- `lead_ms` — ms of contiguous audio ahead of the head (drain indicator).
- `water` = `{ low_ms, target_ms, high_ms }` — survival / target / upgrade gates.
- `coverage[]` — best rung per seq for a bounded window ahead of the anchor, so
  the encoder sees exactly where HD is missing without guessing.

---

## 4. Control plane (web ↔ server)

Every control-plane route is per-stream, matching the media and link planes.
`set_device` is accepted by the schema but not implemented yet — playout
always uses the host's default audio output; requesting it returns
`501 Not Implemented`.

### Status snapshot
```
GET /api/{stream}/status  ->  ControlStatus { stream, server, encoder?, link }
```

### Playout commands
```
POST /api/{stream}/playout   Body: <PlayoutCommand JSON>
```
`PlayoutCommand` variants (tagged by `"cmd"`):

| cmd          | payload                          | effect                              |
|--------------|----------------------------------|-------------------------------------|
| `start`      | `{ position }`                   | start HW output ("on demand" start) |
| `stop`       | —                                | stop HW output                      |
| `pause`/`resume` | —                            | hold / continue                     |
| `seek`       | `{ position }`                   | jump within the DVR window          |
| `go_live`    | —                                | snap to the live edge               |
| `set_device` | `{ device_id }`                  | not implemented — returns 501       |
| `set_volume` | `{ gain }`                       | linear gain                         |

`position` is a `PlayoutPosition`: `{"kind":"live"}`,
`{"kind":"seq","value":123}`, or `{"kind":"seconds_behind_live","value":30}`.
A position outside the DVR window is clamped to `[dvr_start_seq, live_seq]`.

Because the playout head is part of `ServerState`, any seek immediately reshapes
the encoder's upload plan — e.g. seeking back 30 s makes the encoder protect
continuity around the new (earlier) head and stop upgrading near the old one.

### Live updates
```
WS /api/{stream}/ws  ->  stream of ControlEvent
```
`ControlEvent` is internally tagged by `"type"`; for the `Status` variant the
wrapped `ControlStatus` fields are flattened alongside the tag (not nested
under a `value` key) — e.g. `{"type":"status","stream":"obshow","server":{...},...}`.

Sent: a full `status` on connect and on every `ServerState` change (piggybacking
the same broadcast the SSE link-plane feed uses), `position` when the playout
head moves, and `meters` (`{vu_db, ppm_db}`, dBFS, on a fixed ~200ms tick).
`ack` is defined in the schema for future use but not yet sent — commands go
through the plain HTTP response of `POST /api/{stream}/playout` instead. The
socket does not accept inbound commands; it is read-only from the client's
perspective.

`meters` carries two independently-computed ballistics, both fed from the
actual post-gain playout audio callback (`obcast_proto::meter`), meant to be
superimposed on one meter widget: `vu_db` is IEC 60268-17 "standard volume
indicator" (VU) — symmetric attack/release, 300 ms integration time, the slow
"loudness" bar — and `ppm_db` is IEC 60268-10 Type I (DIN) peak programme
meter — fast attack (5 ms burst reads 2 dB down, per the standard's
integration-time definition), slow decay (-20 dB in 1.5 s) — a "flying PPM"
peak needle that stays visible well after a transient has passed. This
reflects what the server is *actually playing out* right now; the encoder
client computes its own independent VU/PPM pair from the captured soundcard
input for its own meters.

### Waveform
```
GET /api/{stream}/waveform?start_seq={seq}&end_seq={seq}
```
BBC waveform-data.js JSON (`version`/`channels`/`sample_rate`/`samples_per_pixel`/
`bits`/`length`/`data`), consumed directly by BBC's [peaks.js](https://github.com/bbc/peaks.js)
on the web remote. `start_seq`/`end_seq` default to the current DVR window.
Extended with two obcast-specific parallel arrays peaks.js itself ignores:
`rungs` (best rung covering each point, `null` for a gap) and `seqs` (the DVR
seq each point belongs to) — together these let the client color-code the
waveform by quality without a second round trip. Computed by decoding every
segment in range via `ffmpeg` (mono, 8kHz, min/max per 40ms), so a request
over a multi-minute DVR window is not free — the web remote refreshes it on a
timer (every 5s), not per frame.

### Web remote (reference UI)
```
GET /remote/  ->  static single-page app (see web/remote/)
```
Talks to the REST + WS endpoints above, plus the waveform endpoint. It
exposes two independent audio paths and does not conflate them:
- **Server hardware output** — the real playout engine (§5), controlled by
  the buttons on the page and by clicking/dragging the peaks.js waveform.
  This is the position that feeds back into `ServerState` and reshapes the
  encoder's upload plan. Since there's no local audio to attach to,
  peaks.js runs against a custom `player` adapter (see
  [customizing.md](https://github.com/bbc/peaks.js/blob/master/doc/customizing.md))
  whose `seek()` posts to `POST /api/{stream}/playout` and whose notion of
  current time/playing state is a mirror of the WS `ControlEvent` stream —
  there is no real decode or playback happening in the browser for this path.
- **Listen-along preview** — a plain `hls.js` pull of `/hls/{stream}/master.m3u8`
  into a browser `<audio>` element, for checking levels/timing by ear. It has
  its own independent buffering and is not the playhead described above.

### Listener endpoints (HLS)
```
GET /hls/{stream}/master.m3u8
GET /hls/{stream}/{rendition}/index.m3u8     # sliding DVR window
GET /hls/{stream}/{rendition}/{seq}.ts
```

---

## 5. The closed loop, end to end

```
              (SSE state feed  +  piggyback on every upload response)
        ServerState  ─────────────────────────────────────────────►  encoder
        (head, frontier, lead, water, coverage)                         │
                                                                        ▼
                                                            plan_uploads(...)  ── tick
                                                                        │
   segment POSTs (rung, seq), idempotent, retryable   ◄────────────────┘
        │        priority: continuity ▸ live-edge ▸ upgrade(ahead-of-head only)
        ▼
     server ingest ▸ DVR store ▸ { HLS origin → listeners ; playout → HW out }
        ▲
   PlayoutCommand (start/seek/...) from web  ─── reshapes the head, hence the plan
```

**Scheduler tiers** (see `scheduler.rs`, all unit-tested):

1. **Continuity** — from the playout head forward, fill any hole at the **low
   rung** until `target_ms` of contiguous audio is secured. Allowed to burst past
   the tick budget; dropout is the worst outcome. When the buffer is draining on
   a flaky link, this is effectively "low quality first, no dropout."
2. **Live edge** — cover the newest ~`target_ms` at the low rung so the DVR stays
   contiguous and go-live is instant. Skipped in survival mode.
3. **Upgrade** — only when `lead_ms ≥ high_ms` and bandwidth remains, raise
   quality **strictly ahead of the playout head**, nearest-first, one rung step
   per tick. This is "add HD back in as speed recovers," and the ahead-of-head
   guard is what stops us upgrading segments that will never be played.

Continuity cost counts against the tick budget when gating tiers 2–3, so a
starved link spends everything on not-dropping and nothing on new/HD segments.
