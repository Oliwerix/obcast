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
  X-Rendition: <rung id>          # lowest currently-enabled rung = survival rung
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
```
POST /ingest/{stream}/heartbeat    Body: <EncoderState JSON>
Header: X-Auth: <ingest token>   (only if OBCAST_INGEST_TOKEN is set)
```
Sent by the encoder client once a second regardless of whether an upload
happened that tick, so telemetry (current rung, throughput, backlog, locally
abandoned seqs) stays fresh even while idling in survival mode with nothing
new to send. The server stores the latest snapshot per stream and surfaces it
as `ControlStatus.encoder` and real `LinkHealth.throughput_kbps` (§4) — purely
additive dashboard data. It never feeds back into `plan_uploads`; the
upload-scheduling loop (§1) runs entirely off `ServerState`, piggybacked on
uploads and the SSE feed above.

`EncoderState.auto_start_buffer_ms` is the one field that *does* feed back
into server behaviour, outside `plan_uploads` — see "Auto-start" below.

### Auto-start
An encoder can ask the server to start playout on its own once enough buffer
has accumulated, instead of waiting for a web operator to press Start —
useful for an unattended/scripted OB where nobody is watching the web remote
at the moment the link comes up. Set `EncoderState.auto_start_buffer_ms`
(sent on every heartbeat, `None` to disable) to the desired buffer in ms —
e.g. `300_000` for "start 5 minutes behind live." The server compares this
against `ServerState.buffered_ms` (contiguous ms of DVR history held from
`dvr_start_seq` forward, independent of the playout anchor — unlike
`lead_ms`, which is 0 while stopped) after every ingest and every heartbeat;
once it's met, playout starts at `dvr_start_seq`, giving an initial `lead_ms`
of roughly the requested buffer. From that point on `lead_ms` *is* the
"buffer remaining" — it shrinks if the encoder stops sending (dead link) and
the head keeps consuming it, which is exactly the signal an operator wants
to see.

This only fires while `playout.state` is `stopped`: a manual start (via
`POST /api/{stream}/playout`) always takes precedence, and once playout has
started by any means the requested buffer is moot. The server also
remembers which exact `auto_start_buffer_ms` value it has already used to
auto-start once, so a later manual stop doesn't cause a surprise second
auto-start for the same standing request; requesting a *different* value
re-arms it.

**Caveat:** while stopped, `buffered_ms` can never exceed the server's DVR
window (`dvr_window_ms`, 5 minutes by default and not currently exposed to
or configurable by the client) — eviction keeps trimming `dvr_start_seq`
forward to hold that window, so a requested buffer larger than it will never
be satisfied and auto-start will simply never fire. Keep the requested
buffer comfortably under the server's DVR window.

### Playout states
`playout.state` (`PlayoutState`) is one of `stopped` / `playing` / `paused` /
`stalled` / `error`:
- `stopped` — nobody has asked it to play.
- `playing` — rendering real audio.
- `paused` — head held in place on request.
- `stalled` — running and not paused, but the hardware output isn't actually
  producing audible audio right now (buffer underrun, or the stall-skip
  backstop in `playout.rs` is bridging a missing segment). Anchors the
  scheduler the same as `playing`/`paused` — the head is still real, just not
  audible this instant.
- `error` — the playout engine itself is broken (e.g. the configured
  hardware output device failed to open, or a runtime stream error) and
  cannot produce audio until the server is reconfigured/restarted. Anchors
  like `stopped` (no head to defend).

`playout.detail` (`Option<String>`) is a human-readable reason, populated for
`error` (why the device/stream failed) and best-effort for `stalled` (which
segment it's waiting on / what happened to it); `None` otherwise. This is
what lets the web remote answer "stalled — why?" / "errored — why?" instead
of just showing a color.

`playout.test_tone` (`bool`, `#[serde(default)]`) reflects whether the 1kHz
test-tone pattern (`set_test_tone`, above) is currently overriding the
hardware output. It's orthogonal to `playout.state` — a pure
wiring/routing check, never part of scheduler input.

`playout.playing_rung` (`Option<RungId>`, `#[serde(default)]`) is the rung
actually reaching the speaker for `playout.position_seq` right now — ground
truth, set once when the playout engine feeds that segment to its decoder
and never revised. Deliberately *not* derivable from `coverage`: the engine
feeds segments many seconds ahead of real-time output, so a quality upgrade
for an already-fed segment can land on disk (and thus into `coverage`)
before that segment is actually heard. Both the web remote's "Current rung"
and the client GUI's on-air quality readout read this field directly rather
than looking up "best rung for this seq" against `coverage`, which would
otherwise report a rung the speaker isn't actually playing yet.

`playout.fed_seq` (`Option<Seq>`, `#[serde(default)]`) is the highest seq the
engine has already fed to its decoder — the same feed-ahead depth that makes
`playing_rung` necessary, exposed as a boundary rather than a per-segment
readout. `None` while stopped or just started/sought. The scheduler's Tier C
(quality upgrades, `scheduler.rs`) uses this — not `position_seq` — as the
"don't bother upgrading at or behind here" line: an upgrade upload for a seq
already fed can never change what's heard, since the engine locked in
whatever rung was on disk the moment it fed that seq. Gating on
`position_seq` alone let Tier C keep spending its whole per-tick budget on
segments that were, in practice, already fed by the time the upload landed —
the engine's feed-ahead depth (`RING_SEGMENTS` in `playout.rs`) is comparable
to or larger than Tier C's own look-ahead window under the default water
levels, so in steady state almost none of those uploads could ever actually
be heard. Falls back to `position_seq` when absent (older server, or
stopped), preserving the previous ahead-of-head-only behavior.

`playout.position_ms_into_segment` (`u32`, `#[serde(default)]`) is how far
into the segment at `position_seq` playout has drained, against that
segment's *nominal* `segment_ms` — best-effort, not sample-exact (see
`PlayoutHandle::ms_into_current_segment` in `playout.rs`). `0` while stopped
or nothing has been fed at the current position yet. This is what lets a
client compute a continuous elapsed-time readout and a smoothly-moving
playhead instead of one that jumps in whole-`segment_ms` steps — the web
remote (`stream.html`) interpolates between updates using this field plus a
local wall clock (see "Web remote" below) rather than only advancing on each
whole-segment change. It's also where a sub-segment seek
(`MillisBehindLive`, below) shows up: the skipped intra-segment offset lands
here immediately rather than the seek looking like it snapped back to the
segment's start.

### `ServerState` fields that drive the scheduler
- `playout.state` + `playout.position_seq` — the anchor for all urgency.
  `position_seq` tracks the seq whose audio is actually draining out of the
  server's hardware output right now, not merely the newest segment queued
  into the playout ring buffer — see `playout.rs`'s `pending`/
  `drain_pending`. (A segment can sit decoded-but-unheard in the ring for up
  to a few seconds; reporting the queue-time position instead of the
  drain-time one used to make the head, and everything derived from it, read
  ahead of what a listener could actually hear.)
- `playout.fed_seq` — highest seq already fed to the decoder; Tier C
  (upgrades) never targets a seq at or behind this, since an upgrade upload
  for it could no longer change what's heard.
- `frontier_seq` — highest seq contiguously playable from the anchor.
- `lead_ms` — ms of contiguous audio ahead of the head (drain indicator).
- `buffered_ms` — ms of contiguous DVR history from `dvr_start_seq` forward,
  regardless of playout state; drives auto-start (see above) and doubles as
  a general "how deep is our buffer" readout while stopped, when `lead_ms`
  alone reads 0.
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
GET /api/{stream}/status  ->  ControlStatus { stream, server, encoder?, link, recent_log }
```
`status`, `waveform`, and `ws` are read-only: unlike ingest (which starts a
new stream on first upload) they never spin up a stream that's never been
ingested into. A name with no in-memory handle and no on-disk show directory
returns `404`, rather than silently creating an empty one — see CLAUDE.md §8
"per-stream resource leak".

`recent_log` is a capped (200 entries), oldest-first backlog of `LogEntry {
at_ms, level, message }` — warn/error-level server status messages (segment
abandons, DVR reap failures, waveform decode failures, playout stalls/device
errors), persisted per-stream so a freshly-opened web remote sees recent
history rather than only what happens to arrive after it connects.
`#[serde(default)]` on the server type, so an older client/server pair
without this field still interoperates.

### Playout commands
```
POST /api/{stream}/playout   Body: <PlayoutCommand JSON>
Header: X-Auth: <control token>   (only if OBCAST_CONTROL_TOKEN is set on the server)
```
Deliberately a separate credential from ingest's `X-Auth` token (§2) — an OB
site's upload credential shouldn't also be able to stop/seek/set-volume the
server's hardware output. Unset `OBCAST_CONTROL_TOKEN` (the default) disables
this check, same "no token configured = auth disabled" semantics as ingest.
A missing/incorrect header is rejected with `401 Unauthorized`.

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
| `set_test_tone` | `{ enabled }`                 | toggle the 1kHz test-tone pattern   |

`set_test_tone` overrides the hardware output with a 1kHz sine test tone —
2s both channels, 0.5s silence, 0.5s left, 0.5s silence, 0.5s right, 0.5s
silence, looping — for checking output wiring/routing independent of the
encoder link. It does not change `start`/`stop`/`seek` state; `PlayoutStatus`
carries the current state as `test_tone: bool`, separate from `state`, so a
stream can be `stopped` (or `playing`) with the tone on or off.

`position` is a `PlayoutPosition`: `{"kind":"live"}`,
`{"kind":"seq","value":123}`, `{"kind":"seconds_behind_live","value":30}`, or
`{"kind":"millis_behind_live","value":30500}`. A position outside the DVR
window is clamped to `[dvr_start_seq, live_seq]`.

`millis_behind_live` is for the web remote's waveform click-to-seek, where
rounding every click to the nearest whole segment made a click land up to
`segment_ms` away from where the user actually clicked. Its reference point
is deliberately *not* the same as `seconds_behind_live`'s: `seconds_behind_live`
counts whole segments back from the *start* of the live segment (so
`{"seconds_behind_live":0}` always lands at the beginning of whatever segment
is newest, however little of it has actually arrived), while
`millis_behind_live`'s `0` is the *end* of the newest available segment — true
"now," matching where the far right edge of the web remote's waveform
actually sits. `0` therefore still lands on the live segment either way, but
the two scales otherwise disagree by up to one `segment_ms` for the same
numeric value (e.g. `{"millis_behind_live":2000}` lands at the *start* of the
live segment, not one segment further back the way
`{"seconds_behind_live":2}` does) — pick whichever reference point matches
what's being converted from. The server resolves the value to a target
segment plus an intra-segment offset (`api.rs::resolve_millis_behind_live`)
that the playout engine skips past before that segment's audio starts
draining (`playout.rs`'s decoder-session skip) —
`playout.position_ms_into_segment` (above) reflects the skip immediately once
it takes effect.

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
head moves, `meters` (`{vu_db_l, vu_db_r, ppm_db_l, ppm_db_r, peak_db_l, peak_db_r}`,
dBFS, on a fixed ~50ms tick), and `log` (a `LogEntry` — `{type:"log", at_ms, level, message}`,
tag flattened alongside the fields same as `status`) each time a new
warn/error status message is recorded, pushed live as it happens rather than
waiting for the next `status` snapshot. `ack` is defined in the schema for
future use but not yet sent — commands go through the plain HTTP response of
`POST /api/{stream}/playout` instead. The socket does not accept inbound
commands; it is read-only from the client's perspective.

The web remote (`stream.html`) seeds its log panel once from the first
`status` event's `recent_log`, then appends each subsequent `log` event live,
capping its own displayed list to the same 200-entry backlog size the server
retains.

`meters` carries three independently-computed ballistics per channel (L/R),
all fed from the actual post-gain playout audio callback
(`obcast_proto::meter`), meant to be superimposed on one meter widget per
channel: `vu_db_{l,r}` is IEC 60268-17 "standard volume indicator" (VU) —
symmetric attack/release, 300 ms integration time, the slow "loudness" bar,
displayed with its 0 reference at -18 dBFS; `ppm_db_{l,r}` is IEC 60268-10
Type I (DIN) peak programme meter — fast attack (5 ms burst reads 2 dB down,
per the standard's integration-time definition), slow decay (-20 dB in 1.5 s)
— a "flying PPM" peak needle that stays visible well after a transient has
passed; and `peak_db_{l,r}` is the true digital sample peak (zero attack,
same -20 dB/1.5 s decay as PPM) — an alternate flying-peak reading a client
can switch its meter to show instead of `ppm_db_{l,r}`. This reflects what
the server is *actually playing out* right now; the encoder client computes
its own independent per-channel VU/PPM/Peak triples from the captured
soundcard input for its own meters.

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
  Its time axis is anchored to the waveform JSON's own first `seqs[0]`
  (`serverPlayer`'s `waveformBaseSeq`) rather than `status.dvr_start_seq` —
  those two are refreshed independently (status on every WS push, the
  waveform every 5s plus an immediate client-side trim on eviction, see
  below), so anchoring the playhead to whichever start seq the *currently
  displayed* waveform data actually begins at is what keeps the cursor over
  the right color segment instead of drifting during the gap between
  waveform refreshes. Between authoritative `status` updates the client
  interpolates the playhead forward from a wall clock (reset and corrected on
  every update using `playout.position_ms_into_segment`) and emits it every
  animation frame, which is what drives peaks.js's `autoScroll` smoothly
  instead of only once per `status` push. A DVR eviction
  (`dvr_start_seq` advancing) is applied to the already-loaded waveform
  immediately by slicing off its leading points client-side — no server round
  trip — so an evicted segment's waveform disappears from the overview
  promptly rather than lingering until the next 5s poll.
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
