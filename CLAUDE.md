# CLAUDE.md — OBCast

Guidance for Claude Code working in this repository. Read this fully before making changes.

---

## 1. What we're building

**OBCast** is a resilient audio streaming system for radio **Outside Broadcast (OB)**, where the
uplink from the broadcast site is unreliable. A continuous push protocol like **Icecast** fails on
flaky networks because one dropped TCP connection kills the stream. OBCast instead uses **segmented
HLS**: audio is chopped into short independent files that upload separately, retry on failure, and
are buffered on local disk — so a network outage causes *delay*, not *loss*.

Three components:

1. **Encoder Client** — a cross-platform GUI app at the OB site. Captures soundcard audio, encodes
   an ABR ladder, buffers segments on disk, and uploads them with a **closed-loop scheduler** that
   defends the server's playout against dropout first and adds quality back as bandwidth recovers.
2. **Server** — ingests segments, keeps a rolling **DVR buffer** (≥5 min), serves HLS to listeners,
   and drives a **hardware audio output** (playout) that operators can start/stop and scrub. It
   continuously publishes its own state back to the encoder.
3. **Web Remote** — browsers that **listen** (with DVR scrubbing) or **control** the server's
   hardware output (start / stop / seek) and watch link health.

### The central idea: both ends share state

The distinguishing feature of OBCast is a **feedback loop** between server and encoder. The server
tells the encoder where playout is, how much contiguous audio sits ahead of that point, and where
the quality holes are. The encoder uses this to aim its scarce bandwidth at exactly the segments
that will actually be played — never wasting it on segments behind the playout head. This is what
makes "prefer high quality, but never drop out" actually work on a bad link. **Do not regress this
loop into a one-way encoder that only measures its own bandwidth.**

---

## 2. Architecture

```
              ServerState feedback (SSE feed + piggybacked on every upload reply)
        ┌───────────────────────────────────────────────────────────────┐
        │  head · frontier · lead · water levels · per-seg coverage      │
        ▼                                                                │
 ┌─────────────────────────┐                            ┌───────────────┴──────────┐        ┌──────────────┐
 │ Encoder Client (GUI)    │                            │      obcast-server       │        │ Web Remote   │
 │  soundcard in           │  segment POSTs (rung,seq)  │  Ingest ─► DVR (≥5 min)  │  HLS   │ Listen +     │
 │  → ffmpeg ABR encode    │  idempotent · retryable    │            │        │    │ ─────► │ DVR scrub    │
 │  → disk ring buffer     │ ─────────────────────────► │            ▼        ▼    │        │              │
 │  → plan_uploads() ──────┤  priority:                 │       HLS origin  Playout│  REST  │ Control      │
 │    continuity ▸ live    │  continuity ▸ live ▸       │                   → HW   │  /WS   │ start/stop/  │
 │    ▸ upgrade(ahead only)│  upgrade                   │                    audio │◄──────►│ seek output  │
 └─────────────────────────┘                            └──────────────────────────┘        └──────────────┘
```

A web `seek`/`start` moves the playout head, which flows back through `ServerState` and immediately
reshapes the encoder's upload plan. Full wire protocol: **`docs/protocol.md`**.

---

## 3. Tech stack — Rust

Chosen for single-binary robustness in the field and one shared type crate across client and server.

| Concern            | Choice                                                             |
|--------------------|-------------------------------------------------------------------|
| Language           | Rust (edition 2021), Cargo workspace                              |
| Shared protocol    | `obcast-proto` crate: `serde` types + the upload scheduler        |
| Async runtime      | `tokio`                                                            |
| Server framework   | `axum` (ingest, HLS origin, control REST, WS) + `tower-http`      |
| Server → encoder feed | SSE over `axum`; also piggybacked on upload responses          |
| Client audio I/O   | `cpal` (capture + playout device access)                          |
| Encode / decode    | `ffmpeg` via subprocess, one process → N rungs. The low rung is HE-AAC via `libfdk_aac` (native `aac` can only emit LC) — auto-detected once at startup; if the local ffmpeg build lacks it, that rung silently falls back to AAC-LC at the same bitrate with a logged warning rather than failing the pipeline. A `libfdk_aac`-enabled ffmpeg is built per-platform in CI (`scripts/build-libfdk-ffmpeg*`, see "packaging" in §8) as a downloadable build artifact; wiring it into the actual release archive so it ships by default is the one remaining piece (also §8). `symphonia` for decode-only paths |
| Client GUI         | `egui`/`eframe` (chosen, done); also a `--headless` CLI path with no GUI |
| DVR store          | Filesystem segments + in-memory index (optionally `rusqlite`)     |
| Web listen / UI    | `hls.js` + `peaks.js`, loaded from CDN by static `web/remote/index.html` (shows overview) + `stream.html` (per-show remote) — no bundler/build step |
| Packaging          | `cargo dist` / static binaries (client), Docker (server)          |

Keep the ingest / control / HLS contracts in `obcast-proto` and `docs/protocol.md` — they are the
language-agnostic seam, so a component could be reimplemented without breaking the others.

---

## 4. Repo layout (Cargo workspace)

```
obcast/
  Cargo.toml                       # workspace
  CLAUDE.md
  README.md
  docs/
    protocol.md                    # control plane + feedback loop (authoritative)
    getting-started.md             # quick-start / run guide
  web/
    remote/index.html              # shows overview (list/delete), served at "/remote/"
    remote/stream.html             # per-show remote (control + listen + waveform), no build step
  crates/
    obcast-proto/                  # state/control types + scheduler + tests
      src/
        lib.rs
        state.rs                   # ServerState, EncoderState, StreamProfile, water levels
        control.rs                 # PlayoutCommand, ControlStatus, ControlEvent, ...
        scheduler.rs               # plan_uploads(): the closed-loop core + tests
    obcast-server/
      src/
        main.rs
        ingest.rs                  # segment upload, abandon, SSE state feed
        store.rs                   # DvrStore: in-mem index, disk bytes, eviction, build_server_state
        origin.rs                  # HLS master + per-rung sliding-window playlists
        playout.rs                 # cpal hardware output: start/stop/seek, ffmpeg decode, meters
        api.rs                     # control REST + WS (status / playout / events)
        waveform.rs                # quality-colored waveform JSON for the web remote
        shows.rs                   # global shows listing (list/delete) for the web UI
        config.rs                  # obcast-server.toml: playout audio subsystem/device
    obcast-client/
      src/
        main.rs
        audio.rs                   # cpal capture, channel-map, gain, level meters
        encode.rs                  # ffmpeg ABR encode (one proc → N rungs)
        inventory.rs               # scans {rung}/{seq}.ts disk ring buffer
        uploader.rs                # tick loop driving plan_uploads; folds ServerState from replies
        sse.rs                     # reconnecting ServerState feed
        shared.rs                  # shared client state/handles
        config.rs                  # persisted operator config (TOML)
        gui/{mod,app,meter}.rs     # egui shell, panels, K-14 meters
```

The layout is flat files per crate (not nested module dirs). On the server, `api.rs` is the control
module and `waveform.rs` backs the web remote's colored waveform. On the client, the on-disk ring
buffer is just the `{rung}/{seq}.ts` file convention read by `inventory.rs` — there is no separate
`buffer` module. The web remote is dependency-free static HTML pulling `hls.js` and `peaks.js` from
CDNs: `web/remote/index.html` is the shows overview served at `/remote/`, linking out to
`web/remote/stream.html?stream={name}` for per-show control/listen/waveform.

---

## 5. Key concepts & invariants

- **Segment is the atom.** Short (default **2 s**) MPEG-TS/AAC segments. Shorter = lower latency and
  finer retry granularity; longer = less overhead. `StreamProfile::segment_ms` is configurable.
- **`Seq` is the canonical clock.** Every segment is `(RungId, Seq)`. Wall-clock is secondary.
- **Ingest is idempotent.** Re-uploading `(rung, seq)` is safe. Retries and reconnect-resume are
  therefore trivial — never add stateful upload handshakes that break this.
- **Never block the live edge / playout head.** Keep the segments about to be played flowing. If a
  segment can't be sent within its retry budget, abandon it (tell the server) rather than stall.
  **Enforced end-to-end** as of the M7 abandon/stall work: the uploader (`uploader.rs`) detects a
  permanent continuity gap via `scheduler::stalled_continuity_seq` and calls `/abandon` after a
  generous (20 s) grace period; `playout.rs` skips an abandoned segment immediately and, as a
  backstop for an encoder that never calls `/abandon` at all (crashed/disconnected), skips anyway
  after `3 * segment_ms` of a segment being neither playable nor abandoned. Verified live: a
  deliberately-missing segment gets skipped by the timeout, and an explicit `/abandon` unsticks
  playout immediately rather than waiting out the timeout.
- **No audio lost to a short outage.** Segments persist on disk until acked and within the DVR
  window; on reconnect, resume from the oldest un-acked segment.
- **Rungs are ordered low→high in the full ladder.** This describes the *default,
  fully-enabled* ladder, not a hardcoded id — the operator may disable any rung, including the
  lowest, from the encoder GUI (any rung except the sole remaining enabled one). Whichever rung
  ends up lowest in the *filtered* profile becomes the effective survival rung for that session;
  `plan_uploads`/`stalled_continuity_seq` always derive "low" from `StreamProfile::low_rung()`,
  never a literal `0`, so the scheduler always has a cheap option to guarantee continuity
  regardless of which rungs are enabled.
- **The server owns the published playlist** and rebuilds it from what it actually received.
- **The feedback loop degrades safely.** If `ServerState` goes stale, the encoder assumes
  `ServerState::unknown()` (stopped, empty buffer) and plays it safe: low rung, protect the live edge.
  **Enforced** as of the M7 work: the client (`shared.rs`) now falls back to `ServerState::unknown()`
  once its held state is more than 5 s old, and the server (`store.rs`) seeds a fresh `DvrStore`'s
  `rev` from wall-clock epoch millis instead of 0, so a post-restart state can never look "older"
  than a rev the client held from before the restart and get permanently rejected.

---

## 6. The closed-loop scheduler (`crates/obcast-proto/src/scheduler.rs`)

`plan_uploads(&SchedulerInput) -> Vec<UploadAction>` is a **pure function** run once per tick. It is
the behavioural heart of the system and is unit-tested; keep it pure and keep the tests green. Given
the latest `ServerState` plus local inventory, it emits prioritized uploads under three tiers
(priority is absolute in this order):

1. **Continuity** — from the playout head forward, fill any hole at the **low rung** until
   `water.target_ms` of contiguous audio is secured. Allowed to **burst past the tick budget**;
   dropout is the worst outcome. This is "server draining on a flaky link → low quality first, no
   dropout." Its cost still counts when gating the tiers below.
2. **Live edge** — cover the newest ~`target_ms` at the low rung so the DVR stays contiguous and
   go-live is instant. Skipped in survival mode (`lead < water.low_ms`).
3. **Upgrade** — only when `lead ≥ water.high_ms` **and** budget remains, raise quality **strictly
   ahead of the playout head**, nearest-first, one rung step per tick. The ahead-of-head guard is
   what prevents wasting bytes on segments that will never be played.

The uploader loop's job is mechanical: call `plan_uploads`, send actions in priority order, update
its model of `ServerState` from feedback, repeat. All policy lives in the scheduler.

---

## 7. Protocol summary (authoritative detail in `docs/protocol.md`)

Three planes: **Media** (encoder→server segments; HLS to listeners), **Link** (server↔encoder
state), **Control** (web↔server).

- Ingest: `POST /ingest/{stream}/segment` with `X-Rendition`, `X-Seq`, `X-Auth`; **response body is
  a `ServerState`**. `POST /ingest/{stream}/abandon` for permanent gaps.
- Link feed: `GET /ingest/{stream}/state` (SSE `ServerState`) so feedback survives upload stalls.
- Control: `GET /api/{stream}/status` → `ControlStatus`; `POST /api/{stream}/playout` ← `PlayoutCommand`
  (`start`/`stop`/`pause`/`resume`/`seek`/`go_live`/`set_device`/`set_volume`); `WS /api/{stream}/ws`
  streams `ControlEvent` (status, position, meters, ack).
- Listen: `GET /hls/{stream}/master.m3u8`, `/{rendition}/index.m3u8`, `/{rendition}/{seq}.ts`.

The `obcast-proto` Rust types are the source of truth for all these schemas.

---

## 8. Build milestones

- **M0 — DONE.** Workspace, `obcast-proto` (state/control types + `plan_uploads` with tests),
  `docs/protocol.md`, server/client skeletons.
- **M1 — DONE.** Server ingest endpoint + `DvrStore` (in-mem index, disk bytes, DVR eviction) +
  `ServerState` computation + SSE state feed with heartbeat. Idempotent `record()`; emits real feedback.
- **M2 — DONE.** Client `cpal` capture + device selection + live channel-map + gain + K-14 level
  meters + `egui` shell + persisted TOML config.
- **M3 — DONE.** Client `ffmpeg` ABR encode (one proc → N rungs) → `{rung}/{seq}.ts` disk ring
  buffer; `inventory.rs` scan; uploader tick loop driving `plan_uploads` against the real server and
  folding `ServerState` from responses.
- **M4 — DONE.** HLS origin (master + per-rung sliding-window playlists, best-rung fallback) done.
  Web has hls.js listen-along with its own scrub bar (`audio.currentTime`/`audio.buffered`, never
  touching `/api/{stream}/playout`) independent of the waveform card's server-playout scrub — see the
  "Browser-side DVR scrub" entry further down for how the listen-along gap was closed.
- **M5 — DONE**, plus a correctness fix. Playout to hardware out via `cpal` (dedicated thread, ffmpeg
  decode, ring buffer, atomics for pos/state/vol/meters), start/stop + seek; head flows into
  `ServerState` (holds rather than skips on a not-yet-on-disk segment). Fixed: `position_seq` used to
  advance the instant a segment's decoded PCM was pushed into the ring buffer, not once it was
  actually drained by the realtime output callback — since a whole segment (up to `ring capacity` =
  3 segments) can sit queued-but-unheard at a time, the reported head (and everything derived from it:
  `ServerState.frontier_seq`/`lead_ms`/`coverage`, and downstream the client's on-air-quality/buffer
  readouts — see the auto-start entry below) could read several seconds ahead of what a listener could
  actually hear. `playout.rs` now tracks a `pending` FIFO of `(seq, remaining samples)` per queued
  segment and advances `position_seq` from the output callback as it genuinely drains through them
  (`PlayoutHandle::drain_pending`, tested via the pure `advance_pending` core). A related, still-open
  issue found in the same review: the ring buffer itself isn't cleared on `stop`/`seek`, so stale
  pre-jump audio can still play out for a few seconds before the new position's audio is reached —
  `position_seq` now truthfully reports whichever seq is actually draining during that window (better
  than before, which claimed the new position immediately), but the stale-audio playback itself is
  unfixed.
- **M6 — DONE**, plus a correctness fix. Control API (REST `status`/`playout` + WS
  status/position/meters) + web remote UI (start/stop/seek, health panel, VU meters, waveform seek).
  Fixed: the shows overview's `live` flag used to mean only "an in-memory `StreamHandle` exists" —
  which is auto-created by any read-only GET (`status`/`waveform`/`ws`) and never torn down, so a
  stream whose encoder died (or was never fed at all, e.g. a typo'd name) still showed a green "live"
  pill indefinitely. `live` now requires a segment within the last 5 s (`shows.rs`, matching
  `api.rs`'s existing link-health window). The web remote's control buttons (`stream.html`) were also
  fire-and-forget — a rejected command (e.g. 409 "no segments buffered yet") failed with zero
  feedback; they now surface the server's rejection reason in a visible banner. The client GUI
  similarly used to go silently dead if its ffmpeg pipeline crashed mid-session (stdin write failure
  just dropped the feed with no error); it now surfaces the failure and drops itself out of "live".
  Together this was the root cause of "start a stream, it says live, but nothing plays and the
  waveform never generates."
- **Follow-up fix (web remote waveform).** Two more `stream.html` bugs, found watching a live show:
  (1) `initWaveform()`'s zoomview was configured with `autoScroll: false`, so the visible waveform
  window never followed the server's real playhead — a normal listen session outlasts the initial
  visible slice (tens of seconds at the fixed 40ms/point scale) and the view then sits frozen
  regardless of how far playout advances, reading as "the waveform always shows the same." Now
  `autoScroll: true` (peaks.js's own default, which this had overridden). (2) The waveform's
  click-to-seek adapter (`serverPlayer.seek(time)`) sent a raw fractional-second `time` value as
  `PlayoutPosition::SecondsBehindLive`, which is a `u32` — any non-integer click position (e.g.
  `4.192`) got rejected with a 422 `invalid type: floating point, expected u32`. Now rounded with
  `Math.round()` before sending, matching the seconds-granularity the server type expects.
- **M7 — DONE**, across this and later sessions (this entry originally shipped marked PARTIAL with a
  long "Open:" list below it; that list is stale — every item on it has since been fixed, in sessions
  whose commits predate this document being updated to say so, which is itself the kind of
  doc/reality drift this session's own CLAUDE.md pass went back and corrected throughout). Done:
  optional single-token ingest auth (`X-Auth`); SSE + uploader reconnect on drop; client-side abandon
  + a bounded stall timeout in `playout.rs` (a permanent segment gap no longer stalls the playout head
  indefinitely — verified live: an unfilled gap gets skipped after `3 * segment_ms`, and an explicit
  `/abandon` unsticks it immediately); stale-`ServerState` fallback (client now discards state older
  than 5 s, and the server seeds a fresh store's `rev` from wall-clock epoch millis so a restart can no
  longer look "older" than what a client already held — see §5 for both). Also fixed since: **auth
  split** — a separate `OBCAST_CONTROL_TOKEN`/`control_token` gates `POST /api/{stream}/playout` via
  `check_control_auth` (`api.rs`), checked before any stream lookup so a bad/missing token can't be
  used to spin up a stream's playout thread just to get rejected (`AppState::control_token`,
  `main.rs`) — distinct from ingest's `X-Auth` by design, since an OB site's upload token shouldn't
  also be able to stop/seek/volume the hardware output. **Per-stream resource leak** — read-only
  control/HLS routes (`status`/`waveform`/`ws`, HLS origin) use `AppState::stream_if_known`, which
  never auto-vivifies a permanent OS thread + `DvrStore` for a name nobody has ingested into (unlike
  `AppState::stream`, reserved for the ingest/media-plane entry points where "doesn't exist yet"
  legitimately means "a new stream is starting"). **DVR file reaping** — `DvrStore::record()` returns
  the on-disk paths any given call evicted from the index; `ingest.rs`'s `reap()` actually deletes them
  via `tokio::fs::remove_file` (best-effort, logged on failure) instead of leaking them forever.
  **Reverse telemetry** — `POST /ingest/{stream}/heartbeat` is wired to `DvrStore::set_encoder_state`,
  and `api.rs::build_status` populates `ControlStatus.encoder` + real `throughput_kbps` from it.
  **Stalled-vs-playing distinction** — `playout.rs`'s `playout_state()` has a `PlayoutState::Stalled`
  variant, distinct from `Playing`, surfaced through `ServerState`/the web pill/`computeReason()` in
  `stream.html`. **Waveform decode-failure surfacing** — `WaveformJson` carries a `decode_failed`
  array parallel to `rungs`/`data`, distinguishing a real gap (no rung ever recorded) from an
  indexed-but-undecodable segment, covered by tests in `waveform.rs`. Packaging (the one remaining item
  from the old "Open:" list) is covered by the "Packaging" entry further down.
- **Two more fixes (found chasing an operator report of "web remote *and* GUI both say HD but the
  server is audibly outputting low quality, and playback sometimes jumps around").** (1) The quality
  mismatch was deeper than it first looked. A first pass made `LinkHealth.current_rung` look up
  `ServerState.playout.position_seq` in `coverage` instead of `live_seq` — necessary (using the live
  edge was definitely wrong) but not sufficient, because the client GUI's `SharedState::playing_quality`
  was already doing that same `coverage`-at-`position_seq` lookup and was *still* wrong, which is what
  the operator saw persist. The real bug: `coverage`/`best_rung(seq)` reports whatever the DVR index
  currently holds for that seq, but `playout.rs`'s decode pipeline feeds segments to its decoder many
  segments ahead of real-time output (`RING_SEGMENTS = 8`, up to ~16s at the 2s default) to survive
  slowdowns without an audible underrun. The rung the engine picks for a segment is locked in at *feed*
  time — but a quality upgrade for that same segment routinely lands on disk well before that segment
  is actually heard (uploads are far faster than the ring is deep), so by the time the head reaches it,
  a live "best rung for this seq" lookup reports the *new* rung even though the decoder already
  committed to the old one. Both readouts were asking the DVR index a question it can't truthfully
  answer. Fixed by tracking the fed rung directly: `PlayoutHandle::pending` (the FIFO already used to
  drain-track `position_seq` truthfully, per the M5 fix above) now carries `(seq, rung, samples)` instead
  of `(seq, samples)`, and a new `PlayoutHandle::playing_rung()`/`PlayoutStatus.playing_rung` exposes
  the rung of whichever entry is actually draining right now — ground truth, no index lookup. Both
  `api.rs`'s `current_rung` and the client's `playing_quality` now read this field directly. (2)
  `origin.rs`'s per-rendition HLS playlist (feeding the web remote's "listen along" hls.js player) built
  `EXT-X-MEDIA-SEQUENCE` and its segment list from only the seqs with media already on disk, skipping
  gaps silently. On the flaky uplink this whole system targets, a segment landing *late* (a retry
  succeeding after later ones already arrived) is normal, not exceptional — and every time that
  happened, the newly-filled seq shifted every later entry down one list position, which is exactly
  what hls.js's playlist-sync logic uses to tell "already buffered" apart from "new." The shift read to
  a listener as playback jumping around. The playlist now walks the true contiguous
  `[dvr_start_seq, live_seq]` range so a segment's list position always equals its real seq number, and
  emits `#EXT-X-GAP` for a seq with no media yet (or ever, if abandoned) instead of omitting it —
  covered by pure unit tests in `origin.rs` (`playlist_body`) demonstrating the position-stability
  property directly, and in `playout.rs` (`advance_pending`) demonstrating that a stale-but-fed low rung
  is reported even once the index has moved on to HD for the same seq.
- **ABR ladder rework: HE-AAC survival rung + a fully operator-controllable ladder.** Closes the
  long-open "low rung should be HE-AAC" gap and adds three new operator controls to the encoder GUI.
  The default ladder (`StreamProfile::default_ladder`, `obcast-proto`) is now four rungs —
  `lo`/48k/HE-AAC, `mid`/96k, `hi`/192k, `hd`/320k (all LC except `lo`) — up from three (32/128/320,
  all LC). `encode.rs` probes `ffmpeg -encoders` once at startup for `libfdk_aac`; if present, `lo`
  encodes as real HE-AAC (`-c:a libfdk_aac -profile:a aac_he`), otherwise it falls back to AAC-LC at
  the same bitrate with a logged warning — never a hard failure. `origin.rs`'s master playlist now
  emits the matching HLS `CODECS` tag per rung (`mp4a.40.5` for HE-AAC, `mp4a.40.2` for LC) instead of
  hardcoding LC for everything. Operators can enable/disable any rung (including the lowest — see the
  §5 invariant update above) and pick a "default quality" (the rung the uploader's bootstrap bandwidth
  guess assumes before real `ServerState`/throughput feedback arrives; purely a starting point, the
  closed-loop scheduler takes over immediately once real data flows) from a new "Rungs" section in the
  GUI that — unlike the rest of "Stream Target" — stays interactive while live. `StreamProfile::filtered()`
  is the mechanism: it narrows the full ladder to the enabled subset (never empty; falls back to the
  single lowest-bitrate rung of the full ladder if a stale config would otherwise empty it), and that
  filtered profile is all `plan_uploads` ever sees — **no scheduler code changed**, since it already
  derived every "low"/"next rung" concept from `profile.rungs` rather than a hardcoded id. Toggling a
  rung while live requires confirming a warning dialog (it restarts the encoder pipeline — ffmpeg can't
  add/remove `-map` outputs without a respawn — causing a few seconds of audio gap that the existing
  continuity/abandon/stall-skip machinery already absorbs); the dialog exists specifically so a live
  toggle is a deliberate operator action, not an accidental one. **Segment-alignment risk, raised by
  adversarial review and since verified**: this repo's stock dev/CI ffmpeg lacks `libfdk_aac`, so a
  second, `libfdk_aac`-enabled ffmpeg (built from source locally against the already-present
  `libfdk-aac` system library — see `dist`/packaging entry below for why this isn't the default build)
  was used to actually exercise the HE-AAC path and check `encode.rs`'s own module-doc claim of
  sample-aligned segment boundaries across rungs. HE-AAC's SBR doubles the encoder's frame size (2048
  samples vs LC's 1024), so in principle `-segment_time`'s frame-boundary snapping could cut the `lo`
  rung's segments at a different sample position than the LC rungs sharing the same `ffmpeg` process.
  Measured with sample-accurate PCM decode (container-reported duration turned out to be unreliable
  here) across a 90s/45-segment run at the default 2s segment length: 39/45 seq boundaries land at the
  *exact* same sample count on both `lo` (HE-AAC) and an LC rung; the other 6 — at the points where
  neither 2048 nor 1024 samples divides the 2s/44.1kHz target evenly, roughly every 7-8 segments —
  differ by exactly one LC frame (1024 samples, ~23ms), and self-correct on the very next segment
  rather than compounding. The two rungs' cumulative position never drifts by more than one LC frame at
  any point, regardless of stream length. Judged acceptable as-is (a bounded ~23ms worst-case,
  self-correcting misalignment at a rung switch, comparable to normal AAC encoder priming/lookahead
  variance that already exists between any two rungs) rather than something to build extra machinery
  around; see `encode.rs`'s `codec_args` doc comment for the same finding in context. Revisit if
  `segment_ms` is ever configured much shorter than the 2000ms default, which would shrink the
  denominator this tolerance is relative to.
- **Browser-side DVR scrub for the listen-along player** (`web/remote/stream.html`), closing the M4
  gap noted below: the independent hls.js `<audio>` preview player gained its own seek bar and "Go
  live" button, driven purely by `audio.currentTime`/`audio.buffered` — it never touches
  `/api/{stream}/playout`, so it stays fully decoupled from the server's real hardware-output playhead
  (the waveform card above it). `backBufferLength: 300` set on the `Hls()` instance so a useful chunk
  of DVR history (matching the server's 5-minute window) actually stays seekable, since hls.js
  otherwise aggressively flushes back-buffer for live streams.
- **Packaging.** The server was already Docker-only (repo-root `Dockerfile`). The client (the
  cross-platform `egui` GUI app run at the OB site) now has a `dist-workspace.toml` (using
  [`cargo-dist`](https://opensource.axo.dev/cargo-dist/)'s modern standalone-file config) targeting
  Windows/macOS (x86_64 + aarch64)/Linux with shell + PowerShell installers; the server crate opts out
  via `dist = false` in its own `Cargo.toml` since it stays Docker-only. `dist` was installed locally
  and actually run (`dist init` / `dist generate-ci`, `dist plan`) rather than just hand-writing config
  against a guessed schema — `dist plan` confirms the four target archives it produces cover only
  `obcast-client`. This generated the real `.github/workflows/release.yml`; don't hand-edit that file,
  edit `dist-workspace.toml` and regenerate instead. Also bundles the `libfdk_aac`-enabled ffmpeg from
  the ABR ladder rework entry above: `scripts/build-libfdk-ffmpeg.sh` (Linux/macOS, from source against
  each platform's `fdk-aac` package) and `-windows.sh` (Windows, an MSYS2/MinGW source build of both
  `fdk-aac` and ffmpeg — a first attempt used vcpkg's `ffmpeg[fdk-aac]` port instead, which builds fine
  but passes `--disable-ffmpeg` internally and so never produces the `ffmpeg.exe` CLI binary this
  actually needs, only the libav* libraries for linking into other software) are wired into
  `.github/workflows/build-libfdk-ffmpeg.yml`, a `workflow_call`-able job matrix covering all four
  `dist` targets. Kept as its own workflow rather than folded into the dist-generated one since `dist`
  has no hook for bundling an externally-built, non-Cargo binary artifact. Actually run on real
  GitHub-hosted runners (not just written and assumed correct) — all four legs came back green with a
  working `libfdk_aac`-enabled `ffmpeg` binary as an artifact, including catching and fixing two real
  bugs a "looks right" review wouldn't have: Windows' first attempt used vcpkg's `ffmpeg[fdk-aac]` port,
  which builds successfully but passes `--disable-ffmpeg` internally and so never produces the actual
  `ffmpeg.exe` CLI binary (only libav* libraries) — replaced with the MSYS2/MinGW source build described
  above; and the macOS x86_64 leg initially targeted the `macos-13` runner label, which sat queued for
  45+ minutes doing nothing because — confirmed with `actionlint`, not just inferred from the hang —
  GitHub has fully decommissioned that label, so a job requesting it can never be scheduled at all, no
  matter how long you wait. Fixed to `macos-15-intel`, the current label for x86_64 macOS runners.
  `.github/workflows/attach-libfdk-ffmpeg.yml` wires these binaries into the actual release: triggered
  by `release: published` (i.e. after `dist`'s own `release.yml` creates the release), it builds all
  four ffmpeg binaries, downloads and unpacks each platform's release archive, injects the matching
  ffmpeg binary alongside `obcast-client`, repacks, and re-uploads (regenerating that asset's checksum
  in both its own `.sha256` file and the aggregate `sha256.sum`). The archive-manipulation logic (finding
  dist's single top-level directory inside each archive, repacking without introducing a stray leading
  `./` on every path, matching dist's own `{hash} *{filename}` checksum format) was verified locally
  byte-for-byte against a real `dist build` output rather than assumed from `dist plan`'s summary alone.
  Both workflow files pass `actionlint` with zero errors. Firing it end-to-end requires actually
  publishing a release (a real version tag) — asked explicitly rather than assumed either way, and the
  maintainer's call was to cut that first real release themselves rather than have an agent do it
  (even a disposable test tag) on a feature branch; that first release is what exercises this workflow
  for real. Implementation-wise this is complete: the archive/checksum logic is verified byte-for-byte
  against a real `dist build` output, and the trigger, job wiring, and artifact-naming contract with
  `build-libfdk-ffmpeg.yml` are all in place and lint-clean — what's left is the maintainer's own act of
  shipping, not unfinished work on this end.
- **De-duplicated `StreamProfile` construction in the client crate.** `client/main.rs`'s single-use
  `fn profile(segment_ms) -> StreamProfile` wrapper (just `StreamProfile::default_ladder(segment_ms)`)
  was deleted and inlined at its one call site. `client/gui/app.rs`'s `profile(&self)` method was left
  as-is — it's a genuine 5-call-site accessor now doing real work (`.filtered()` against the operator's
  enabled-rungs config, from the ABR ladder rework above), not redundant indirection.
- **Fixed a still-live "server has HD, plays low quality" report — the upgrade tier was gated on the
  wrong boundary.** The earlier "Two more fixes" entry above made `playing_rung` ground truth so
  dashboards stopped *lying* about the rung on air, but didn't touch why the *wrong rung actually plays*
  in the first place — that turned out to be a live, structural bug, not a fixed-but-still-reported one.
  `scheduler.rs`'s Tier C (quality upgrades) only ever looked ahead of `anchor`/`position_seq` — the
  *audible* head — when deciding which seqs were still worth upgrading. But `playout.rs`'s decode
  pipeline feeds segments to ffmpeg many seconds ahead of real-time output (`RING_SEGMENTS = 8`, ~16s at
  the 2s default) to survive slowdowns without an audible underrun, and a seq's rung is locked in the
  moment it's fed — well before `position_seq` ever reaches it. Under the default water levels
  (`high_ms = 20_000`), Tier C's own look-ahead window (`high_ms / seg_ms` = 10 segments/20s ahead of
  `anchor`) overlaps almost entirely with what the engine has, in steady state, already fed — so an
  upgrade upload routinely "succeeded" (HD really did land on the server) for a seq that could no longer
  ever be played at that rung, because playout's feed loop had already grabbed the low rung Tier B
  uploads first and locked it in. This is exactly the symptom reported: HD visibly present on the
  server/DVR, low quality audibly on air, persistently rather than as a brief transient. Fixed by
  threading the engine's actual feed-ahead boundary through the feedback loop: `PlayoutHandle::fed_seq`
  (new atomic, set alongside `enqueue_pending` and reset on `Start`/`Stop`/`Seek` next to
  `position_seq`/`playing_rung`) tracks the highest seq already fed; exposed as
  `PlayoutStatus::fed_seq`/`ServerState.playout.fed_seq` (`#[serde(default)]`, so an older server talking
  to a newer client — or vice versa — just falls back to the previous `anchor`-only gating rather than
  breaking). Tier C's candidate range now starts strictly after `max(anchor, fed_seq)` instead of just
  `anchor`, so it only ever spends upload budget on seqs an upgrade can actually still affect — covered
  by two new `scheduler.rs` tests (`upgrades_never_target_a_seq_already_fed_to_the_decoder`,
  `upgrades_yield_nothing_when_the_whole_comfortable_window_is_already_fed`) exercising both the partial-
  overlap and fully-overlapping-window cases. `docs/protocol.md` updated in the same change (wire-compat
  rule, §9).
- **Fixed a live regression from the fix above: upgrades stopped firing at all once a real 60s buffer
  built up, despite ample bandwidth.** Reported directly against a running server/client pair (`test02`
  stream): `/api/{stream}/status` showed `lead_ms: 60000` (comfortably over `water.high_ms: 20000`, so
  Tier C should have been active) yet every seq in `coverage` sat at `best_rung: 0` — never upgraded.
  Root cause: the previous fix gated Tier C's candidate range on `already_fed = max(anchor, fed_seq)` but
  left the range's *far* boundary at `anchor + cap_segs` (`cap_segs = high_ms / seg_ms`) — a boundary
  measured from `anchor`, not from `already_fed`. Observed live, `fed_seq` (248) ran 13 segments ahead of
  `anchor` (235) — deeper than the assumption baked into the old "Two more fixes" entry's `RING_SEGMENTS =
  8` estimate — while `cap_segs` was only 10, so the range became `(already_fed+1)..=end` =
  `249..=245`: empty, permanently, since both boundaries advance together in steady state. Tier C produced
  zero upgrade actions every tick no matter how much buffer or bandwidth was available, because the window
  it was allowed to consider had already collapsed to nothing. Fixed by anchoring the far boundary to
  `already_fed` too (`end = already_fed + cap_segs`), so the window always starts right after whatever's
  already locked in rather than potentially behind it. `scheduler.rs`'s
  `upgrades_yield_nothing_when_the_whole_comfortable_window_is_already_fed` test encoded the *buggy*
  behavior as correct ("nothing in that window can still be upgraded in time") — replaced with
  `upgrades_still_happen_when_feed_ahead_outruns_the_anchor_relative_window`, asserting upgrades resume
  starting right after the feed boundary instead of asserting silence.
- **Fixed the web remote's big playhead clock jumping backward and capping out around the DVR window
  size.** Reported as "the current time in the stream starts jumping back in time, so we stay at like
  4 minutes." `stream.html`'s `updatePlayoutTime()` computed the displayed elapsed time as
  `(position_seq - dvr_start_seq) * segment_ms` — distance from the start of the currently-buffered
  DVR window, not the broadcast's actual elapsed time. `dvr_start_seq` advances continuously with real
  time as the DVR evicts old segments, regardless of whether playout is actually advancing, so once a
  stream outlived the ~5-minute DVR window this reading was structurally capped near the window size,
  and any moment `position_seq` lagged behind real time (pause, stall, catching up after a network
  drop) let eviction outpace it, making the display visibly jump backward. Fixed to read
  `position_seq * segment_ms` directly — `Seq` is a `u64` starting at 0 for the encoder session (§5:
  "Seq is the canonical clock"), so that product is already the broadcast's true monotonic elapsed
  time and needs no window-relative anchor at all. Unrelated to (and doesn't touch) the waveform's own
  `waveformBaseSeq`-anchored scrub coordinate frame described above, which is correct as-is for its own
  purpose (positioning within the currently-loaded waveform data).
- **Same symptom, still live after the fix above: the real culprit was the waveform's own scroll
  cursor, not the text clock.** The text-clock fix didn't touch `makeServerPlayer()`'s
  `currentTimeSecs()` — the position peaks.js actually draws its cursor at and scrolls to, i.e. the
  thing an operator watching the waveform card is looking at — which had the identical bug in a
  different spot: `anchorMs` was computed as `(position_seq - waveformBaseSeq) * segMs` *once*, at the
  moment a `status` update arrived, already converted into the waveform's local coordinate frame at
  that instant. But `waveformBaseSeq` keeps sliding forward independently on every later eviction
  (`trimEvictedSegments`, called on nearly every subsequent status push) without that anchor ever being
  recomputed — so between authoritative updates the interpolated reading was stale relative to the
  *current* base: systematically too high, clamped down to `durationSecs` (itself capped to the DVR
  window size), reading as the cursor pinning near the window size and visibly snapping backward on
  each correcting update. Fixed by making the interpolation anchor (`anchorAbsMs`) an absolute
  elapsed-time reading (`position_seq * segMs`, same fixed seq-0 origin as the text-clock fix) instead
  of pre-converting into the local frame, and converting to `waveformBaseSeq`-relative coordinates
  fresh on every `currentTimeSecs()` call rather than storing a conversion that can go stale. This also
  simplified `updateFromStatus`: it no longer needs to gate the anchor update on `waveformBaseSeq`
  being known yet.
- **DVR window size is now configurable, with an explicit "unbounded" mode.** `dvr_window_ms` used to
  be a hardcoded 5-minute constant in `main.rs`, and `DvrStore::new` additionally clamped it to a
  minimum of 1 segment (`.max(1)`) — so there was no way to ask for "never evict," and the auto-start
  caveat in `docs/protocol.md` explicitly called this out as "not currently exposed." Now
  `OBCAST_DVR_WINDOW_MS` (env var, matching the rest of `main.rs`'s per-run config — the TOML file in
  `config.rs` stays reserved for genuinely per-machine settings like the audio device) sets it, default
  unchanged at 5 minutes; `0` disables eviction entirely rather than clamping to a 1-segment window.
  `DvrStore`'s internal `dvr_window_segs` is now `Option<u64>` (`None` = unbounded) so `evict_old`
  short-circuits to "evict nothing" instead of computing a floor at all — covered by a new test,
  `zero_dvr_window_ms_disables_eviction`, recording 500 segments and asserting none are evicted and
  `dvr_start_seq` stays at the first seq ever recorded. An unbounded window means unbounded disk use for
  the life of the stream — that's the operator's explicit choice in setting `0`, not a default; both
  `docs/getting-started.md`'s env var table and the auto-start caveat in `docs/protocol.md` say so.
- **Fixed: the client GUI's "Default quality" picker (`AppConfig::default_rung`, the "ABR ladder
  rework" milestone above) was a complete no-op.** Reported directly by an operator: picking a
  higher default quality (e.g. "hd") had no effect at all — the stream always started at the
  lowest rung. Root cause: the picker only ever fed `uploader::Config::bootstrap_rung` into a
  one-time seed of `throughput_kbps` before the first real upload. That seed could never actually
  reach any observable behavior: Tier A (continuity) ignores `throughput_kbps` entirely and always
  targets the low rung; Tier B (live edge) is skipped outright while in survival mode
  (`lead_ms < water.low_ms`), which covers essentially the whole ramp-up period right after going
  live; and Tier C (upgrade) doesn't engage until `lead_ms >= water.high_ms` (20s of buffer), by
  which point dozens of real uploads have already overwritten the seed (`throughput_kbps` is
  recalculated after *every* successful upload, not just the first). So the picker's chosen value
  was read once, influenced nothing, and was stale before any tier that might have consulted it
  ever ran. Fixed by giving `SchedulerInput` a new `preferred_rung` field that Tier B (live edge)
  now actually consults: it tries the operator's chosen rung first for newest-segment coverage
  when it's both locally available and affordable within the tick's bandwidth budget, falling back
  to the profile's low rung otherwise (unavailable, or budget-constrained) — so a link that can
  sustain it visibly starts higher, while a link that can't still gets the safe fallback. Tier A
  (continuity) is deliberately untouched — it always uses the low rung regardless of
  `preferred_rung`, so the no-dropout guarantee never depends on the operator's quality pick being
  achievable. `uploader.rs` threads the already-resolved `bootstrap_rung` (via
  `StreamProfile::nearest_enabled_or_low`, in case the picked rung has since been disabled) into
  `preferred_rung` every tick, not just at startup, so a config change takes effect on the very
  next go-live as the GUI's hover text already promised. Five new `scheduler.rs` tests cover the
  new behavior and its edges: live edge prefers the picked rung when affordable, falls back to low
  when the picked rung exceeds the tick budget, falls back to low when the picked rung isn't
  locally encoded yet, and continuity ignores `preferred_rung` even with ample budget and a
  high-quality pick. All pre-existing tests were updated to pass `preferred_rung: low_rung()`
  (recovering the previous always-low-rung live-edge behavior) and pass unchanged, confirming the
  fix is additive rather than a behavior change for callers that don't set a preference.

**Beyond the roadmap (built, not on the original M-list):** a BBC peaks.js quality-colored waveform
(`server/waveform.rs` + `GET /api/{stream}/waveform`, color-coded by ABR rung with click-to-seek);
live channel-mapping capture (pick 2 of N device channels as L/R); K-14 + per-channel metering;
persisted TOML operator config; a `--headless` client path (ffmpeg captures a device or sine tone
directly, no GUI); `README.md` + `docs/getting-started.md`; encoder-requested auto-start
(`EncoderState::auto_start_buffer_ms` + `ServerState::buffered_ms`, `docs/protocol.md` §3 "Auto-start") —
the client GUI can ask the server to start playout on its own once a chosen buffer (e.g. 5 minutes)
has accumulated, rather than waiting on a web operator, and only while playout is still `stopped` (a
manual start always wins). The client GUI also gained a "Link" panel: a buffer gauge (pre-roll
progress toward the auto-start target, or `lead_ms` once playing — the same number that drains if the
uplink dies), a bandwidth gauge (primary rung's bitrate vs. last measured link throughput — 100% is
the boundary where the link can just sustain it), and an on-air quality readout that's ground truth
from `ServerState.coverage` while connected and falls back to a same-crate guess — extrapolating the
playout head's position from elapsed time and looking up what rung *we* sent for that seq in a local
upload-history map (`SharedState::playing_quality`) — once the link (and thus `ServerState`) goes
stale, flagged as "(estimated)" in the UI. All three also plot a rolling last-60s history (hand-painted
line graphs, `gui/meter::sparkline`, sampled once a second independent of the ~30fps repaint) — no
plotting crate: a dependency scan found no `egui_plot` release compatible with this workspace's egui
version (its latest, 0.35.0, pins `egui ^0.34`), so this follows the same hand-painted-widget approach
`level_meter`/`mini_meter` already use rather than fighting that mismatch.

**Web remote playhead accuracy, sub-segment seeking, and waveform improvements.** Prompted by an
operator report that the web remote's playhead position visibly disagreed with the waveform, jumped
around instead of scrolling smoothly, and could only seek to whole-`segment_ms` boundaries. Four
changes, all landing together since they share one root cause (the playhead only ever moved in whole-
segment steps):
- **Sub-segment position, server-side.** `PlayoutHandle::pending`'s per-entry remaining-sample count
  (already tracked for `drain_pending`'s sample-accurate `position_seq`, see the M5 fix above) already
  implicitly encodes how far into the current segment playout has drained — it just wasn't exposed. New
  `PlayoutHandle::ms_into_current_segment()` (pure core factored out as `elapsed_ms_from_remaining`, unit
  tested) converts it to ms against the segment's nominal duration, surfaced as
  `PlayoutStatus::position_ms_into_segment` (`#[serde(default)]`, so it degrades to the old
  whole-segment behavior against an older peer).
- **Sub-segment seeking**, answering "is this possible?": yes. New `PlayoutPosition::MillisBehindLive`
  resolves (`api.rs::resolve_millis_behind_live`, unit tested) to a target segment plus an intra-segment
  ms offset, deliberately anchored to the *end* of the live segment (true "now") rather than
  `SecondsBehindLive`'s "start of the live segment" convention — see its doc comment in
  `docs/protocol.md` for why the two scales disagree by up to one `segment_ms` at the same numeric
  value. The offset reaches the playout engine as `EngineCommand::Start`/`Seek`'s new `skip_ms` field;
  `spawn_decoder_session`'s reader thread silently drops that many leading interleaved samples once, and
  the first `pending` entry after the seek starts its remaining-sample count already short by that
  amount — which is also what makes `position_ms_into_segment` read the skip as already-elapsed rather
  than the seek looking like it snapped to the segment's start. Unit tested (`skip_samples`,
  `elapsed_ms_from_remaining`'s truncated-entry case).
- **Fixed the position/waveform mismatch.** The web remote's playhead time was computed from
  `status.dvr_start_seq`, while the waveform's own x-axis started at whatever seq its last independent
  fetch happened to return — two values refreshed on different schedules (every WS push vs. every 5s),
  so a DVR eviction between waveform polls left the cursor computed against a start seq the currently-
  displayed waveform didn't share. Now anchored to the waveform JSON's own `seqs[0]`
  (`serverPlayer.setWaveformBase`), so both are exact by construction.
- **Smooth, continuous playhead and waveform scrolling.** `stream.html`'s `serverPlayer` used to emit
  peaks.js's `player.timeupdate` only when a `status` push moved `position_seq` by a whole segment,
  which is also what drove peaks.js's `autoScroll` — visibly jumping once every ~2s. It now anchors an
  elapsed-ms reading (`position_seq` + `position_ms_into_segment`) to a wall-clock timestamp on every
  authoritative update and interpolates forward from that anchor once per animation frame while playing,
  self-correcting on the next update — smooth motion between the same ~2s authoritative ticks rather
  than needing a higher-frequency feed.
- **Evicted segments now disappear from the waveform promptly.** The waveform endpoint already
  defaults to the current DVR window, so eviction was already reflected — just only on the next 5s poll
  (each of which re-decodes the whole range via `ffmpeg` server-side, not cheap enough to do more often
  — see `waveform.rs`'s module docs on the lock-holding fix that was specifically about avoiding that
  cost on every tick). `stream.html` now also prunes the already-loaded waveform client-side the moment
  a `status` push reports `dvr_start_seq` advancing (`trimEvictedSegments`, a plain array slice, no
  server round trip), so an evicted segment's color disappears within one WS push instead of lingering
  for up to 5s.

**Client GUI Link panel: buffer graph survives a dropped link, and per-rung stacked buffer quality.**
Two fixes to `gui/app.rs`'s Link panel, prompted by an operator watching the buffer gauge stay pinned
at its last-known value through a connection outage instead of visibly draining. (1) `link_panel` used
to read `self.shared.server` straight off the mutex and use `lead_ms`/`buffered_ms` verbatim — accurate
while the feed is live, but frozen at whatever number arrived with the last `ServerState` once the SSE
feed/upload-reply path actually stalls, which reads as "the buffer is fine" during the exact outage
where it isn't. Both quantities in fact keep draining in real time with nothing arriving to replenish
them: while playing, `lead_ms` is consumed by playout with no new segment upload to top it back up;
while stopped, `buffered_ms` (contiguous DVR depth from the live edge) shrinks as the server's DVR
window evicts its oldest end while the live edge sits frozen (no new segments extending it). New
`SharedState::server_snapshot()` returns the held state alongside how long it's been since `update()`
last refreshed it; the Link panel now extrapolates `buffer_ms = raw_buffer_ms.saturating_sub(age_ms)`
unconditionally — in normal operation `age` stays near zero (state refreshes every tick) so this is a
no-op, and it only visibly kicks in once the feed actually goes quiet, giving exactly the "60s buffered,
10s of silence, now shows 50s" behaviour rather than a frozen readout; a "(estimated — link down Ns)"
suffix appears once `age` crosses the existing `STALE_AFTER` (5s) threshold also used by
`playing_quality`'s stale-link fallback. (2) "Buffer quality (HD)" — previously a single progress bar
for "% of covered buffer at the top rung only" — is now "Buffer quality (by rung)", a 100%-stacked bar
(new `meter::stacked_bar`, plus `meter::rung_color` mapping each enabled rung to a point on a
red→yellow→green hue ramp, low→high) showing every enabled rung's share of the outstanding buffer at
once, so the segments always sum to the full covered fraction instead of collapsing the distribution to
one number. The Link panel's 60s trend sparkline for this metric is kept (relabeled "Buffer quality
trend (% at top rung)") since a stacked bar has no natural single-line history equivalent.

**Waveform overview: duration anchoring, size, and scroll behavior.** Requested after an operator
found the waveform card small and its auto-scroll disorienting. Three changes to `stream.html`:
(1) the same class of bug fixed earlier for the playhead's *position* anchor (see the "playhead
clock jumping backward"/"waveform's own scroll cursor" entries above) also existed, unfixed, in its
*duration*: `serverPlayer`'s `durationSecs` was still computed from `status` as
`(live_seq - dvr_start_seq + 1) * segMs` — the DVR window's nominal span, refreshed on every WS
push — while the waveform actually on screen is anchored to its own `waveformBaseSeq`/point count
(set by `applyWaveform`/`trimEvictedSegments`, refreshed on a slower 5s poll plus client-side
eviction trims). The two drift out of step the same way the position anchor used to, and
`durationSecs` clamps `currentTimeSecs()` against a length that may not match what's actually
drawn. Fixed by deriving `durationSecs` directly from the loaded waveform's own point count
(`json.length * pointMs(json)`) via a new `serverPlayer.setDuration()`, called everywhere the
waveform data changes, instead of from `status`. (2) `#zoomview-container`'s height went from
100px to 220px — just a bigger card. (3) peaks.js's own `autoScroll` re-anchors the playhead a
fixed 100px (its default `autoScrollOffset`) from the left edge the instant it comes within 100px
of the right edge; on this card's full-page-width container that reads as "crawls most of the way
across, waits at the right edge, then snaps most of the way back" rather than anything smooth or
centered. `applyAutoScrollOffset()` now sets the offset to 25% of the view's actual pixel width
(`view.enableAutoScroll(true, { offset })`) instead of the fixed default, so the jump always
triggers 25% of the width from the right border and lands 25% from the left, regardless of
container size — re-applied on load and on a debounced `ResizeObserver` of the container (which
also calls peaks.js's own `view.fitToContainer()`, since per its docs it does not track container
resizes on its own). True continuous (non-jumping) scroll is possible but would mean disabling
peaks.js's autoScroll and driving `view.updateWaveform()` every animation frame ourselves — left
as a follow-up if the 25%-edge jump still isn't smooth enough in practice.

**What's next.** Everything from the previous "auth split / resource leak / DVR reaping / stalled
playout state / waveform decode failures / reverse telemetry / packaging / StreamProfile dedup /
browser DVR scrub / HE-AAC survival rung / scheduler edge-case tests" punch list is now DONE — see the
milestone entries above (M7's auth-split and resource-leak fixes; the DVR-file-reaping, stalled-state,
and waveform-decode-failure fixes folded into store.rs/playout.rs/waveform.rs; the reverse-telemetry
heartbeat wiring in store.rs/api.rs; and the ABR ladder rework / browser DVR scrub / packaging /
StreamProfile dedup entries just above), including the three items this section used to list as
follow-ups, all now genuinely closed rather than left as documented-but-unverified: the HE-AAC
segment-alignment risk is measured empirically (not theoretical); `libfdk_aac` ffmpeg bundling has a
working build **actually confirmed green on all four `dist` targets on real GitHub-hosted runners**
(Linux, Windows, macOS aarch64, and macOS x86_64 — the last of which required finding and fixing a
real bug, not just re-running the same job: it targeted the `macos-13` runner label, which
`actionlint` confirms GitHub has fully decommissioned, so the job could never have been scheduled no
matter how long it waited; fixed to `macos-15-intel`); and the built ffmpeg binaries are wired into
the actual release archives by `attach-libfdk-ffmpeg.yml`, with its archive-manipulation logic verified
locally byte-for-byte against a real `dist build` output. See the "Packaging" milestone entry above for
the full detail on all three. Nothing left in this list requires further code changes — the maintainer
has chosen to cut the project's first real release themselves, which is what exercises
`attach-libfdk-ffmpeg.yml` end-to-end; that's a shipping decision, not outstanding implementation work.

**LUFS-I/LUFS-S/LUFS-M loudness metering (client).** New `obcast-proto/src/loudness.rs`:
`Loudness`, an ITU-R BS.1770-4 / EBU R128 implementation computing momentary (400 ms), short-term
(3 s) and integrated (gated, whole-programme) LUFS from post-gain L/R PCM — the same K-weighting +
gating algorithm behind every standards-based loudness meter, built from scratch (no crate pulled in,
matching `Vu`/`Ppm`/`Peak` in the same file's sibling `meter.rs`). K-weighting (a high-shelf pre-filter
cascaded with the RLB high-pass) is re-derived from its analog prototype at whatever sample rate the
open device actually runs, not hardcoded to the standard's published 48 kHz coefficients, so it stays
correct at 44.1 kHz too. The integrated reading's two-pass gating (absolute gate at -70 LUFS, then a
relative gate 10 LU below the resulting mean) is backed by a fixed-size histogram
(`LoudnessHistogram`, 750 buckets at the same 0.1 LU resolution EBU R128 itself requires for logged
values) rather than a growing list of every 400 ms block ever measured — deliberate, since an OB
session can run for hours or days and this is metering, not a one-shot file analysis. Wired into
`obcast-client/src/audio.rs`: `MeterState` now owns a `Loudness` alongside the existing VU/PPM/Peak
ballistics, fed the same post-gain `scratch_l`/`scratch_r` blocks every callback (mono duplicates
into both channels same as the other meters, which reads +3 LU relative to a true single-channel
signal — the technically correct BS.1770 behavior for genuinely-duplicated dual-mono, not a bug);
`AudioHandle::lufs()` exposes `(momentary, short_term, integrated)` and
`AudioHandle::reset_integrated_lufs()` clears just the gated history (e.g. for a new
programme/segment) via a flag the audio thread applies on its next callback, since the `Loudness`
instance itself lives on the audio thread and the GUI never touches it directly. Displayed in the
encoder GUI's Levels panel (`gui/app.rs::meter_panel`) as a compact M/S/I row with a "Reset I" button.

**Status bar overflow fix.** The encoder GUI's top status bar (`gui/app.rs::status_bar`) laid out the
Log toggle and Stop/Go Live button via `ui.with_layout(right_to_left, ...)` *after* a run of
unbounded-length left-side content (server `detail` strings, the latest operator log message) in a
plain `ui.horizontal` — which never wraps or clips, so a long log line or server detail pushed those
controls past the window's right edge instead of just crowding them. Rewritten with
`egui::Sides::new().shrink_left().truncate()`: the right side (fixed controls) lays out first at its
natural size, and the left side gets only whatever width remains, eliding overflow text with "…"
instead of growing past it — the controls now always stay on-screen regardless of message length.

**Fixed: seeking on the web remote played the new position, then audibly jumped back to the old
one.** Reported directly: "seek to a specific point... it starts playing from there but jumps back
to the point it was before." `Start`/`Seek` (`playout.rs`) only ever tore down and restarted the
ffmpeg decoder session — the shared cpal output ring buffer (`RING_SEGMENTS = 8`, up to ~16s at the
default 2s segments) and the `pending` seq-tracking queue that `drain_pending`/`position_seq` walk
were never cleared. Audio already decoded for the pre-jump position was still sitting in the ring
and kept draining out after the jump, so playout briefly reached the new position and then fell
back to that stale backlog — `drain_pending` reading `pending`'s leftover pre-seek entries dragged
`position_seq` back down with it the whole time it drained, which is exactly the "jumps back"
symptom (this is also the ring-buffer gap called out as still-open in the M5 entry above — this
closes it). Fixed with `flush_ring_and_pending`: `pending` is cleared directly (plain mutex,
reachable from the engine command thread), but the ring's consumer half lives only inside the
real-time cpal output callback closure, so it can't be touched directly from there — the command
loop instead bumps a `flush_generation` counter, the callback clears the ring (`consumer.clear()`)
on its next invocation and acks via `flushed_generation`, and the command loop waits briefly
(bounded to 200ms, so a stalled/paused device can't hang command processing) before letting the
freshly spawned decoder session start writing — otherwise a flush racing a few milliseconds late
could wipe the new session's first samples along with the stale ones.

**Automatic reconnect when the hardware audio device disconnects mid-session (server playout output
and client capture input).** Prompted by an operator question: "what happens if the audio device
disconnects, e.g. the mixer goes down?" Investigation found both sides *detected* and *surfaced* a
device error already (not silent), but neither *recovered*: the server's `cpal` output stream was
built once at engine startup and never rebuilt, so once its `err_fn` fired (or the configured device
was missing/mis-configured at startup), `device_error`/`PlayoutState::Error` stuck **permanently** —
`playout_state()` checks `device_error` first, and the only place that ever cleared it
(`restart_decoder_session`'s success path) is unrelated to the output device itself, so recovery
required restarting the server process for that stream. The client's capture `err_fn` at least
demoted `running`/set `last_error` cleanly, and an existing PCM-stall watchdog (`gui/app.rs`) already
dropped a dead pipeline out of "live" rather than broadcasting silence forever (the M6 fix) — but
still required the operator to manually click "Open" again once the mixer came back.
- **Server (`playout.rs`).** `run_engine`'s device/stream setup and its command-processing loop are
  now wrapped in an outer `'engine` reconnect loop. On any device/stream failure — missing device or
  no default config at startup, a failed `build_output_stream`, or the stream's `err_fn` firing
  mid-session (new `stream_failed: Arc<AtomicBool>`, checked once per inner-loop tick) — the loop
  tears down what it can and retries every `DEVICE_RETRY_INTERVAL` (3s) until the device/stream opens
  successfully again, rather than the old permanent `drain_forever`. `current` (the playout head)
  survives across reconnects, and if playout was (or was asked to be) running when the device died,
  it resumes automatically from the same seq once reconnected — no operator action needed. While the
  device is down, `wait_for_retry_or_shutdown` still drains and applies incoming `EngineCommand`s via
  `apply_command_offline` (state bookkeeping only — `current`/`running`/`position_seq`/etc. — since
  there's no stream/decoder to actually drive), so a `Stop`/`Start` issued mid-outage isn't lost and
  takes effect immediately rather than only once the retry clock happens to elapse. A hard mid-session
  failure can't safely reuse `teardown_session_safely` (which needs a *working* output callback to
  drain the ring while joining the old decoder's reader thread — exactly what's gone) — a new
  `abandon_decoder_session` kills the ffmpeg process but deliberately does not join the reader thread,
  which might be parked forever inside `push_all` waiting for ring space that a dead stream will never
  free again; a fresh ring/session is built for the next connection attempt regardless. Logging (the
  actual originating ask — "make sure the server logs it"): `report_device_error` logs loudly
  (`tracing::error!` plus a pushed web-remote log line, so it shows up in `stream.html`'s log panel,
  not just the server's own stdout) on the *first* failure of an outage, then quietly
  (`tracing::warn!` only) on each subsequent retry so a long outage doesn't spam the web remote; a
  successful reconnect logs "audio output device reconnected, resuming playout" at `info` level plus
  a matching web-remote log line. Five new `playout.rs` tests cover `apply_command_offline`'s state
  bookkeeping (`Start`/`Stop`/`Seek`/`SetVolume` while offline).
- **Client (`audio.rs`).** `run_engine`'s command loop switched from a blocking `cmd_rx.recv()` to
  `cmd_rx.recv_timeout(DEVICE_RETRY_INTERVAL)` (also 3s) so it wakes up periodically even with no new
  command. A new `last_open: Option<(String, String)>` remembers the most recently requested
  host/device; on each timeout wakeup, if a stream was open but `running` has gone false (the
  capture `err_fn` fired since the last wakeup — e.g. the mixer lost power), the loop begins retrying
  against that same host/device via a shared `try_open` helper (factored out of the `Open` command
  handler, which also now retries on a failed *explicit* open rather than giving up after one try).
  Retrying stops the moment `try_open` succeeds (logs "capture device reconnected") or the operator
  sends an explicit `Close`. Confirmed (via a read-only research pass over `gui/app.rs`) that nothing
  in the GUI assumes `AudioHandle::is_running()`/`last_error()` only change in direct response to a
  GUI-issued `Open` — the device panel, Go Live button enablement, and the PCM-stall watchdog are all
  re-evaluated fresh every frame with no edge-triggered/GUI-owned latch — so `running` flipping true
  again from a background retry (rather than a button click) is safe and requires no GUI changes.
  Deliberately unchanged: reconnecting the *capture device* does not itself resume the encoder's
  "live" (broadcasting) state — that still requires an explicit operator "Go Live" after an
  interruption of unknown length, per the existing M6 behavior; this fix is scoped to the device
  connection itself; per CLAUDE.md's own spirit, resuming *what* to broadcast after an unknown gap
  stays a deliberate operator decision, not something to automate silently.

---

## 9. Conventions

- Async for all network I/O (`tokio`); audio runs on dedicated threads/callbacks, never on the async
  executor.
- `cargo fmt` and `cargo clippy` must be clean; treat clippy warnings as errors in CI.
- `cargo test` must stay green — especially the scheduler suite. Add a test with every scheduler
  change; new behaviour without a test is not done.
- Keep `plan_uploads` and the `obcast-proto` types **pure and dependency-light** (serde only). No I/O,
  no async, no ffmpeg in that crate.
- Config via a TOML file + env overrides; secrets only via env, never committed.
- Structured logging (`tracing`); log every dropped/abandoned segment and every rung change.
- Don't break protocol wire-compat casually; if you change a schema, update `docs/protocol.md` in the
  same commit.

## 10. Non-goals (v1)

DRM; server-side transcoding (the client produces the ABR ladder); Low-Latency HLS (revisit later);
native mobile apps (web only); multi-tenant/multi-stream orchestration.

## 11. Glossary

**OB** outside broadcast · **ABR ladder** set of bitrate rungs · **rung/rendition** one bitrate
variant (the lowest-bitrate *enabled* rung is the survival rung — not necessarily id `0`; see §5) ·
**segment** one short media file, keyed `(rung, seq)` · **DVR window**
rolling buffer of retained segments · **live edge** newest segment · **playout head** the segment
being rendered to the server's HW output · **frontier** highest seq contiguously playable from the
head · **lead** ms of contiguous audio ahead of the head · **water levels** low/target/high buffer
thresholds the scheduler defends · **playout** decoding + sending audio to the server's hardware out.
