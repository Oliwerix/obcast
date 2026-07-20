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
  has no hook for bundling an externally-built, non-Cargo binary artifact.
- **De-duplicated `StreamProfile` construction in the client crate.** `client/main.rs`'s single-use
  `fn profile(segment_ms) -> StreamProfile` wrapper (just `StreamProfile::default_ladder(segment_ms)`)
  was deleted and inlined at its one call site. `client/gui/app.rs`'s `profile(&self)` method was left
  as-is — it's a genuine 5-call-site accessor now doing real work (`.filtered()` against the operator's
  enabled-rungs config, from the ABR ladder rework above), not redundant indirection.

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

**What's next.** Everything from the previous "auth split / resource leak / DVR reaping / stalled
playout state / waveform decode failures / reverse telemetry / packaging / StreamProfile dedup /
browser DVR scrub / HE-AAC survival rung / scheduler edge-case tests" punch list is now DONE — see the
milestone entries above (M7's auth-split and resource-leak fixes; the DVR-file-reaping, stalled-state,
and waveform-decode-failure fixes folded into store.rs/playout.rs/waveform.rs; the reverse-telemetry
heartbeat wiring in store.rs/api.rs; and the ABR ladder rework / browser DVR scrub / packaging /
StreamProfile dedup entries just above), including the two items this section used to list as
follow-ups: the HE-AAC segment-alignment risk is now measured (see that entry) rather than
theoretical, and `libfdk_aac` ffmpeg bundling has a working, CI-verified build for every `dist`
target. `build-libfdk-ffmpeg.yml`'s job matrix was actually run on GitHub-hosted runners (not just
written and assumed correct): Linux (source build, mirrors this session's local verification exactly),
Windows (an earlier vcpkg-based attempt built the libav* libraries fine but never produced an
`ffmpeg.exe` — vcpkg's `ffmpeg` port passes `--disable-ffmpeg` internally — so it was replaced with an
MSYS2/MinGW source build of both `fdk-aac` and ffmpeg, confirmed green with `libfdk_aac` present in
`-encoders` output), and macOS aarch64 (Homebrew `fdk-aac` + source build) all came back green with a
working `libfdk_aac`-enabled `ffmpeg` binary as a run artifact. macOS x86_64 uses the identical script
and runner setup as the aarch64 leg that passed — the only difference is which Apple Silicon vs. Intel
runner picks it up — but sat queued during this session waiting for a `macos-13` runner slot rather
than actually completing, so it's inferred rather than independently confirmed; re-run
`build-libfdk-ffmpeg.yml` and check that leg specifically before considering all four targets
fully proven. Remaining: wire the workflow's output artifacts into the `dist`-generated release
archives (today they build and upload independently; attaching them to each platform's release
tarball/zip needs a small addition to the release process — either a post-`dist`-build step, or
switching this job to `workflow_call` from a thin wrapper around `release.yml`'s own trigger).

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
