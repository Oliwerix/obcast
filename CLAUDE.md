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
| Encode / decode    | `ffmpeg` via subprocess, one process → N rungs (currently AAC-LC on every rung; native `aac` can't emit HE-AAC, so the low rung needs `libfdk_aac` to get its intended win). `symphonia` for decode-only paths |
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
- **Rungs are ordered low→high; rung 0 is the survival rung.** The scheduler always has a cheap
  option to guarantee continuity.
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
- **M4 — DONE (server) / PARTIAL (web listen).** HLS origin (master + per-rung sliding-window
  playlists, best-rung fallback) done. Web has hls.js listen-along, but the scrub bar drives *server*
  playout via the waveform — browser-side DVR scrub of the listen-along player itself is **OPEN**.
- **M5 — DONE.** Playout to hardware out via `cpal` (dedicated thread, ffmpeg decode, ring buffer,
  atomics for pos/state/vol/meters), start/stop + seek; head flows into `ServerState` (holds rather
  than skips on a not-yet-on-disk segment).
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
- **M7 — PARTIAL.** Done: optional single-token ingest auth (`X-Auth`); SSE + uploader reconnect on
  drop; client-side abandon + a bounded stall timeout in `playout.rs` (a permanent segment gap no
  longer stalls the playout head indefinitely — verified live: an unfilled gap gets skipped after
  `3 * segment_ms`, and an explicit `/abandon` unsticks it immediately); stale-`ServerState` fallback
  (client now discards state older than 5 s, and the server seeds a fresh store's `rev` from
  wall-clock epoch millis so a restart can no longer look "older" than what a client already held —
  see §5 for both). Open: auth split (separate ingest/control/listen tokens) — note the control-plane
  POST (`/api/{stream}/playout`) has **no auth check at all** today, unlike ingest's `X-Auth`, so
  anyone reachable can stop/seek/volume any stream's hardware output; server-side DVR file reaping
  (`evict_old()` drops index entries but the underlying `.ts` files are never deleted — unbounded disk
  growth on a long OB, verified by Rust review); reverse telemetry path (client sends `EncoderState`;
  server populates `ControlStatus.encoder` + real `throughput_kbps`, incl. the
  `POST /ingest/{stream}/heartbeat` route `docs/protocol.md` already documents — confirmed purely
  cosmetic/dashboard-visibility, not load-bearing for scheduling); packaging (server Docker, static
  client binaries); an unbounded resource leak where any GET against a new/typo'd stream name spawns
  a permanent OS thread + `DvrStore` with no idle reaping (only explicit `DELETE /api/shows/{name}`
  tears one down — verified by adversarial review); `playout_state()` reports `Playing` from the
  `running` flag alone, so it (and the web pill) can say "playing" while the output is genuinely
  silent (cpal zero-fills underruns) — the new stall-skip logging at least surfaces *why*, but the
  state itself is still not a lie-proof signal; `waveform.rs` silently swallows any per-segment decode
  failure as a flat `(0,0)` line, indistinguishable from real silence.

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
stale, flagged as "(estimated)" in the UI.

**What's next (priority order, re-ranked after Rust/design review and a follow-up adversarial
review — correctness/security risks before features):** (1) Auth split, and in particular give
`/api/{stream}/playout` *some* auth check — right now it has none at all, unlike ingest, so any
reachable client can stop/seek/volume a live hardware output. (2) Fix the per-stream resource leak:
any GET against a new stream name spawns a permanent OS thread + `DvrStore` with no idle timeout, so
unauthenticated probing/typos leak threads forever — pair naturally with (1) since gating the routes
that auto-vivify a stream also bounds this. (3) Server-side DVR file reaping so `.ts` files are
actually deleted when evicted from the index (currently unbounded disk growth on a long OB). (4) Make
`playout_state()`/`ServerState` distinguish "playing" from "playing but the head is stalled/skipping"
— today a stalled or silently-underrunning playout still reports plain `Playing`. (5) Stop
`waveform.rs` from swallowing per-segment decode failures as a flat `(0,0)` line; surface the failure
instead so it isn't indistinguishable from real silence. (6) Close the reverse telemetry path — client
sends `EncoderState` periodically, server fills `ControlStatus.encoder` + real `throughput_kbps`,
wiring the documented `/ingest/{stream}/heartbeat` route (confirmed cosmetic/dashboard-only; the core
server→encoder loop of §1 is intact and must stay that way). (7) Packaging (Docker + cargo-dist).
(8) De-duplicate `StreamProfile` (currently copied in `server/main.rs`, `client/main.rs`,
`client/gui/app.rs`) into one shared/config-driven source. (9) Lower priority: browser-side DVR scrub
for listen-along; a true HE-AAC survival rung via `libfdk_aac` (native `aac` emits LC for all rungs);
add scheduler test coverage for the coverage-window-vs-far-behind-head case and misconfigured water
levels, both identified as untested edge cases by adversarial review.

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
variant (rung 0 = survival) · **segment** one short media file, keyed `(rung, seq)` · **DVR window**
rolling buffer of retained segments · **live edge** newest segment · **playout head** the segment
being rendered to the server's HW output · **frontier** highest seq contiguously playable from the
head · **lead** ms of contiguous audio ahead of the head · **water levels** low/target/high buffer
thresholds the scheduler defends · **playout** decoding + sending audio to the server's hardware out.
