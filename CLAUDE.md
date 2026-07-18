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
| Web listen / UI    | `hls.js` + `peaks.js`, loaded from CDN by a single static `web/remote/index.html` — no bundler/build step |
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
    remote/index.html              # static web remote (control + listen + waveform), no build step
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
`buffer` module. The web remote is a single dependency-free `web/remote/index.html` pulling `hls.js`
and `peaks.js` from CDNs.

---

## 5. Key concepts & invariants

- **Segment is the atom.** Short (default **2 s**) MPEG-TS/AAC segments. Shorter = lower latency and
  finer retry granularity; longer = less overhead. `StreamProfile::segment_ms` is configurable.
- **`Seq` is the canonical clock.** Every segment is `(RungId, Seq)`. Wall-clock is secondary.
- **Ingest is idempotent.** Re-uploading `(rung, seq)` is safe. Retries and reconnect-resume are
  therefore trivial — never add stateful upload handshakes that break this.
- **Never block the live edge / playout head.** Keep the segments about to be played flowing. If a
  segment can't be sent within its retry budget, abandon it (tell the server) rather than stall.
  **Not yet enforced end-to-end** (verified by design review): the client never calls `/abandon`,
  and `playout.rs` has no timeout/skip on a missing segment — a permanent gap freezes the playout
  head indefinitely instead of skipping past it. See §8 M7.
- **No audio lost to a short outage.** Segments persist on disk until acked and within the DVR
  window; on reconnect, resume from the oldest un-acked segment.
- **Rungs are ordered low→high; rung 0 is the survival rung.** The scheduler always has a cheap
  option to guarantee continuity.
- **The server owns the published playlist** and rebuilds it from what it actually received.
- **The feedback loop degrades safely.** If `ServerState` goes stale, the encoder assumes
  `ServerState::unknown()` (stopped, empty buffer) and plays it safe: low rung, protect the live edge.
  **Not yet implemented** (verified by design review): there is no staleness timeout on the client's
  held `ServerState` today, and a server restart resets `rev` to 0, which makes the client's rev-gate
  reject the fresh post-restart state and pin the stale one. See §8 M7.

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
- **M6 — DONE.** Control API (REST `status`/`playout` + WS status/position/meters) + web remote UI
  (start/stop/seek, health panel, VU meters, waveform seek).
- **M7 — PARTIAL / mostly OPEN.** Done: optional single-token ingest auth (`X-Auth`); SSE + uploader
  reconnect on drop. Open: auth split (separate ingest/control/listen tokens); client-side
  abandon + retry budget (**correctness risk, not just a missing feature** — a permanent segment gap
  currently stalls the playout head indefinitely, see §5); stale-`ServerState` fallback (no staleness
  timeout, plus a server-restart `rev`-reset bug that pins the client on stale state, see §5); server-side
  DVR file reaping (`evict_old()` drops index entries but the underlying `.ts` files are never deleted —
  unbounded disk growth on a long OB, verified by Rust review); reverse telemetry path (client sends
  `EncoderState`; server populates `ControlStatus.encoder` + real `throughput_kbps`, incl. the
  `POST /ingest/{stream}/heartbeat` route `docs/protocol.md` already documents — confirmed purely
  cosmetic/dashboard-visibility, not load-bearing for scheduling); packaging (server Docker, static
  client binaries).

**Beyond the roadmap (built, not on the original M-list):** a BBC peaks.js quality-colored waveform
(`server/waveform.rs` + `GET /api/{stream}/waveform`, color-coded by ABR rung with click-to-seek);
live channel-mapping capture (pick 2 of N device channels as L/R); K-14 + per-channel metering;
persisted TOML operator config; a `--headless` client path (ffmpeg captures a device or sine tone
directly, no GUI); `README.md` + `docs/getting-started.md`.

**What's next (priority order, re-ranked after Rust/design review — correctness risks before
features):** (1) Client-side abandon + a timeout/skip in `playout.rs` for a missing segment, so a
permanent gap can no longer stall the playout head indefinitely — this is the highest-priority item,
it violates the §5 "never block the playout head" invariant today. (2) Stale-`ServerState` fallback:
add a staleness timeout on the client's held state, and fix the server-restart `rev`-reset bug that
pins the client on stale state — implements the §5 "degrades safely" invariant, which currently isn't
enforced. (3) Server-side DVR file reaping so `.ts` files are actually deleted when evicted from the
index (currently unbounded disk growth on a long OB). (4) Close the reverse telemetry path — client
sends `EncoderState` periodically, server fills `ControlStatus.encoder` + real `throughput_kbps`,
wiring the documented `/ingest/{stream}/heartbeat` route (confirmed cosmetic/dashboard-only; the core
server→encoder loop of §1 is intact and must stay that way). (5) Auth split. (6) Packaging (Docker +
cargo-dist). (7) De-duplicate `StreamProfile` (currently copied in `server/main.rs`, `client/main.rs`,
`client/gui/app.rs`) into one shared/config-driven source. (8) Lower priority: browser-side DVR scrub
for listen-along; a true HE-AAC survival rung via `libfdk_aac` (native `aac` emits LC for all rungs).

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
