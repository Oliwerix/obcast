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
| Encode / decode    | `ffmpeg` via subprocess (AAC-LC; HE-AAC low rung). `symphonia` for decode-only paths |
| Client GUI         | `egui`/`eframe` (simplest cross-platform), or Tauri if a web UI is preferred |
| DVR store          | Filesystem segments + in-memory index (optionally `rusqlite`)     |
| Web listen / UI    | `hls.js` (native HLS on Safari); small Vite app                   |
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
  crates/
    obcast-proto/                  # DONE (built + unit-tested)
      src/
        lib.rs
        state.rs                   # ServerState, EncoderState, StreamProfile, water levels
        control.rs                 # PlayoutCommand, ControlStatus, ControlEvent, ...
        scheduler.rs               # plan_uploads(): the closed-loop core + tests
    obcast-server/                 # SKELETON — build out ingest/DVR/origin/playout/control
      src/main.rs
    obcast-client/                 # SKELETON — build out capture/encode/buffer/uploader/gui
      src/main.rs
```

When the server/client crates grow, use modules like `ingest/`, `store/`, `origin/`, `playout/`,
`control/` (server) and `audio/`, `encode/`, `buffer/`, `upload/`, `gui/` (client).

---

## 5. Key concepts & invariants

- **Segment is the atom.** Short (default **2 s**) MPEG-TS/AAC segments. Shorter = lower latency and
  finer retry granularity; longer = less overhead. `StreamProfile::segment_ms` is configurable.
- **`Seq` is the canonical clock.** Every segment is `(RungId, Seq)`. Wall-clock is secondary.
- **Ingest is idempotent.** Re-uploading `(rung, seq)` is safe. Retries and reconnect-resume are
  therefore trivial — never add stateful upload handshakes that break this.
- **Never block the live edge / playout head.** Keep the segments about to be played flowing. If a
  segment can't be sent within its retry budget, abandon it (tell the server) rather than stall.
- **No audio lost to a short outage.** Segments persist on disk until acked and within the DVR
  window; on reconnect, resume from the oldest un-acked segment.
- **Rungs are ordered low→high; rung 0 is the survival rung.** The scheduler always has a cheap
  option to guarantee continuity.
- **The server owns the published playlist** and rebuilds it from what it actually received.
- **The feedback loop degrades safely.** If `ServerState` goes stale, the encoder assumes
  `ServerState::unknown()` (stopped, empty buffer) and plays it safe: low rung, protect the live edge.

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
- Control: `GET /api/status` → `ControlStatus`; `POST /api/playout` ← `PlayoutCommand`
  (`start`/`stop`/`pause`/`resume`/`seek`/`go_live`/`set_device`/`set_volume`); `WS /api/ws` streams
  `ControlEvent` (status, position, meters, ack).
- Listen: `GET /hls/{stream}/master.m3u8`, `/{rendition}/index.m3u8`, `/{rendition}/{seq}.ts`.

The `obcast-proto` Rust types are the source of truth for all these schemas.

---

## 8. Build milestones

- **M0 — DONE.** Workspace, `obcast-proto` (state/control types + `plan_uploads` with tests),
  `docs/protocol.md`, server/client skeletons.
- **M1** Server: ingest endpoint + DVR store + `ServerState` computation + SSE state feed. Stand up
  a running loop that emits real feedback.
- **M2** Client: `cpal` capture + device selection + level meters + `egui` shell.
- **M3** Client: `ffmpeg` ABR encode → disk ring buffer; uploader loop driving `plan_uploads`
  against the real server (or a mock).
- **M4** Server: HLS origin (master + per-rung playlists over the DVR window). Web listen with
  hls.js + scrub bar.
- **M5** Server: playout to hardware out via `cpal`, with start/stop + seek; wire the head into
  `ServerState`.
- **M6** Control API (REST + WS) + web remote UI (start/stop/seek server output, health panel).
- **M7** Auth (separate ingest/control/listen tokens), reconnect-resume/abandon hardening,
  packaging (static client binaries, server Docker), telemetry.

Prefer a thin end-to-end slice (M1 → M3 minimal → M4 listen) before deepening playout and control.

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
