//! The encoder's model of `ServerState`, updated by both the SSE feed and
//! every upload response (the piggyback path is usually the fresher one).
//! Also carries small upload telemetry and an operator-facing status/error
//! log for the GUI status panel — read via `try_lock`/atomics so the GUI's
//! per-frame poll never blocks on network tasks.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use obcast_proto::control::{LogEntry, LogLevel};
use obcast_proto::state::{RungId, Seq, ServerState};
use tokio::sync::Mutex;

/// How long a held `ServerState` is trusted before the feedback loop degrades
/// to `ServerState::unknown()` (low rung, protect the live edge) per
/// CLAUDE.md §5. A silently stalled SSE connection or upload-reply feed
/// otherwise pins the encoder on arbitrarily old data forever — there was
/// previously no staleness check at all. Matches the server's own
/// link-down window (`STALE_AFTER` in `obcast-server/src/api.rs`).
pub(crate) const STALE_AFTER: Duration = Duration::from_secs(5);

/// Cap on retained client-side log lines — enough operator history for a
/// session (device errors, upload/abandon warnings, ffmpeg pipeline
/// lifecycle) without unbounded growth; oldest entries are dropped first.
const LOG_CAP: usize = 200;

/// Cap on the retained (seq -> rung) upload history used to guess the
/// on-air quality when the link's gone quiet (see `playing_quality`).
/// Generous relative to any real DVR window (a couple hours at a 2s
/// segment length) without growing unbounded across a long OB.
const UPLOAD_HISTORY_CAP: usize = 4096;

/// What quality is (probably) reaching listeners right now, from
/// `SharedState::playing_quality`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QualityEstimate {
    pub rung: RungId,
    pub seq: Seq,
    /// `false` when read straight from a fresh `ServerState` (ground truth);
    /// `true` when the link's gone stale and this was extrapolated from our
    /// own upload history instead — see `playing_quality`.
    pub estimated: bool,
}

pub struct SharedState {
    pub server: Mutex<ServerState>,
    /// When `server` was last refreshed by `update()`. `None` until the first
    /// state ever arrives.
    server_updated_at: Mutex<Option<Instant>>,
    last_uploaded_seq: AtomicI64,
    throughput_kbps: AtomicU32,
    /// Rung the scheduler most recently prioritized (`plan_uploads`'
    /// first/most-urgent action each tick, mirroring `EncoderState::primary_rung`
    /// in the last heartbeat) — the reference bitrate for the bandwidth meter.
    primary_rung: AtomicU32,
    /// Which rung we uploaded for each seq, so `playing_quality` can guess
    /// what's on air from our own upload record when the link (and thus
    /// `ServerState`) has gone stale. Bounded by `UPLOAD_HISTORY_CAP`.
    upload_history: std::sync::Mutex<BTreeMap<Seq, RungId>>,
    /// Operator-facing status/error log — a capped ring buffer, oldest first.
    /// Shares `obcast_proto::control::LogEntry`'s shape with the server's own
    /// operator log so both UIs render the same way, even though a client
    /// entry never crosses the wire. Read by the GUI each frame (via
    /// `recent_log`/`latest_log`) so a dead "live" session surfaces instead
    /// of silently producing nothing.
    log: std::sync::Mutex<VecDeque<LogEntry>>,
    /// Total number of log lines ever pushed (never decremented, even as
    /// `log` itself evicts old entries at `LOG_CAP`). Lets the GUI's status
    /// bar dismiss the latest entry without an unstable dismissal key: it
    /// just remembers the counter value at the moment of dismissal and
    /// compares against the current value (see `log_seq`/the status bar's
    /// infobar-dismiss handling in `gui/app.rs`).
    log_seq: AtomicU64,
    /// Latched one-shot: an `Error`-level line was logged since the GUI last
    /// checked, so the GUI should drop its own `live` state back to idle.
    encoder_failed: AtomicBool,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            server: Mutex::new(ServerState::unknown()),
            server_updated_at: Mutex::new(None),
            last_uploaded_seq: AtomicI64::new(-1),
            throughput_kbps: AtomicU32::new(0),
            primary_rung: AtomicU32::new(0),
            upload_history: std::sync::Mutex::new(BTreeMap::new()),
            log: std::sync::Mutex::new(VecDeque::new()),
            log_seq: AtomicU64::new(0),
            encoder_failed: AtomicBool::new(false),
        }
    }

    /// Append a status/error line to the operator-facing log. `Error`-level
    /// entries also latch `encoder_failed`, so a dead pipeline flips the GUI
    /// out of "live" without every call site needing to track that
    /// separately (see `take_encoder_failed`).
    /// Returns the `log_seq` value assigned to this entry, so a caller that
    /// needs to refer back to this exact line later (e.g. the GUI's alarm
    /// highlight — see `gui::app::ObcastApp::fire_alarm`) doesn't need a
    /// second, racy lookup.
    pub fn push_log(&self, level: LogLevel, message: impl Into<String>) -> u64 {
        let entry = LogEntry {
            at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            level,
            message: message.into(),
        };
        if level == LogLevel::Error {
            self.encoder_failed.store(true, Ordering::Relaxed);
        }
        let seq = self.log_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let mut log = self.log.lock().unwrap();
        log.push_back(entry);
        while log.len() > LOG_CAP {
            log.pop_front();
        }
        seq
    }

    /// Snapshot of the retained log, oldest first, for the GUI panel to
    /// render each frame.
    pub fn recent_log(&self) -> Vec<LogEntry> {
        self.log.lock().unwrap().iter().cloned().collect()
    }

    /// The single most recent log line, if any — cheap enough to poll every
    /// GUI frame for the status bar's compact summary without cloning the
    /// whole ring buffer (that's `recent_log`, used only while the full
    /// panel is open).
    pub fn latest_log(&self) -> Option<LogEntry> {
        self.log.lock().unwrap().back().cloned()
    }

    /// Current value of the monotonic log counter — pair with `latest_log`
    /// so the GUI can dismiss the infobar's summary of it and only have it
    /// reappear once a genuinely new line is pushed (see `log_seq` field doc).
    pub fn log_seq(&self) -> u64 {
        self.log_seq.load(Ordering::Relaxed)
    }

    /// Returns true exactly once after an `Error`-level line was logged, so
    /// the GUI can flip itself out of "live" without repeatedly fighting the
    /// operator.
    pub fn take_encoder_failed(&self) -> bool {
        self.encoder_failed.swap(false, Ordering::Relaxed)
    }

    /// Discard stale/out-of-order feedback per the link-plane contract.
    pub async fn update(&self, new_state: ServerState) {
        let mut cur = self.server.lock().await;
        if new_state.rev >= cur.rev {
            *cur = new_state;
            *self.server_updated_at.lock().await = Some(Instant::now());
        }
    }

    /// The held `ServerState`, or `ServerState::unknown()` if it hasn't been
    /// refreshed within `STALE_AFTER` — the scheduler then plays safe (low
    /// rung, protect the live edge) instead of trusting a feed that may have
    /// gone silent, per CLAUDE.md §5 ("the feedback loop degrades safely").
    pub async fn server_state_or_unknown(&self) -> ServerState {
        let fresh = self
            .server_updated_at
            .lock()
            .await
            .is_some_and(|t| t.elapsed() < STALE_AFTER);
        if fresh {
            self.server.lock().await.clone()
        } else {
            ServerState::unknown()
        }
    }

    /// The held `ServerState` together with how long it's been since
    /// `update()` last refreshed it. Lets callers extrapolate values that
    /// should keep moving in real time even once the feed itself has gone
    /// quiet — e.g. the Link panel's buffer graph, which should show the
    /// buffer/lead draining as playout consumes it (or the server's DVR
    /// window evicts its oldest end) with nothing coming in to replenish
    /// it, rather than freezing at the last number the server happened to
    /// report. `None` only on lock contention (never blocks).
    pub fn server_snapshot(&self) -> Option<(ServerState, Duration)> {
        let state = self.server.try_lock().ok()?.clone();
        let age = self
            .server_updated_at
            .try_lock()
            .ok()?
            .map(|t| t.elapsed())
            .unwrap_or(Duration::ZERO);
        Some((state, age))
    }

    pub fn note_upload(&self, seq: u64, throughput_kbps: u32) {
        self.last_uploaded_seq.store(seq as i64, Ordering::Relaxed);
        self.throughput_kbps
            .store(throughput_kbps, Ordering::Relaxed);
    }

    pub fn last_uploaded_seq(&self) -> Option<u64> {
        let v = self.last_uploaded_seq.load(Ordering::Relaxed);
        (v >= 0).then_some(v as u64)
    }

    pub fn throughput_kbps(&self) -> u32 {
        self.throughput_kbps.load(Ordering::Relaxed)
    }

    /// Records which rung the scheduler most recently prioritized, for the
    /// bandwidth meter (see `playing_quality`'s sibling read, `primary_rung`).
    pub fn note_primary_rung(&self, rung: RungId) {
        self.primary_rung.store(rung as u32, Ordering::Relaxed);
    }

    pub fn primary_rung(&self) -> RungId {
        self.primary_rung.load(Ordering::Relaxed) as RungId
    }

    /// Records that `seq` was successfully uploaded at `rung`, for
    /// `playing_quality`'s stale-link fallback. Bounded to
    /// `UPLOAD_HISTORY_CAP` entries, oldest seq evicted first.
    pub fn note_sent(&self, seq: Seq, rung: RungId) {
        let mut hist = self.upload_history.lock().unwrap();
        hist.insert(seq, rung);
        while hist.len() > UPLOAD_HISTORY_CAP {
            let Some(&oldest) = hist.keys().next() else {
                break;
            };
            hist.remove(&oldest);
        }
    }

    /// Best guess at the rung currently reaching listeners. While the link
    /// is fresh this is ground truth, read straight off
    /// `ServerState.playout.playing_rung` — the rung the server's playout
    /// engine actually fed its decoder for the segment now draining, tracked
    /// independently of the DVR index (see `PlayoutStatus::playing_rung`'s
    /// doc comment on `obcast-proto`). This is deliberately *not* a lookup
    /// of "best rung for this seq" against `ServerState.coverage`: the
    /// engine feeds segments many seconds ahead of real-time output, so a
    /// quality upgrade for an already-fed segment can land on disk before
    /// that segment is actually heard, and a `coverage` lookup would then
    /// report the new rung while the speaker is still on the old one — the
    /// exact "GUI says HD, server is playing low" bug this field exists to
    /// prevent. Once the feed's gone stale (no fresh `ServerState` within
    /// `STALE_AFTER`), there's nothing authoritative left to read, so this
    /// extrapolates instead: assume the head has kept advancing at roughly
    /// one segment per `segment_ms` since the last state we actually heard,
    /// then look up which rung *we* sent for that seq in `upload_history`.
    /// Best-effort — flagged via `QualityEstimate::estimated` so the GUI can
    /// label it as a guess rather than fact.
    pub fn playing_quality(&self, segment_ms: u32) -> Option<QualityEstimate> {
        let server = self.server.try_lock().ok()?;
        let pos = server.playout.position_seq?;
        let updated_at = *self.server_updated_at.try_lock().ok()?;
        let fresh = updated_at.is_some_and(|t| t.elapsed() < STALE_AFTER);

        if fresh {
            let rung = server.playout.playing_rung?;
            return Some(QualityEstimate {
                rung,
                seq: pos,
                estimated: false,
            });
        }

        let elapsed_ms = updated_at?.elapsed().as_millis() as u64;
        let guessed_seq = pos + elapsed_ms / (segment_ms.max(1) as u64);
        let hist = self.upload_history.lock().unwrap();
        let rung = *hist.range(..=guessed_seq).next_back()?.1;
        Some(QualityEstimate {
            rung,
            seq: guessed_seq,
            estimated: true,
        })
    }
}
