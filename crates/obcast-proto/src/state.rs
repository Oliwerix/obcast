//! Shared-state types exchanged on the encoder <-> server *link plane*.
//!
//! The whole design rests on both ends knowing each other's state:
//!  - the **server** tells the encoder where playout is, how much contiguous
//!    audio sits ahead of that point, and where the quality holes are;
//!  - the **encoder** tells the server what it has produced, what rung it is
//!    currently prioritizing, and which segments it has permanently abandoned.
//!
//! `rev` fields are monotonic so a receiver can discard stale/out-of-order
//! feedback that arrives late over a flaky link.

use serde::{Deserialize, Serialize};

/// Segment sequence number. Monotonic, gap-free at the encoder; the canonical
/// clock of the system. `segment_ms` converts seq deltas to wall time.
pub type Seq = u64;

/// Quality rung id. Ordered low -> high by the position in [`StreamProfile::rungs`];
/// rung `0` is always the "survival" rung sent to prevent dropout.
pub type RungId = u8;

/// One encoded quality rung.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rung {
    pub id: RungId,
    /// Human label, e.g. "lo", "mid", "hd".
    pub name: String,
    pub bitrate_kbps: u32,
}

/// Static description of a stream: segment length and the ABR ladder.
/// `rungs` MUST be sorted ascending by bitrate; index 0 is the survival rung.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamProfile {
    pub segment_ms: u32,
    pub rungs: Vec<Rung>,
}

impl StreamProfile {
    /// The one ABR ladder every OBCast component ships today (32/128/320
    /// kbps AAC, low/mid/hd) — previously copied separately in
    /// `obcast-server::main`, `obcast-client::main`, and
    /// `obcast-client::gui::app`, which could silently drift out of sync.
    /// `segment_ms` is the only axis that's actually configurable per run.
    pub fn default_ladder(segment_ms: u32) -> Self {
        Self {
            segment_ms,
            rungs: vec![
                Rung {
                    id: 0,
                    name: "lo".into(),
                    bitrate_kbps: 32,
                },
                Rung {
                    id: 1,
                    name: "mid".into(),
                    bitrate_kbps: 128,
                },
                Rung {
                    id: 2,
                    name: "hd".into(),
                    bitrate_kbps: 320,
                },
            ],
        }
    }

    pub fn low_rung(&self) -> RungId {
        self.rungs.first().map(|r| r.id).unwrap_or(0)
    }
    pub fn top_rung(&self) -> RungId {
        self.rungs.last().map(|r| r.id).unwrap_or(0)
    }
    pub fn bitrate_of(&self, rung: RungId) -> u32 {
        self.rungs
            .iter()
            .find(|r| r.id == rung)
            .map(|r| r.bitrate_kbps)
            .unwrap_or(0)
    }
    /// Next rung strictly above `rung`, if any (one incremental step up).
    pub fn rung_above(&self, rung: RungId) -> Option<RungId> {
        self.rungs
            .iter()
            .filter(|r| r.id > rung)
            .min_by_key(|r| r.bitrate_kbps)
            .map(|r| r.id)
    }
}

/// Playout engine state on the server (drives the hardware audio out).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlayoutState {
    Stopped,
    Playing,
    Paused,
    /// Running and not paused, but the hardware output is not actually
    /// rendering real audio right now — e.g. `cpal` is zero-filling an
    /// underrun because decode/segment availability can't keep pace, or the
    /// stall-skip backstop (see `playout.rs`) is bridging a missing segment.
    /// Distinct from `Playing` so operator dashboards can't mistake silence
    /// for real playback.
    Stalled,
    /// The playout engine itself is broken — e.g. the configured hardware
    /// output device failed to open — and cannot produce audio at all until
    /// the server is reconfigured/restarted. Distinct from `Stopped` (which
    /// just means nobody asked it to play) so operator dashboards can tell
    /// "idle" apart from "broken." See `PlayoutStatus::detail` for why.
    Error,
}

/// Full playout status snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlayoutStatus {
    pub state: PlayoutState,
    /// Segment currently being rendered. `None` when stopped.
    pub position_seq: Option<Seq>,
    /// Selected hardware output device id, if any.
    pub device: Option<String>,
    /// Linear gain 0.0..=1.0 (or higher for boost).
    pub volume: f32,
    /// Human-readable reason behind `Error` (device/stream failure) or
    /// `Stalled` (why the head isn't producing audible audio right now);
    /// `None` for `Stopped`/`Playing`/`Paused` and whenever the specific
    /// cause isn't known. Lets operator UIs answer "stalled/errored — why?"
    /// instead of just showing a color.
    pub detail: Option<String>,
    /// True while the 1kHz test-tone pattern (see `PlayoutCommand::SetTestTone`)
    /// is overriding the hardware output, independent of `state`.
    #[serde(default)]
    pub test_tone: bool,
}

/// Buffer thresholds (in ms of contiguous audio ahead of the playout head) that
/// the encoder is expected to defend. The scheduler reads these directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaterLevels {
    /// Below this: SURVIVAL. Send only the lowest rung, extend the frontier,
    /// cancel all quality upgrades.
    pub low_ms: u32,
    /// The margin the encoder should aim to keep ahead of the playout head.
    pub target_ms: u32,
    /// At/above this (and with spare bandwidth) quality upgrades are permitted.
    pub high_ms: u32,
}

impl Default for WaterLevels {
    fn default() -> Self {
        Self {
            low_ms: 4_000,
            target_ms: 12_000,
            high_ms: 20_000,
        }
    }
}

/// The best rung the server currently holds for one segment.
/// `best_rung == None` means the segment is missing entirely (a gap).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegCoverage {
    pub seq: Seq,
    pub best_rung: Option<RungId>,
}

/// SERVER -> ENCODER feedback. Sent on the link plane (SSE feed and piggybacked
/// on every segment-upload response). This is what lets the encoder aim its
/// bandwidth at exactly the segments that will actually be played.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServerState {
    /// Monotonic revision for staleness detection.
    pub rev: u64,
    /// Newest seq the server holds at any rung.
    pub live_seq: Option<Seq>,
    /// Oldest seq retained in the DVR window.
    pub dvr_start_seq: Option<Seq>,
    pub playout: PlayoutStatus,
    /// Highest seq F such that every segment in [anchor, F] is present at >=1
    /// rung (contiguous playable frontier). See [`crate::scheduler`] for how the
    /// anchor is chosen. `None` if nothing playable is buffered.
    pub frontier_seq: Option<Seq>,
    /// Contiguous ms available ahead of the playout head (server-computed
    /// convenience; the scheduler can also derive it from `coverage`).
    pub lead_ms: u32,
    pub water: WaterLevels,
    /// Per-seq best rung for a bounded window ahead of the anchor. Lets the
    /// encoder see precisely where HD is missing without guessing.
    pub coverage: Vec<SegCoverage>,
}

impl ServerState {
    /// A conservative "we've heard nothing" default: assume stopped, empty
    /// buffer, so the encoder falls back to safe low-rung behaviour.
    pub fn unknown() -> Self {
        Self {
            rev: 0,
            live_seq: None,
            dvr_start_seq: None,
            playout: PlayoutStatus {
                state: PlayoutState::Stopped,
                position_seq: None,
                device: None,
                volume: 1.0,
                detail: None,
                test_tone: false,
            },
            frontier_seq: None,
            lead_ms: 0,
            water: WaterLevels::default(),
            coverage: Vec::new(),
        }
    }
}

/// ENCODER -> SERVER telemetry. Lets the server render link health for web
/// operators and stop waiting on segments the encoder has given up on.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EncoderState {
    pub rev: u64,
    /// Rungs currently being produced by the local encoder.
    pub active_rungs: Vec<RungId>,
    /// Highest seq produced locally.
    pub encoded_seq: Option<Seq>,
    /// Rung the uploader is currently prioritizing.
    pub primary_rung: RungId,
    /// Recent measured upload throughput.
    pub throughput_kbps: u32,
    /// Segments queued locally and not yet acked by the server.
    pub backlog: u32,
    /// Seqs the encoder will never send (permanent gaps) so the server can stop
    /// blocking playout on them.
    pub abandoned: Vec<Seq>,
}
