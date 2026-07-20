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

/// Which AAC profile a rung is encoded with. HE-AAC roughly doubles the
/// perceptual quality-per-bit of plain LC at low bitrates, which is why the
/// survival rung wants it — but it requires an ffmpeg build with
/// `libfdk_aac` (native `aac` can only emit LC); see `encode.rs`'s
/// auto-detect + fallback.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AacCodec {
    /// AAC-LC via ffmpeg's native `aac` encoder. HLS `CODECS` tag `mp4a.40.2`.
    Lc,
    /// HE-AAC (v1/SBR) via `libfdk_aac`. HLS `CODECS` tag `mp4a.40.5`.
    He,
}

/// One encoded quality rung.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rung {
    pub id: RungId,
    /// Human label, e.g. "lo", "mid", "hd".
    pub name: String,
    pub bitrate_kbps: u32,
    pub codec: AacCodec,
}

/// Static description of a stream: segment length and the full ABR ladder.
/// `rungs` MUST be sorted ascending by bitrate.
///
/// This is the *complete* ladder the client/server agree on; which rungs are
/// actually active for a given session is a separate, dynamic concern (see
/// [`StreamProfile::filtered`]) — any rung, including the lowest, can be
/// disabled by the operator, so "rung 0" is not a hardcoded survival id.
/// Whichever rung ends up lowest in the *filtered* profile becomes the
/// effective survival rung the continuity tier defends (CLAUDE.md §5/§6);
/// `low_rung()`/`rung_above()` below always derive "low"/"next" from
/// whatever's actually in `rungs`, never a literal id.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamProfile {
    pub segment_ms: u32,
    pub rungs: Vec<Rung>,
}

impl StreamProfile {
    /// The one ABR ladder every OBCast component ships today (48/96/192/320
    /// kbps AAC, low/mid/hi/hd, low rung HE-AAC) — previously copied
    /// separately in `obcast-server::main`, `obcast-client::main`, and
    /// `obcast-client::gui::app`, which could silently drift out of sync.
    /// `segment_ms` is the only axis that's actually configurable per run.
    pub fn default_ladder(segment_ms: u32) -> Self {
        Self {
            segment_ms,
            rungs: vec![
                Rung {
                    id: 0,
                    name: "lo".into(),
                    bitrate_kbps: 48,
                    codec: AacCodec::He,
                },
                Rung {
                    id: 1,
                    name: "mid".into(),
                    bitrate_kbps: 96,
                    codec: AacCodec::Lc,
                },
                Rung {
                    id: 2,
                    name: "hi".into(),
                    bitrate_kbps: 192,
                    codec: AacCodec::Lc,
                },
                Rung {
                    id: 3,
                    name: "hd".into(),
                    bitrate_kbps: 320,
                    codec: AacCodec::Lc,
                },
            ],
        }
    }

    /// A view of this profile containing only `enabled` rungs, still sorted
    /// ascending by bitrate. Never empty: if `enabled` excludes every rung
    /// (an empty selection, or a stale config referencing ids that no longer
    /// exist), falls back to just the single lowest-bitrate rung of the full
    /// ladder — the scheduler's continuity tier always needs *some* cheap
    /// option to guarantee playback, so a filtered view degrading to "no
    /// rungs at all" would be worse than ignoring an invalid selection.
    pub fn filtered(&self, enabled: &std::collections::BTreeSet<RungId>) -> Self {
        let mut rungs: Vec<Rung> = self
            .rungs
            .iter()
            .filter(|r| enabled.contains(&r.id))
            .cloned()
            .collect();
        if rungs.is_empty() {
            if let Some(lowest) = self.rungs.iter().min_by_key(|r| r.bitrate_kbps) {
                rungs.push(lowest.clone());
            }
        }
        Self {
            segment_ms: self.segment_ms,
            rungs,
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
    pub fn codec_of(&self, rung: RungId) -> Option<AacCodec> {
        self.rungs.iter().find(|r| r.id == rung).map(|r| r.codec)
    }
    /// Next rung strictly above `rung`, if any (one incremental step up).
    pub fn rung_above(&self, rung: RungId) -> Option<RungId> {
        self.rungs
            .iter()
            .filter(|r| r.id > rung)
            .min_by_key(|r| r.bitrate_kbps)
            .map(|r| r.id)
    }
    /// Resolves a persisted "preferred rung" (e.g. an operator's default-
    /// quality setting) against this profile: returns `preferred` if it's
    /// actually present, else falls back to `low_rung()` — handles the case
    /// where the operator's saved preference was since disabled.
    pub fn nearest_enabled_or_low(&self, preferred: RungId) -> RungId {
        if self.rungs.iter().any(|r| r.id == preferred) {
            preferred
        } else {
            self.low_rung()
        }
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
    /// The rung whose bytes are actually draining out of the hardware output
    /// right now, for `position_seq` — ground truth for "what quality is on
    /// air," distinct from (and not derivable from) whatever the DVR index
    /// currently reports as the best rung available for that seq. The
    /// engine's decode pipeline runs many segments ahead of real-time
    /// playback (it feeds a deep ring buffer to survive slowdowns without an
    /// audible underrun — see `playout.rs` module docs), so the rung it
    /// chose to feed for a given seq is locked in well before that seq's
    /// audio is actually heard. If a quality upgrade for that same seq lands
    /// on disk in the meantime (which routinely happens — uploads are far
    /// faster than the ring's depth in playback time), the DVR index will
    /// report the *new*, higher rung for that seq even though the decoder
    /// already committed to the lower one. Looking up "best rung for this
    /// seq" live at listen time therefore lies about what's actually
    /// playing; this field is set once, at the moment the engine feeds the
    /// segment, and never revised — see `PlayoutHandle::playing_rung`.
    /// `None` when stopped or the segment was skipped without ever
    /// producing audio (decode failure/abandoned/stall-timeout).
    #[serde(default)]
    pub playing_rung: Option<RungId>,
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
    /// Contiguous ms of DVR history held from `dvr_start_seq` forward,
    /// stopping at the first gap — independent of the playout anchor (unlike
    /// `lead_ms`, which is 0 while stopped since it walks from the live
    /// edge instead). Doubles as the auto-start progress readout (see
    /// `EncoderState::auto_start_buffer_ms`) and a general "how deep is our
    /// buffered history" indicator.
    #[serde(default)]
    pub buffered_ms: u32,
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
                playing_rung: None,
                device: None,
                volume: 1.0,
                detail: None,
                test_tone: false,
            },
            frontier_seq: None,
            lead_ms: 0,
            water: WaterLevels::default(),
            coverage: Vec::new(),
            buffered_ms: 0,
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
    /// Requested auto-start buffer, in ms: once `ServerState::buffered_ms`
    /// reaches this while playout is `stopped`, the server starts playout on
    /// its own (from `dvr_start_seq`) without any web operator action.
    /// `None` disables auto-start. Irrelevant once playout has been started
    /// by any means (manual or automatic) — see `docs/protocol.md` §3.
    #[serde(default)]
    pub auto_start_buffer_ms: Option<u32>,
}

#[cfg(test)]
mod stream_profile_tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn default_ladder_is_four_rungs_lo_he_rest_lc() {
        let profile = StreamProfile::default_ladder(2000);
        assert_eq!(profile.rungs.len(), 4);
        assert_eq!(profile.bitrate_of(0), 48);
        assert_eq!(profile.bitrate_of(1), 96);
        assert_eq!(profile.bitrate_of(2), 192);
        assert_eq!(profile.bitrate_of(3), 320);
        assert_eq!(profile.codec_of(0), Some(AacCodec::He));
        assert_eq!(profile.codec_of(1), Some(AacCodec::Lc));
        assert_eq!(profile.codec_of(2), Some(AacCodec::Lc));
        assert_eq!(profile.codec_of(3), Some(AacCodec::Lc));
    }

    #[test]
    fn filtered_keeps_only_enabled_rungs_in_order() {
        let profile = StreamProfile::default_ladder(2000);
        let enabled: BTreeSet<RungId> = [1, 3].into_iter().collect();
        let filtered = profile.filtered(&enabled);
        assert_eq!(
            filtered.rungs.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(filtered.low_rung(), 1);
        assert_eq!(filtered.top_rung(), 3);
    }

    #[test]
    fn filtered_excluding_rung_zero_makes_the_next_lowest_the_effective_survival_rung() {
        let profile = StreamProfile::default_ladder(2000);
        let enabled: BTreeSet<RungId> = [1, 2, 3].into_iter().collect();
        let filtered = profile.filtered(&enabled);
        assert_eq!(filtered.low_rung(), 1);
        assert!(filtered.rungs.iter().all(|r| r.id != 0));
    }

    #[test]
    fn filtered_never_returns_empty() {
        let profile = StreamProfile::default_ladder(2000);
        let filtered = profile.filtered(&BTreeSet::new());
        assert_eq!(filtered.rungs.len(), 1);
        assert_eq!(filtered.low_rung(), 0); // lowest-bitrate rung of the full ladder
    }

    #[test]
    fn filtered_ignores_unknown_ids() {
        let profile = StreamProfile::default_ladder(2000);
        let enabled: BTreeSet<RungId> = [99].into_iter().collect();
        let filtered = profile.filtered(&enabled);
        assert_eq!(filtered.rungs.len(), 1);
        assert_eq!(filtered.low_rung(), 0);
    }

    #[test]
    fn nearest_enabled_or_low_prefers_the_preferred_rung_when_present() {
        let profile = StreamProfile::default_ladder(2000);
        let filtered = profile.filtered(&[0, 2].into_iter().collect());
        assert_eq!(filtered.nearest_enabled_or_low(2), 2);
    }

    #[test]
    fn nearest_enabled_or_low_falls_back_when_preferred_was_disabled() {
        let profile = StreamProfile::default_ladder(2000);
        let filtered = profile.filtered(&[0, 1].into_iter().collect());
        // Operator's saved default (rung 3) isn't in this session's ladder.
        assert_eq!(filtered.nearest_enabled_or_low(3), filtered.low_rung());
    }
}
