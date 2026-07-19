//! In-memory DVR index. Segment bytes live on disk (see `ingest`); this tracks
//! which `(seq, rung)` pairs exist and derives `ServerState` from that plus the
//! current playout status. Pure and I/O-free so it's cheap to unit-test.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use obcast_proto::state::{
    EncoderState, PlayoutStatus, RungId, SegCoverage, Seq, ServerState, StreamProfile, WaterLevels,
};

/// How many segments ahead of the anchor `coverage` reports.
const COVERAGE_WINDOW_SEGS: u64 = 64;

fn epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub struct DvrStore {
    profile: StreamProfile,
    water: WaterLevels,
    dvr_window_segs: u64,
    data_dir: PathBuf,
    rev: u64,
    /// seq -> set of rungs present on disk.
    index: BTreeMap<Seq, BTreeSet<RungId>>,
    /// Seqs the encoder gave up on; treated as satisfied for frontier purposes.
    abandoned: BTreeSet<Seq>,
    /// Latest encoder telemetry from `POST /ingest/{stream}/heartbeat` (or
    /// piggybacked on an upload), for the control API's `ControlStatus.encoder`
    /// and real `throughput_kbps` — see `docs/protocol.md` §3. `None` until the
    /// first heartbeat arrives.
    encoder_state: Option<EncoderState>,
}

impl DvrStore {
    pub fn new(
        profile: StreamProfile,
        water: WaterLevels,
        dvr_window_ms: u32,
        data_dir: PathBuf,
    ) -> Self {
        let dvr_window_segs = (dvr_window_ms / profile.segment_ms.max(1)).max(1) as u64;
        Self {
            profile,
            water,
            dvr_window_segs,
            data_dir,
            // Seeded from wall-clock epoch millis rather than 0: a client that
            // held a high `rev` from before a server restart must never see a
            // *lower* rev from the fresh process, or `SharedState::update`
            // (client-side) permanently rejects every post-restart state as
            // "stale," pinning the encoder on data the restart already wiped
            // (see CLAUDE.md §5/§8, the "rev-reset" gap). Epoch millis grows
            // far faster than `record()`'s per-segment +1 could ever catch up
            // to across a real restart, so a fresh process's rev is always
            // ahead of whatever a client held from a prior one.
            rev: epoch_millis(),
            index: BTreeMap::new(),
            abandoned: BTreeSet::new(),
            encoder_state: None,
        }
    }

    pub fn segment_path(&self, rung: RungId, seq: Seq) -> PathBuf {
        self.data_dir
            .join(rung.to_string())
            .join(format!("{seq}.ts"))
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn profile(&self) -> &StreamProfile {
        &self.profile
    }

    /// Seqs that have media on disk at any rung, ascending. Abandoned seqs
    /// with no media are excluded — there's nothing to serve for them.
    pub fn playable_seqs(&self) -> impl Iterator<Item = Seq> + '_ {
        self.index.keys().copied()
    }

    pub fn has_rung(&self, seq: Seq, rung: RungId) -> bool {
        self.index.get(&seq).is_some_and(|r| r.contains(&rung))
    }

    /// Record that `(rung, seq)` now exists on disk. Idempotent — re-recording
    /// the same pair is a no-op on the index (the caller may still overwrite
    /// the file, which is also safe).
    ///
    /// Returns the on-disk paths of any segments this record just evicted
    /// from the DVR window index — this type stays I/O-free (see module
    /// docs), so it's on the caller to actually delete them.
    pub fn record(&mut self, rung: RungId, seq: Seq) -> Vec<PathBuf> {
        let is_new = self.index.entry(seq).or_default().insert(rung);
        if is_new {
            self.rev += 1;
            return self
                .evict_old()
                .into_iter()
                .flat_map(|(seq, rungs)| rungs.into_iter().map(move |r| (seq, r)))
                .map(|(seq, r)| self.segment_path(r, seq))
                .collect();
        }
        Vec::new()
    }

    /// Latest encoder telemetry, if any heartbeat has arrived yet.
    pub fn encoder_state(&self) -> Option<&EncoderState> {
        self.encoder_state.as_ref()
    }

    /// Record encoder telemetry from a heartbeat. Idempotent-ish: an
    /// out-of-order/older `rev` is dropped rather than overwriting a newer
    /// snapshot, matching how `ServerState.rev` is treated on the client side.
    pub fn set_encoder_state(&mut self, state: EncoderState) {
        if self
            .encoder_state
            .as_ref()
            .is_some_and(|cur| cur.rev >= state.rev)
        {
            return;
        }
        self.encoder_state = Some(state);
    }

    /// Mark seqs as permanently abandoned so frontier/playout can skip them.
    pub fn abandon(&mut self, seqs: &[Seq]) {
        let mut changed = false;
        for &s in seqs {
            changed |= self.abandoned.insert(s);
        }
        if changed {
            self.rev += 1;
        }
    }

    pub fn live_seq(&self) -> Option<Seq> {
        self.index.keys().next_back().copied()
    }

    pub fn dvr_start_seq(&self) -> Option<Seq> {
        self.index.keys().next().copied()
    }

    fn has_any(&self, seq: Seq) -> bool {
        self.index.get(&seq).is_some_and(|r| !r.is_empty()) || self.abandoned.contains(&seq)
    }

    /// Whether the encoder has explicitly given up on `seq` via `/abandon`.
    /// Playout uses this to skip a permanently missing segment instead of
    /// freezing the head on it forever (see `playout.rs::best_available_path`).
    pub fn is_abandoned(&self, seq: Seq) -> bool {
        self.abandoned.contains(&seq)
    }

    pub fn best_rung(&self, seq: Seq) -> Option<RungId> {
        self.index.get(&seq).and_then(|r| r.iter().max().copied())
    }

    /// Drop index entries older than the DVR window behind the live edge and
    /// return the `(seq, rungs)` pairs removed, so `record()` can hand the
    /// caller the on-disk paths to reap. Stays pure/I/O-free itself — no
    /// files are touched here (see module docs).
    fn evict_old(&mut self) -> Vec<(Seq, BTreeSet<RungId>)> {
        let Some(live) = self.live_seq() else {
            return Vec::new();
        };
        let floor = live.saturating_sub(self.dvr_window_segs);
        let stale: Vec<Seq> = self.index.range(..floor).map(|(s, _)| *s).collect();
        let mut evicted = Vec::with_capacity(stale.len());
        for s in stale {
            if let Some(rungs) = self.index.remove(&s) {
                evicted.push((s, rungs));
            }
            self.abandoned.remove(&s);
        }
        evicted
    }

    pub fn build_server_state(&self, playout: PlayoutStatus) -> ServerState {
        let live_seq = self.live_seq();
        let dvr_start_seq = self.dvr_start_seq();

        use obcast_proto::state::PlayoutState::*;
        let anchor = match playout.state {
            // Stalled is still nominally "playing" position-wise — the head
            // just isn't producing audible audio right now — so it anchors
            // the same as Playing/Paused.
            Playing | Paused | Stalled => playout.position_seq,
            Stopped => None,
        }
        .or(playout.position_seq)
        .or(live_seq);

        let (frontier_seq, lead_ms) = match (anchor, live_seq) {
            (Some(start), Some(live)) => {
                let mut frontier = None;
                let mut lead_ms = 0u32;
                let mut s = start;
                while s <= live && self.has_any(s) {
                    frontier = Some(s);
                    lead_ms = lead_ms.saturating_add(self.profile.segment_ms);
                    s += 1;
                }
                (frontier, lead_ms)
            }
            _ => (None, 0),
        };

        let coverage = match (anchor, live_seq) {
            (Some(start), Some(live)) => {
                let end = live.min(start.saturating_add(COVERAGE_WINDOW_SEGS));
                (start..=end)
                    .map(|s| SegCoverage {
                        seq: s,
                        best_rung: self.best_rung(s),
                    })
                    .collect()
            }
            _ => Vec::new(),
        };

        ServerState {
            rev: self.rev,
            live_seq,
            dvr_start_seq,
            playout,
            frontier_seq,
            lead_ms,
            water: self.water,
            coverage,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obcast_proto::state::{PlayoutState, Rung};

    fn profile() -> StreamProfile {
        StreamProfile {
            segment_ms: 2000,
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

    fn stopped() -> PlayoutStatus {
        PlayoutStatus {
            state: PlayoutState::Stopped,
            position_seq: None,
            device: None,
            volume: 1.0,
        }
    }

    fn store(dvr_window_ms: u32) -> DvrStore {
        DvrStore::new(
            profile(),
            WaterLevels::default(),
            dvr_window_ms,
            PathBuf::from("/tmp/obcast-test"),
        )
    }

    #[test]
    fn empty_store_has_no_frontier() {
        let s = store(60_000);
        let state = s.build_server_state(stopped());
        assert_eq!(state.live_seq, None);
        assert_eq!(state.frontier_seq, None);
        assert_eq!(state.lead_ms, 0);
    }

    #[test]
    fn stopped_anchors_on_live_edge_and_reports_contiguous_frontier() {
        let mut s = store(60_000);
        for seq in 0..=5 {
            s.record(0, seq);
        }
        let state = s.build_server_state(stopped());
        assert_eq!(state.live_seq, Some(5));
        assert_eq!(state.dvr_start_seq, Some(0));
        // Anchor is the live edge when stopped; frontier is just that one seq.
        assert_eq!(state.frontier_seq, Some(5));
        assert_eq!(state.lead_ms, 2000);
    }

    #[test]
    fn playing_anchor_walks_frontier_from_head() {
        let mut s = store(60_000);
        for seq in 0..=10 {
            s.record(0, seq);
        }
        let playing = PlayoutStatus {
            state: PlayoutState::Playing,
            position_seq: Some(3),
            device: None,
            volume: 1.0,
        };
        let state = s.build_server_state(playing);
        assert_eq!(state.frontier_seq, Some(10));
        assert_eq!(state.lead_ms, 8 * 2000); // seqs 3..=10 inclusive
    }

    #[test]
    fn hole_breaks_frontier_but_abandon_heals_it() {
        let mut s = store(60_000);
        for seq in [0, 1, 3, 4] {
            s.record(0, seq);
        }
        let playing = PlayoutStatus {
            state: PlayoutState::Playing,
            position_seq: Some(0),
            device: None,
            volume: 1.0,
        };
        let state = s.build_server_state(playing.clone());
        assert_eq!(state.frontier_seq, Some(1)); // stops at the gap (seq 2)

        s.abandon(&[2]);
        let state = s.build_server_state(playing);
        assert_eq!(state.frontier_seq, Some(4));
    }

    #[test]
    fn record_is_idempotent_and_bumps_rev_once() {
        let mut s = store(60_000);
        s.record(0, 1);
        let rev_after_first = s.rev;
        s.record(0, 1);
        assert_eq!(s.rev, rev_after_first);
    }

    #[test]
    fn fresh_store_seeds_rev_from_wall_clock_not_zero() {
        // A client holding a `rev` from a prior server incarnation must never
        // see a fresh store's rev come in lower — otherwise it rejects every
        // post-restart state as stale forever (the rev-reset deadlock).
        let s = store(60_000);
        assert!(
            s.rev > 1_000_000_000_000,
            "rev should look like epoch millis, not a small counter"
        );
    }

    #[test]
    fn is_abandoned_reflects_abandon_calls() {
        let mut s = store(60_000);
        assert!(!s.is_abandoned(3));
        s.abandon(&[3]);
        assert!(s.is_abandoned(3));
        assert!(!s.is_abandoned(4));
    }

    #[test]
    fn old_segments_are_evicted_outside_the_dvr_window() {
        // 4 segments * 2000ms window -> keep only the newest 4 seqs (plus fencepost).
        let mut s = store(8_000);
        for seq in 0..=20 {
            s.record(0, seq);
        }
        assert!(s.dvr_start_seq().unwrap() > 0);
        assert_eq!(s.live_seq(), Some(20));
    }

    #[test]
    fn record_returns_evicted_paths_for_the_caller_to_reap() {
        // Same window as above: eviction should start kicking in well before
        // seq 20, and every returned path should point at the rung/seq that
        // fell out of the window, so the caller (ingest.rs) can delete the
        // right files instead of leaking them on disk forever.
        let mut s = store(8_000);
        let mut all_evicted = Vec::new();
        for seq in 0..=20 {
            all_evicted.extend(s.record(0, seq));
        }
        assert!(
            !all_evicted.is_empty(),
            "old segments falling out of the DVR window should be returned for reaping"
        );
        for path in &all_evicted {
            assert!(path.to_string_lossy().ends_with(".ts"));
            assert!(path.starts_with(s.data_dir()));
        }
        // A still-open write (no eviction yet) returns nothing to reap.
        let mut fresh = store(8_000);
        assert!(fresh.record(0, 0).is_empty());
    }

    #[test]
    fn record_is_a_noop_on_reupload_and_reaps_nothing() {
        // Re-recording an already-indexed (rung, seq) must not report it as
        // newly evicted — it never left the index in the first place.
        let mut s = store(60_000);
        assert!(s.record(0, 1).is_empty());
        assert!(s.record(0, 1).is_empty());
    }

    #[test]
    fn encoder_state_is_none_until_a_heartbeat_arrives_and_rejects_stale_revs() {
        let mut s = store(60_000);
        assert!(s.encoder_state().is_none());

        let base = obcast_proto::state::EncoderState {
            rev: 5,
            active_rungs: vec![0, 1, 2],
            encoded_seq: Some(10),
            primary_rung: 1,
            throughput_kbps: 128,
            backlog: 0,
            abandoned: vec![],
        };
        s.set_encoder_state(base.clone());
        assert_eq!(s.encoder_state(), Some(&base));

        // Lower rev: dropped, latest snapshot is kept.
        let mut stale = base.clone();
        stale.rev = 4;
        stale.throughput_kbps = 999;
        s.set_encoder_state(stale);
        assert_eq!(s.encoder_state().unwrap().throughput_kbps, 128);

        // Newer rev: accepted.
        let mut newer = base;
        newer.rev = 6;
        newer.throughput_kbps = 256;
        s.set_encoder_state(newer);
        assert_eq!(s.encoder_state().unwrap().throughput_kbps, 256);
    }
}
