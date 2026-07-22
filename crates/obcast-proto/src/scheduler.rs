//! The closed-loop upload scheduler: a pure function that decides, each tick,
//! which segments to upload and at which rung, given the server's state and the
//! encoder's local inventory.
//!
//! It encodes exactly the required behaviour:
//!
//!  1. **Continuity first.** Around the server's playout head, guarantee every
//!     segment exists at *some* rung so playout never hits a gap. When the
//!     server is draining (buffer emptying) on a flaky link, this means blasting
//!     the *lowest* rung to extend the playable frontier — no dropout.
//!  2. **Live edge next.** Keep the newest segments covered at the low rung so
//!     the DVR window stays contiguous and go-live works instantly.
//!  3. **Quality last, and only forward of playout.** When there is a healthy
//!     margin and spare bandwidth, upgrade segments *ahead of the playout head*
//!     to higher rungs, nearest-first — so listeners hear HD soonest and we
//!     never spend bytes on segments behind the head that will never be played.
//!
//! Priorities are absolute: continuity always outranks live-edge, which always
//! outranks upgrades. Bandwidth is handed out in that order.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::state::{RungId, Seq, ServerState, StreamProfile};

/// Why a segment is being uploaded (drives priority and observability).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadReason {
    /// Fill a hole at/ahead of the playout head to prevent a dropout.
    Continuity,
    /// Cover the newest segments at the low rung (keep the DVR contiguous).
    LiveEdge,
    /// Raise quality forward of the playout head with spare bandwidth.
    Upgrade,
}

/// A single planned upload for this tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadAction {
    pub seq: Seq,
    pub rung: RungId,
    pub reason: UploadReason,
    /// Lower = more urgent. Continuity < LiveEdge < Upgrade, then nearest-first.
    pub priority: u32,
}

/// What the encoder currently holds on disk, plus its model of what the server
/// already has (built from [`ServerState::coverage`] and upload acks).
#[derive(Clone, Debug, Default)]
pub struct LocalInventory {
    /// Newest seq produced locally.
    pub encoded_seq: Seq,
    /// Oldest seq still on local disk.
    pub oldest_seq: Seq,
    /// Rungs available locally per seq (not yet acked by the server).
    pub available: BTreeMap<Seq, Vec<RungId>>,
    /// Encoder's belief of the server's best rung per seq.
    /// `Some(Some(r))` = server has rung r; `Some(None)` = known missing;
    /// absent key = unknown (treated as missing).
    pub server_best: BTreeMap<Seq, Option<RungId>>,
}

impl LocalInventory {
    fn has_rung(&self, seq: Seq, rung: RungId) -> bool {
        self.available
            .get(&seq)
            .map(|v| v.contains(&rung))
            .unwrap_or(false)
    }
    fn server_best(&self, seq: Seq) -> Option<RungId> {
        self.server_best.get(&seq).copied().flatten()
    }
    fn server_has_any(&self, seq: Seq) -> bool {
        self.server_best(seq).is_some()
    }
}

/// Inputs for one planning tick.
pub struct SchedulerInput<'a> {
    pub profile: &'a StreamProfile,
    pub server: &'a ServerState,
    pub inv: &'a LocalInventory,
    /// Recent measured upload throughput.
    pub throughput_kbps: u32,
    /// Safety factor applied to the throughput budget (e.g. 0.9).
    pub headroom: f32,
    /// Cap on actions returned per tick.
    pub max_actions: usize,
}

const P_CONTINUITY: u32 = 0;
const P_LIVE_EDGE: u32 = 1_000_000;
const P_UPGRADE: u32 = 2_000_000;

/// Choose the segment whose imminence anchors all urgency: the playout head when
/// playing/paused, else the live edge (so an instant go-live is protected).
fn anchor(server: &ServerState, inv: &LocalInventory) -> Seq {
    use crate::state::PlayoutState::*;
    match server.playout.state {
        // Stalled is still nominally "playing" position-wise (see
        // `store::build_server_state`'s matching anchor logic on the server
        // side) — the scheduler should keep defending the same head a
        // stalled playout is stuck on, not fall back to the live edge.
        // Error means the engine can't produce audio at all (e.g. no output
        // device) — same "no anchor, fall back to live edge" treatment as
        // Stopped, since there's no head to defend.
        Playing | Paused | Stalled => server.playout.position_seq,
        Stopped | Error => None,
    }
    .or(server.playout.position_seq)
    .or(server.live_seq)
    .unwrap_or(inv.oldest_seq)
}

/// Plan uploads for this tick, ordered by priority.
pub fn plan_uploads(input: &SchedulerInput) -> Vec<UploadAction> {
    let SchedulerInput {
        profile,
        server,
        inv,
        throughput_kbps,
        headroom,
        max_actions,
    } = *input;

    let seg_ms = profile.segment_ms.max(1);
    let low = profile.low_rung();
    let anchor = anchor(server, inv);

    // Budget in kilobits for this tick (one segment interval), continuity exempt.
    let budget_kbits = (throughput_kbps as f32 * (seg_ms as f32 / 1000.0) * headroom).max(0.0);
    let cost = |rung: RungId| -> f32 { profile.bitrate_of(rung) as f32 * (seg_ms as f32 / 1000.0) };

    let mut out: Vec<UploadAction> = Vec::new();
    // Continuity is allowed to burst past the tick budget (dropout avoidance is
    // worth it), but its cost still counts when gating the lower tiers so we
    // don't over-schedule a starved link.
    let mut continuity_cost = 0.0f32;

    // ---- Tier A: Continuity. Walk the contiguous frontier from the anchor
    // forward; fill any hole with the low rung until we've secured `target_ms`.
    let mut projected_lead_ms: u32 = 0;
    let mut seq = anchor;
    while projected_lead_ms < server.water.target_ms && seq <= inv.encoded_seq {
        if inv.server_has_any(seq) {
            projected_lead_ms = projected_lead_ms.saturating_add(seg_ms);
        } else if inv.has_rung(seq, low) {
            out.push(UploadAction {
                seq,
                rung: low,
                reason: UploadReason::Continuity,
                priority: P_CONTINUITY + (seq.saturating_sub(anchor)) as u32,
            });
            continuity_cost += cost(low);
            projected_lead_ms = projected_lead_ms.saturating_add(seg_ms);
        } else {
            // A near hole we cannot fill (not yet encoded / purged). Contiguity
            // is broken here; stop extending the frontier.
            break;
        }
        if out.len() >= max_actions {
            return finish(out, max_actions);
        }
        seq += 1;
    }

    // True contiguous lead from the anchor, counting server holdings *and* the
    // continuity fills just scheduled — this can exceed `target_ms` when the
    // server already holds a deep buffer, which is what lets us tell whether the
    // margin is comfortable enough to start upgrading quality.
    let secured = |s: Seq| -> bool {
        inv.server_has_any(s)
            || out
                .iter()
                .any(|a| a.seq == s && a.reason == UploadReason::Continuity)
    };
    let mut lead_ms: u32 = 0;
    let mut w = anchor;
    while w <= inv.encoded_seq && secured(w) {
        lead_ms = lead_ms.saturating_add(seg_ms);
        w += 1;
    }

    let survival = lead_ms < server.water.low_ms;

    // ---- Tier B: Live edge. Ensure the newest ~target window is covered at the
    // low rung so the DVR stays contiguous and go-live is instant. Skipped in
    // survival mode (every byte must go to continuity near the head).
    let mut spent = continuity_cost;
    if !survival {
        let window_segs = (server.water.target_ms / seg_ms).max(1) as u64;
        let start = inv.encoded_seq.saturating_sub(window_segs - 1).max(anchor);
        for s in start..=inv.encoded_seq {
            if out.len() >= max_actions {
                return finish(out, max_actions);
            }
            if !inv.server_has_any(s) && inv.has_rung(s, low) && !already(&out, s) {
                let c = cost(low);
                if spent + c > budget_kbits {
                    break;
                }
                spent += c;
                out.push(UploadAction {
                    seq: s,
                    rung: low,
                    reason: UploadReason::LiveEdge,
                    // newest-first within the tier
                    priority: P_LIVE_EDGE + (inv.encoded_seq.saturating_sub(s)) as u32,
                });
            }
        }
    }

    // ---- Tier C: Quality upgrades. Only with a comfortable margin and leftover
    // budget, and ONLY forward of the playout head (never < anchor), nearest
    // first. One incremental rung step per segment per tick.
    //
    // Also strictly ahead of whatever the engine has already fed to its
    // decoder (`server.playout.fed_seq`), not just ahead of the audible head
    // (`anchor`/`position_seq`). The engine feeds its ring many segments
    // ahead of real-time output to survive slowdowns without an audible
    // underrun (see `playout.rs` module docs), so a seq's rung is locked in
    // well before `anchor` ever reaches it — an upgrade upload that only
    // clears the `anchor` bar can still lose the race to a feed loop that
    // already grabbed the low rung moments after Tier B first uploaded it.
    // Gating on `fed_seq` instead means Tier C only ever spends budget on
    // seqs an upgrade can actually still affect. Falls back to `anchor` when
    // unknown (stopped, just started/sought, or an older server not yet
    // sending the field), matching the previous ahead-of-head-only behavior.
    //
    // The window's far boundary must track `already_fed`, not `anchor`: an
    // earlier version capped it at `anchor + cap_segs`, which goes empty
    // (`already_fed + 1 > anchor + cap_segs`) whenever the engine's real
    // feed-ahead depth exceeds the `high_ms`-sized window — which it does in
    // practice, since feed-ahead is sized by the playout ring (an
    // independent constant), not by `high_ms`. That produced zero upgrade
    // actions forever once steady-state feed-ahead outran the window, even
    // with tens of seconds of un-upgraded buffer sitting past it (observed
    // live: fed_seq 13 segments ahead of anchor vs. a 10-segment window).
    // Anchoring the window to start right after `already_fed` instead means
    // it always covers real, still-upgradeable segments.
    let comfortable = lead_ms >= server.water.high_ms;
    if comfortable && !survival {
        let already_fed = server.playout.fed_seq.unwrap_or(anchor).max(anchor);
        let cap_segs = (server.water.high_ms / seg_ms).max(1) as u64;
        let end = (already_fed + cap_segs).min(inv.encoded_seq);
        // strictly ahead of both the head and whatever's already fed
        for s in (already_fed + 1)..=end {
            if out.len() >= max_actions {
                break;
            }
            if already(&out, s) {
                continue;
            }
            let current = inv.server_best(s).unwrap_or(low);
            if let Some(next) = profile.rung_above(current) {
                if inv.has_rung(s, next) {
                    let c = cost(next);
                    if spent + c > budget_kbits {
                        break;
                    }
                    spent += c;
                    out.push(UploadAction {
                        seq: s,
                        rung: next,
                        reason: UploadReason::Upgrade,
                        priority: P_UPGRADE + (s - anchor) as u32,
                    });
                }
            }
        }
    }

    finish(out, max_actions)
}

fn already(out: &[UploadAction], seq: Seq) -> bool {
    out.iter().any(|a| a.seq == seq)
}

/// The first seq (if any) currently blocking continuity extension because the
/// encoder has already moved past it (it's older than `encoded_seq`, the
/// newest locally-produced segment) yet it exists at the low rung on neither
/// the encoder's disk nor the server. Such a gap can never simply "arrive" —
/// the encoder already produced everything up to `encoded_seq` and this seq
/// isn't among it — so it needs an explicit `/abandon` rather than an
/// indefinite retry (CLAUDE.md §5, "never block the playout head").
///
/// `encoded_seq` itself is deliberately excluded: it's still mid-write (see
/// `inventory::scan`, which withholds the newest file per rung as
/// not-yet-finalized), not missing.
///
/// Pure and side-effect free, like `plan_uploads` — this only identifies
/// *where* a permanent gap is; the uploader decides *when* (after how long
/// persisting) to actually call `/abandon`, and is the only place with I/O.
pub fn stalled_continuity_seq(
    server: &ServerState,
    inv: &LocalInventory,
    profile: &StreamProfile,
) -> Option<Seq> {
    let low = profile.low_rung();
    let mut seq = anchor(server, inv);
    while seq < inv.encoded_seq {
        if inv.server_has_any(seq) {
            seq += 1;
            continue;
        }
        if inv.has_rung(seq, low) {
            return None; // still on local disk — uploadable, not a permanent gap
        }
        return Some(seq);
    }
    None
}

fn finish(mut out: Vec<UploadAction>, max_actions: usize) -> Vec<UploadAction> {
    out.sort_by_key(|a| a.priority);
    out.truncate(max_actions);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::*;

    fn profile() -> StreamProfile {
        StreamProfile::default_ladder(2000)
    }

    fn server(state: PlayoutState, pos: Option<Seq>, water: WaterLevels) -> ServerState {
        server_with_fed(state, pos, None, water)
    }

    /// Like `server`, but also sets `playout.fed_seq` — for tests exercising
    /// the upgrade tier's fed-boundary gating (see Tier C below).
    fn server_with_fed(
        state: PlayoutState,
        pos: Option<Seq>,
        fed: Option<Seq>,
        water: WaterLevels,
    ) -> ServerState {
        ServerState {
            rev: 1,
            live_seq: Some(50),
            dvr_start_seq: Some(0),
            playout: PlayoutStatus {
                state,
                position_seq: pos,
                playing_rung: None,
                fed_seq: fed,
                position_ms_into_segment: 0,
                device: None,
                volume: 1.0,
                detail: None,
                test_tone: false,
            },
            frontier_seq: pos,
            lead_ms: 0,
            water,
            coverage: vec![],
            buffered_ms: 0,
        }
    }

    /// Encoder has every rung for a range; server has nothing there.
    fn inv_full(oldest: Seq, newest: Seq, server_best: &[(Seq, Option<RungId>)]) -> LocalInventory {
        let mut available = BTreeMap::new();
        for s in oldest..=newest {
            available.insert(s, vec![0, 1, 2, 3]);
        }
        LocalInventory {
            encoded_seq: newest,
            oldest_seq: oldest,
            available,
            server_best: server_best.iter().copied().collect(),
        }
    }

    #[test]
    fn draining_flaky_link_sends_low_rung_at_playout_head_only() {
        // Playing at seq 20, server has nothing ahead, tiny throughput -> survival.
        let water = WaterLevels {
            low_ms: 4000,
            target_ms: 8000,
            high_ms: 16000,
        };
        let srv = server(PlayoutState::Playing, Some(20), water);
        let inv = inv_full(0, 50, &[]);
        let input = SchedulerInput {
            profile: &profile(),
            server: &srv,
            inv: &inv,
            throughput_kbps: 40, // barely enough for the 32k rung
            headroom: 0.9,
            max_actions: 8,
        };
        let plan = plan_uploads(&input);

        // Everything is continuity, low rung, starting exactly at the head and
        // marching forward (never behind it).
        assert!(!plan.is_empty());
        assert!(plan.iter().all(|a| a.rung == 0));
        assert!(plan.iter().all(|a| a.reason == UploadReason::Continuity));
        assert!(plan.iter().all(|a| a.seq >= 20));
        assert_eq!(plan[0].seq, 20);
        // No HD upgrades while draining.
        assert!(plan.iter().all(|a| a.reason != UploadReason::Upgrade));
    }

    #[test]
    fn healthy_link_upgrades_only_ahead_of_playout_head() {
        // Playout at 30. Server already holds low rung for a big margin ahead, so
        // the frontier is comfortable; plenty of bandwidth -> upgrades allowed.
        let water = WaterLevels {
            low_ms: 4000,
            target_ms: 8000,
            high_ms: 10000,
        };
        let srv = server(PlayoutState::Playing, Some(30), water);
        // Server has the low rung for 25..=50 (covers past + future); HD missing.
        let cov: Vec<(Seq, Option<RungId>)> = (25..=50).map(|s| (s, Some(0))).collect();
        let inv = inv_full(0, 50, &cov);
        let input = SchedulerInput {
            profile: &profile(),
            server: &srv,
            inv: &inv,
            throughput_kbps: 5000, // huge headroom
            headroom: 0.9,
            max_actions: 20,
        };
        let plan = plan_uploads(&input);

        let upgrades: Vec<_> = plan
            .iter()
            .filter(|a| a.reason == UploadReason::Upgrade)
            .collect();
        assert!(!upgrades.is_empty(), "expected some HD upgrades");
        // CRITICAL: never upgrade a segment behind the playout head.
        assert!(upgrades.iter().all(|a| a.seq > 30));
        // Nearest-to-head upgraded first.
        assert_eq!(upgrades.iter().map(|a| a.seq).min(), Some(31));
    }

    #[test]
    fn upgrades_never_target_a_seq_already_fed_to_the_decoder() {
        // Playout head (audible position) at 30, but the engine's decode
        // pipeline has already fed segments up through 32 into its ring
        // (the realistic case: it runs many segments ahead of real-time
        // output — see `playout.rs` module docs). Even though 31/32 are
        // "ahead of the head" and would have been upgrade candidates under
        // the old anchor-only gating, their rung is already locked in — an
        // upgrade upload for them can never change what's heard, so Tier C
        // must skip straight to 33.
        let water = WaterLevels {
            low_ms: 4000,
            target_ms: 8000,
            high_ms: 10000,
        };
        let srv = server_with_fed(PlayoutState::Playing, Some(30), Some(32), water);
        let cov: Vec<(Seq, Option<RungId>)> = (25..=50).map(|s| (s, Some(0))).collect();
        let inv = inv_full(0, 50, &cov);
        let input = SchedulerInput {
            profile: &profile(),
            server: &srv,
            inv: &inv,
            throughput_kbps: 5000,
            headroom: 0.9,
            max_actions: 20,
        };
        let plan = plan_uploads(&input);

        let upgrades: Vec<_> = plan
            .iter()
            .filter(|a| a.reason == UploadReason::Upgrade)
            .collect();
        assert!(!upgrades.is_empty(), "expected some HD upgrades beyond 32");
        assert!(
            upgrades.iter().all(|a| a.seq > 32),
            "an upgrade landed at or behind the already-fed boundary (32): {upgrades:?}"
        );
        assert_eq!(upgrades.iter().map(|a| a.seq).min(), Some(33));
    }

    #[test]
    fn upgrades_still_happen_when_feed_ahead_outruns_the_anchor_relative_window() {
        // Same as above, but the engine has fed all the way out to 38 —
        // past `anchor + cap_segs` (30 + 5 = 35), which used to be the
        // window's fixed far boundary. That was a real, reproduced-live bug:
        // once the engine's feed-ahead depth exceeds a `high_ms`-sized
        // window measured from `anchor`, the window (`already_fed+1..=end`)
        // goes empty and Tier C produces zero upgrades forever, even with
        // plenty of un-upgraded buffer (39..=50 here) still sitting past the
        // feed boundary. The window's far boundary must instead track
        // `already_fed` (38 + 5 = 43), so upgrades resume starting at 39.
        let water = WaterLevels {
            low_ms: 4000,
            target_ms: 8000,
            high_ms: 10000,
        };
        let srv = server_with_fed(PlayoutState::Playing, Some(30), Some(38), water);
        let cov: Vec<(Seq, Option<RungId>)> = (25..=50).map(|s| (s, Some(0))).collect();
        let inv = inv_full(0, 50, &cov);
        let input = SchedulerInput {
            profile: &profile(),
            server: &srv,
            inv: &inv,
            throughput_kbps: 5000,
            headroom: 0.9,
            max_actions: 20,
        };
        let plan = plan_uploads(&input);
        let upgrades: Vec<_> = plan
            .iter()
            .filter(|a| a.reason == UploadReason::Upgrade)
            .collect();
        assert!(
            !upgrades.is_empty(),
            "expected upgrades past the feed boundary (38), got none"
        );
        assert!(
            upgrades.iter().all(|a| a.seq > 38),
            "an upgrade landed at or behind the already-fed boundary (38): {upgrades:?}"
        );
        assert_eq!(upgrades.iter().map(|a| a.seq).min(), Some(39));
        assert!(
            upgrades.iter().all(|a| a.seq <= 43),
            "upgrade window should stay capped at already_fed + cap_segs (43): {upgrades:?}"
        );
    }

    #[test]
    fn never_backfills_hd_into_already_played_segments() {
        let water = WaterLevels {
            low_ms: 4000,
            target_ms: 8000,
            high_ms: 10000,
        };
        let srv = server(PlayoutState::Playing, Some(40), water);
        // Segments behind the head (0..40) only have the low rung on the server;
        // it would be wasteful to upgrade them.
        let cov: Vec<(Seq, Option<RungId>)> = (0..=50).map(|s| (s, Some(0))).collect();
        let inv = inv_full(0, 50, &cov);
        let input = SchedulerInput {
            profile: &profile(),
            server: &srv,
            inv: &inv,
            throughput_kbps: 5000,
            headroom: 0.9,
            max_actions: 50,
        };
        let plan = plan_uploads(&input);
        assert!(plan
            .iter()
            .all(|a| a.seq > 40 || a.reason != UploadReason::Upgrade));
    }

    #[test]
    fn stalled_continuity_seq_is_none_when_frontier_is_healthy() {
        let water = WaterLevels::default();
        let srv = server(PlayoutState::Playing, Some(20), water);
        let inv = inv_full(0, 50, &[]); // encoder holds everything locally
        assert_eq!(stalled_continuity_seq(&srv, &inv, &profile()), None);
    }

    #[test]
    fn stalled_continuity_seq_is_none_when_gap_is_still_uploadable() {
        // Missing on the server but present locally — plain upload will fill
        // it eventually, this is not a permanent gap.
        let water = WaterLevels::default();
        let srv = server(PlayoutState::Playing, Some(20), water);
        let inv = inv_full(0, 50, &[]);
        assert_eq!(stalled_continuity_seq(&srv, &inv, &profile()), None);
    }

    #[test]
    fn stalled_continuity_seq_finds_a_genuine_permanent_gap() {
        // Seq 20 exists on neither side, and the encoder has already moved on
        // to seq 50 — it will never appear.
        let water = WaterLevels::default();
        let srv = server(PlayoutState::Playing, Some(20), water);
        let mut inv = inv_full(0, 50, &[]);
        inv.available.remove(&20);
        assert_eq!(stalled_continuity_seq(&srv, &inv, &profile()), Some(20));
    }

    #[test]
    fn stalled_continuity_seq_excludes_the_still_writing_newest_seq() {
        // The gap is exactly at `encoded_seq` — that's the segment ffmpeg is
        // still writing, not a permanent gap, so it must not be flagged.
        let water = WaterLevels::default();
        let srv = server(PlayoutState::Playing, Some(20), water);
        let mut inv = inv_full(0, 20, &[]);
        inv.available.remove(&20);
        assert_eq!(stalled_continuity_seq(&srv, &inv, &profile()), None);
    }

    #[test]
    fn far_behind_head_only_fills_near_anchor_and_near_live_edge_not_the_middle() {
        // Playout paused/seeked far back (anchor=5) while the encoder has kept
        // producing all the way out to 205 — e.g. a long DVR pause. Continuity
        // only needs to secure `target_ms` ahead of the anchor; live-edge only
        // needs to keep the newest `target_ms` covered. The large middle
        // region between those two windows must be left alone this tick —
        // deliberate (bounded per-tick budget), but previously unasserted
        // (the "coverage-window-vs-far-behind-head" gap flagged in CLAUDE.md).
        let water = WaterLevels {
            low_ms: 4000,
            target_ms: 8000,
            high_ms: 16000,
        };
        let srv = server(PlayoutState::Playing, Some(5), water);
        let inv = inv_full(0, 205, &[]); // nothing acked by the server anywhere yet
        let input = SchedulerInput {
            profile: &profile(),
            server: &srv,
            inv: &inv,
            throughput_kbps: 5000,
            headroom: 0.9,
            max_actions: 100,
        };
        let plan = plan_uploads(&input);

        assert!(!plan.is_empty());
        // Every action is either near the anchor (continuity window) or near
        // the live/encoded edge (live-edge window) — never in the untouched
        // middle.
        for a in &plan {
            assert!(
                a.seq <= 8 || a.seq >= 202,
                "seq {} falls in the middle region neither window should touch this tick",
                a.seq
            );
        }
        for mid in [50, 100, 150, 201] {
            assert!(plan.iter().all(|a| a.seq != mid));
        }
        // Lead from continuity alone (8000ms) doesn't reach `high_ms`
        // (16000ms), so no upgrades this tick.
        assert!(plan.iter().all(|a| a.reason != UploadReason::Upgrade));
    }

    #[test]
    fn misconfigured_water_levels_with_low_above_target_stays_in_survival() {
        // Water levels are meant to satisfy low_ms <= target_ms <= high_ms,
        // but nothing enforces that — an operator typo could easily invert
        // them. Here low_ms > target_ms: continuity secures the full
        // target_ms lead, but `survival` (lead_ms < low_ms) still reads true
        // because low_ms was misconfigured above target_ms. Not a crash —
        // this locks in the actual (slightly surprising) behavior so a
        // future change doesn't silently alter it unnoticed.
        let water = WaterLevels {
            low_ms: 8000,
            target_ms: 4000,
            high_ms: 2000,
        };
        let srv = server(PlayoutState::Playing, Some(20), water);
        let inv = inv_full(0, 50, &[]);
        let input = SchedulerInput {
            profile: &profile(),
            server: &srv,
            inv: &inv,
            throughput_kbps: 5000,
            headroom: 0.9,
            max_actions: 50,
        };
        let plan = plan_uploads(&input);

        assert!(!plan.is_empty());
        assert!(plan.iter().all(|a| a.reason == UploadReason::Continuity));
        assert!(plan.iter().all(|a| a.rung == 0));
        assert!(plan.iter().all(|a| a.seq >= 20));
        // Survival (mis-triggered by low_ms > target_ms) suppresses live-edge
        // and upgrade tiers entirely, even though `high_ms` is trivially
        // satisfied.
        assert!(plan.iter().all(|a| a.reason != UploadReason::LiveEdge));
        assert!(plan.iter().all(|a| a.reason != UploadReason::Upgrade));
    }

    #[test]
    fn misconfigured_water_levels_all_zero_does_not_panic() {
        // Degenerate all-zero config (e.g. an unset/defaulted-wrong TOML)
        // must not panic or divide by zero — `seg_ms.max(1)` and the derived
        // window sizes' `.max(1)` guards are what protect this; assert the
        // contract holds rather than relying on it silently.
        let water = WaterLevels {
            low_ms: 0,
            target_ms: 0,
            high_ms: 0,
        };
        let srv = server(PlayoutState::Playing, Some(20), water);
        let inv = inv_full(0, 50, &[]);
        let input = SchedulerInput {
            profile: &profile(),
            server: &srv,
            inv: &inv,
            throughput_kbps: 5000,
            headroom: 0.9,
            max_actions: 50,
        };
        let plan = plan_uploads(&input); // must not panic
                                         // Hard invariant that must hold no matter how water levels are
                                         // misconfigured: never upgrade a segment at or behind the playout head.
        assert!(plan
            .iter()
            .filter(|a| a.reason == UploadReason::Upgrade)
            .all(|a| a.seq > 20));
    }

    #[test]
    fn stopped_protects_live_edge() {
        let water = WaterLevels::default();
        let srv = server(PlayoutState::Stopped, None, water);
        // Server missing the newest segments entirely.
        let cov: Vec<(Seq, Option<RungId>)> = (0..=40).map(|s| (s, Some(0))).collect();
        let inv = inv_full(0, 50, &cov);
        let input = SchedulerInput {
            profile: &profile(),
            server: &srv,
            inv: &inv,
            throughput_kbps: 5000,
            headroom: 0.9,
            max_actions: 20,
        };
        let plan = plan_uploads(&input);
        // Anchor is the live edge (50); continuity fills the missing tail so a
        // go-live has audio ready.
        assert!(plan.iter().any(|a| a.seq >= 41));
    }

    #[test]
    fn errored_playout_anchors_on_live_edge_same_as_stopped() {
        // A broken playout engine (e.g. no output device) has no head to
        // defend, so it should get the exact same treatment as Stopped:
        // protect the live edge for an instant go-live once fixed.
        let water = WaterLevels::default();
        let srv = server(PlayoutState::Error, None, water);
        let cov: Vec<(Seq, Option<RungId>)> = (0..=40).map(|s| (s, Some(0))).collect();
        let inv = inv_full(0, 50, &cov);
        let input = SchedulerInput {
            profile: &profile(),
            server: &srv,
            inv: &inv,
            throughput_kbps: 5000,
            headroom: 0.9,
            max_actions: 20,
        };
        let plan = plan_uploads(&input);
        assert!(plan.iter().any(|a| a.seq >= 41));
    }

    /// An operator can disable any rung, including 0 — `filtered()` just
    /// narrows `profile.rungs`, and `plan_uploads` derives "low"/"next" from
    /// whatever's actually in there, never a hardcoded id. With rung 0
    /// disabled, continuity must fall back to whatever rung *is* now lowest.
    #[test]
    fn continuity_uses_the_lowest_enabled_rung_when_rung_zero_is_disabled() {
        let filtered = profile().filtered(&[1, 2, 3].into_iter().collect());
        assert_eq!(
            filtered.low_rung(),
            1,
            "rung 1 is now the effective survival rung"
        );

        let water = WaterLevels {
            low_ms: 4000,
            target_ms: 8000,
            high_ms: 16000,
        };
        let srv = server(PlayoutState::Playing, Some(20), water);
        let inv = inv_full(0, 50, &[]); // server has nothing; encoder has every rung locally
        let input = SchedulerInput {
            profile: &filtered,
            server: &srv,
            inv: &inv,
            throughput_kbps: 100, // barely enough for the 96k rung, not more
            headroom: 0.9,
            max_actions: 8,
        };
        let plan = plan_uploads(&input);

        assert!(!plan.is_empty());
        assert!(
            plan.iter().all(|a| a.rung == 1),
            "must use rung 1, never disabled rung 0"
        );
        assert!(plan.iter().all(|a| a.reason == UploadReason::Continuity));
    }

    /// A disabled rung must never be an upgrade target, even after several
    /// ticks of ratcheting up from a comfortable, well-fed link.
    #[test]
    fn upgrades_never_ratchet_into_a_disabled_top_rung() {
        let filtered = profile().filtered(&[0, 1, 2].into_iter().collect()); // rung 3 (hd) disabled
        let water = WaterLevels {
            low_ms: 4000,
            target_ms: 8000,
            high_ms: 10000,
        };
        let srv = server(PlayoutState::Playing, Some(30), water);

        // Server already holds the low rung 25..=50 so the frontier is
        // comfortable from tick one; encoder has every rung on local disk.
        let mut server_best: BTreeMap<Seq, Option<RungId>> =
            (25..=50).map(|s| (s, Some(1))).collect();

        for _ in 0..6 {
            let inv = LocalInventory {
                encoded_seq: 50,
                oldest_seq: 0,
                available: (0..=50).map(|s| (s, vec![0, 1, 2, 3])).collect(),
                server_best: server_best.clone(),
            };
            let input = SchedulerInput {
                profile: &filtered,
                server: &srv,
                inv: &inv,
                throughput_kbps: 5000, // huge headroom
                headroom: 0.9,
                max_actions: 20,
            };
            let plan: Vec<UploadAction> = plan_uploads(&input);
            assert!(
                plan.iter().all(|a| a.rung != 3),
                "disabled rung 3 must never be targeted, tick plan: {plan:?}"
            );
            for a in &plan {
                server_best.insert(a.seq, Some(a.rung));
            }
        }

        // After several ticks of ratcheting, the segments within the upgrade
        // window (anchor+1..=anchor+high_ms/seg_ms, i.e. 31..=35) should have
        // plateaued at rung 2 (hi) — the top of the *enabled* ladder — never
        // the disabled rung 3.
        assert!(
            (31..=35).all(|s| server_best.get(&s) == Some(&Some(2))),
            "expected every upgraded segment to plateau at the enabled top rung (2): {server_best:?}"
        );
    }
}
