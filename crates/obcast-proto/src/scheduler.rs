//! The closed-loop upload scheduler: a pure function that decides, each tick,
//! which segments to upload and at which rung, given the server's state and the
//! encoder's local inventory.
//!
//! It encodes exactly the required behaviour:
//!
//!  1. **Continuity first.** Around the server's playout head, guarantee every
//!     segment exists at *some* rung so playout never hits a gap, and keep
//!     rebuilding that low-rung margin — burst-priority, ignoring the
//!     bandwidth budget — until it reaches `water.high_ms`. A link that just
//!     dropped can drop again; resilience (buffer depth) is defended before
//!     quality, not just before an imminent dropout. When the server is
//!     draining (buffer emptying) on a flaky link, this means blasting the
//!     *lowest* rung to extend the playable frontier — no dropout.
//!  2. **Live edge next.** Keep the newest segments covered at the low rung so
//!     the DVR window stays contiguous and go-live works instantly.
//!  3. **Quality last, and only forward of playout.** Only once continuity has
//!     rebuilt a comfortable margin (`lead_ms >= water.high_ms`) and spare
//!     bandwidth remains, upgrade segments *ahead of the playout head* to
//!     higher rungs, nearest-first — so listeners hear HD soonest and we
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
    /// Operator's chosen "default quality" rung (resolved against `profile`
    /// via `StreamProfile::nearest_enabled_or_low`, so this is always a rung
    /// actually present in `profile`). Tier B (live edge) tries this rung
    /// first for newest-segment coverage when it's locally available and
    /// fits the tick's bandwidth budget, falling back to `profile.low_rung()`
    /// otherwise — so the picker has a real, immediate effect on a link that
    /// can sustain it, without weakening the dropout guarantee: Tier A
    /// (continuity) is untouched and always uses the cheap low rung
    /// regardless of this field. Set equal to `profile.low_rung()` to
    /// recover the previous always-low-rung live-edge behavior.
    pub preferred_rung: RungId,
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
        preferred_rung,
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

    // Whether there's an actual playout head being defended right now. When
    // there isn't (Stopped/Error — no position has ever been initialized,
    // e.g. a stream that hasn't gone live yet, or is buffering toward
    // auto-start), `anchor` falls back to the live edge itself (see
    // `anchor()`), so "secure `target_ms` of lead ahead of the anchor" always
    // resolves to "every segment as it's produced" — there's no pre-existing
    // buffer ahead of the tip for a healthy link to ever separate from it.
    // Left unconditionally low-rung, this silently ate 100% of every tick's
    // uploads for as long as the stream stays stopped, which starves Tier B
    // entirely (its newest-window scan finds these seqs already claimed) and
    // means the operator's "default quality" picker has zero effect during
    // exactly the phase that matters most for it: pre-roll buffering before a
    // manual or auto-start. See CLAUDE.md's "default quality selection...
    // playhead position is not yet initialized" entry.
    let has_active_head = matches!(
        server.playout.state,
        crate::state::PlayoutState::Playing
            | crate::state::PlayoutState::Paused
            | crate::state::PlayoutState::Stalled
    );

    // ---- Tier A: Continuity. Walk the contiguous frontier from the anchor
    // forward; fill any hole until we've secured `water.high_ms` of
    // contiguous audio — not just `target_ms`. With an active head to
    // defend, always the low rung — the no-dropout guarantee must never
    // depend on the operator's quality pick being achievable. Without one,
    // try `preferred_rung` first (same affordability/availability check
    // Tier B uses) so pre-roll buffering banks the requested quality, still
    // falling back to `low` — unconditionally, bursting past budget just like
    // the active-head case — so a starved link never leaves a gap.
    //
    // `target_ms` alone used to be the stopping point here, which left a real
    // gap: everything between `target_ms` and `high_ms` (and beyond, out to
    // wherever the live-edge tier's own tail window starts) was left
    // completely unfilled every tick, regardless of how much idle bandwidth
    // was available or how much backlog was sitting on local disk (e.g. after
    // a network outage, where the encoder kept producing segments locally the
    // whole time). That backlog only got touched once the anchor's real-time
    // playout advance happened to walk the near-anchor window into it — i.e.
    // the buffer would sit flat at ~`target_ms` for however long that took,
    // then suddenly lurch upward once it did, since continuity uploads are
    // burst-priority and could then race through the backlog unbounded by
    // budget (see CLAUDE.md's "buffer rebuild ignored idle bandwidth" entry).
    //
    // Reaching all the way to `high_ms` here means: after a dropout, the
    // encoder spends its (budget-exempt) continuity priority rebuilding the
    // full resilience margin — the same margin `water.high_ms` already
    // defines as "comfortable enough to spend bytes on quality" — before
    // Tier C ever gets a look-in. A link that just dropped can drop again;
    // depth is worth more than quality until that margin is back. Operators
    // wanting a deeper standing buffer before quality resumes just configure
    // a larger `high_ms` (e.g. 300_000 for a 5-minute margin); no separate
    // "recovery" threshold is needed since `high_ms` already meant exactly
    // this, it just wasn't being filled.
    let mut projected_lead_ms: u32 = 0;
    let mut seq = anchor;
    while projected_lead_ms < server.water.high_ms && seq <= inv.encoded_seq {
        if inv.server_has_any(seq) {
            projected_lead_ms = projected_lead_ms.saturating_add(seg_ms);
        } else {
            let preferred_fits = !has_active_head
                && preferred_rung != low
                && inv.has_rung(seq, preferred_rung)
                && continuity_cost + cost(preferred_rung) <= budget_kbits;
            let rung = if preferred_fits {
                Some(preferred_rung)
            } else if inv.has_rung(seq, low) {
                Some(low)
            } else {
                None
            };
            let Some(rung) = rung else {
                // A near hole we cannot fill (not yet encoded / purged).
                // Contiguity is broken here; stop extending the frontier.
                break;
            };
            out.push(UploadAction {
                seq,
                rung,
                reason: UploadReason::Continuity,
                priority: P_CONTINUITY + (seq.saturating_sub(anchor)) as u32,
            });
            continuity_cost += cost(rung);
            projected_lead_ms = projected_lead_ms.saturating_add(seg_ms);
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

    // ---- Tier B: Live edge. Ensure the newest ~target window is covered so
    // the DVR stays contiguous and go-live is instant. Skipped in survival
    // mode (every byte must go to continuity near the head).
    //
    // Tries the operator's `preferred_rung` (the "default quality" picker)
    // first when it's cheaply affordable and locally available, falling back
    // to the guaranteed-cheap low rung otherwise — this is the only tier
    // `preferred_rung` affects; continuity (Tier A) always uses `low`
    // unconditionally, so the dropout guarantee never depends on this
    // picker's accuracy. Without this, an operator's chosen default rung had
    // no observable effect at all: continuity ignores it, live-edge was
    // hardcoded to `low`, and upgrades don't engage until well past when any
    // seeded bandwidth guess would have been overwritten by real
    // measurements — see CLAUDE.md's "default quality selector" entry.
    let mut spent = continuity_cost;
    if !survival {
        let window_segs = (server.water.target_ms / seg_ms).max(1) as u64;
        let start = inv.encoded_seq.saturating_sub(window_segs - 1).max(anchor);
        for s in start..=inv.encoded_seq {
            if out.len() >= max_actions {
                return finish(out, max_actions);
            }
            if inv.server_has_any(s) || already(&out, s) {
                continue;
            }
            let preferred_fits = preferred_rung != low
                && inv.has_rung(s, preferred_rung)
                && spent + cost(preferred_rung) <= budget_kbits;
            let rung = if preferred_fits {
                Some(preferred_rung)
            } else if inv.has_rung(s, low) {
                Some(low)
            } else {
                None
            };
            let Some(rung) = rung else { continue };
            let c = cost(rung);
            if spent + c > budget_kbits {
                // Even the cheapest option (`low`) doesn't fit — `low` is by
                // construction the lowest-cost rung in the profile, so no
                // later, equally-or-more-expensive segment this tick will
                // fit either; stop scanning.
                break;
            }
            spent += c;
            out.push(UploadAction {
                seq: s,
                rung,
                reason: UploadReason::LiveEdge,
                // newest-first within the tier
                priority: P_LIVE_EDGE + (inv.encoded_seq.saturating_sub(s)) as u32,
            });
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
            preferred_rung: 0,
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
            preferred_rung: 0,
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
            preferred_rung: 0,
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
            preferred_rung: 0,
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
            preferred_rung: 0,
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
    fn far_behind_head_rebuilds_to_high_ms_and_still_fills_the_live_edge() {
        // Playout paused/seeked far back (anchor=5) while the encoder has kept
        // producing all the way out to 205 — e.g. a long DVR pause, or the
        // aftermath of a network outage where the encoder kept encoding
        // locally the whole time. Continuity now rebuilds all the way to
        // `high_ms` (8 segments: 5..=12) rather than stopping at the smaller
        // `target_ms` — the fix for the "buffer plateaus at target_ms, then
        // suddenly lurches upward once the anchor happens to walk into the
        // backlog" behavior (see CLAUDE.md's "buffer rebuild ignored idle
        // bandwidth" entry). Live-edge still separately covers the newest
        // `target_ms` tail (202..=205). The genuine middle — beyond what
        // continuity's `high_ms` reach and the live-edge tail cover — is
        // still deliberately left alone this tick (bounded per-tick work);
        // that's still real backlog (13..=201) waiting for later ticks.
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
            preferred_rung: 0,
        };
        let plan = plan_uploads(&input);

        assert!(!plan.is_empty());
        let continuity: Vec<_> = plan
            .iter()
            .filter(|a| a.reason == UploadReason::Continuity)
            .collect();
        assert_eq!(
            continuity.iter().map(|a| a.seq).collect::<Vec<_>>(),
            (5..=12).collect::<Vec<_>>(),
            "continuity should rebuild the full high_ms margin (8 segments from the anchor): {continuity:?}"
        );
        assert!(continuity.iter().all(|a| a.rung == 0));

        let live_edge: Vec<_> = plan
            .iter()
            .filter(|a| a.reason == UploadReason::LiveEdge)
            .collect();
        assert!(
            live_edge.iter().all(|a| a.seq >= 202),
            "live edge should still only touch its own tail window: {live_edge:?}"
        );
        assert!(!live_edge.is_empty());

        // Nothing touches the genuine middle backlog this tick (still bounded
        // per-tick work) except the single upgrade right at the freshly
        // rebuilt boundary (see below).
        for mid in [50, 100, 150, 201] {
            assert!(plan.iter().all(|a| a.seq != mid));
        }

        // Once continuity's rebuild reaches exactly `high_ms` this tick, the
        // margin is "comfortable" and Tier C may begin — but only strictly
        // past what continuity just secured (13), never re-touching the
        // rebuilt segments themselves.
        let upgrades: Vec<_> = plan
            .iter()
            .filter(|a| a.reason == UploadReason::Upgrade)
            .collect();
        assert!(
            upgrades.iter().all(|a| a.seq > 12),
            "an upgrade landed inside the just-rebuilt continuity window: {upgrades:?}"
        );
    }

    /// End-to-end regression for the reported scenario: a large configured
    /// standing buffer (`high_ms` = 300s, matching a deliberately deep
    /// margin an operator wants so a link that already dropped once has room
    /// to drop again without an audible gap), a network outage that leaves a
    /// big local backlog, then reconnect. Simulates the real per-tick loop
    /// (`max_actions` capped at 16/tick, matching `uploader.rs`) across many
    /// ticks, folding each tick's continuity fills back into `server_best`
    /// like the real uploader folds `ServerState` from upload replies. Must
    /// show: zero upgrades while the margin is being rebuilt, and upgrades
    /// only starting once the margin has actually reached `high_ms` — never
    /// the old behavior of sitting flat at a small `target_ms` for many
    /// ticks while idle continuity budget went unused.
    #[test]
    fn after_reconnect_rebuilds_the_full_margin_before_any_upgrade_across_ticks() {
        let water = WaterLevels {
            low_ms: 4000,
            target_ms: 8000,
            high_ms: 300_000, // 300s standing buffer target
        };
        let seg_ms = 2000u32;
        let total_segs = (water.high_ms / seg_ms) as u64; // 150
        let profile = profile();
        let srv_base = |anchor: Seq| server(PlayoutState::Playing, Some(anchor), water);

        // Encoder produced a deep backlog locally during the outage — far
        // more than the 150 segments needed to satisfy `high_ms` — and the
        // server has acked nothing yet at/after the anchor.
        let anchor: Seq = 1000;
        let encoded_seq = anchor + total_segs + 200;
        let mut server_best: BTreeMap<Seq, Option<RungId>> = BTreeMap::new();

        let mut ticks = 0u32;
        let mut first_upgrade_tick = None;
        let mut saw_any_upgrade_before_rebuild = false;
        loop {
            ticks += 1;
            assert!(ticks < 100, "rebuild should complete well within 100 ticks");

            let inv = LocalInventory {
                encoded_seq,
                oldest_seq: 0,
                available: (0..=encoded_seq).map(|s| (s, vec![0, 1, 2, 3])).collect(),
                server_best: server_best.clone(),
            };
            let srv = srv_base(anchor);
            let input = SchedulerInput {
                profile: &profile,
                server: &srv,
                inv: &inv,
                throughput_kbps: 5000, // ample bandwidth, matching a healthy reconnected link
                headroom: 0.9,
                max_actions: 16, // matches the real uploader's per-tick cap
                preferred_rung: 0,
            };
            let plan = plan_uploads(&input);

            // Fold this tick's continuity fills in *before* judging whether
            // the rebuild is complete — a tick that finishes the rebuild is
            // allowed to also start upgrading within that same tick (the
            // scheduler's own internal `lead_ms` already reflects its own
            // just-scheduled fills when it decides `comfortable`), so the
            // external view needs to match that rather than lag it by one
            // tick.
            for a in &plan {
                if a.reason == UploadReason::Continuity {
                    server_best.insert(a.seq, Some(a.rung));
                }
            }

            let rebuilt_segs = (0..total_segs)
                .filter(|i| server_best.get(&(anchor + i)) == Some(&Some(0)))
                .count() as u64;
            let rebuild_complete = rebuilt_segs >= total_segs;

            if plan.iter().any(|a| a.reason == UploadReason::Upgrade) {
                if first_upgrade_tick.is_none() {
                    first_upgrade_tick = Some(ticks);
                }
                if !rebuild_complete {
                    saw_any_upgrade_before_rebuild = true;
                }
            }

            if rebuild_complete && plan.iter().all(|a| a.reason != UploadReason::Continuity) {
                break;
            }
        }

        assert!(
            !saw_any_upgrade_before_rebuild,
            "an upgrade fired before the {total_segs}-segment margin was fully rebuilt"
        );
        assert!(
            first_upgrade_tick.is_some(),
            "expected upgrades to start once the margin was rebuilt"
        );
        assert!(
            ticks > 1,
            "the 150-segment rebuild should take multiple ticks at a 16-action/tick cap, not complete in one"
        );
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
            preferred_rung: 0,
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
            preferred_rung: 0,
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
            preferred_rung: 0,
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
            preferred_rung: 0,
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
            preferred_rung: 1, // low_rung() of filtered=[1,2,3]
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
                preferred_rung: 0, // low_rung() of filtered=[0,1,2]
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

    /// The operator's "default quality" picker (`preferred_rung`) must have a
    /// real, immediate effect: with ample bandwidth, Tier B (live edge)
    /// should cover the newest window at the preferred rung, not just `low`.
    /// Regression test for the picker being a complete no-op (see CLAUDE.md).
    #[test]
    fn live_edge_prefers_the_operator_chosen_default_rung_when_budget_allows() {
        let water = WaterLevels {
            low_ms: 4000,
            target_ms: 8000,
            high_ms: 16000,
        };
        // Anchor at the live edge (stopped) with plenty already secured
        // behind it isn't needed here — just enough lead to leave survival
        // mode so Tier B actually runs.
        let srv = server(PlayoutState::Playing, Some(20), water);
        // Server already holds everything up to 27 (>= target_ms/seg_ms=4
        // segments past the anchor), so continuity is fully satisfied and
        // lead_ms comfortably clears `low_ms` — not in survival mode.
        let cov: Vec<(Seq, Option<RungId>)> = (20..=30).map(|s| (s, Some(0))).collect();
        let inv = inv_full(0, 50, &cov);
        let input = SchedulerInput {
            profile: &profile(),
            server: &srv,
            inv: &inv,
            throughput_kbps: 5000, // huge headroom, preferred rung easily affordable
            headroom: 0.9,
            max_actions: 20,
            preferred_rung: 3, // operator picked "hd"
        };
        let plan = plan_uploads(&input);

        let live_edge: Vec<_> = plan
            .iter()
            .filter(|a| a.reason == UploadReason::LiveEdge)
            .collect();
        assert!(!live_edge.is_empty(), "expected some live-edge coverage");
        assert!(
            live_edge.iter().all(|a| a.rung == 3),
            "live edge should use the operator's preferred rung when affordable: {live_edge:?}"
        );
    }

    /// When the preferred rung doesn't fit the tick's bandwidth budget,
    /// live edge must still fall back to the cheap low rung rather than
    /// leaving the newest window uncovered.
    #[test]
    fn live_edge_falls_back_to_low_rung_when_preferred_rung_exceeds_budget() {
        let water = WaterLevels {
            low_ms: 4000,
            target_ms: 8000,
            high_ms: 16000,
        };
        let srv = server(PlayoutState::Playing, Some(20), water);
        let cov: Vec<(Seq, Option<RungId>)> = (20..=30).map(|s| (s, Some(0))).collect();
        let inv = inv_full(0, 50, &cov);
        let input = SchedulerInput {
            profile: &profile(),
            server: &srv,
            inv: &inv,
            // Barely enough for the 48kbps low rung, nowhere near the
            // 320kbps "hd" rung the operator picked.
            throughput_kbps: 60,
            headroom: 0.9,
            max_actions: 20,
            preferred_rung: 3, // operator picked "hd"
        };
        let plan = plan_uploads(&input);

        let live_edge: Vec<_> = plan
            .iter()
            .filter(|a| a.reason == UploadReason::LiveEdge)
            .collect();
        assert!(
            !live_edge.is_empty(),
            "expected a low-rung fallback, not silence"
        );
        assert!(
            live_edge.iter().all(|a| a.rung == 0),
            "starved link must fall back to the cheap low rung: {live_edge:?}"
        );
    }

    /// If the preferred rung isn't locally available yet (e.g. that rung's
    /// encode hasn't caught up), live edge must fall back to low rather than
    /// skip the segment entirely.
    #[test]
    fn live_edge_falls_back_to_low_rung_when_preferred_rung_not_locally_available() {
        let water = WaterLevels {
            low_ms: 4000,
            target_ms: 8000,
            high_ms: 16000,
        };
        let srv = server(PlayoutState::Playing, Some(20), water);
        let cov: Vec<(Seq, Option<RungId>)> = (20..=30).map(|s| (s, Some(0))).collect();
        let mut inv = inv_full(0, 50, &cov);
        // Only the low rung has actually been encoded locally for the newest
        // segments — the "hd" rung hasn't been produced yet.
        for s in 31..=50 {
            inv.available.insert(s, vec![0]);
        }
        let input = SchedulerInput {
            profile: &profile(),
            server: &srv,
            inv: &inv,
            throughput_kbps: 5000,
            headroom: 0.9,
            max_actions: 20,
            preferred_rung: 3,
        };
        let plan = plan_uploads(&input);

        let live_edge: Vec<_> = plan
            .iter()
            .filter(|a| a.reason == UploadReason::LiveEdge)
            .collect();
        assert!(!live_edge.is_empty());
        assert!(
            live_edge.iter().all(|a| a.rung == 0),
            "must fall back to low when the preferred rung isn't on disk yet: {live_edge:?}"
        );
    }

    /// `preferred_rung` must never leak into Tier A (continuity) while an
    /// actual playout head is being defended — continuity is the
    /// dropout-safety net there and must always use the cheap low rung
    /// regardless of the operator's chosen default quality. (Continuity
    /// *does* honor `preferred_rung`, with the same low-rung fallback, when
    /// there's no active head to defend — see
    /// `continuity_prefers_operator_chosen_rung_while_stopped_and_uninitialized`
    /// below — since there's no dropout to protect against yet.)
    #[test]
    fn continuity_ignores_preferred_rung_even_with_ample_budget() {
        let water = WaterLevels {
            low_ms: 4000,
            target_ms: 8000,
            high_ms: 16000,
        };
        // Draining scenario, same shape as
        // `draining_flaky_link_sends_low_rung_at_playout_head_only`, but now
        // with a high-quality `preferred_rung` and huge bandwidth to prove
        // continuity still never touches anything but low.
        let srv = server(PlayoutState::Playing, Some(20), water);
        let inv = inv_full(0, 50, &[]);
        let input = SchedulerInput {
            profile: &profile(),
            server: &srv,
            inv: &inv,
            throughput_kbps: 5000,
            headroom: 0.9,
            max_actions: 8,
            preferred_rung: 3,
        };
        let plan = plan_uploads(&input);

        assert!(!plan.is_empty());
        assert!(
            plan.iter()
                .filter(|a| a.reason == UploadReason::Continuity)
                .all(|a| a.rung == 0),
            "continuity must never use preferred_rung: {plan:?}"
        );
    }

    /// Regression test for the "default quality" picker being a no-op while
    /// the playhead has never been initialized (`Stopped`, `position_seq:
    /// None`) — e.g. right after starting the encoder, before a manual or
    /// auto-start. Anchor falls back to the live edge itself in this case,
    /// so continuity's "secure `target_ms` ahead of anchor" walk claims
    /// every newly-produced segment, starving Tier B (whose identical newest-
    /// window scan finds nothing left to do) and silently pinning pre-roll
    /// buffering to the low rung regardless of the operator's pick. With
    /// ample bandwidth, continuity should now bank the preferred rung
    /// instead.
    #[test]
    fn continuity_prefers_operator_chosen_rung_while_stopped_and_uninitialized() {
        let water = WaterLevels {
            low_ms: 4000,
            target_ms: 8000,
            high_ms: 16000,
        };
        let srv = server(PlayoutState::Stopped, None, water);
        // Fresh stream: server has nothing yet, encoder has a little local
        // backlog (56..=... in the range the loop can consider) at every
        // rung.
        let inv = inv_full(0, 55, &[]);
        let input = SchedulerInput {
            profile: &profile(),
            server: &srv,
            inv: &inv,
            throughput_kbps: 5000, // huge headroom, "mid" easily affordable
            headroom: 0.9,
            max_actions: 20,
            preferred_rung: 1, // operator picked "mid"
        };
        let plan = plan_uploads(&input);

        let continuity: Vec<_> = plan
            .iter()
            .filter(|a| a.reason == UploadReason::Continuity)
            .collect();
        assert!(!continuity.is_empty(), "expected pre-roll uploads");
        assert!(
            continuity.iter().all(|a| a.rung == 1),
            "continuity should bank the operator's preferred rung while no \
             playout head is being defended: {continuity:?}"
        );
    }

    /// Same uninitialized-playhead scenario, but the link can't afford the
    /// preferred rung — continuity must still fall back to `low` (bursting
    /// past budget, same as its ordinary dropout-safety behavior) rather
    /// than leaving pre-roll segments unfilled.
    #[test]
    fn continuity_falls_back_to_low_rung_while_stopped_when_preferred_rung_unaffordable() {
        let water = WaterLevels {
            low_ms: 4000,
            target_ms: 8000,
            high_ms: 16000,
        };
        let srv = server(PlayoutState::Stopped, None, water);
        let inv = inv_full(0, 55, &[]);
        let input = SchedulerInput {
            profile: &profile(),
            server: &srv,
            inv: &inv,
            // Barely enough for the 48kbps low rung, nowhere near "hd".
            throughput_kbps: 60,
            headroom: 0.9,
            max_actions: 20,
            preferred_rung: 3,
        };
        let plan = plan_uploads(&input);

        let continuity: Vec<_> = plan
            .iter()
            .filter(|a| a.reason == UploadReason::Continuity)
            .collect();
        assert!(
            !continuity.is_empty(),
            "starved link must still fill pre-roll segments, not go silent"
        );
        assert!(
            continuity.iter().all(|a| a.rung == 0),
            "starved link must fall back to the cheap low rung: {continuity:?}"
        );
    }

    /// Same uninitialized-playhead scenario; the preferred rung hasn't been
    /// encoded locally yet — continuity must fall back to `low` rather than
    /// stall waiting for it.
    #[test]
    fn continuity_falls_back_to_low_rung_while_stopped_when_preferred_rung_not_locally_available() {
        let water = WaterLevels {
            low_ms: 4000,
            target_ms: 8000,
            high_ms: 16000,
        };
        let srv = server(PlayoutState::Stopped, None, water);
        let mut inv = inv_full(0, 55, &[]);
        for s in 0..=55 {
            inv.available.insert(s, vec![0]); // only the low rung is on disk
        }
        let input = SchedulerInput {
            profile: &profile(),
            server: &srv,
            inv: &inv,
            throughput_kbps: 5000,
            headroom: 0.9,
            max_actions: 20,
            preferred_rung: 1,
        };
        let plan = plan_uploads(&input);

        let continuity: Vec<_> = plan
            .iter()
            .filter(|a| a.reason == UploadReason::Continuity)
            .collect();
        assert!(!continuity.is_empty());
        assert!(
            continuity.iter().all(|a| a.rung == 0),
            "must fall back to low when the preferred rung isn't on disk yet: {continuity:?}"
        );
    }
}
