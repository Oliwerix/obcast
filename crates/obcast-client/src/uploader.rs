//! The uploader loop: call `plan_uploads` every tick, send actions in
//! priority order, fold feedback back into the shared `ServerState` model.
//! All policy lives in the scheduler — this loop is mechanical.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;

use obcast_proto::control::LogLevel;
use obcast_proto::scheduler::{
    plan_uploads, stalled_continuity_seq, LocalInventory, SchedulerInput,
};
use obcast_proto::state::{EncoderState, RungId, Seq, ServerState, StreamProfile};

use crate::inventory;
use crate::shared::SharedState;

pub struct Config {
    pub base_url: String,
    pub stream: String,
    pub ingest_token: Option<String>,
    pub out_dir: PathBuf,
    pub profile: StreamProfile,
    /// Requested auto-start buffer, in ms — forwarded to the server on every
    /// heartbeat as `EncoderState::auto_start_buffer_ms`. `None` disables it.
    pub auto_start_buffer_ms: Option<u32>,
    /// Operator's "default quality" pick: the rung to assume for the
    /// bootstrap bandwidth guess before real throughput/`ServerState`
    /// feedback arrives, *and* (every tick, not just at bootstrap) the rung
    /// `plan_uploads`'s live-edge tier tries first for newest-segment
    /// coverage, falling back to the profile's low rung when it's not
    /// affordable or not yet encoded locally (see
    /// `scheduler::SchedulerInput::preferred_rung`). Continuity is
    /// unaffected — it always uses the low rung regardless, so this never
    /// weakens the no-dropout guarantee. Resolved against `profile` via
    /// `StreamProfile::nearest_enabled_or_low` in case it's since been
    /// disabled.
    pub bootstrap_rung: RungId,
}

/// How long a permanent-looking continuity gap (missing on both the local
/// disk and the server, with the encoder already past it) must persist
/// before the uploader gives up and calls `/abandon`. Generous relative to a
/// normal short outage — CLAUDE.md §5 promises "no audio lost to a short
/// outage" — but bounded, so a gap that will truly never fill doesn't
/// freeze the playout head forever (see `playout.rs`'s matching backstop).
const ABANDON_AFTER: Duration = Duration::from_secs(20);

/// How often to POST `EncoderState` to `/ingest/{stream}/heartbeat`. Purely
/// dashboard/observability telemetry (see `docs/protocol.md` §3) — the real
/// upload-scheduling feedback loop (CLAUDE.md §1) runs off `ServerState`,
/// piggybacked on every upload reply and the SSE feed, independent of this.
/// Matches the server's own SSE heartbeat cadence (`ingest.rs::state_feed`).
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Serialize)]
struct AbandonBody<'a> {
    seqs: &'a [Seq],
}

pub async fn run(client: reqwest::Client, cfg: Config, shared: Arc<SharedState>) {
    let rungs: Vec<RungId> = cfg.profile.rungs.iter().map(|r| r.id).collect();
    let low = cfg.profile.low_rung();
    // Seed from the operator's preferred bootstrap rung (falling back to the
    // ladder's low rung if that preference was since disabled); corrected
    // from real upload timing after the first send.
    let bootstrap_rung = cfg.profile.nearest_enabled_or_low(cfg.bootstrap_rung);
    let mut throughput_kbps: u32 = cfg.profile.bitrate_of(bootstrap_rung) * 4;
    let mut tick = tokio::time::interval(Duration::from_millis(500));

    // Tracks the one continuity gap currently under an abandon countdown, and
    // every seq this client has already told the server to give up on (so we
    // don't re-POST `/abandon` for it every tick forever).
    let mut stalled: Option<(Seq, tokio::time::Instant)> = None;
    let mut abandoned_locally: BTreeSet<Seq> = BTreeSet::new();

    let mut heartbeat_rev: u64 = 0;
    let mut last_heartbeat: Option<tokio::time::Instant> = None;

    loop {
        tick.tick().await;

        let scan = inventory::scan(&cfg.out_dir, &rungs);
        let server = shared.server_state_or_unknown().await;
        let mut server_best: BTreeMap<_, _> = server
            .coverage
            .iter()
            .map(|c| (c.seq, c.best_rung))
            .collect();
        // Overlay our own abandon decisions: the server's coverage reports
        // these as missing (it has no media for them either), but as far as
        // the scheduler is concerned they're now "satisfied" — nothing will
        // ever fill them, so continuity should extend past them rather than
        // staying stuck at the gap forever.
        for &seq in &abandoned_locally {
            server_best.insert(seq, Some(low));
        }

        let mut inv = LocalInventory {
            encoded_seq: scan.encoded_seq,
            oldest_seq: scan.oldest_seq,
            available: scan.available,
            server_best,
        };

        if let Some(seq) = stalled_continuity_seq(&server, &inv, &cfg.profile) {
            let fire = match stalled {
                Some((s, since)) if s == seq => since.elapsed() >= ABANDON_AFTER,
                _ => {
                    stalled = Some((seq, tokio::time::Instant::now()));
                    false
                }
            };
            if fire {
                let mut req = client
                    .post(format!("{}/ingest/{}/abandon", cfg.base_url, cfg.stream))
                    .json(&AbandonBody { seqs: &[seq] });
                if let Some(token) = &cfg.ingest_token {
                    req = req.header("X-Auth", token.clone());
                }
                match req.send().await {
                    Ok(resp) if resp.status().is_success() => {
                        tracing::warn!(seq, "gave up on permanently missing segment, abandoned");
                        shared.push_log(
                            LogLevel::Warn,
                            format!("gave up on permanently missing segment {seq}, abandoned"),
                        );
                        abandoned_locally.insert(seq);
                        inv.server_best.insert(seq, Some(low));
                        stalled = None;
                    }
                    Ok(resp) => {
                        tracing::warn!(seq, status = %resp.status(), "abandon request rejected, will retry");
                        shared.push_log(
                            LogLevel::Warn,
                            format!(
                                "abandon request for seq {seq} rejected ({}), will retry",
                                resp.status()
                            ),
                        );
                    }
                    Err(err) => {
                        tracing::warn!(seq, error = %err, "abandon request failed, will retry");
                        shared.push_log(
                            LogLevel::Warn,
                            format!("abandon request for seq {seq} failed ({err}), will retry"),
                        );
                    }
                }
            }
        } else {
            stalled = None;
        }

        let actions = plan_uploads(&SchedulerInput {
            profile: &cfg.profile,
            server: &server,
            inv: &inv,
            throughput_kbps,
            headroom: 0.9,
            max_actions: 16,
            preferred_rung: bootstrap_rung,
        });

        // Updated every tick (not gated by the heartbeat interval below) so
        // the GUI's bandwidth meter — primary rung's bitrate vs. measured
        // link throughput — stays responsive.
        let primary_rung = actions.first().map(|a| a.rung).unwrap_or(low);
        shared.note_primary_rung(primary_rung);

        if last_heartbeat.is_none_or(|t| t.elapsed() >= HEARTBEAT_INTERVAL) {
            heartbeat_rev += 1;
            last_heartbeat = Some(tokio::time::Instant::now());
            let backlog = inv
                .available
                .keys()
                .filter(|seq| !inv.server_best.contains_key(seq))
                .count() as u32;
            let body = EncoderState {
                rev: heartbeat_rev,
                active_rungs: rungs.clone(),
                encoded_seq: (!inv.available.is_empty()).then_some(inv.encoded_seq),
                primary_rung,
                throughput_kbps,
                backlog,
                abandoned: abandoned_locally.iter().copied().collect(),
                auto_start_buffer_ms: cfg.auto_start_buffer_ms,
            };
            let mut req = client
                .post(format!("{}/ingest/{}/heartbeat", cfg.base_url, cfg.stream))
                .json(&body);
            if let Some(token) = &cfg.ingest_token {
                req = req.header("X-Auth", token.clone());
            }
            if let Err(err) = req.send().await {
                tracing::warn!(error = %err, "heartbeat failed, will retry next tick");
                shared.push_log(
                    LogLevel::Warn,
                    format!("heartbeat failed ({err}), will retry next tick"),
                );
            }
        }

        for action in actions {
            let path = cfg
                .out_dir
                .join(action.rung.to_string())
                .join(format!("{}.ts", action.seq));
            let Ok(bytes) = tokio::fs::read(&path).await else {
                continue;
            };
            let len = bytes.len();
            let started = tokio::time::Instant::now();

            let mut req = client
                .post(format!("{}/ingest/{}/segment", cfg.base_url, cfg.stream))
                .header("X-Rendition", action.rung.to_string())
                .header("X-Seq", action.seq.to_string())
                .body(bytes);
            if let Some(token) = &cfg.ingest_token {
                req = req.header("X-Auth", token.clone());
            }

            match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    let elapsed = started.elapsed().as_secs_f32().max(0.001);
                    throughput_kbps = ((len as f32 * 8.0 / 1000.0) / elapsed) as u32;
                    shared.note_upload(action.seq, throughput_kbps);
                    shared.note_sent(action.seq, action.rung);
                    if let Ok(state) = resp.json::<ServerState>().await {
                        shared.update(state).await;
                    }
                    tracing::info!(seq = action.seq, rung = action.rung, reason = ?action.reason, "uploaded");
                }
                Ok(resp) => {
                    tracing::warn!(seq = action.seq, rung = action.rung, status = %resp.status(), "upload rejected");
                    shared.push_log(
                        LogLevel::Warn,
                        format!(
                            "upload of seq {} (rung {}) rejected ({})",
                            action.seq,
                            action.rung,
                            resp.status()
                        ),
                    );
                }
                Err(err) => {
                    tracing::warn!(seq = action.seq, rung = action.rung, error = %err, "upload failed, retrying next tick");
                    shared.push_log(
                        LogLevel::Warn,
                        format!(
                            "upload of seq {} (rung {}) failed ({err}), retrying next tick",
                            action.seq, action.rung
                        ),
                    );
                }
            }
        }
    }
}
