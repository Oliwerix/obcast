//! The uploader loop: call `plan_uploads` every tick, send actions in
//! priority order, fold feedback back into the shared `ServerState` model.
//! All policy lives in the scheduler — this loop is mechanical.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use obcast_proto::scheduler::{plan_uploads, LocalInventory, SchedulerInput};
use obcast_proto::state::{RungId, ServerState, StreamProfile};

use crate::inventory;
use crate::shared::SharedState;

pub struct Config {
    pub base_url: String,
    pub stream: String,
    pub ingest_token: Option<String>,
    pub out_dir: PathBuf,
    pub profile: StreamProfile,
}

pub async fn run(client: reqwest::Client, cfg: Config, shared: Arc<SharedState>) {
    let rungs: Vec<RungId> = cfg.profile.rungs.iter().map(|r| r.id).collect();
    // Seed conservatively; corrected from real upload timing after the first send.
    let mut throughput_kbps: u32 = cfg.profile.bitrate_of(cfg.profile.low_rung()) * 4;
    let mut tick = tokio::time::interval(Duration::from_millis(500));

    loop {
        tick.tick().await;

        let scan = inventory::scan(&cfg.out_dir, &rungs);
        let server = shared.server.lock().await.clone();
        let server_best: BTreeMap<_, _> = server
            .coverage
            .iter()
            .map(|c| (c.seq, c.best_rung))
            .collect();

        let inv = LocalInventory {
            encoded_seq: scan.encoded_seq,
            oldest_seq: scan.oldest_seq,
            available: scan.available,
            server_best,
        };

        let actions = plan_uploads(&SchedulerInput {
            profile: &cfg.profile,
            server: &server,
            inv: &inv,
            throughput_kbps,
            headroom: 0.9,
            max_actions: 16,
        });

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
                    if let Ok(state) = resp.json::<ServerState>().await {
                        shared.update(state).await;
                    }
                    tracing::info!(seq = action.seq, rung = action.rung, reason = ?action.reason, "uploaded");
                }
                Ok(resp) => {
                    tracing::warn!(seq = action.seq, rung = action.rung, status = %resp.status(), "upload rejected");
                }
                Err(err) => {
                    tracing::warn!(seq = action.seq, rung = action.rung, error = %err, "upload failed, retrying next tick");
                }
            }
        }
    }
}
