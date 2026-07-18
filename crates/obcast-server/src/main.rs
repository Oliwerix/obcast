//! obcast-server: ingest + DVR store + `ServerState` computation + SSE state
//! feed (M1), plus an HLS origin (M4). Hardware playout and the control API
//! land in later milestones — see CLAUDE.md. `debug::set_playout` is a
//! stand-in for M5 so the scheduler's upgrade tier can be exercised.

mod debug;
mod ingest;
mod origin;
mod store;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use obcast_proto::state::{
    PlayoutState, PlayoutStatus, Rung, ServerState, StreamProfile, WaterLevels,
};
use tokio::sync::{broadcast, Mutex};

use store::DvrStore;

/// Per-stream state: the DVR index, the playout stub, and the SSE broadcaster.
pub struct StreamHandle {
    pub store: Mutex<DvrStore>,
    pub playout: Mutex<PlayoutStatus>,
    pub tx: broadcast::Sender<ServerState>,
    pub ingest_token: Option<String>,
}

pub struct AppState {
    streams: Mutex<HashMap<String, Arc<StreamHandle>>>,
    data_dir: PathBuf,
    profile: StreamProfile,
    water: WaterLevels,
    dvr_window_ms: u32,
    ingest_token: Option<String>,
}

impl AppState {
    /// Look up a stream's handle, lazily creating it on first contact.
    pub async fn stream(&self, name: &str) -> Arc<StreamHandle> {
        let mut streams = self.streams.lock().await;
        if let Some(handle) = streams.get(name) {
            return handle.clone();
        }
        let store = DvrStore::new(
            self.profile.clone(),
            self.water,
            self.dvr_window_ms,
            self.data_dir.join(name),
        );
        let (tx, _rx) = broadcast::channel(64);
        let handle = Arc::new(StreamHandle {
            store: Mutex::new(store),
            playout: Mutex::new(PlayoutStatus {
                state: PlayoutState::Stopped,
                position_seq: None,
                device: None,
                volume: 1.0,
            }),
            tx,
            ingest_token: self.ingest_token.clone(),
        });
        streams.insert(name.to_string(), handle.clone());
        handle
    }
}

fn default_profile() -> StreamProfile {
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

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let data_dir: PathBuf = std::env::var("OBCAST_DATA_DIR")
        .unwrap_or_else(|_| "./data".to_string())
        .into();
    let listen_addr: SocketAddr = std::env::var("OBCAST_LISTEN_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()
        .expect("invalid OBCAST_LISTEN_ADDR");
    let ingest_token = std::env::var("OBCAST_INGEST_TOKEN").ok();

    let app_state = Arc::new(AppState {
        streams: Mutex::new(HashMap::new()),
        data_dir,
        profile: default_profile(),
        water: WaterLevels::default(),
        dvr_window_ms: 5 * 60 * 1000,
        ingest_token,
    });

    let app = Router::new()
        .route("/ingest/:stream/segment", post(ingest::upload_segment))
        .route("/ingest/:stream/abandon", post(ingest::abandon))
        .route("/ingest/:stream/state", get(ingest::state_feed))
        .route("/hls/:stream/master.m3u8", get(origin::master_playlist))
        .route("/hls/:stream/:rendition/:tail", get(origin::rendition_tail))
        .route("/debug/:stream/playout", post(debug::set_playout))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(listen_addr)
        .await
        .expect("failed to bind listen address");
    tracing::info!(%listen_addr, "obcast-server listening");
    axum::serve(listener, app).await.expect("server error");
}
