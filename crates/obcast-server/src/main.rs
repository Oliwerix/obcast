//! obcast-server: ingest + DVR store + `ServerState` computation + SSE state
//! feed (M1), an HLS origin (M4), hardware playout via cpal (M5), and the
//! control API (REST + WS `ControlEvent`s) plus a static web remote (M6).
//! Auth hardening and packaging are M7 — see CLAUDE.md.

mod api;
mod ingest;
mod origin;
mod playout;
mod store;
mod waveform;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use axum::routing::{get, post};
use axum::Router;
use obcast_proto::state::{PlayoutStatus, Rung, ServerState, StreamProfile, WaterLevels};
use tokio::sync::{broadcast, Mutex};
use tower_http::services::ServeDir;

use playout::PlayoutHandle;
use store::DvrStore;

/// Per-stream state: the DVR index, the playout engine handle, and the SSE
/// broadcaster. `store` is behind an `Arc` (not just a `Mutex`) because the
/// playout engine thread holds its own clone alongside every HTTP handler.
pub struct StreamHandle {
    pub store: Arc<Mutex<DvrStore>>,
    pub playout: Arc<PlayoutHandle>,
    pub tx: broadcast::Sender<ServerState>,
    pub ingest_token: Option<String>,
    pub last_ingest: Mutex<Option<Instant>>,
}

impl StreamHandle {
    pub fn playout_status(&self) -> PlayoutStatus {
        PlayoutStatus {
            state: self.playout.playout_state(),
            position_seq: self.playout.position(),
            device: None,
            volume: self.playout.volume(),
        }
    }
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
    /// Look up a stream's handle, lazily creating it (and its playout
    /// engine thread) on first contact.
    pub async fn stream(&self, name: &str) -> Arc<StreamHandle> {
        let mut streams = self.streams.lock().await;
        if let Some(handle) = streams.get(name) {
            return handle.clone();
        }
        let store = Arc::new(Mutex::new(DvrStore::new(
            self.profile.clone(),
            self.water,
            self.dvr_window_ms,
            self.data_dir.join(name),
        )));
        let rungs = self.profile.rungs.iter().map(|r| r.id).collect();
        let playout = playout::spawn(store.clone(), rungs);

        let (tx, _rx) = broadcast::channel(64);
        let handle = Arc::new(StreamHandle {
            store,
            playout,
            tx,
            ingest_token: self.ingest_token.clone(),
            last_ingest: Mutex::new(None),
        });
        streams.insert(name.to_string(), handle.clone());
        handle
    }
}

/// Static web remote assets. Overridable so the server can be run from
/// outside the repo root (e.g. packaged deployment); defaults to the path
/// relative to this crate for local dev.
fn web_remote_dir() -> PathBuf {
    std::env::var("OBCAST_WEB_REMOTE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../web/remote"))
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
        .route("/api/:stream/status", get(api::status))
        .route("/api/:stream/playout", post(api::set_playout))
        .route("/api/:stream/ws", get(api::ws_handler))
        .route("/api/:stream/waveform", get(api::waveform_handler))
        .with_state(app_state)
        .nest_service("/remote", ServeDir::new(web_remote_dir()));

    let listener = tokio::net::TcpListener::bind(listen_addr)
        .await
        .expect("failed to bind listen address");
    tracing::info!(%listen_addr, "obcast-server listening");
    axum::serve(listener, app).await.expect("server error");
}
