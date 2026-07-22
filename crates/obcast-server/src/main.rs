//! obcast-server: ingest + DVR store + `ServerState` computation + SSE state
//! feed (M1), an HLS origin (M4), hardware playout via cpal (M5), and the
//! control API (REST + WS `ControlEvent`s) plus a static web remote (M6).
//! Auth hardening and packaging are M7 — see CLAUDE.md.

mod api;
mod config;
mod ingest;
mod logs;
mod origin;
mod playout;
mod shows;
mod store;
mod waveform;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use axum::routing::{delete, get, post};
use axum::Router;
use obcast_proto::control::{LogEntry, LogLevel};
use obcast_proto::state::{PlayoutStatus, ServerState, StreamProfile, WaterLevels};
use tokio::sync::{broadcast, Mutex};
use tower_http::services::ServeDir;

use config::AudioConfig;
use logs::LogSink;
use playout::PlayoutHandle;
use store::DvrStore;

/// Per-stream state: the DVR index, the playout engine handle, the SSE
/// broadcaster, and the status/error log sink. `store` is behind an `Arc`
/// (not just a `Mutex`) because the playout engine thread holds its own
/// clone alongside every HTTP handler; `log` is the same `Arc<LogSink>` the
/// playout thread was handed at spawn time (see `AppState::create_stream_handle`),
/// so both sides append to (and broadcast from) one shared backlog without a
/// circular `Arc<StreamHandle>` reference into the playout engine.
pub struct StreamHandle {
    pub store: Arc<Mutex<DvrStore>>,
    pub playout: Arc<PlayoutHandle>,
    pub tx: broadcast::Sender<ServerState>,
    pub log: Arc<LogSink>,
    pub ingest_token: Option<String>,
    pub last_ingest: Mutex<Option<Instant>>,
}

impl StreamHandle {
    pub fn playout_status(&self) -> PlayoutStatus {
        PlayoutStatus {
            state: self.playout.playout_state(),
            position_seq: self.playout.position(),
            playing_rung: self.playout.playing_rung(),
            fed_seq: self.playout.fed_seq(),
            position_ms_into_segment: self.playout.ms_into_current_segment(),
            device: self.playout.device_name(),
            volume: self.playout.volume(),
            detail: self.playout.detail(),
            test_tone: self.playout.test_tone(),
        }
    }

    /// Records a warn/error-level status message to this stream's log
    /// backlog and pushes it live to any subscribed WS clients. Additive to
    /// `tracing` — call alongside the existing `tracing::warn!`/`error!` at a
    /// call site, not instead of it.
    pub fn push_log(&self, level: LogLevel, message: impl Into<String>) {
        self.log.push(level, message);
    }

    pub fn recent_log(&self) -> Vec<LogEntry> {
        self.log.recent()
    }
}

pub struct AppState {
    streams: Mutex<HashMap<String, Arc<StreamHandle>>>,
    data_dir: PathBuf,
    profile: StreamProfile,
    water: WaterLevels,
    dvr_window_ms: u32,
    ingest_token: Option<String>,
    control_token: Option<String>,
    audio: AudioConfig,
    playout_ring_segments: usize,
}

impl AppState {
    fn create_stream_handle(&self, name: &str) -> Arc<StreamHandle> {
        let store = Arc::new(Mutex::new(DvrStore::new(
            self.profile.clone(),
            self.water,
            self.dvr_window_ms,
            self.data_dir.join(name),
        )));
        let rungs = self.profile.rungs.iter().map(|r| r.id).collect();
        // Constructed before both `playout::spawn` and `StreamHandle` below
        // and cloned into each, so the playout engine's dedicated thread and
        // every HTTP handler push to (and can subscribe to) the same log
        // backlog without the playout thread needing a handle back to the
        // `StreamHandle` that wraps it (which would be a circular `Arc`).
        let log = Arc::new(LogSink::new());
        let playout = playout::spawn(
            store.clone(),
            rungs,
            self.audio.clone(),
            self.profile.segment_ms,
            self.playout_ring_segments,
            log.clone(),
        );

        let (tx, _rx) = broadcast::channel(64);
        Arc::new(StreamHandle {
            store,
            playout,
            tx,
            log,
            ingest_token: self.ingest_token.clone(),
            last_ingest: Mutex::new(None),
        })
    }

    /// Look up a stream's handle, lazily creating it (and its playout
    /// engine thread) on first contact. Reserved for the ingest/media-plane
    /// entry points (`ingest.rs`), where "this name doesn't exist yet"
    /// legitimately means "a new stream is starting" — see
    /// `stream_if_known` for every read-only route, which must NOT do this.
    pub async fn stream(&self, name: &str) -> Arc<StreamHandle> {
        let mut streams = self.streams.lock().await;
        if let Some(handle) = streams.get(name) {
            return handle.clone();
        }
        let handle = self.create_stream_handle(name);
        streams.insert(name.to_string(), handle.clone());
        handle
    }

    /// Same lazy lookup as `stream()`, but safe for read-only control/HLS
    /// routes (`status`/`waveform`/`ws`, the HLS origin): never spins up a
    /// brand-new stream (permanent OS thread + `DvrStore`) for a name
    /// nobody has ever ingested into. Re-attaches an existing in-memory
    /// handle, or lazily re-opens one for a name with a real on-disk show
    /// directory (e.g. after a server restart); anything else — typos,
    /// probes, a listener guessing at names — gets `None` instead of
    /// leaking a thread forever. See CLAUDE.md §8 "per-stream resource
    /// leak".
    pub async fn stream_if_known(&self, name: &str) -> Option<Arc<StreamHandle>> {
        let mut streams = self.streams.lock().await;
        if let Some(handle) = streams.get(name) {
            return Some(handle.clone());
        }
        if !tokio::fs::try_exists(self.data_dir.join(name))
            .await
            .unwrap_or(false)
        {
            return None;
        }
        let handle = self.create_stream_handle(name);
        streams.insert(name.to_string(), handle.clone());
        Some(handle)
    }

    /// A snapshot of currently in-memory streams, without creating any new
    /// ones. Used to compute the `live` flag on the shows listing.
    pub async fn stream_snapshot(&self) -> Vec<(String, Arc<StreamHandle>)> {
        self.streams
            .lock()
            .await
            .iter()
            .map(|(name, handle)| (name.clone(), handle.clone()))
            .collect()
    }

    /// Removes and returns a stream's handle without creating one if absent.
    pub async fn remove_stream(&self, name: &str) -> Option<Arc<StreamHandle>> {
        self.streams.lock().await.remove(name)
    }

    pub(crate) fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// The control-plane credential, checked *before* any stream lookup —
    /// see `api.rs::set_playout`. Deliberately not read off a `StreamHandle`
    /// (unlike `control_token` there, which is just a per-handle cache of
    /// this same value): checking here means auth can reject a request
    /// before `stream()` ever runs, so a bad/missing token can't be used to
    /// spin up a stream's permanent playout thread just to get rejected.
    pub(crate) fn control_token(&self) -> Option<&str> {
        self.control_token.as_deref()
    }

    pub(crate) fn segment_ms(&self) -> u32 {
        self.profile.segment_ms
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
    // Deliberately a separate credential from `ingest_token`: an OB site's
    // upload token shouldn't also be able to stop/seek/set-volume the
    // server's hardware output. See CLAUDE.md §8 "auth split".
    let control_token = std::env::var("OBCAST_CONTROL_TOKEN").ok();
    // 0 disables eviction entirely (an unbounded DVR window, retaining every
    // segment for the life of the stream) — see `DvrStore::new`.
    let dvr_window_ms: u32 = std::env::var("OBCAST_DVR_WINDOW_MS")
        .ok()
        .map(|v| v.parse().expect("invalid OBCAST_DVR_WINDOW_MS"))
        .unwrap_or(5 * 60 * 1000);
    // How many segments deep the playout engine decodes ahead of real-time
    // output (see `playout.rs::run_engine`'s `ring_segments`) — trades
    // resilience to a transient decode/disk slowdown against ingest-to-
    // audible latency. Default 4 (8s at the 2s default segment length).
    let playout_ring_segments: usize = std::env::var("OBCAST_PLAYOUT_RING_SEGMENTS")
        .ok()
        .map(|v| v.parse().expect("invalid OBCAST_PLAYOUT_RING_SEGMENTS"))
        .unwrap_or(4);
    let server_cfg = config::ServerConfig::load();

    let app_state = Arc::new(AppState {
        streams: Mutex::new(HashMap::new()),
        data_dir,
        profile: StreamProfile::default_ladder(2000),
        water: WaterLevels::default(),
        dvr_window_ms,
        ingest_token,
        control_token,
        audio: server_cfg.audio,
        playout_ring_segments,
    });

    let app = Router::new()
        .route("/ingest/:stream/segment", post(ingest::upload_segment))
        .route("/ingest/:stream/abandon", post(ingest::abandon))
        .route("/ingest/:stream/heartbeat", post(ingest::heartbeat))
        .route("/ingest/:stream/state", get(ingest::state_feed))
        .route("/hls/:stream/master.m3u8", get(origin::master_playlist))
        .route("/hls/:stream/:rendition/:tail", get(origin::rendition_tail))
        .route("/api/:stream/status", get(api::status))
        .route("/api/:stream/playout", post(api::set_playout))
        .route("/api/:stream/ws", get(api::ws_handler))
        .route("/api/:stream/waveform", get(api::waveform_handler))
        .route("/api/shows", get(shows::list_shows))
        .route("/api/shows/:name", delete(shows::delete_show))
        .with_state(app_state)
        .nest_service("/remote", ServeDir::new(web_remote_dir()));

    let listener = tokio::net::TcpListener::bind(listen_addr)
        .await
        .expect("failed to bind listen address");
    tracing::info!(%listen_addr, "obcast-server listening");
    axum::serve(listener, app).await.expect("server error");
}
