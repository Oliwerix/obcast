//! Control-plane REST + WebSocket surface (M6): `GET /api/{stream}/status`,
//! `POST /api/{stream}/playout`, and `WS /api/{stream}/ws` streaming
//! `ControlEvent`s. See `docs/protocol.md` §4.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::Json;
use serde::Deserialize;

use obcast_proto::control::{
    ControlEvent, ControlStatus, LinkHealth, PlayoutCommand, PlayoutPosition,
};
use obcast_proto::state::Seq;

use crate::ingest::ApiError;
use crate::playout::EngineCommand;
use crate::waveform::{self, WaveformJson};
use crate::{AppState, StreamHandle};

/// A stream is considered "link down" (and no longer "live" in the shows
/// overview) once this long has passed since the last successful ingest.
pub(crate) const STALE_AFTER: Duration = Duration::from_secs(5);

/// How often the WS pushes `Meters`, independent of state-change events.
const METERS_INTERVAL: Duration = Duration::from_millis(200);

async fn resolve_position(handle: &StreamHandle, pos: PlayoutPosition) -> Result<Seq, ApiError> {
    let store = handle.store.lock().await;
    let live = store
        .live_seq()
        .ok_or_else(|| ApiError(StatusCode::CONFLICT, "no segments buffered yet".into()))?;
    let start = store.dvr_start_seq().unwrap_or(live);
    let seg_ms = store.profile().segment_ms.max(1);

    let target = match pos {
        PlayoutPosition::Live => live,
        PlayoutPosition::Seq(s) => s,
        PlayoutPosition::SecondsBehindLive(secs) => {
            live.saturating_sub((secs * 1000 / seg_ms) as u64)
        }
    };
    Ok(target.clamp(start, live))
}

pub async fn set_playout(
    State(app): State<Arc<AppState>>,
    Path(stream): Path<String>,
    Json(cmd): Json<PlayoutCommand>,
) -> Result<StatusCode, ApiError> {
    let handle = app.stream(&stream).await;

    match cmd {
        PlayoutCommand::Start { position } => {
            let seq = resolve_position(&handle, position).await?;
            handle.playout.send(EngineCommand::Start { position: seq });
        }
        PlayoutCommand::Stop => handle.playout.send(EngineCommand::Stop),
        PlayoutCommand::Pause => handle.playout.send(EngineCommand::Pause),
        PlayoutCommand::Resume => handle.playout.send(EngineCommand::Resume),
        PlayoutCommand::Seek { position } => {
            let seq = resolve_position(&handle, position).await?;
            handle.playout.send(EngineCommand::Seek { position: seq });
        }
        PlayoutCommand::GoLive => {
            let seq = resolve_position(&handle, PlayoutPosition::Live).await?;
            handle.playout.send(EngineCommand::Seek { position: seq });
        }
        PlayoutCommand::SetVolume { gain } => {
            handle.playout.send(EngineCommand::SetVolume { gain })
        }
        PlayoutCommand::SetDevice { .. } => {
            return Err(ApiError(
                StatusCode::NOT_IMPLEMENTED,
                "device selection not implemented; playout uses the system default output".into(),
            ));
        }
    }

    let state = {
        let store = handle.store.lock().await;
        store.build_server_state(handle.playout_status())
    };
    let _ = handle.tx.send(state);
    Ok(StatusCode::NO_CONTENT)
}

async fn build_status(stream: &str, handle: &StreamHandle) -> ControlStatus {
    let server = {
        let store = handle.store.lock().await;
        store.build_server_state(handle.playout_status())
    };

    let last_ingest = *handle.last_ingest.lock().await;
    let (connected, last_segment_age_ms) = match last_ingest {
        Some(t) => {
            let age = Instant::now().saturating_duration_since(t);
            (
                age < STALE_AFTER,
                age.as_millis().min(u32::MAX as u128) as u32,
            )
        }
        None => (false, u32::MAX),
    };
    let current_rung = server
        .live_seq
        .and_then(|live| server.coverage.iter().find(|c| c.seq == live)?.best_rung);
    let gaps = server
        .coverage
        .iter()
        .filter(|c| c.best_rung.is_none())
        .count() as u32;

    ControlStatus {
        stream: stream.to_string(),
        server,
        encoder: None,
        link: LinkHealth {
            connected,
            last_segment_age_ms,
            current_rung,
            throughput_kbps: 0,
            gaps,
        },
    }
}

pub async fn status(
    State(app): State<Arc<AppState>>,
    Path(stream): Path<String>,
) -> Json<ControlStatus> {
    let handle = app.stream(&stream).await;
    Json(build_status(&stream, &handle).await)
}

/// `WS /api/{stream}/ws`: pushes a full `Status` snapshot on connect and on
/// every `ServerState` change (piggybacking the same broadcast channel the
/// SSE link-plane feed uses), `Position` between snapshots as playout
/// advances, and `Meters` on a fixed tick for VU display.
pub async fn ws_handler(
    State(app): State<Arc<AppState>>,
    Path(stream): Path<String>,
    ws: WebSocketUpgrade,
) -> Response {
    let handle = app.stream(&stream).await;
    ws.on_upgrade(move |socket| ws_session(stream, handle, socket))
}

async fn ws_session(stream: String, handle: Arc<StreamHandle>, mut socket: WebSocket) {
    let mut state_rx = handle.tx.subscribe();
    let mut meters_tick = tokio::time::interval(METERS_INTERVAL);
    let mut last_position = handle.playout.position();

    let initial = build_status(&stream, &handle).await;
    if send_event(&mut socket, &ControlEvent::Status(Box::new(initial)))
        .await
        .is_err()
    {
        return;
    }

    loop {
        tokio::select! {
            msg = state_rx.recv() => {
                match msg {
                    Ok(_) => {
                        let status = build_status(&stream, &handle).await;
                        if send_event(&mut socket, &ControlEvent::Status(Box::new(status))).await.is_err() {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
            _ = meters_tick.tick() => {
                let pos = handle.playout.position();
                if pos != last_position {
                    last_position = pos;
                    if let Some(seq) = pos {
                        if send_event(&mut socket, &ControlEvent::Position { seq }).await.is_err() {
                            return;
                        }
                    }
                }
                let (vu_db, ppm_db) = handle.playout.meters();
                let event = ControlEvent::Meters { vu_db, ppm_db };
                if send_event(&mut socket, &event).await.is_err() {
                    return;
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None => return,
                    Some(Err(_)) => return,
                    _ => {} // ignore inbound text/ping/pong; commands go through POST /api/{stream}/playout
                }
            }
        }
    }
}

async fn send_event(socket: &mut WebSocket, event: &ControlEvent) -> Result<(), axum::Error> {
    let text = serde_json::to_string(event).expect("ControlEvent is always serializable");
    socket.send(Message::Text(text)).await
}

#[derive(Deserialize)]
pub struct WaveformQuery {
    /// Bound the decode work per request; defaults to the whole DVR window,
    /// which for a multi-minute buffer means dozens of `ffmpeg` spawns.
    start_seq: Option<Seq>,
    end_seq: Option<Seq>,
}

/// `GET /api/{stream}/waveform` — BBC waveform-data.js JSON (+ obcast's
/// `rungs`/`seqs` extension) covering `[start_seq, end_seq]`, defaulting to
/// the full DVR window. Decodes every segment in range via `ffmpeg`, so this
/// runs on a blocking task rather than the async executor.
pub async fn waveform_handler(
    State(app): State<Arc<AppState>>,
    Path(stream): Path<String>,
    Query(q): Query<WaveformQuery>,
) -> Result<Json<WaveformJson>, ApiError> {
    let handle = app.stream(&stream).await;

    let store = handle.store.clone();
    let result = tokio::task::spawn_blocking(move || {
        let store = store.blocking_lock();
        let start = q
            .start_seq
            .unwrap_or_else(|| store.dvr_start_seq().unwrap_or(0));
        let end = q.end_seq.unwrap_or_else(|| store.live_seq().unwrap_or(0));
        if end < start {
            return Err("end_seq must be >= start_seq");
        }
        Ok(waveform::build(&store, store.profile(), start, end))
    })
    .await
    .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    result
        .map(Json)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))
}
