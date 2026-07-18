//! Minimal control-plane REST surface so playout can be driven without a
//! GUI: `GET /api/{stream}/status` and `POST /api/{stream}/playout`. The
//! `ControlEvent` WebSocket in `docs/protocol.md` §4 is M6 — full control
//! API + web remote; this is just enough to exercise M5's hardware playout
//! engine end to end.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;

use obcast_proto::control::{ControlStatus, LinkHealth, PlayoutCommand, PlayoutPosition};
use obcast_proto::state::Seq;

use crate::ingest::ApiError;
use crate::playout::EngineCommand;
use crate::{AppState, StreamHandle};

/// A stream is considered "link down" once this long has passed since the
/// last successful ingest.
const STALE_AFTER: Duration = Duration::from_secs(5);

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

pub async fn status(
    State(app): State<Arc<AppState>>,
    Path(stream): Path<String>,
) -> Json<ControlStatus> {
    let handle = app.stream(&stream).await;
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

    Json(ControlStatus {
        stream,
        server,
        encoder: None,
        link: LinkHealth {
            connected,
            last_segment_age_ms,
            current_rung,
            throughput_kbps: 0,
            gaps,
        },
    })
}
