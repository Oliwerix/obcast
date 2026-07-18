//! Media-plane handlers: segment upload, abandon, and the link-plane SSE feed.
//! See `docs/protocol.md` §2–3 for the wire contract.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::Stream;
use serde::Deserialize;

use obcast_proto::state::{RungId, Seq, ServerState};

use crate::{AppState, StreamHandle};

pub struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, self.1).into_response()
    }
}

fn bad_request(msg: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, msg.into())
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, ApiError> {
    headers
        .get(name)
        .ok_or_else(|| bad_request(format!("missing header {name}")))?
        .to_str()
        .map_err(|_| bad_request(format!("header {name} is not valid ASCII")))
}

fn header_u8(headers: &HeaderMap, name: &str) -> Result<u8, ApiError> {
    header_str(headers, name)?
        .parse()
        .map_err(|_| bad_request(format!("invalid {name}")))
}

fn header_u64(headers: &HeaderMap, name: &str) -> Result<u64, ApiError> {
    header_str(headers, name)?
        .parse()
        .map_err(|_| bad_request(format!("invalid {name}")))
}

fn check_auth(handle: &StreamHandle, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(expected) = &handle.ingest_token else {
        return Ok(());
    };
    let got = headers.get("x-auth").and_then(|v| v.to_str().ok());
    if got == Some(expected.as_str()) {
        Ok(())
    } else {
        Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "invalid or missing X-Auth".into(),
        ))
    }
}

async fn current_state(handle: &StreamHandle) -> ServerState {
    let store = handle.store.lock().await;
    let playout = handle.playout.lock().await.clone();
    store.build_server_state(playout)
}

pub async fn upload_segment(
    State(app): State<Arc<AppState>>,
    Path(stream): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<ServerState>, ApiError> {
    let handle = app.stream(&stream).await;
    check_auth(&handle, &headers)?;

    let rung: RungId = header_u8(&headers, "x-rendition")?;
    let seq: Seq = header_u64(&headers, "x-seq")?;

    let path = {
        let store = handle.store.lock().await;
        store.segment_path(rung, seq)
    };
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    tokio::fs::write(&path, &body)
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let state = {
        let mut store = handle.store.lock().await;
        store.record(rung, seq);
        let playout = handle.playout.lock().await.clone();
        store.build_server_state(playout)
    };
    let _ = handle.tx.send(state.clone());
    tracing::info!(stream, rung, seq, bytes = body.len(), "segment ingested");
    Ok(Json(state))
}

#[derive(Deserialize)]
pub struct AbandonRequest {
    seqs: Vec<Seq>,
}

pub async fn abandon(
    State(app): State<Arc<AppState>>,
    Path(stream): Path<String>,
    headers: HeaderMap,
    Json(body): Json<AbandonRequest>,
) -> Result<StatusCode, ApiError> {
    let handle = app.stream(&stream).await;
    check_auth(&handle, &headers)?;

    {
        let mut store = handle.store.lock().await;
        store.abandon(&body.seqs);
    }
    for seq in &body.seqs {
        tracing::warn!(stream, seq, "segment abandoned");
    }
    let state = current_state(&handle).await;
    let _ = handle.tx.send(state);
    Ok(StatusCode::NO_CONTENT)
}

/// SSE state feed. Pushes immediately on subscribe, on every server-state
/// change, and at least once a second so the feed survives upload stalls.
pub async fn state_feed(
    State(app): State<Arc<AppState>>,
    Path(stream): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, axum::Error>>> {
    let handle = app.stream(&stream).await;
    let mut rx = handle.tx.subscribe();
    let initial = current_state(&handle).await;

    let stream = async_stream::stream! {
        yield Event::default().event("state").json_data(&initial);

        let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(1));
        heartbeat.tick().await; // first tick fires immediately; we already sent `initial`

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Ok(state) => yield Event::default().event("state").json_data(&state),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = heartbeat.tick() => {
                    let state = current_state(&handle).await;
                    yield Event::default().event("state").json_data(&state);
                }
            }
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}
