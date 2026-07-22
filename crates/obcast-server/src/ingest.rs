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

use obcast_proto::control::LogLevel;
use obcast_proto::state::{EncoderState, PlayoutState, RungId, Seq, ServerState};

use crate::playout::EngineCommand;
use crate::{AppState, StreamHandle};

pub struct ApiError(pub StatusCode, pub String);

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

/// Starts playout on its own once the encoder's requested auto-start buffer
/// (`EncoderState::auto_start_buffer_ms`, set via heartbeat) has accumulated
/// — see `DvrStore::due_for_auto_start`. A no-op whenever playout isn't
/// `stopped`, which is what makes a manual start take precedence: once
/// started (by any means), this simply has nothing to do. Called after both
/// a segment ingest and a heartbeat, since either can be what pushes the
/// buffer over the requested target.
async fn maybe_auto_start(handle: &StreamHandle) {
    if handle.playout.playout_state() != PlayoutState::Stopped {
        return;
    }
    let fire_at = {
        let mut store = handle.store.lock().await;
        match store.due_for_auto_start() {
            Some(seq) => {
                let target = store
                    .encoder_state()
                    .and_then(|e| e.auto_start_buffer_ms)
                    .unwrap_or(0);
                store.mark_auto_start_fired(target);
                Some(seq)
            }
            None => None,
        }
    };
    let Some(seq) = fire_at else { return };
    tracing::info!(
        seq,
        "auto-start: requested buffer reached, starting playout"
    );
    handle.push_log(
        LogLevel::Info,
        format!("auto-start: requested buffer reached, starting playout at seq {seq}"),
    );
    handle.playout.send(EngineCommand::Start {
        position: seq,
        skip_ms: 0,
    });
    let state = handle.current_state().await;
    let _ = handle.tx.send(state);
}

/// Whether an incoming upload should reset this stream to a fresh DVR
/// session: the client marks this as the start of a new encoder session
/// (`X-New-Session`, sent only on the first upload after a real
/// go-live/ffmpeg respawn — see `uploader.rs::run`) *and* the stream has
/// gone at least `stale_reset_ms` without an ingested segment. Both
/// conditions are required: the marker alone (a quick restart, or a live
/// rung-toggle respawn, both well under the stale window) should just
/// append per CLAUDE.md §8, and the timer alone can't tell a genuine
/// restart apart from the same encoder recovering after a long-but-
/// survivable outage — a pure timeout would wipe real DVR history from an
/// ongoing broadcast exactly when the system is supposed to be proving its
/// resilience.
fn should_reset_for_new_session(new_session: bool, is_stale: bool, has_content: bool) -> bool {
    new_session && is_stale && has_content
}

/// Wipes an existing stream's DVR index and on-disk segments in place (the
/// `StreamHandle` stays in `AppState`'s map, so SSE/WS subscribers and the
/// playout thread keep the same handle — only its contents change) when
/// `should_reset_for_new_session` says this upload starts a genuinely new
/// broadcast. A no-op otherwise, which is the default: a same-name upload
/// just appends.
async fn maybe_reset_stale_session(
    app: &AppState,
    stream: &str,
    handle: &StreamHandle,
    new_session: bool,
) {
    let is_stale = handle.is_stale().await;
    let has_content = handle.store.lock().await.live_seq().is_some();
    if !should_reset_for_new_session(new_session, is_stale, has_content) {
        return;
    }
    tracing::warn!(
        stream,
        "X-New-Session marker past the stale-reset window: starting a fresh DVR session"
    );
    handle.push_log(
        LogLevel::Warn,
        "stream restarted after a long gap; starting a fresh DVR session".to_string(),
    );
    handle.playout.send(EngineCommand::Stop);
    *handle.store.lock().await = app.fresh_store(stream);
    let dir = app.data_dir().join(stream);
    match tokio::fs::remove_dir_all(&dir).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            tracing::warn!(stream, error = %e, "failed to clear stale segment directory");
            handle.push_log(
                LogLevel::Warn,
                format!("failed to clear stale segment directory: {e}"),
            );
        }
    }
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

    maybe_reset_stale_session(
        &app,
        &stream,
        &handle,
        headers.contains_key("x-new-session"),
    )
    .await;

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

    let evicted = {
        let mut store = handle.store.lock().await;
        // Never let eviction outrun the playout head — see `DvrStore::evict_old`.
        store.record(rung, seq, handle.playout.position())
    };
    let state = handle.current_state().await;
    *handle.last_ingest.lock().await = Some(std::time::Instant::now());
    let _ = handle.tx.send(state.clone());
    tracing::info!(stream, rung, seq, bytes = body.len(), "segment ingested");
    reap(&stream, &handle, evicted).await;
    maybe_auto_start(&handle).await;
    Ok(Json(state))
}

/// Delete segment files evicted from the DVR window index. Best-effort: a
/// missing file (already gone, or never written because the encoder
/// abandoned it before uploading) isn't an error, just nothing to reap.
async fn reap(stream: &str, handle: &StreamHandle, paths: Vec<std::path::PathBuf>) {
    for path in paths {
        match tokio::fs::remove_file(&path).await {
            Ok(()) => tracing::debug!(stream, path = %path.display(), "reaped evicted DVR segment"),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(stream, path = %path.display(), error = %err, "failed to reap evicted DVR segment");
                handle.push_log(
                    LogLevel::Warn,
                    format!(
                        "failed to reap evicted DVR segment {}: {err}",
                        path.display()
                    ),
                );
            }
        }
    }
}

/// `POST /ingest/{stream}/heartbeat`: encoder telemetry, independent of any
/// segment upload — lets the server populate `ControlStatus.encoder` and
/// real `throughput_kbps` even during a lull where nothing is being
/// uploaded (e.g. survival mode holding steady). See `docs/protocol.md` §3.
/// Purely additive dashboard/observability data — the encoder<->server
/// upload-scheduling feedback loop (CLAUDE.md §1) is driven entirely by
/// `ServerState`, piggybacked on uploads and the SSE feed; this route never
/// participates in that loop.
pub async fn heartbeat(
    State(app): State<Arc<AppState>>,
    Path(stream): Path<String>,
    headers: HeaderMap,
    Json(body): Json<EncoderState>,
) -> Result<StatusCode, ApiError> {
    let handle = app.stream(&stream).await;
    check_auth(&handle, &headers)?;

    handle.store.lock().await.set_encoder_state(body);
    maybe_auto_start(&handle).await;
    Ok(StatusCode::NO_CONTENT)
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
        handle.push_log(LogLevel::Warn, format!("segment {seq} abandoned"));
    }
    let state = handle.current_state().await;
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
    let initial = handle.current_state().await;

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
                    let state = handle.current_state().await;
                    yield Event::default().event("state").json_data(&state);
                }
            }
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quick_restart_under_the_stale_window_appends_rather_than_resets() {
        // A quick Stop→Go-Live (or a live rung-toggle respawn) sends the
        // new-session marker, but well under the stale window — CLAUDE.md
        // §8 says this should just append, not reset.
        assert!(!should_reset_for_new_session(true, false, true));
    }

    #[test]
    fn long_outage_on_the_same_encoder_process_never_resets() {
        // No marker was ever sent (the encoder process never restarted), so
        // even a very long gap since the last ingest must not reset — this
        // is exactly the "outage causes delay, not loss" scenario the whole
        // system exists to survive.
        assert!(!should_reset_for_new_session(false, true, true));
    }

    #[test]
    fn marker_past_the_stale_window_resets() {
        assert!(should_reset_for_new_session(true, true, true));
    }

    #[test]
    fn never_ingested_before_has_nothing_to_reset() {
        // A brand-new stream name: no prior content, so even a marker past
        // a (vacuously true) stale window is a no-op — there's no old
        // session to discard.
        assert!(!should_reset_for_new_session(true, true, false));
    }

    #[test]
    fn no_marker_and_not_stale_never_resets() {
        assert!(!should_reset_for_new_session(false, false, true));
    }
}
