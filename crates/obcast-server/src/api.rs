//! Control-plane REST + WebSocket surface (M6): `GET /api/{stream}/status`,
//! `POST /api/{stream}/playout`, and `WS /api/{stream}/ws` streaming
//! `ControlEvent`s. See `docs/protocol.md` §4.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
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
/// 50ms (20Hz) keeps the web remote's meters feeling live rather than
/// visibly stepping — the ballistics themselves (`obcast_proto::meter`)
/// already do the smoothing, so there's no need for a slower tick.
const METERS_INTERVAL: Duration = Duration::from_millis(50);

/// Gate on the control-plane token, same `X-Auth` convention and
/// "no token configured = auth disabled" semantics as ingest's
/// `check_auth` (`ingest.rs`) — kept as a small local duplicate rather than
/// shared, since the two check different tokens for different trust
/// domains (upload credential vs. hardware-output control credential).
/// Takes the expected token directly (`AppState::control_token()`) rather
/// than a `StreamHandle`, so it can run *before* any stream lookup — see
/// `set_playout`, where checking against an already-fetched handle would
/// mean a bad/missing token still paid for spinning up a stream's
/// permanent playout thread before being rejected.
fn check_control_auth(expected: Option<&str>, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(expected) = expected else {
        return Ok(());
    };
    let got = headers.get("x-auth").and_then(|v| v.to_str().ok());
    if got == Some(expected) {
        Ok(())
    } else {
        Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "invalid or missing X-Auth".into(),
        ))
    }
}

/// Resolves a `PlayoutPosition` against the current DVR window, returning
/// the target segment plus an intra-segment offset (in ms) to skip past
/// before that segment's audio starts draining — nonzero only for
/// `MillisBehindLive`'s sub-segment precision; every other variant lands
/// exactly on a segment boundary. See `resolve_millis_behind_live` for the
/// pure math this delegates to.
async fn resolve_position(
    handle: &StreamHandle,
    pos: PlayoutPosition,
) -> Result<(Seq, u32), ApiError> {
    let store = handle.store.lock().await;
    let live = store
        .live_seq()
        .ok_or_else(|| ApiError(StatusCode::CONFLICT, "no segments buffered yet".into()))?;
    let start = store.dvr_start_seq().unwrap_or(live);
    let seg_ms = store.profile().segment_ms.max(1);

    let (target, skip_ms) = match pos {
        PlayoutPosition::Live => (live, 0),
        PlayoutPosition::Seq(s) => (s, 0),
        PlayoutPosition::SecondsBehindLive(secs) => {
            (live.saturating_sub((secs * 1000 / seg_ms) as u64), 0)
        }
        PlayoutPosition::MillisBehindLive(ms) => {
            resolve_millis_behind_live(start, live, seg_ms, ms)
        }
    };
    Ok((target.clamp(start, live), skip_ms))
}

/// Pure core of `MillisBehindLive` resolution: maps "N ms behind the live
/// edge" onto a `(segment, intra-segment ms offset)` pair, treating the DVR
/// window as one continuous timeline where `start`'s segment begins at 0 and
/// `live`'s segment nominally ends at `(live - start + 1) * seg_ms` (i.e. the
/// live segment counts as a full nominal segment even if still filling in).
/// Clamped into `[start, live]` with `skip_ms = 0` at either edge, matching
/// `SecondsBehindLive`'s existing clamp-to-window behavior.
fn resolve_millis_behind_live(start: Seq, live: Seq, seg_ms: u32, ms: u64) -> (Seq, u32) {
    let span_ms = (live - start + 1) * seg_ms as u64;
    let instant_ms = span_ms.saturating_sub(ms);
    let seg_ms = seg_ms as u64;
    let offset_segs = instant_ms / seg_ms;
    let skip_ms = (instant_ms % seg_ms) as u32;
    if offset_segs > live - start {
        (live, 0)
    } else {
        (start + offset_segs, skip_ms)
    }
}

pub async fn set_playout(
    State(app): State<Arc<AppState>>,
    Path(stream): Path<String>,
    headers: HeaderMap,
    Json(cmd): Json<PlayoutCommand>,
) -> Result<StatusCode, ApiError> {
    check_control_auth(app.control_token(), &headers)?;
    let handle = app.stream(&stream).await;

    match cmd {
        PlayoutCommand::Start { position } => {
            let (seq, skip_ms) = resolve_position(&handle, position).await?;
            handle.playout.send(EngineCommand::Start {
                position: seq,
                skip_ms,
            });
        }
        PlayoutCommand::Stop => handle.playout.send(EngineCommand::Stop),
        PlayoutCommand::Pause => handle.playout.send(EngineCommand::Pause),
        PlayoutCommand::Resume => handle.playout.send(EngineCommand::Resume),
        PlayoutCommand::Seek { position } => {
            let (seq, skip_ms) = resolve_position(&handle, position).await?;
            handle.playout.send(EngineCommand::Seek {
                position: seq,
                skip_ms,
            });
        }
        PlayoutCommand::GoLive => {
            let (seq, skip_ms) = resolve_position(&handle, PlayoutPosition::Live).await?;
            handle.playout.send(EngineCommand::Seek {
                position: seq,
                skip_ms,
            });
        }
        PlayoutCommand::SetVolume { gain } => {
            handle.playout.send(EngineCommand::SetVolume { gain })
        }
        PlayoutCommand::SetTestTone { enabled } => {
            handle.playout.send(EngineCommand::SetTestTone { enabled })
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
    let (server, encoder) = {
        let store = handle.store.lock().await;
        (
            store.build_server_state(handle.playout_status()),
            store.encoder_state().cloned(),
        )
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
    // On-air ground truth, straight from the playout engine: which rung it
    // actually fed the decoder for the segment currently draining. NOT a
    // live "best rung for this seq" lookup against `coverage`/the DVR index
    // — the engine feeds bytes many segments ahead of real-time playback
    // (see `playout.rs`'s `RING_SEGMENTS`), so a quality upgrade for a
    // segment already fed can land on disk before that segment is actually
    // heard; a live lookup would then report the new rung while the speaker
    // is still on the old one. See `PlayoutStatus::playing_rung`'s doc
    // comment and `LinkHealth::current_rung`'s.
    let current_rung = server.playout.playing_rung;
    let gaps = server
        .coverage
        .iter()
        .filter(|c| c.best_rung.is_none())
        .count() as u32;
    // Real telemetry once a heartbeat has arrived (see `ingest::heartbeat`);
    // 0 until then, same as before this was wired up.
    let throughput_kbps = encoder.as_ref().map_or(0, |e| e.throughput_kbps);

    ControlStatus {
        stream: stream.to_string(),
        server,
        encoder,
        link: LinkHealth {
            connected,
            last_segment_age_ms,
            current_rung,
            throughput_kbps,
            gaps,
        },
        recent_log: handle.recent_log(),
    }
}

pub async fn status(
    State(app): State<Arc<AppState>>,
    Path(stream): Path<String>,
) -> Result<Json<ControlStatus>, ApiError> {
    // Read-only: must not auto-vivify a stream nobody has ever ingested
    // into (see `AppState::stream_if_known`).
    let Some(handle) = app.stream_if_known(&stream).await else {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("no such stream: {stream}"),
        ));
    };
    Ok(Json(build_status(&stream, &handle).await))
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
    // Read-only: same no-auto-vivify rule as `status` above.
    let Some(handle) = app.stream_if_known(&stream).await else {
        return (StatusCode::NOT_FOUND, format!("no such stream: {stream}")).into_response();
    };
    ws.on_upgrade(move |socket| ws_session(stream, handle, socket))
}

async fn ws_session(stream: String, handle: Arc<StreamHandle>, mut socket: WebSocket) {
    let mut state_rx = handle.tx.subscribe();
    let mut log_rx = handle.log.subscribe();
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
            msg = log_rx.recv() => {
                match msg {
                    Ok(entry) => {
                        if send_event(&mut socket, &ControlEvent::Log(entry)).await.is_err() {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {} // LogSink outlives the socket; never actually closes
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
                let (vu_db_l, vu_db_r, ppm_db_l, ppm_db_r, peak_db_l, peak_db_r) =
                    handle.playout.meters();
                let event = ControlEvent::Meters {
                    vu_db_l,
                    vu_db_r,
                    ppm_db_l,
                    ppm_db_r,
                    peak_db_l,
                    peak_db_r,
                };
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
///
/// The store lock is held only long enough to snapshot which rung/path each
/// seq resolves to (`waveform::snapshot_index`) — never across the actual
/// `ffmpeg` decode loop (`waveform::build_from_index`). A multi-minute DVR
/// window means dozens of serial decodes here; holding the store's lock for
/// all of that would starve every other store user for the same span —
/// segment ingest and the playout engine's own per-tick segment lookup both
/// need it too — which is exactly what was surfacing as spurious playout
/// stalls ("waiting on segment N from the encoder") on an otherwise healthy
/// link once the web remote's 5s waveform poll had enough buffer to decode.
pub async fn waveform_handler(
    State(app): State<Arc<AppState>>,
    Path(stream): Path<String>,
    Query(q): Query<WaveformQuery>,
) -> Result<Json<WaveformJson>, ApiError> {
    // Read-only: same no-auto-vivify rule as `status`.
    let Some(handle) = app.stream_if_known(&stream).await else {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("no such stream: {stream}"),
        ));
    };

    let store = handle.store.clone();
    let log = handle.log.clone();
    let result = tokio::task::spawn_blocking(move || {
        let (entries, profile) = {
            let store = store.blocking_lock();
            let start = q
                .start_seq
                .unwrap_or_else(|| store.dvr_start_seq().unwrap_or(0));
            let end = q.end_seq.unwrap_or_else(|| store.live_seq().unwrap_or(0));
            if end < start {
                return Err("end_seq must be >= start_seq");
            }
            (
                waveform::snapshot_index(&store, start, end),
                store.profile().clone(),
            )
            // `store` (the MutexGuard) drops here, before any ffmpeg decode runs.
        };
        Ok(waveform::build_from_index(&profile, &entries, &log))
    })
    .await
    .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    result
        .map(Json)
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn millis_behind_live_zero_lands_exactly_on_the_live_edge() {
        assert_eq!(resolve_millis_behind_live(0, 10, 2000, 0), (10, 0));
    }

    #[test]
    fn millis_behind_live_mid_segment_offset_within_the_live_segment() {
        // 500ms behind live, with a 2000ms segment: still within the live
        // segment (10), 1500ms into it.
        assert_eq!(resolve_millis_behind_live(0, 10, 2000, 500), (10, 1500));
    }

    #[test]
    fn millis_behind_live_exact_segment_boundary_has_no_skip() {
        // Exactly one segment's duration (2000ms) behind the live edge is
        // exactly the *start* of the live segment itself — segment 10 spans
        // [20000, 22000) in this window, and 2000ms back from its end (22000)
        // is 20000, its own start.
        assert_eq!(resolve_millis_behind_live(0, 10, 2000, 2000), (10, 0));
    }

    #[test]
    fn millis_behind_live_partway_into_an_earlier_segment() {
        // 2500ms behind live: 500ms past segment 10's start, i.e. inside
        // segment 9 (which spans [18000, 20000)), 1500ms into it.
        assert_eq!(resolve_millis_behind_live(0, 10, 2000, 2500), (9, 1500));
    }

    #[test]
    fn millis_behind_live_beyond_the_dvr_window_clamps_to_start() {
        assert_eq!(resolve_millis_behind_live(5, 10, 2000, 1_000_000), (5, 0));
    }

    #[test]
    fn millis_behind_live_single_segment_window() {
        assert_eq!(resolve_millis_behind_live(7, 7, 2000, 0), (7, 0));
        assert_eq!(resolve_millis_behind_live(7, 7, 2000, 500), (7, 1500));
        assert_eq!(resolve_millis_behind_live(7, 7, 2000, 5000), (7, 0));
    }
}
