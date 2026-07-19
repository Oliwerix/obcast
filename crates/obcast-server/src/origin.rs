//! HLS origin: master playlist, per-rendition sliding-window playlists, and
//! segment serving. A requested rendition/seq that isn't on disk falls back
//! to the best rung the server actually holds for that seq — the DVR store
//! only ever guarantees *some* rung is present, never a specific one, so the
//! origin has to make the same "never drop out" trade the scheduler makes.

use std::fmt::Write as _;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::Response;

use obcast_proto::state::{RungId, Seq};

use crate::AppState;

fn playlist_response(body: String) -> Response {
    Response::builder()
        .header(header::CONTENT_TYPE, "application/vnd.apple.mpegurl")
        .body(Body::from(body))
        .unwrap()
}

fn segment_response(bytes: Vec<u8>) -> Response {
    Response::builder()
        .header(header::CONTENT_TYPE, "video/mp2t")
        .body(Body::from(bytes))
        .unwrap()
}

pub async fn master_playlist(
    State(app): State<Arc<AppState>>,
    Path(stream): Path<String>,
) -> Result<Response, StatusCode> {
    // Read-only listener entry point: must not auto-vivify a stream nobody
    // has ever ingested into (see `AppState::stream_if_known`).
    let handle = app
        .stream_if_known(&stream)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    let store = handle.store.lock().await;
    let profile = store.profile();

    let mut out = String::from("#EXTM3U\n#EXT-X-VERSION:3\n");
    for rung in &profile.rungs {
        // Native `aac` only produces LC; the "lo" rung is HE-AAC in the design
        // doc but this MVP encoder can't produce that profile (no libfdk_aac).
        let _ = writeln!(
            out,
            "#EXT-X-STREAM-INF:BANDWIDTH={},CODECS=\"mp4a.40.2\"",
            rung.bitrate_kbps * 1000
        );
        let _ = writeln!(out, "{}/index.m3u8", rung.id);
    }

    Ok(playlist_response(out))
}

/// Handles both `/{rendition}/index.m3u8` and `/{rendition}/{seq}.ts` under
/// one route so a static "index.m3u8" segment never has to compete with a
/// dynamic seq param for the same path shape.
pub async fn rendition_tail(
    State(app): State<Arc<AppState>>,
    Path((stream, rendition, tail)): Path<(String, RungId, String)>,
) -> Result<Response, StatusCode> {
    if tail == "index.m3u8" {
        return rendition_playlist(&app, &stream).await;
    }
    let Some(seq_str) = tail.strip_suffix(".ts") else {
        return Err(StatusCode::NOT_FOUND);
    };
    let seq: Seq = seq_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    segment(&app, &stream, rendition, seq).await
}

async fn rendition_playlist(app: &AppState, stream: &str) -> Result<Response, StatusCode> {
    // Read-only listener entry point: same no-auto-vivify rule as
    // `master_playlist`.
    let handle = app
        .stream_if_known(stream)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    let store = handle.store.lock().await;
    let seg_secs = store.profile().segment_ms as f32 / 1000.0;
    let seqs: Vec<Seq> = store.playable_seqs().collect();

    let target_duration = seg_secs.ceil() as u32;
    let media_sequence = seqs.first().copied().unwrap_or(0);

    let mut out = String::new();
    let _ = writeln!(out, "#EXTM3U");
    let _ = writeln!(out, "#EXT-X-VERSION:3");
    let _ = writeln!(out, "#EXT-X-TARGETDURATION:{target_duration}");
    let _ = writeln!(out, "#EXT-X-MEDIA-SEQUENCE:{media_sequence}");
    for seq in seqs {
        let _ = writeln!(out, "#EXTINF:{seg_secs:.3},");
        let _ = writeln!(out, "{seq}.ts");
    }

    Ok(playlist_response(out))
}

async fn segment(
    app: &AppState,
    stream: &str,
    rendition: RungId,
    seq: Seq,
) -> Result<Response, StatusCode> {
    // Read-only listener entry point: same no-auto-vivify rule as
    // `master_playlist`.
    let handle = app
        .stream_if_known(stream)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    let store = handle.store.lock().await;

    let rung = if store.has_rung(seq, rendition) {
        rendition
    } else {
        store.best_rung(seq).ok_or(StatusCode::NOT_FOUND)?
    };
    let path = store.segment_path(rung, seq);
    drop(store);

    let bytes = tokio::fs::read(&path)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(segment_response(bytes))
}
