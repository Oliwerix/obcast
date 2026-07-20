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
    let target_duration = seg_secs.ceil() as u32;

    let mut out = String::new();
    let _ = writeln!(out, "#EXTM3U");
    let _ = writeln!(out, "#EXT-X-VERSION:3");
    let _ = writeln!(out, "#EXT-X-TARGETDURATION:{target_duration}");
    if let (Some(start), Some(end)) = (store.dvr_start_seq(), store.live_seq()) {
        let _ = writeln!(out, "#EXT-X-MEDIA-SEQUENCE:{start}");
        out.push_str(&playlist_body(seg_secs, start, end, |s| store.has_media(s)));
    }

    Ok(playlist_response(out))
}

/// Builds the `#EXTINF`/URI entries for one rendition playlist, walking the
/// *real* contiguous seq range `[start, end]` rather than only the seqs that
/// happen to have media on disk. This keeps every entry's position in the
/// list equal to its true seq number (`EXT-X-MEDIA-SEQUENCE + index`) at all
/// times — which is exactly what hls.js's live-playlist sync uses to tell
/// "already played" apart from "new" across refreshes. On the flaky OB
/// uplink this whole system targets, a segment commonly lands *after* later
/// ones already have (retry succeeds late) rather than being lost outright;
/// building the list from only the present seqs meant such a segment's
/// eventual arrival shifted every later entry's implied seq number by one,
/// which reads to hls.js as its already-buffered segments having silently
/// changed identity — the "playback jumps around" symptom. A seq with no
/// media yet (or ever, if abandoned) gets `#EXT-X-GAP` instead, per the HLS
/// spec extension for "skip this, don't stall on it."
fn playlist_body(seg_secs: f32, start: Seq, end: Seq, has_media: impl Fn(Seq) -> bool) -> String {
    let mut out = String::new();
    for seq in start..=end {
        if !has_media(seq) {
            let _ = writeln!(out, "#EXT-X-GAP");
        }
        let _ = writeln!(out, "#EXTINF:{seg_secs:.3},");
        let _ = writeln!(out, "{seq}.ts");
    }
    out
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn every_entry_position_equals_its_true_seq_number() {
        // seq 12 is missing (still in flight); every later entry must still
        // land at its real seq, not get shifted down to fill the hole.
        let present: BTreeSet<Seq> = [10, 11, 13, 14].into_iter().collect();
        let body = playlist_body(2.0, 10, 14, |s| present.contains(&s));
        assert_eq!(
            body,
            "#EXTINF:2.000,\n\
             10.ts\n\
             #EXTINF:2.000,\n\
             11.ts\n\
             #EXT-X-GAP\n\
             #EXTINF:2.000,\n\
             12.ts\n\
             #EXTINF:2.000,\n\
             13.ts\n\
             #EXTINF:2.000,\n\
             14.ts\n"
        );
    }

    #[test]
    fn a_late_arrival_only_flips_its_own_entry_and_leaves_every_other_position_untouched() {
        // This is the actual bug scenario: seq 12 lands late (a retry
        // succeeding after 13/14 already arrived, normal on a flaky OB
        // uplink). Before this fix, the playlist only ever listed present
        // seqs, so 12 showing up shifted 13 and 14 down by one list
        // position — exactly the identity-shift that confuses hls.js's
        // playlist-sync into replaying/skipping content ("jumps around").
        let before: BTreeSet<Seq> = [10, 11, 13, 14].into_iter().collect();
        let after: BTreeSet<Seq> = [10, 11, 12, 13, 14].into_iter().collect();

        // Compare by *segment slot* (i.e. which EXTINF entry a URI is),
        // not raw text line number — `#EXT-X-GAP` adds an extra line to
        // whichever entry it tags, so a raw line-number comparison would
        // spuriously "fail" even though the segment ordering hls.js cares
        // about (each EXTINF+URI is one slot) never actually shifted.
        let uris = |body: &str| -> Vec<String> {
            body.lines()
                .filter(|l| l.ends_with(".ts"))
                .map(String::from)
                .collect()
        };
        let before_uris = uris(&playlist_body(2.0, 10, 14, |s| before.contains(&s)));
        let after_uris = uris(&playlist_body(2.0, 10, 14, |s| after.contains(&s)));

        let slot_of = |uris: &[String], uri: &str| uris.iter().position(|u| u == uri);
        assert_eq!(
            slot_of(&before_uris, "13.ts"),
            slot_of(&after_uris, "13.ts")
        );
        assert_eq!(
            slot_of(&before_uris, "14.ts"),
            slot_of(&after_uris, "14.ts")
        );
    }

    #[test]
    fn no_gap_marker_when_every_seq_in_range_has_media() {
        let body = playlist_body(2.0, 5, 7, |_| true);
        assert!(!body.contains("GAP"));
        assert_eq!(body.matches("#EXTINF").count(), 3);
    }

    #[test]
    fn single_seq_range_is_one_entry() {
        let body = playlist_body(2.0, 5, 5, |s| s == 5);
        assert_eq!(body, "#EXTINF:2.000,\n5.ts\n");
    }
}
