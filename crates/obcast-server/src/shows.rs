//! Admin API for the web UI's global shows overview: list every show (a show
//! is one named stream's directory under `data_dir`, live or historical) and
//! delete a past show, reaping its on-disk segments. Server-local admin
//! surface, not part of the encoder/server wire protocol — see
//! `docs/protocol.md` for what is.

use std::path::Path;
use std::sync::Arc;
use std::time::{Instant, UNIX_EPOCH};

use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;

use crate::ingest::ApiError;
use crate::playout::EngineCommand;
use crate::AppState;

#[derive(Serialize)]
pub struct ShowInfo {
    name: String,
    live: bool,
    segment_count: u64,
    size_bytes: u64,
    duration_ms: Option<u64>,
    modified_unix_ms: u64,
}

#[derive(Default)]
struct ShowDiskStats {
    segment_count: u64,
    size_bytes: u64,
    modified_unix_ms: u64,
    seqs: Vec<u64>,
}

/// `GET /api/shows`: every show directory under `data_dir`, sorted by
/// `modified_unix_ms` descending. Never touches `AppState::stream()` — a show
/// with no in-memory `StreamHandle` must not get one just from being listed.
pub async fn list_shows(State(app): State<Arc<AppState>>) -> Json<Vec<ShowInfo>> {
    // "live" means a segment actually arrived recently — not merely that an
    // in-memory `StreamHandle` exists. A handle lingers after the encoder dies
    // (and is auto-created just by opening the remote), so keying `live` on its
    // existence reports dead/never-fed streams as live. Match the link-health
    // staleness window used by `/api/{stream}/status`.
    let now = Instant::now();
    let mut live_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (name, handle) in app.stream_snapshot().await {
        let recent = handle
            .last_ingest
            .lock()
            .await
            .is_some_and(|t| now.saturating_duration_since(t) < crate::api::STALE_AFTER);
        if recent {
            live_names.insert(name);
        }
    }

    let mut entries = match tokio::fs::read_dir(app.data_dir()).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Json(Vec::new()),
        Err(e) => {
            tracing::warn!(error = %e, "failed to read data dir for shows listing");
            return Json(Vec::new());
        }
    };

    let mut shows = Vec::new();
    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(e) => {
                tracing::warn!(error = %e, "error scanning data dir");
                break;
            }
        };

        let file_type = match entry.file_type().await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(path = %entry.path().display(), error = %e, "failed to stat entry, skipping");
                continue;
            }
        };
        if !file_type.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            tracing::warn!(path = %entry.path().display(), "skipping non-UTF-8 show directory name");
            continue;
        };

        let stats = scan_show_dir(&entry.path()).await;
        shows.push(ShowInfo {
            live: live_names.contains(&name),
            segment_count: stats.segment_count,
            size_bytes: stats.size_bytes,
            duration_ms: duration_ms_from_seqs(&stats.seqs, app.segment_ms()),
            modified_unix_ms: stats.modified_unix_ms,
            name,
        });
    }

    shows.sort_by_key(|s| std::cmp::Reverse(s.modified_unix_ms));
    Json(shows)
}

/// Scans one show's rung subdirectories (`data_dir/{name}/{rung}/*.ts`),
/// aggregating segment count, total size, the max rung-directory mtime, and
/// every seq seen (duplicated across rungs) for duration computation.
async fn scan_show_dir(show_path: &Path) -> ShowDiskStats {
    let mut stats = ShowDiskStats::default();

    let mut rung_entries = match tokio::fs::read_dir(show_path).await {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(path = %show_path.display(), error = %e, "failed to read show dir");
            return stats;
        }
    };

    loop {
        let rung_entry = match rung_entries.next_entry().await {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(e) => {
                tracing::warn!(path = %show_path.display(), error = %e, "error scanning rung dirs");
                break;
            }
        };

        let is_dir = matches!(rung_entry.file_type().await, Ok(t) if t.is_dir());
        if !is_dir {
            continue;
        }

        if let Ok(meta) = rung_entry.metadata().await {
            if let Ok(modified) = meta.modified() {
                if let Ok(since_epoch) = modified.duration_since(UNIX_EPOCH) {
                    stats.modified_unix_ms =
                        stats.modified_unix_ms.max(since_epoch.as_millis() as u64);
                }
            }
        }

        let rung_path = rung_entry.path();
        let mut seg_entries = match tokio::fs::read_dir(&rung_path).await {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(path = %rung_path.display(), error = %e, "failed to read rung dir");
                continue;
            }
        };

        loop {
            let seg_entry = match seg_entries.next_entry().await {
                Ok(Some(e)) => e,
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(path = %rung_path.display(), error = %e, "error scanning segment files");
                    break;
                }
            };
            let file_name = seg_entry.file_name();
            let Some(seq) = file_name.to_str().and_then(parse_seq) else {
                continue;
            };
            let size = seg_entry.metadata().await.map(|m| m.len()).unwrap_or(0);
            stats.segment_count += 1;
            stats.size_bytes += size;
            stats.seqs.push(seq);
        }
    }

    stats
}

/// `DELETE /api/shows/{name}`: stop any live playout for the show, drop it
/// from the in-memory stream map, and recursively delete its segments from
/// disk. 404s if neither an in-memory handle nor an on-disk directory exists.
pub async fn delete_show(
    State(app): State<Arc<AppState>>,
    AxumPath(name): AxumPath<String>,
) -> Result<StatusCode, ApiError> {
    validate_show_name(&name)?;

    let handle = app.remove_stream(&name).await;
    if let Some(handle) = &handle {
        handle.playout.send(EngineCommand::Stop);
    }

    let path = app.data_dir().join(&name);
    match tokio::fs::remove_dir_all(&path).await {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if handle.is_some() {
                // The in-memory handle existed but its directory was already
                // gone (e.g. never ingested a segment) — still success.
                Ok(StatusCode::NO_CONTENT)
            } else {
                Err(ApiError(
                    StatusCode::NOT_FOUND,
                    format!("no such show: {name}"),
                ))
            }
        }
        Err(e) => Err(ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

/// Rejects empty names, path separators, and `..` components so
/// `data_dir.join(name)` can never escape `data_dir`.
fn validate_show_name(name: &str) -> Result<(), ApiError> {
    let valid = !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.contains('\\');
    if valid {
        Ok(())
    } else {
        Err(ApiError(
            StatusCode::BAD_REQUEST,
            "invalid show name".into(),
        ))
    }
}

fn parse_seq(filename: &str) -> Option<u64> {
    filename.strip_suffix(".ts")?.parse().ok()
}

/// Duration spanning the min/max seq seen across all rungs, at `segment_ms`
/// per seq. `None` when there are no segments at all.
fn duration_ms_from_seqs(seqs: &[u64], segment_ms: u32) -> Option<u64> {
    let min = *seqs.iter().min()?;
    let max = *seqs.iter().max()?;
    Some((max - min + 1) * segment_ms as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_seq_accepts_ts_files() {
        assert_eq!(parse_seq("42.ts"), Some(42));
        assert_eq!(parse_seq("0.ts"), Some(0));
    }

    #[test]
    fn parse_seq_rejects_other_files() {
        assert_eq!(parse_seq("42.m3u8"), None);
        assert_eq!(parse_seq("notaseq.ts"), None);
        assert_eq!(parse_seq("42"), None);
    }

    #[test]
    fn duration_ms_from_seqs_empty_is_none() {
        assert_eq!(duration_ms_from_seqs(&[], 2000), None);
    }

    #[test]
    fn duration_ms_from_seqs_single_seq_is_one_segment() {
        assert_eq!(duration_ms_from_seqs(&[7], 2000), Some(2000));
    }

    #[test]
    fn duration_ms_from_seqs_spans_min_max_regardless_of_order_or_duplicates() {
        // Duplicates come from the same seq existing at multiple rungs.
        assert_eq!(
            duration_ms_from_seqs(&[5, 2, 5, 9, 2], 2000),
            Some(8 * 2000)
        );
    }

    #[test]
    fn validate_show_name_accepts_normal_names() {
        assert!(validate_show_name("obshow").is_ok());
        assert!(validate_show_name("my-show_1").is_ok());
    }

    #[test]
    fn validate_show_name_rejects_traversal_and_empty() {
        assert!(validate_show_name("").is_err());
        assert!(validate_show_name("..").is_err());
        assert!(validate_show_name(".").is_err());
        assert!(validate_show_name("../etc").is_err());
        assert!(validate_show_name("foo/bar").is_err());
        assert!(validate_show_name("foo\\bar").is_err());
    }
}
