//! Server-computed waveform peaks for the web remote's BBC peaks.js display.
//!
//! Segments are MPEG-TS/AAC, so there's no reliable way to decode them for
//! visualization directly in the browser (Web Audio's `decodeAudioData`
//! doesn't handle raw `.ts` well across browsers). Instead this reuses the
//! same `ffmpeg`-subprocess decode approach as playout (`playout.rs`) and
//! emits peaks in the waveform-data.js JSON format peaks.js consumes
//! natively: <https://github.com/bbc/waveform-data.js/blob/master/doc/JSON.md>.
//!
//! Extended with a parallel `rungs` array (one entry per waveform point,
//! naming which ABR rung that slice of audio came from) so the web remote
//! can color-code the waveform by quality — this is an obcast-specific
//! addition, not part of the upstream JSON schema; peaks.js ignores unknown
//! fields, so this remains a valid input to `WaveformData.create()`.

use std::path::PathBuf;
use std::process::Stdio;

use serde::Serialize;

use obcast_proto::control::LogLevel;
use obcast_proto::state::{RungId, Seq, StreamProfile};

use crate::logs::LogSink;
use crate::store::DvrStore;

/// One waveform point per this many milliseconds of audio. Coarse enough to
/// keep the JSON payload small for a multi-minute DVR window, fine enough to
/// still look like a waveform rather than a bar chart.
const POINT_MS: u32 = 40;
/// Sample rate used for the peak-extraction decode. Low on purpose — peaks
/// don't need fidelity, and a lower rate makes the ffmpeg decode cheaper.
const DECODE_SAMPLE_RATE: u32 = 8_000;

#[derive(Serialize)]
pub struct WaveformJson {
    version: u32,
    channels: u32,
    sample_rate: u32,
    samples_per_pixel: u32,
    bits: i32,
    length: usize,
    /// Interleaved [min, max] per point, per the waveform-data.js JSON format.
    data: Vec<i8>,
    /// obcast extension: best rung covering each point, parallel to `data`
    /// (i.e. `rungs[i]` corresponds to `data[2*i..2*i+2]`). `None` = gap.
    rungs: Vec<Option<RungId>>,
    /// obcast extension: seq each point belongs to, for click-to-seek math
    /// on the client without re-deriving it from time + segment_ms.
    seqs: Vec<Seq>,
    /// obcast extension: true at points whose segment *had* a recorded rung
    /// (i.e. `rungs[i]` is `Some`) but couldn't actually be decoded — file
    /// missing on disk despite being indexed, or `ffmpeg` failed on it. A
    /// flat `(0, 0)` line at such a point is an artifact of the failure, not
    /// necessarily real silence; distinct from a true gap, where `rungs[i]`
    /// is `None` because no rung was ever recorded for that seq at all.
    decode_failed: Vec<bool>,
}

/// A snapshot of what `[start, end]` looks like in the DVR index right now:
/// per seq, the best rung and where its bytes live on disk (or `None` for a
/// true gap). Cheap, index-only lookups — safe to build while holding the
/// store lock. See `build_from_index` for why this is split out from the
/// actual decode work.
pub fn snapshot_index(
    store: &DvrStore,
    start: Seq,
    end: Seq,
) -> Vec<(Seq, Option<(RungId, PathBuf)>)> {
    (start..=end)
        .map(|seq| {
            let entry = store
                .best_rung(seq)
                .map(|rung| (rung, store.segment_path(rung, seq)));
            (seq, entry)
        })
        .collect()
}

/// Decode every playable segment described by `entries` and build one JSON
/// waveform covering the whole range, at a fixed `points_per_segment` so
/// every segment — present or missing — occupies the same visual width.
/// Blocking (spawns `ffmpeg` per segment): this deliberately takes an owned
/// snapshot rather than `&DvrStore` so the caller can drop the store lock
/// before this runs. A multi-minute DVR window means dozens of serial
/// `ffmpeg` spawns here — holding the store's lock across that would starve
/// every other store user (segment ingest, the playout engine's per-tick
/// segment lookup) for the whole decode, which is exactly the kind of stall
/// this split avoids.
pub fn build_from_index(
    profile: &StreamProfile,
    entries: &[(Seq, Option<(RungId, PathBuf)>)],
    log: &LogSink,
) -> WaveformJson {
    let points_per_segment = (profile.segment_ms / POINT_MS).max(1) as usize;
    let samples_per_point = (DECODE_SAMPLE_RATE * POINT_MS / 1000).max(1);

    let mut data = Vec::new();
    let mut rungs = Vec::new();
    let mut seqs = Vec::new();
    let mut decode_failed = Vec::new();

    for (seq, entry) in entries {
        let seq = *seq;
        let rung = entry.as_ref().map(|(r, _)| *r);
        let (segment_points, failed) = match entry {
            // A true gap: no rung was ever recorded for this seq. Not a
            // failure — there's simply nothing to decode.
            None => (vec![(0, 0); points_per_segment], false),
            Some((r, path)) => {
                let r = *r;
                if !path.exists() {
                    tracing::warn!(
                        seq,
                        rung = r,
                        path = %path.display(),
                        "waveform: segment recorded in the DVR index but missing on disk"
                    );
                    log.push(
                        LogLevel::Warn,
                        format!(
                            "waveform: segment {seq} (rung {r}) recorded in the DVR index but missing on disk"
                        ),
                    );
                    (vec![(0, 0); points_per_segment], true)
                } else {
                    match decode_mono_i16(path, DECODE_SAMPLE_RATE) {
                        Some(samples) => (
                            extract_peaks(&samples, samples_per_point as usize, points_per_segment),
                            false,
                        ),
                        None => {
                            tracing::warn!(
                                seq,
                                rung = r,
                                path = %path.display(),
                                "waveform: failed to decode segment, rendering as a flagged flat line"
                            );
                            log.push(
                                LogLevel::Warn,
                                format!(
                                    "waveform: failed to decode segment {seq} (rung {r}), rendering as a flagged flat line"
                                ),
                            );
                            (vec![(0, 0); points_per_segment], true)
                        }
                    }
                }
            }
        };

        for (min, max) in segment_points {
            data.push(min);
            data.push(max);
            rungs.push(rung);
            seqs.push(seq);
            decode_failed.push(failed);
        }
    }

    let length = rungs.len();
    WaveformJson {
        version: 2,
        channels: 1,
        sample_rate: DECODE_SAMPLE_RATE,
        samples_per_pixel: samples_per_point,
        bits: 8,
        length,
        data,
        rungs,
        seqs,
        decode_failed,
    }
}

/// Convenience wrapper combining `snapshot_index` + `build_from_index` for
/// callers (tests, mainly) that don't need to control the store lock's
/// lifetime separately. `api.rs`'s real handler calls the two halves
/// directly so it can drop the lock between them — see `build_from_index`'s
/// doc comment.
#[cfg(test)]
fn build(
    store: &DvrStore,
    profile: &StreamProfile,
    start: Seq,
    end: Seq,
    log: &LogSink,
) -> WaveformJson {
    let entries = snapshot_index(store, start, end);
    build_from_index(profile, &entries, log)
}

/// Splits `samples` into exactly `target_points` chunks (the last chunk
/// absorbs any remainder from uneven division) and returns each chunk's
/// (min, max) scaled to i8.
fn extract_peaks(samples: &[i16], samples_per_point: usize, target_points: usize) -> Vec<(i8, i8)> {
    let mut out = Vec::with_capacity(target_points);
    for i in 0..target_points {
        let start = (i * samples_per_point).min(samples.len());
        let end = if i + 1 == target_points {
            samples.len()
        } else {
            ((i + 1) * samples_per_point).min(samples.len())
        };
        let chunk = &samples[start..end];
        if chunk.is_empty() {
            out.push((0, 0));
        } else {
            let (min, max) = chunk
                .iter()
                .fold((i16::MAX, i16::MIN), |(mn, mx), &s| (mn.min(s), mx.max(s)));
            out.push((scale_to_i8(min), scale_to_i8(max)));
        }
    }
    out
}

fn scale_to_i8(sample_i16: i16) -> i8 {
    (sample_i16 >> 8) as i8
}

/// Decode one segment to mono i16 PCM at `sample_rate` via `ffmpeg`.
/// Blocking, mirrors `playout::decode_to_pcm` but mono/i16 (peaks don't need
/// stereo or float precision) and tolerant of decode failure (returns
/// `None` rather than erroring the whole waveform build).
fn decode_mono_i16(path: &std::path::Path, sample_rate: u32) -> Option<Vec<i16>> {
    let output = std::process::Command::new("ffmpeg")
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-i")
        .arg(path)
        .arg("-f")
        .arg("s16le")
        .arg("-ac")
        .arg("1")
        .arg("-ar")
        .arg(sample_rate.to_string())
        .arg("-")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(
        output
            .stdout
            .chunks_exact(2)
            .map(|c| i16::from_le_bytes([c[0], c[1]]))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::DvrStore;
    use obcast_proto::state::{AacCodec, Rung, WaterLevels};
    use std::path::PathBuf;

    fn profile() -> StreamProfile {
        StreamProfile {
            segment_ms: 2000,
            rungs: vec![Rung {
                id: 0,
                name: "lo".into(),
                bitrate_kbps: 32,
                codec: AacCodec::He,
            }],
        }
    }

    #[test]
    fn gap_segments_are_not_flagged_as_decode_failures() {
        // Nothing was ever recorded for seq 0, so `best_rung` is `None` — a
        // true gap, distinct from a decode failure.
        let store = DvrStore::new(
            profile(),
            WaterLevels::default(),
            60_000,
            PathBuf::from("/tmp/obcast-waveform-test-gap"),
        );
        let json = build(&store, &profile(), 0, 0, &LogSink::new());
        assert!(json.rungs.iter().all(|r| r.is_none()));
        assert!(
            json.decode_failed.iter().all(|f| !f),
            "a true gap must not be flagged as a decode failure"
        );
    }

    #[test]
    fn indexed_but_missing_file_is_flagged_as_a_decode_failure() {
        // Recorded in the index, but no file was ever actually written to
        // disk at that path — should be flagged, not silently rendered as a
        // flat (0, 0) line indistinguishable from real silence.
        let mut store = DvrStore::new(
            profile(),
            WaterLevels::default(),
            60_000,
            PathBuf::from("/tmp/obcast-waveform-test-missing"),
        );
        store.record(0, 0, None);
        let json = build(&store, &profile(), 0, 0, &LogSink::new());
        assert!(json.rungs.iter().all(|r| *r == Some(0)));
        assert!(
            json.decode_failed.iter().all(|f| *f),
            "an indexed-but-missing-on-disk segment must be flagged as a decode failure"
        );
    }
}
