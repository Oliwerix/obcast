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

use std::process::Stdio;

use serde::Serialize;

use obcast_proto::state::{RungId, Seq, StreamProfile};

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
}

/// Decode every playable segment in `[start, end]` and build one JSON
/// waveform covering the whole range, at a fixed `points_per_segment` so
/// every segment — present or missing — occupies the same visual width.
/// Blocking (spawns `ffmpeg` per segment) — call from a blocking task, not
/// directly on the async runtime.
pub fn build(store: &DvrStore, profile: &StreamProfile, start: Seq, end: Seq) -> WaveformJson {
    let points_per_segment = (profile.segment_ms / POINT_MS).max(1) as usize;
    let samples_per_point = (DECODE_SAMPLE_RATE * POINT_MS / 1000).max(1);

    let mut data = Vec::new();
    let mut rungs = Vec::new();
    let mut seqs = Vec::new();

    for seq in start..=end {
        let rung = store.best_rung(seq);
        let peaks = rung
            .and_then(|r| {
                let path = store.segment_path(r, seq);
                path.exists()
                    .then(|| decode_mono_i16(&path, DECODE_SAMPLE_RATE))
                    .flatten()
            })
            .map(|samples| extract_peaks(&samples, samples_per_point as usize, points_per_segment));

        let segment_points = peaks.unwrap_or_else(|| vec![(0, 0); points_per_segment]);
        for (min, max) in segment_points {
            data.push(min);
            data.push(max);
            rungs.push(rung);
            seqs.push(seq);
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
    }
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
