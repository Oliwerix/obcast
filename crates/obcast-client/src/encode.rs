//! Spawns one `ffmpeg` process that captures once and encodes+segments every
//! ABR rung as a separate `-map`'d output. A single decode feeding N encoders
//! keeps segment boundaries sample-aligned across rungs, which is the
//! invariant the scheduler's per-seq coverage model assumes.

use std::path::Path;
use std::process::Stdio;
use std::sync::OnceLock;

use tokio::process::{Child, Command};

use obcast_proto::state::{AacCodec, Rung, StreamProfile};

#[derive(Debug, Clone)]
pub enum Source {
    /// A PulseAudio source name (`pactl list sources`), or "default".
    /// Linux-only and superseded by `Pcm` for the GUI, which captures via
    /// `cpal` for cross-platform device/channel selection; kept for the
    /// headless CLI path.
    Device(String),
    /// A synthetic tone, paced at real-time via `-re` — for testing without
    /// audio hardware. Real devices already pace themselves; `-re` would
    /// only introduce drift there, so it's applied to this branch only.
    SineTest,
    /// Interleaved stereo f32 PCM piped in on stdin, already captured and
    /// channel-mapped by `crate::audio` (cpal). This is what the GUI uses —
    /// ffmpeg only ever sees a plain PCM stream, never a device name, which
    /// is what makes device/channel selection actually cross-platform.
    Pcm { sample_rate: u32 },
}

/// Whether the local `ffmpeg` binary was built with `libfdk_aac` — required
/// to actually emit HE-AAC (native `aac` only ever produces LC). Probed once
/// per process via `ffmpeg -encoders` and cached: this runs synchronously
/// on whatever task calls `spawn` (matching that function's own already-sync
/// subprocess/filesystem calls), so it's worth avoiding on every rung.
fn ffmpeg_has_libfdk_aac() -> bool {
    static HAS_LIBFDK: OnceLock<bool> = OnceLock::new();
    *HAS_LIBFDK.get_or_init(|| {
        std::process::Command::new("ffmpeg")
            .arg("-hide_banner")
            .arg("-encoders")
            .output()
            .map(|out| encoders_list_has_libfdk_aac(&String::from_utf8_lossy(&out.stdout)))
            .unwrap_or(false)
    })
}

/// Pure string check pulled out of `ffmpeg_has_libfdk_aac` so the parsing
/// logic has unit coverage even though the `ffmpeg` subprocess call itself
/// doesn't.
fn encoders_list_has_libfdk_aac(encoders_output: &str) -> bool {
    encoders_output.contains("libfdk_aac")
}

/// Codec args (`-c:a ...` plus any codec-specific profile flag) for one rung,
/// plus a fallback warning if it wanted HE-AAC but the local ffmpeg can't
/// produce it — encoding falls back to plain AAC-LC at the same bitrate
/// rather than failing the whole pipeline (CLAUDE.md §5: "the feedback loop
/// degrades safely").
///
/// UNVERIFIED on a real `libfdk_aac` build (this repo's dev/CI ffmpeg lacks
/// it, so the `AacCodec::He if has_libfdk` arm is currently dead in
/// practice): HE-AAC's SBR doubles the encoder's frame size (~2048 samples
/// vs LC's 1024), which can shift where `-segment_time` actually cuts
/// relative to the LC rungs sharing this same `ffmpeg` process. This
/// module's own doc comment states sample-aligned segment boundaries across
/// rungs as an invariant the scheduler's per-seq coverage model assumes —
/// before relying on this in production, confirm on a libfdk-enabled build
/// that a given seq covers the same audio window on the HE-AAC rung as on
/// every LC rung (see CLAUDE.md §8 "what's next").
fn codec_args(rung: &Rung, has_libfdk: bool) -> (&'static str, Vec<&'static str>, Option<String>) {
    match rung.codec {
        AacCodec::He if has_libfdk => ("libfdk_aac", vec!["-profile:a", "aac_he"], None),
        AacCodec::He => (
            "aac",
            vec![],
            Some(format!(
                "ffmpeg build lacks libfdk_aac: rung \"{}\" encoding as AAC-LC instead of HE-AAC",
                rung.name
            )),
        ),
        AacCodec::Lc => ("aac", vec![], None),
    }
}

/// Spawns the ffmpeg pipeline. Returns the child process plus any codec
/// fallback warnings (one per rung that wanted HE-AAC but couldn't get it) —
/// the caller decides how to surface those (e.g. the GUI logs them).
pub fn spawn(
    source: &Source,
    profile: &StreamProfile,
    out_dir: &Path,
) -> std::io::Result<(Child, Vec<String>)> {
    let segment_secs = (profile.segment_ms as f32 / 1000.0).to_string();
    let has_libfdk = ffmpeg_has_libfdk_aac();
    let mut warnings = Vec::new();

    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-hide_banner")
        .arg("-loglevel")
        .arg("warning")
        .arg("-y");

    match source {
        Source::Device(name) => {
            cmd.arg("-f").arg("pulse").arg("-i").arg(name);
        }
        Source::SineTest => {
            cmd.arg("-re")
                .arg("-f")
                .arg("lavfi")
                .arg("-i")
                .arg("sine=frequency=440:sample_rate=44100");
        }
        Source::Pcm { sample_rate } => {
            cmd.arg("-f")
                .arg("f32le")
                .arg("-ar")
                .arg(sample_rate.to_string())
                .arg("-ac")
                .arg("2")
                .arg("-i")
                .arg("pipe:0");
        }
    }

    for rung in &profile.rungs {
        let dir = out_dir.join(rung.id.to_string());
        // ffmpeg's segment muxer always numbers a fresh process's output
        // from 0 (`%d.ts`), overwriting only as far as it has re-encoded so
        // far this session. If `out_dir` is reused across runs, leftover
        // higher-numbered files from a *previous* session survive and
        // `inventory::scan` can't tell them apart from this session's own
        // output — it reports them as available and as the newest encoded
        // seq, which sends stale audio, inflates `encoded_seq` far past what
        // this session has actually produced, and ultimately makes the
        // scheduler abandon real (not-yet-encoded) segments as permanent
        // gaps. Clearing each rung dir before spawning keeps a reused
        // `out_dir` in sync with this session's own numbering.
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        std::fs::create_dir_all(&dir)?;
        let (codec_name, profile_args, warning) = codec_args(rung, has_libfdk);
        if let Some(w) = warning {
            warnings.push(w);
        }
        cmd.arg("-map").arg("0:a").arg("-c:a").arg(codec_name);
        for arg in profile_args {
            cmd.arg(arg);
        }
        cmd.arg("-b:a")
            .arg(format!("{}k", rung.bitrate_kbps))
            .arg("-f")
            .arg("segment")
            .arg("-segment_time")
            .arg(&segment_secs)
            .arg("-segment_format")
            .arg("mpegts")
            .arg("-reset_timestamps")
            .arg("1")
            .arg(dir.join("%d.ts"));
    }

    let stdin = match source {
        Source::Pcm { .. } => Stdio::piped(),
        _ => Stdio::null(),
    };
    cmd.stdin(stdin)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    cmd.spawn().map(|child| (child, warnings))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rung(id: u8, codec: AacCodec) -> Rung {
        Rung {
            id,
            name: "lo".into(),
            bitrate_kbps: 48,
            codec,
        }
    }

    #[test]
    fn encoders_list_has_libfdk_aac_detects_a_real_encoders_dump() {
        // Trimmed real `ffmpeg -hide_banner -encoders` output.
        let output = "Encoders:\n V..... libx264   H.264\n A....D aac      AAC (Advanced Audio Coding)\n A....D libfdk_aac libfdk AAC\n";
        assert!(encoders_list_has_libfdk_aac(output));
    }

    #[test]
    fn encoders_list_has_libfdk_aac_is_false_without_it() {
        // Matches this dev machine's actual `ffmpeg -encoders` output: only
        // the native `aac` encoder, no libfdk_aac — the fallback path this
        // whole feature depends on being safe on.
        let output =
            "Encoders:\n V..... libx264   H.264\n A....D aac      AAC (Advanced Audio Coding)\n";
        assert!(!encoders_list_has_libfdk_aac(output));
    }

    #[test]
    fn encoders_list_has_libfdk_aac_handles_empty_output() {
        assert!(!encoders_list_has_libfdk_aac(""));
    }

    #[test]
    fn codec_args_uses_libfdk_for_he_when_available() {
        let (name, args, warning) = codec_args(&rung(0, AacCodec::He), true);
        assert_eq!(name, "libfdk_aac");
        assert_eq!(args, vec!["-profile:a", "aac_he"]);
        assert!(warning.is_none());
    }

    #[test]
    fn codec_args_falls_back_to_lc_for_he_when_libfdk_unavailable() {
        let (name, args, warning) = codec_args(&rung(0, AacCodec::He), false);
        assert_eq!(name, "aac");
        assert!(args.is_empty());
        assert!(warning.is_some(), "must warn when falling back from HE-AAC");
    }

    #[test]
    fn codec_args_never_warns_for_a_plain_lc_rung() {
        let (name, args, warning) = codec_args(&rung(1, AacCodec::Lc), true);
        assert_eq!(name, "aac");
        assert!(args.is_empty());
        assert!(warning.is_none());

        let (name, args, warning) = codec_args(&rung(1, AacCodec::Lc), false);
        assert_eq!(name, "aac");
        assert!(args.is_empty());
        assert!(warning.is_none());
    }
}
