//! Spawns one `ffmpeg` process that captures once and encodes+segments every
//! ABR rung as a separate `-map`'d output. A single decode feeding N encoders
//! keeps segment boundaries sample-aligned across rungs, which is the
//! invariant the scheduler's per-seq coverage model assumes.

use std::path::Path;
use std::process::Stdio;

use tokio::process::{Child, Command};

use obcast_proto::state::StreamProfile;

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

pub fn spawn(source: &Source, profile: &StreamProfile, out_dir: &Path) -> std::io::Result<Child> {
    let segment_secs = (profile.segment_ms as f32 / 1000.0).to_string();

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
        cmd.arg("-map")
            .arg("0:a")
            .arg("-c:a")
            .arg("aac")
            .arg("-b:a")
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
    cmd.spawn()
}
