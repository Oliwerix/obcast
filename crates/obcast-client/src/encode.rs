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
    Device(String),
    /// A synthetic tone, paced at real-time via `-re` — for testing without
    /// audio hardware. Real devices already pace themselves; `-re` would
    /// only introduce drift there, so it's applied to this branch only.
    SineTest,
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
    }

    for rung in &profile.rungs {
        let dir = out_dir.join(rung.id.to_string());
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

    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    cmd.spawn()
}
