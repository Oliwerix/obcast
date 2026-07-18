//! obcast-client: encoder — ffmpeg capture+encode, disk ring buffer, and the
//! closed-loop uploader (M2/M3, CLI only — GUI is a later milestone).

mod encode;
mod inventory;
mod shared;
mod sse;
mod uploader;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use obcast_proto::state::{Rung, StreamProfile};

/// OBCast encoder client.
#[derive(Parser)]
struct Args {
    /// Base URL of the obcast-server ingest API.
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    server: String,
    #[arg(long, default_value = "obshow")]
    stream: String,
    #[arg(long)]
    ingest_token: Option<String>,
    /// Local segment ring-buffer directory.
    #[arg(long, default_value = "./client-buffer")]
    out_dir: PathBuf,
    /// PulseAudio source name to capture from. Omit to use a synthetic test tone.
    #[arg(long)]
    device: Option<String>,
    #[arg(long, default_value_t = 2000)]
    segment_ms: u32,
}

fn profile(segment_ms: u32) -> StreamProfile {
    StreamProfile {
        segment_ms,
        rungs: vec![
            Rung {
                id: 0,
                name: "lo".into(),
                bitrate_kbps: 32,
            },
            Rung {
                id: 1,
                name: "mid".into(),
                bitrate_kbps: 128,
            },
            Rung {
                id: 2,
                name: "hd".into(),
                bitrate_kbps: 320,
            },
        ],
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let profile = profile(args.segment_ms);

    std::fs::create_dir_all(&args.out_dir).expect("failed to create output dir");

    let source = match &args.device {
        Some(d) => encode::Source::Device(d.clone()),
        None => encode::Source::SineTest,
    };
    let mut ffmpeg =
        encode::spawn(&source, &profile, &args.out_dir).expect("failed to spawn ffmpeg");
    tracing::info!(?source, out_dir = ?args.out_dir, "encoder started");

    let client = reqwest::Client::new();
    let shared = Arc::new(shared::SharedState::new());

    let sse_task = tokio::spawn(sse::run(
        client.clone(),
        args.server.clone(),
        args.stream.clone(),
        shared.clone(),
    ));
    let upload_task = tokio::spawn(uploader::run(
        client,
        uploader::Config {
            base_url: args.server.clone(),
            stream: args.stream.clone(),
            ingest_token: args.ingest_token.clone(),
            out_dir: args.out_dir.clone(),
            profile,
        },
        shared,
    ));

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down");
        }
        _ = sse_task => {}
        _ = upload_task => {}
    }

    let _ = ffmpeg.kill().await;
}
