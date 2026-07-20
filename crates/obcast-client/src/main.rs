//! obcast-client: encoder — cpal capture, ffmpeg ABR encode, disk ring
//! buffer, and the closed-loop uploader, behind an egui GUI by default.
//! `--headless` keeps the original CLI-only path (ffmpeg captures the
//! device itself by platform-specific name) for unattended/server use.

mod audio;
mod config;
mod encode;
mod gui;
mod inventory;
mod shared;
mod sse;
mod uploader;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use obcast_proto::state::StreamProfile;

/// OBCast encoder client.
#[derive(Parser)]
struct Args {
    /// Run the original CLI-only pipeline instead of the GUI (no device/
    /// channel picker — pass --device explicitly, or omit for a test tone).
    #[arg(long)]
    headless: bool,

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
    /// PulseAudio source name to capture from (headless mode only). Omit
    /// to use a synthetic test tone.
    #[arg(long)]
    device: Option<String>,
    #[arg(long, default_value_t = 2000)]
    segment_ms: u32,
    /// Ask the server to start playout on its own once this many seconds of
    /// contiguous buffer have accumulated, instead of waiting for a web
    /// operator to press Start. Omit to disable auto-start.
    #[arg(long)]
    auto_start_secs: Option<u32>,
}

fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    if args.headless {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime");
        rt.block_on(run_headless(args));
        return;
    }

    let mut cfg = config::AppConfig::load();
    // CLI overrides only apply if the caller actually passed non-default
    // flags; the GUI otherwise owns these via its own persisted settings.
    if args.server != "http://127.0.0.1:8080" {
        cfg.server = args.server;
    }
    if args.stream != "obshow" {
        cfg.stream = args.stream;
    }
    if let Some(token) = args.ingest_token {
        cfg.ingest_token = token;
    }
    if args.out_dir.as_os_str() != "./client-buffer" {
        cfg.out_dir = args.out_dir.to_string_lossy().into_owned();
    }
    if args.segment_ms != 2000 {
        cfg.segment_ms = args.segment_ms;
    }
    if let Some(secs) = args.auto_start_secs {
        cfg.auto_start = true;
        cfg.auto_start_buffer_secs = secs;
    }

    if let Err(err) = gui::run(cfg) {
        tracing::error!(error = %err, "GUI exited with an error");
    }
}

async fn run_headless(args: Args) {
    let profile = StreamProfile::default_ladder(args.segment_ms);

    std::fs::create_dir_all(&args.out_dir).expect("failed to create output dir");

    let source = match &args.device {
        Some(d) => encode::Source::Device(d.clone()),
        None => encode::Source::SineTest,
    };
    let (mut ffmpeg, warnings) =
        encode::spawn(&source, &profile, &args.out_dir).expect("failed to spawn ffmpeg");
    for warning in &warnings {
        tracing::warn!(%warning, "codec fallback");
    }
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
            auto_start_buffer_ms: args.auto_start_secs.map(|s| s * 1000),
            bootstrap_rung: 0,
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
