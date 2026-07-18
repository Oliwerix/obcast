//! Hardware playout: decode DVR segments and stream them to a `cpal` output
//! device, with start/stop/seek. `plan_uploads` in `obcast-proto` only knows
//! about the playout *head* (a `Seq`); this module is what actually advances
//! it, so every seek here immediately reshapes the encoder's upload plan on
//! the next `ServerState` publish.
//!
//! `symphonia` has no MPEG-TS demuxer, so segments are decoded to raw PCM via
//! an `ffmpeg` subprocess (already a hard runtime dependency for the
//! encoder) and fed into cpal through a lock-free ring buffer, one segment
//! at a time, in playout order. The `cpal::Stream` is `!Send`, so the whole
//! engine — decode, ring buffer, and the stream itself — lives on one
//! dedicated OS thread and is driven by commands over a channel.

use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use ringbuf::traits::{Consumer, Observer, Producer, Split};
use ringbuf::HeapRb;
use tokio::sync::{mpsc, Mutex};

use obcast_proto::state::{PlayoutState, RungId, Seq};

use crate::store::DvrStore;

const CHANNELS: usize = 2;
const RING_CAPACITY_FRAMES: usize = 48_000 * CHANNELS; // ~1s at 48kHz stereo

pub enum EngineCommand {
    Start { position: Seq },
    Stop,
    Pause,
    Resume,
    Seek { position: Seq },
    SetVolume { gain: f32 },
}

/// Shared with the ingest/control layer so `ServerState` always reflects the
/// playout engine's true position, even between control commands.
pub struct PlayoutHandle {
    running: AtomicBool,
    paused: AtomicBool,
    position_seq: AtomicI64,  // -1 = none
    volume_millis: AtomicU32, // gain * 1000, fixed-point for atomic access
    // Linear peak/RMS of the most recent audio callback's output (post-gain),
    // stored as raw f32 bits since there's no stable AtomicF32.
    peak_bits: AtomicU32,
    rms_bits: AtomicU32,
    cmd_tx: mpsc::UnboundedSender<EngineCommand>,
}

impl PlayoutHandle {
    pub fn position(&self) -> Option<Seq> {
        let v = self.position_seq.load(Ordering::Relaxed);
        (v >= 0).then_some(v as Seq)
    }

    pub fn playout_state(&self) -> PlayoutState {
        if !self.running.load(Ordering::Relaxed) {
            PlayoutState::Stopped
        } else if self.paused.load(Ordering::Relaxed) {
            PlayoutState::Paused
        } else {
            PlayoutState::Playing
        }
    }

    pub fn volume(&self) -> f32 {
        self.volume_millis.load(Ordering::Relaxed) as f32 / 1000.0
    }

    /// Linear (peak, rms) of the most recently played audio, 0.0..=1.0ish.
    /// Convert to dBFS at the call site for display.
    pub fn meters(&self) -> (f32, f32) {
        (
            f32::from_bits(self.peak_bits.load(Ordering::Relaxed)),
            f32::from_bits(self.rms_bits.load(Ordering::Relaxed)),
        )
    }

    pub fn send(&self, cmd: EngineCommand) {
        let _ = self.cmd_tx.send(cmd);
    }
}

/// Spawns the playout engine on its own OS thread and returns a handle for
/// issuing commands and reading live status.
pub fn spawn(store: Arc<Mutex<DvrStore>>, rungs: Vec<RungId>) -> Arc<PlayoutHandle> {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let handle = Arc::new(PlayoutHandle {
        running: AtomicBool::new(false),
        paused: AtomicBool::new(false),
        position_seq: AtomicI64::new(-1),
        volume_millis: AtomicU32::new(1000),
        peak_bits: AtomicU32::new(0),
        rms_bits: AtomicU32::new(0),
        cmd_tx,
    });

    // Captured on the calling (async) thread, where a runtime is guaranteed
    // to exist; the engine thread has no runtime of its own to call
    // `Handle::current()` on.
    let rt = tokio::runtime::Handle::current();
    let worker_handle = handle.clone();
    std::thread::spawn(move || run_engine(rt, store, rungs, worker_handle, cmd_rx));

    handle
}

fn run_engine(
    rt: tokio::runtime::Handle,
    store: Arc<Mutex<DvrStore>>,
    rungs: Vec<RungId>,
    handle: Arc<PlayoutHandle>,
    mut cmd_rx: mpsc::UnboundedReceiver<EngineCommand>,
) {
    let host = cpal::default_host();
    let Some(device) = host.default_output_device() else {
        tracing::error!("no default audio output device; playout disabled");
        drain_forever(&mut cmd_rx);
        return;
    };
    let Ok(supported) = device.default_output_config() else {
        tracing::error!("output device has no default config; playout disabled");
        drain_forever(&mut cmd_rx);
        return;
    };
    let sample_rate = supported.sample_rate();
    let config = cpal::StreamConfig {
        channels: CHANNELS as u16,
        sample_rate,
        buffer_size: cpal::BufferSize::Default,
    };

    let ring = HeapRb::<f32>::new(RING_CAPACITY_FRAMES);
    let (mut producer, mut consumer) = ring.split();

    let volume_handle = handle.clone();
    let stream = device.build_output_stream(
        config,
        move |data: &mut [f32], _| {
            let gain = volume_handle.volume();
            let filled = consumer.pop_slice(data);
            for s in &mut data[..filled] {
                *s *= gain;
            }
            for s in &mut data[filled..] {
                *s = 0.0;
            }

            let peak = data.iter().fold(0.0f32, |m, &s| m.max(s.abs()));
            let mean_sq = if data.is_empty() {
                0.0
            } else {
                data.iter().map(|s| s * s).sum::<f32>() / data.len() as f32
            };
            volume_handle
                .peak_bits
                .store(peak.to_bits(), Ordering::Relaxed);
            volume_handle
                .rms_bits
                .store(mean_sq.sqrt().to_bits(), Ordering::Relaxed);
        },
        |err| tracing::error!(error = %err, "playout stream error"),
        None,
    );
    let stream = match stream {
        Ok(s) => s,
        Err(err) => {
            tracing::error!(error = %err, "failed to build output stream; playout disabled");
            drain_forever(&mut cmd_rx);
            return;
        }
    };

    let mut current: Option<Seq> = None;

    loop {
        match cmd_rx.try_recv() {
            Ok(EngineCommand::Start { position }) => {
                current = Some(position);
                handle
                    .position_seq
                    .store(position as i64, Ordering::Relaxed);
                handle.running.store(true, Ordering::Relaxed);
                handle.paused.store(false, Ordering::Relaxed);
                let _ = stream.play();
                tracing::info!(seq = position, "playout started");
            }
            Ok(EngineCommand::Stop) => {
                current = None;
                handle.running.store(false, Ordering::Relaxed);
                handle.position_seq.store(-1, Ordering::Relaxed);
                let _ = stream.pause();
                tracing::info!("playout stopped");
            }
            Ok(EngineCommand::Pause) => {
                handle.paused.store(true, Ordering::Relaxed);
                let _ = stream.pause();
            }
            Ok(EngineCommand::Resume) => {
                handle.paused.store(false, Ordering::Relaxed);
                let _ = stream.play();
            }
            Ok(EngineCommand::Seek { position }) => {
                current = Some(position);
                handle
                    .position_seq
                    .store(position as i64, Ordering::Relaxed);
                tracing::info!(seq = position, "playout seek");
            }
            Ok(EngineCommand::SetVolume { gain }) => {
                handle
                    .volume_millis
                    .store((gain.max(0.0) * 1000.0) as u32, Ordering::Relaxed);
            }
            Err(mpsc::error::TryRecvError::Disconnected) => return,
            Err(mpsc::error::TryRecvError::Empty) => {}
        }

        let should_feed =
            handle.running.load(Ordering::Relaxed) && !handle.paused.load(Ordering::Relaxed);
        if should_feed {
            if let Some(seq) = current {
                // Only decode the next segment once the ring has room for a
                // full second, so decode paces itself to playback instead of
                // racing ahead of it unbounded.
                if producer.vacant_len() > sample_rate as usize {
                    let path = rt.block_on(async {
                        let store = store.lock().await;
                        best_available_path(&store, &rungs, seq)
                    });
                    // A segment not yet on disk holds the head in place —
                    // advancing past it would silently skip audio instead of
                    // waiting for the encoder, breaking the "no audio lost to
                    // a short outage" invariant.
                    if let Some(path) = path {
                        match decode_to_pcm(&path, sample_rate) {
                            Ok(pcm) => push_all(&mut producer, &pcm),
                            Err(err) => {
                                tracing::warn!(seq, error = %err, "decode failed, skipping segment")
                            }
                        }
                        current = Some(seq + 1);
                        handle
                            .position_seq
                            .store((seq + 1) as i64, Ordering::Relaxed);
                    }
                }
            }
        }

        std::thread::sleep(Duration::from_millis(20));
    }
}

fn drain_forever(cmd_rx: &mut mpsc::UnboundedReceiver<EngineCommand>) {
    while cmd_rx.blocking_recv().is_some() {}
}

fn push_all(producer: &mut ringbuf::HeapProd<f32>, mut pcm: &[f32]) {
    while !pcm.is_empty() {
        let n = producer.push_slice(pcm);
        pcm = &pcm[n..];
        if !pcm.is_empty() {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

fn best_available_path(store: &DvrStore, rungs: &[RungId], seq: Seq) -> Option<std::path::PathBuf> {
    let rung = rungs
        .iter()
        .rev()
        .find(|&&r| store.has_rung(seq, r))
        .or_else(|| rungs.first())?;
    let path = store.segment_path(*rung, seq);
    path.exists().then_some(path)
}

/// Decode one MPEG-TS/AAC segment to interleaved f32 PCM at `sample_rate` via
/// an `ffmpeg` subprocess. Blocking — called only from the dedicated playout
/// thread, never from the async runtime.
fn decode_to_pcm(path: &std::path::Path, sample_rate: u32) -> std::io::Result<Vec<f32>> {
    let output = std::process::Command::new("ffmpeg")
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-i")
        .arg(path)
        .arg("-f")
        .arg("f32le")
        .arg("-ac")
        .arg(CHANNELS.to_string())
        .arg("-ar")
        .arg(sample_rate.to_string())
        .arg("-")
        .stdin(Stdio::null())
        .stderr(Stdio::inherit())
        .output()?;
    Ok(output
        .stdout
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect())
}
