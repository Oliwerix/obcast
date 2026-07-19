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
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use ringbuf::traits::{Consumer, Observer, Producer, Split};
use ringbuf::HeapRb;
use tokio::sync::{mpsc, Mutex};

use obcast_proto::control::LogLevel;
use obcast_proto::state::{PlayoutState, RungId, Seq};

use crate::config::AudioConfig;
use crate::logs::LogSink;
use crate::store::DvrStore;

/// `Device` only exposes its name via `description()` (or the `Display`
/// impl, which panics through `to_string()` if the backend call fails) —
/// this never panics. Mirrors `obcast-client`'s `audio::device_name`.
fn device_name(d: &cpal::Device) -> String {
    d.description()
        .map(|desc| desc.name().to_string())
        .unwrap_or_else(|_| "<unknown device>".to_string())
}

/// Resolves a host name (from the config file) to a cpal `Host`, falling
/// back to the platform default for an empty name or one unavailable on
/// this machine.
fn resolve_host(name: &str) -> cpal::Host {
    if name.is_empty() {
        return cpal::default_host();
    }
    cpal::available_hosts()
        .into_iter()
        .find(|id| id.name().eq_ignore_ascii_case(name))
        .and_then(|id| cpal::host_from_id(id).ok())
        .unwrap_or_else(cpal::default_host)
}

fn resolve_output_device(host: &cpal::Host, name: &str) -> Option<cpal::Device> {
    if name.is_empty() {
        return host.default_output_device();
    }
    host.output_devices().ok()?.find(|d| device_name(d) == name)
}

const CHANNELS: usize = 2;

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
    // Per-channel dBFS VU/PPM/Peak ballistics of the playout output
    // (post-gain), stored as raw f32 bits since there's no stable AtomicF32.
    vu_l_bits: AtomicU32,
    vu_r_bits: AtomicU32,
    ppm_l_bits: AtomicU32,
    ppm_r_bits: AtomicU32,
    /// True digital sample peak (`obcast_proto::meter::Peak`) — the
    /// alternate flying-peak reading alongside PPM above.
    peak_l_bits: AtomicU32,
    peak_r_bits: AtomicU32,
    /// Set by the output audio callback whenever it has to zero-pad because
    /// the ring buffer ran dry (decode/segment availability can't keep pace,
    /// or the stall-skip backstop is bridging a missing segment) — the
    /// ground truth for "is real audio actually coming out right now,"
    /// independent of `running`/`paused`. See `playout_state()`.
    underrun: AtomicBool,
    /// Name of the output device actually opened, set once by the engine
    /// thread at startup; empty until then or if no device was found.
    device_name: RwLock<String>,
    /// Set once, permanently, if the engine thread fails to open the audio
    /// device/stream at startup — makes `playout_state()` report `Error`
    /// instead of a silently-dead `Stopped` (see `run_engine`'s early-return
    /// branches). `None` means playout initialized fine.
    device_error: RwLock<Option<String>>,
    /// Human-readable reason for the most recent stall-causing event (decode
    /// failure, encoder-abandoned segment, or the stall-skip timeout);
    /// cleared as soon as a segment decodes and plays normally again. Best
    /// effort — read alongside `underrun` by `detail()`, not perfectly
    /// synchronized with it since they're set from different code paths.
    stall_reason: RwLock<Option<String>>,
    /// Same `Arc<LogSink>` the owning `StreamHandle` holds (see
    /// `AppState::create_stream_handle`) — lets the engine thread push
    /// warn/error status lines to the web remote's log panel without a
    /// circular reference back to `StreamHandle`.
    log: Arc<LogSink>,
    cmd_tx: mpsc::UnboundedSender<EngineCommand>,
}

impl PlayoutHandle {
    pub fn device_name(&self) -> Option<String> {
        let name = self.device_name.read().unwrap();
        (!name.is_empty()).then(|| name.clone())
    }

    pub fn position(&self) -> Option<Seq> {
        let v = self.position_seq.load(Ordering::Relaxed);
        (v >= 0).then_some(v as Seq)
    }

    pub fn playout_state(&self) -> PlayoutState {
        if self.device_error.read().unwrap().is_some() {
            PlayoutState::Error
        } else if !self.running.load(Ordering::Relaxed) {
            PlayoutState::Stopped
        } else if self.paused.load(Ordering::Relaxed) {
            PlayoutState::Paused
        } else if self.underrun.load(Ordering::Relaxed) {
            PlayoutState::Stalled
        } else {
            PlayoutState::Playing
        }
    }

    /// Human-readable reason behind the current `Error`/`Stalled` state, for
    /// operator UIs that want to answer "why?" rather than just show a
    /// color. `None` for `Stopped`/`Playing`/`Paused`, and also `None` for
    /// `Stalled` if no specific cause has been recorded yet.
    pub fn detail(&self) -> Option<String> {
        if let Some(err) = self.device_error.read().unwrap().clone() {
            return Some(err);
        }
        if self.underrun.load(Ordering::Relaxed) {
            return self.stall_reason.read().unwrap().clone();
        }
        None
    }

    pub fn volume(&self) -> f32 {
        self.volume_millis.load(Ordering::Relaxed) as f32 / 1000.0
    }

    /// `(vu_db_l, vu_db_r, ppm_db_l, ppm_db_r, peak_db_l, peak_db_r)` of the
    /// playout output — IEC ballistics in dBFS, ready to display without
    /// further conversion. `peak_db_{l,r}` is the true digital sample peak
    /// (`obcast_proto::meter::Peak`), the alternate flying-peak reading
    /// alongside PPM.
    pub fn meters(&self) -> (f32, f32, f32, f32, f32, f32) {
        (
            f32::from_bits(self.vu_l_bits.load(Ordering::Relaxed)),
            f32::from_bits(self.vu_r_bits.load(Ordering::Relaxed)),
            f32::from_bits(self.ppm_l_bits.load(Ordering::Relaxed)),
            f32::from_bits(self.ppm_r_bits.load(Ordering::Relaxed)),
            f32::from_bits(self.peak_l_bits.load(Ordering::Relaxed)),
            f32::from_bits(self.peak_r_bits.load(Ordering::Relaxed)),
        )
    }

    pub fn send(&self, cmd: EngineCommand) {
        let _ = self.cmd_tx.send(cmd);
    }

    /// Records a warn/error-level status message to the shared log sink
    /// (see `StreamHandle::push_log`, which pushes to the same underlying
    /// `Arc<LogSink>`). Additive to `tracing`, called alongside the existing
    /// `tracing::warn!`/`error!` at each call site, not instead of it.
    fn push_log(&self, level: LogLevel, message: impl Into<String>) {
        self.log.push(level, message);
    }
}

/// Once a segment has been neither playable nor confirmed-abandoned for this
/// long, playout skips it anyway rather than freezing the head forever — a
/// backstop for when the encoder never calls `/abandon` at all (crashed,
/// disconnected). Generous relative to a normal short outage (CLAUDE.md §5),
/// but bounded: `3 * segment_ms` gives the encoder several retry ticks first.
fn stall_timeout(segment_ms: u32) -> Duration {
    Duration::from_millis(segment_ms.max(1) as u64 * 3)
}

/// Spawns the playout engine on its own OS thread and returns a handle for
/// issuing commands and reading live status. `audio_cfg` picks the audio
/// subsystem (cpal host) and output device from `obcast-server.toml`;
/// `segment_ms` sizes the stall timeout that skips a permanently missing
/// segment instead of freezing the playout head.
pub fn spawn(
    store: Arc<Mutex<DvrStore>>,
    rungs: Vec<RungId>,
    audio_cfg: AudioConfig,
    segment_ms: u32,
    log: Arc<LogSink>,
) -> Arc<PlayoutHandle> {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let handle = Arc::new(PlayoutHandle {
        running: AtomicBool::new(false),
        paused: AtomicBool::new(false),
        position_seq: AtomicI64::new(-1),
        volume_millis: AtomicU32::new(1000),
        vu_l_bits: AtomicU32::new(0),
        vu_r_bits: AtomicU32::new(0),
        ppm_l_bits: AtomicU32::new(0),
        ppm_r_bits: AtomicU32::new(0),
        peak_l_bits: AtomicU32::new(0),
        peak_r_bits: AtomicU32::new(0),
        underrun: AtomicBool::new(false),
        device_name: RwLock::new(String::new()),
        device_error: RwLock::new(None),
        stall_reason: RwLock::new(None),
        log,
        cmd_tx,
    });

    // Captured on the calling (async) thread, where a runtime is guaranteed
    // to exist; the engine thread has no runtime of its own to call
    // `Handle::current()` on.
    let rt = tokio::runtime::Handle::current();
    let worker_handle = handle.clone();
    std::thread::spawn(move || {
        run_engine(
            rt,
            store,
            rungs,
            audio_cfg,
            segment_ms,
            worker_handle,
            cmd_rx,
        )
    });

    handle
}

fn run_engine(
    rt: tokio::runtime::Handle,
    store: Arc<Mutex<DvrStore>>,
    rungs: Vec<RungId>,
    audio_cfg: AudioConfig,
    segment_ms: u32,
    handle: Arc<PlayoutHandle>,
    mut cmd_rx: mpsc::UnboundedReceiver<EngineCommand>,
) {
    let host = resolve_host(&audio_cfg.host);
    let Some(device) = resolve_output_device(&host, &audio_cfg.device) else {
        let msg = format!(
            "no matching audio output device (host={:?}, device={:?})",
            audio_cfg.host, audio_cfg.device
        );
        tracing::error!(host = %audio_cfg.host, device = %audio_cfg.device, "{msg}; playout disabled");
        handle.push_log(LogLevel::Error, format!("{msg}; playout disabled"));
        *handle.device_error.write().unwrap() = Some(msg);
        drain_forever(&mut cmd_rx);
        return;
    };
    *handle.device_name.write().unwrap() = device_name(&device);
    let Ok(supported) = device.default_output_config() else {
        let msg = "output device has no default config".to_string();
        tracing::error!("{msg}; playout disabled");
        handle.push_log(LogLevel::Error, format!("{msg}; playout disabled"));
        *handle.device_error.write().unwrap() = Some(msg);
        drain_forever(&mut cmd_rx);
        return;
    };
    let sample_rate = supported.sample_rate();
    let config = cpal::StreamConfig {
        channels: CHANNELS as u16,
        sample_rate,
        buffer_size: cpal::BufferSize::Default,
    };

    // Sized in segments rather than a fixed duration: decode is one blocking
    // `ffmpeg` subprocess per segment (see `decode_to_pcm`), and a higher rung
    // (e.g. the 320kbps "hd" rung vs. 32kbps "lo") has more bytes to read and
    // demux per segment, so it eats more of a fixed time margin. Previously
    // this was a flat ~1s capacity with the refill gate leaving only ~0.5s of
    // slack before the ring ran dry — plenty for spawning ffmpeg to decode a
    // small `lo` segment, but not always enough for a `hd` one under any load
    // (competing disk/CPU from the encoder side, a slow spawn, etc.), which
    // surfaced as spurious "buffer underrun" stalls specifically on the high
    // rung. Sizing off `segment_ms` instead gives decode a full segment's
    // wall-clock time of slack (see the refill gate below) regardless of
    // sample rate or segment length.
    let segment_samples = (segment_ms as u64 * sample_rate as u64 / 1000) as usize * CHANNELS;
    let ring = HeapRb::<f32>::new(segment_samples * 3);
    let (mut producer, mut consumer) = ring.split();

    let volume_handle = handle.clone();
    // Persistent across callbacks: the 300 ms VU and 650 ms PPM time
    // constants span many callbacks, so these must not reset each call.
    let mut vu_l = obcast_proto::meter::Vu::new();
    let mut vu_r = obcast_proto::meter::Vu::new();
    let mut ppm_l = obcast_proto::meter::Ppm::new();
    let mut ppm_r = obcast_proto::meter::Ppm::new();
    let mut peak_l = obcast_proto::meter::Peak::new();
    let mut peak_r = obcast_proto::meter::Peak::new();
    // Reused each callback to hand the ballistics a contiguous per-channel
    // slice without allocating on the real-time audio thread.
    let mut scratch_l: Vec<f32> = Vec::new();
    let mut scratch_r: Vec<f32> = Vec::new();
    let stream = device.build_output_stream(
        config,
        move |data: &mut [f32], _| {
            let gain = volume_handle.volume();
            let filled = consumer.pop_slice(data);
            // A partial fill means the ring ran dry this callback — real
            // audio, but not all of it; still an underrun, since some of
            // `data` below gets zero-padded either way.
            volume_handle
                .underrun
                .store(filled < data.len(), Ordering::Relaxed);
            for s in &mut data[..filled] {
                *s *= gain;
            }
            for s in &mut data[filled..] {
                *s = 0.0;
            }

            // Feed the whole post-gain buffer, including any zero-padded
            // underrun tail — that silence is genuinely being output.
            // `data` is interleaved L/R (CHANNELS == 2); deinterleave into
            // scratch buffers so each channel gets its own ballistic.
            scratch_l.clear();
            scratch_r.clear();
            for frame in data.chunks_exact(CHANNELS) {
                scratch_l.push(frame[0]);
                scratch_r.push(frame[1]);
            }
            vu_l.process(&scratch_l, sample_rate);
            ppm_l.process(&scratch_l, sample_rate);
            peak_l.process(&scratch_l, sample_rate);
            vu_r.process(&scratch_r, sample_rate);
            ppm_r.process(&scratch_r, sample_rate);
            peak_r.process(&scratch_r, sample_rate);
            volume_handle
                .vu_l_bits
                .store(vu_l.value_db().to_bits(), Ordering::Relaxed);
            volume_handle
                .vu_r_bits
                .store(vu_r.value_db().to_bits(), Ordering::Relaxed);
            volume_handle
                .ppm_l_bits
                .store(ppm_l.value_db().to_bits(), Ordering::Relaxed);
            volume_handle
                .ppm_r_bits
                .store(ppm_r.value_db().to_bits(), Ordering::Relaxed);
            volume_handle
                .peak_l_bits
                .store(peak_l.value_db().to_bits(), Ordering::Relaxed);
            volume_handle
                .peak_r_bits
                .store(peak_r.value_db().to_bits(), Ordering::Relaxed);
        },
        {
            let err_handle = handle.clone();
            move |err| {
                tracing::error!(error = %err, "playout stream error");
                err_handle.push_log(LogLevel::Error, format!("playout stream error: {err}"));
                *err_handle.device_error.write().unwrap() =
                    Some(format!("output stream error: {err}"));
            }
        },
        None,
    );
    let stream = match stream {
        Ok(s) => s,
        Err(err) => {
            let msg = format!("failed to build output stream: {err}");
            tracing::error!("{msg}; playout disabled");
            handle.push_log(LogLevel::Error, format!("{msg}; playout disabled"));
            *handle.device_error.write().unwrap() = Some(msg);
            drain_forever(&mut cmd_rx);
            return;
        }
    };

    let mut current: Option<Seq> = None;
    // When the current seq first became neither playable nor abandoned; reset
    // whenever the head moves (advance, seek, or a fresh start) so the clock
    // always measures how long *this* seq has been stuck, not a stale one.
    let mut stall_since: Option<Instant> = None;
    let timeout = stall_timeout(segment_ms);

    loop {
        match cmd_rx.try_recv() {
            Ok(EngineCommand::Start { position }) => {
                current = Some(position);
                stall_since = None;
                *handle.stall_reason.write().unwrap() = None;
                handle
                    .position_seq
                    .store(position as i64, Ordering::Relaxed);
                handle.running.store(true, Ordering::Relaxed);
                handle.paused.store(false, Ordering::Relaxed);
                // No audio has actually reached the ring yet; the output
                // callback will clear this on its first full buffer.
                handle.underrun.store(true, Ordering::Relaxed);
                let _ = stream.play();
                tracing::info!(seq = position, "playout started");
            }
            Ok(EngineCommand::Stop) => {
                current = None;
                stall_since = None;
                *handle.stall_reason.write().unwrap() = None;
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
                stall_since = None;
                *handle.stall_reason.write().unwrap() = None;
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
                // full segment, so decode paces itself to playback instead of
                // racing ahead of it unbounded — while still giving the
                // blocking `ffmpeg` decode (see comment at the ring's
                // creation) a full segment_ms of real-time slack rather than
                // starting only once the ring is nearly drained.
                if producer.vacant_len() >= segment_samples {
                    let (path, abandoned) = rt.block_on(async {
                        let store = store.lock().await;
                        (
                            best_available_path(&store, &rungs, seq),
                            store.is_abandoned(seq),
                        )
                    });

                    let mut advance = false;
                    if let Some(path) = path {
                        // Playable — the common case. A segment not yet on
                        // disk holds the head in place rather than advancing
                        // past it (see the `abandoned`/timeout branches
                        // below for when it stops waiting), protecting "no
                        // audio lost to a short outage" per CLAUDE.md §5.
                        match decode_to_pcm(&path, sample_rate) {
                            Ok(pcm) => {
                                push_all(&mut producer, &pcm);
                                // Real audio decoded and queued — whatever
                                // stalled things before is no longer why.
                                *handle.stall_reason.write().unwrap() = None;
                            }
                            Err(err) => {
                                tracing::warn!(seq, error = %err, "decode failed, skipping segment");
                                handle.push_log(
                                    LogLevel::Warn,
                                    format!("segment {seq} failed to decode, skipping: {err}"),
                                );
                                *handle.stall_reason.write().unwrap() =
                                    Some(format!("segment {seq} failed to decode: {err}"));
                            }
                        }
                        advance = true;
                    } else if abandoned {
                        // The encoder explicitly gave up on this seq via
                        // `/abandon` — nothing will ever fill it, so waiting
                        // any longer would freeze the head on a gap that's
                        // already known permanent.
                        tracing::warn!(seq, "segment abandoned by encoder, skipping");
                        handle.push_log(
                            LogLevel::Warn,
                            format!("segment {seq} was abandoned by the encoder, skipping"),
                        );
                        *handle.stall_reason.write().unwrap() =
                            Some(format!("segment {seq} was abandoned by the encoder"));
                        advance = true;
                    } else {
                        // Missing, not (yet) abandoned. Wait up to `timeout`
                        // in case the encoder is just late/retrying, then
                        // skip anyway — a backstop for the case where the
                        // encoder crashed or disconnected and will never call
                        // `/abandon` at all. Without this bound a permanent
                        // gap freezes `position_seq` forever, violating the
                        // "never block the playout head" invariant.
                        let since = stall_since.get_or_insert_with(Instant::now);
                        *handle.stall_reason.write().unwrap() =
                            Some(format!("waiting on segment {seq} from the encoder"));
                        if since.elapsed() >= timeout {
                            tracing::warn!(
                                seq,
                                timeout_ms = timeout.as_millis() as u64,
                                "segment missing past stall timeout, skipping to avoid freezing the playout head"
                            );
                            handle.push_log(
                                LogLevel::Warn,
                                format!(
                                    "segment {seq} missing for over {}ms, skipped to avoid freezing the playout head",
                                    timeout.as_millis()
                                ),
                            );
                            *handle.stall_reason.write().unwrap() = Some(format!(
                                "segment {seq} missing for over {}ms, skipped",
                                timeout.as_millis()
                            ));
                            advance = true;
                        }
                    }

                    if advance {
                        current = Some(seq + 1);
                        stall_since = None;
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
