//! Cross-platform input capture via `cpal`. Owns the audio device so the
//! encoder never has to speak a platform-specific device syntax (PulseAudio
//! names, DirectShow monikers, AVFoundation indices, ...): we open the
//! device at its native channel count and let the operator pick *which*
//! two of those channels are L/R in software. That's what makes an
//! arbitrary-channel-count interface (e.g. a 32-channel stage box) usable —
//! the channel map is just two indices, changeable live without reopening
//! the stream.
//!
//! `cpal::Stream` is `!Send`, so the stream and everything that touches it
//! lives on one dedicated OS thread, driven by commands over a channel —
//! same pattern as the server's playout engine
//! (`obcast-server/src/playout.rs`).

use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use obcast_proto::loudness::Loudness;
use obcast_proto::meter::{Peak, Ppm, Vu};
use tokio::sync::mpsc as tokio_mpsc;

/// The encoder always receives L/R PCM regardless of the source device's
/// native channel count.
pub const OUT_CHANNELS: usize = 2;

/// How long a peak-hold reading stays pinned before it starts decaying.
/// Lives here so both the audio thread's raw meters and the GUI's hold
/// ballistics agree on the underlying convention (dBFS, linear 0..=~1.4).
pub const CLIP_THRESHOLD: f32 = 1.0;

#[derive(Clone, Debug)]
pub struct DeviceInfo {
    pub name: String,
    pub channels: u16,
    pub sample_rate: u32,
}

/// `Device` only exposes its name via `description()` (or the `Display`
/// impl, which panics through `to_string()` if the backend call fails) —
/// this never panics.
fn device_name(d: &cpal::Device) -> String {
    d.description()
        .map(|desc| desc.name().to_string())
        .unwrap_or_else(|_| "<unknown device>".to_string())
}

/// Names of the audio subsystems (cpal hosts) available on this platform —
/// e.g. `["ALSA", "JACK", "PulseAudio", "PipeWire"]` on Linux, `["WASAPI"]`
/// (or `["WASAPI", "ASIO"]`) on Windows, `["CoreAudio"]` on macOS. Always
/// non-empty: cpal guarantees at least one host per platform.
pub fn list_hosts() -> Vec<String> {
    cpal::available_hosts()
        .into_iter()
        .map(|id| id.name().to_string())
        .collect()
}

/// Resolves a host name from [`list_hosts`] to a cpal `Host`, falling back
/// to the platform default for an empty name or one that isn't currently
/// available (e.g. a subsystem picked on a different machine).
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

/// Enumerate input devices on the given audio subsystem (cpal host); an
/// empty `host_name` means the platform default. Works the same way on
/// Windows (WASAPI), macOS (CoreAudio) and Linux (ALSA/JACK/PulseAudio/
/// PipeWire) — no platform branching here, just which host the operator
/// picked.
pub fn list_input_devices(host_name: &str) -> Vec<DeviceInfo> {
    let host = resolve_host(host_name);
    let Ok(devices) = host.input_devices() else {
        return Vec::new();
    };
    devices
        .filter_map(|d| {
            let cfg = d.default_input_config().ok()?;
            Some(DeviceInfo {
                name: device_name(&d),
                channels: cfg.channels(),
                sample_rate: cfg.sample_rate(),
            })
        })
        .collect()
}

enum AudioCommand {
    Open { host: String, device: String },
    Close,
}

/// Live handle to the capture engine: device selection, channel map, gain
/// and metering, all readable/writable from the GUI thread without
/// blocking the audio callback (atomics + a couple of `try_lock`s that
/// simply skip a frame under contention rather than stalling audio).
pub struct AudioHandle {
    running: AtomicBool,
    device_channels: AtomicU16,
    sample_rate: AtomicU32,

    mono: AtomicBool,
    left_ch: AtomicU16,
    right_ch: AtomicU16,
    gain_db_bits: AtomicU32,

    vu_l_bits: AtomicU32,
    ppm_l_bits: AtomicU32,
    vu_r_bits: AtomicU32,
    ppm_r_bits: AtomicU32,
    /// True digital sample peak (see `obcast_proto::meter::Peak`) — the
    /// alternate reading the operator can switch the flying peak marker to
    /// instead of the IEC PPM ballistic above.
    peak_l_bits: AtomicU32,
    peak_r_bits: AtomicU32,
    clip_l: AtomicBool,
    clip_r: AtomicBool,

    /// ITU-R BS.1770-4 / EBU R128 K-weighted programme loudness (LUFS),
    /// combined across L/R — see `obcast_proto::loudness::Loudness`. Unlike
    /// the per-channel meters above, this is one reading for the whole
    /// signal, not two.
    momentary_lufs_bits: AtomicU32,
    short_term_lufs_bits: AtomicU32,
    integrated_lufs_bits: AtomicU32,
    /// Set by `reset_integrated_lufs()`, cleared by the audio thread once
    /// it has actually reset `Loudness`'s gated history — a flag rather
    /// than a direct call since the `Loudness` instance lives on the audio
    /// thread's `MeterState`, not on this handle.
    reset_integrated_lufs: AtomicBool,

    /// Instantaneous (per-callback) peak for every input channel of the
    /// currently open device, pre-gain — lets the GUI show a meter per
    /// physical channel so the operator can find signal on a large stage
    /// box before assigning L/R.
    channel_peaks: RwLock<Vec<f32>>,

    device_name: RwLock<String>,
    last_error: RwLock<Option<String>>,

    /// Gate on whether the audio thread forwards PCM into `pcm_tx` at all —
    /// off while just monitoring levels, on once the operator goes live, so
    /// idle capture never queues audio nobody is reading.
    live: AtomicBool,

    cmd_tx: std_mpsc::Sender<AudioCommand>,
}

impl AudioHandle {
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    pub fn device_name(&self) -> String {
        self.device_name.read().unwrap().clone()
    }

    pub fn device_channels(&self) -> u16 {
        self.device_channels.load(Ordering::Relaxed)
    }

    pub fn sample_rate(&self) -> u32 {
        self.sample_rate.load(Ordering::Relaxed)
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error.read().unwrap().clone()
    }

    pub fn open(&self, host_name: &str, device_name: &str) {
        let _ = self.cmd_tx.send(AudioCommand::Open {
            host: host_name.to_string(),
            device: device_name.to_string(),
        });
    }

    pub fn close(&self) {
        let _ = self.cmd_tx.send(AudioCommand::Close);
    }

    pub fn set_mono(&self, mono: bool) {
        self.mono.store(mono, Ordering::Relaxed);
    }
    pub fn mono(&self) -> bool {
        self.mono.load(Ordering::Relaxed)
    }

    pub fn set_left_channel(&self, ch: u16) {
        self.left_ch.store(ch, Ordering::Relaxed);
    }
    pub fn set_right_channel(&self, ch: u16) {
        self.right_ch.store(ch, Ordering::Relaxed);
    }
    pub fn left_channel(&self) -> u16 {
        self.left_ch.load(Ordering::Relaxed)
    }
    pub fn right_channel(&self) -> u16 {
        self.right_ch.load(Ordering::Relaxed)
    }

    pub fn set_gain_db(&self, db: f32) {
        self.gain_db_bits.store(db.to_bits(), Ordering::Relaxed);
    }
    pub fn gain_db(&self) -> f32 {
        f32::from_bits(self.gain_db_bits.load(Ordering::Relaxed))
    }

    pub fn set_live(&self, live: bool) {
        self.live.store(live, Ordering::Relaxed);
    }

    /// Standards-based (VU, PPM) ballistics in dBFS for the selected L and R
    /// channels, post-gain: `((vu_l_db, ppm_l_db), (vu_r_db, ppm_r_db))`. The
    /// IEC 60268-17 VU is the slow loudness reading, the IEC 60268-10 PPM the
    /// fast peak reading; both are computed sample-accurately on the audio
    /// thread, so these are already dB — no conversion needed at the call site.
    /// See also `peaks_db()` for the alternate true-digital-peak reading.
    pub fn meters(&self) -> ((f32, f32), (f32, f32)) {
        let l = (
            f32::from_bits(self.vu_l_bits.load(Ordering::Relaxed)),
            f32::from_bits(self.ppm_l_bits.load(Ordering::Relaxed)),
        );
        let r = (
            f32::from_bits(self.vu_r_bits.load(Ordering::Relaxed)),
            f32::from_bits(self.ppm_r_bits.load(Ordering::Relaxed)),
        );
        (l, r)
    }

    /// True digital sample peak in dBFS for L/R (`obcast_proto::meter::Peak`) —
    /// the alternate flying-peak reading alongside the IEC PPM in `meters()`.
    pub fn peaks_db(&self) -> (f32, f32) {
        (
            f32::from_bits(self.peak_l_bits.load(Ordering::Relaxed)),
            f32::from_bits(self.peak_r_bits.load(Ordering::Relaxed)),
        )
    }

    pub fn take_clip_l(&self) -> bool {
        self.clip_l.load(Ordering::Relaxed)
    }
    pub fn take_clip_r(&self) -> bool {
        self.clip_r.load(Ordering::Relaxed)
    }
    pub fn reset_clips(&self) {
        self.clip_l.store(false, Ordering::Relaxed);
        self.clip_r.store(false, Ordering::Relaxed);
    }

    /// ITU-R BS.1770-4 K-weighted programme loudness in LUFS:
    /// `(momentary, short_term, integrated)`. Momentary covers the last
    /// 400 ms, short-term the last 3 s (both ungated); integrated is the
    /// gated whole-programme-so-far reading, reset via
    /// `reset_integrated_lufs()`. Combined across L/R, post-gain — one
    /// reading for the signal as a whole, unlike the per-channel meters
    /// above. Floors at -100.0 (matching this crate's dBFS silence
    /// convention) before enough audio has been processed.
    pub fn lufs(&self) -> (f32, f32, f32) {
        (
            f32::from_bits(self.momentary_lufs_bits.load(Ordering::Relaxed)),
            f32::from_bits(self.short_term_lufs_bits.load(Ordering::Relaxed)),
            f32::from_bits(self.integrated_lufs_bits.load(Ordering::Relaxed)),
        )
    }

    /// Requests that the audio thread clear the integrated LUFS reading's
    /// gated history — e.g. an operator starting a new programme/segment —
    /// without reopening the device or disturbing momentary/short-term.
    pub fn reset_integrated_lufs(&self) {
        self.reset_integrated_lufs.store(true, Ordering::Relaxed);
    }

    /// Per-channel instantaneous peak (linear, pre-gain) for the currently
    /// open device. Empty until a device is open.
    pub fn channel_peaks(&self) -> Vec<f32> {
        self.channel_peaks.read().unwrap().clone()
    }
}

/// Spawns the capture engine on its own OS thread. `pcm_tx` receives
/// interleaved stereo f32 PCM blocks whenever `set_live(true)` and a device
/// is open; the encoder pipeline reads it and feeds `ffmpeg`'s stdin.
pub fn spawn(pcm_tx: tokio_mpsc::UnboundedSender<Vec<f32>>) -> Arc<AudioHandle> {
    let (cmd_tx, cmd_rx) = std_mpsc::channel();
    let handle = Arc::new(AudioHandle {
        running: AtomicBool::new(false),
        device_channels: AtomicU16::new(0),
        sample_rate: AtomicU32::new(0),
        mono: AtomicBool::new(false),
        left_ch: AtomicU16::new(0),
        right_ch: AtomicU16::new(1),
        gain_db_bits: AtomicU32::new(0.0f32.to_bits()),
        vu_l_bits: AtomicU32::new((-100.0f32).to_bits()),
        ppm_l_bits: AtomicU32::new((-100.0f32).to_bits()),
        vu_r_bits: AtomicU32::new((-100.0f32).to_bits()),
        ppm_r_bits: AtomicU32::new((-100.0f32).to_bits()),
        peak_l_bits: AtomicU32::new((-100.0f32).to_bits()),
        peak_r_bits: AtomicU32::new((-100.0f32).to_bits()),
        clip_l: AtomicBool::new(false),
        clip_r: AtomicBool::new(false),
        momentary_lufs_bits: AtomicU32::new((-100.0f32).to_bits()),
        short_term_lufs_bits: AtomicU32::new((-100.0f32).to_bits()),
        integrated_lufs_bits: AtomicU32::new((-100.0f32).to_bits()),
        reset_integrated_lufs: AtomicBool::new(false),
        channel_peaks: RwLock::new(Vec::new()),
        device_name: RwLock::new(String::new()),
        last_error: RwLock::new(None),
        live: AtomicBool::new(false),
        cmd_tx,
    });

    let worker_handle = handle.clone();
    std::thread::spawn(move || run_engine(cmd_rx, worker_handle, pcm_tx));

    handle
}

/// How long the engine waits between automatic attempts to reopen the
/// capture device after it's lost mid-session (e.g. an OB site's mixer loses
/// power or is unplugged) or an explicit `Open` fails — see `run_engine`'s
/// retry branch. Short enough that a brief power-cycle doesn't read as a
/// long outage, long enough that a genuinely absent device doesn't spam
/// retries/logs.
const DEVICE_RETRY_INTERVAL: Duration = Duration::from_secs(3);

/// Attempts to open `host`/`device`, applying the resulting state to
/// `handle` and swapping it into `*stream` on success — shared by the
/// explicit `Open` command handler and the automatic reconnect retry below,
/// which differ only in what they log around the call.
fn try_open(
    host: &str,
    device: &str,
    handle: &Arc<AudioHandle>,
    pcm_tx: &tokio_mpsc::UnboundedSender<Vec<f32>>,
    stream: &mut Option<cpal::Stream>,
) -> Result<(u16, u32)> {
    match open_stream(host, device, handle.clone(), pcm_tx.clone()) {
        Ok((s, channels, rate, opened_name)) => {
            handle.device_channels.store(channels, Ordering::Relaxed);
            handle.sample_rate.store(rate, Ordering::Relaxed);
            *handle.device_name.write().unwrap() = opened_name;
            *handle.channel_peaks.write().unwrap() = vec![0.0; channels as usize];
            handle.running.store(true, Ordering::Relaxed);
            *handle.last_error.write().unwrap() = None;
            *stream = Some(s);
            Ok((channels, rate))
        }
        Err(err) => {
            handle.running.store(false, Ordering::Relaxed);
            *handle.last_error.write().unwrap() = Some(err.to_string());
            Err(err)
        }
    }
}

// `stream` below is held only for its `Drop` (which stops the device) —
// it's intentionally never read, just reassigned to swap/close devices.
#[allow(unused_assignments, unused_variables)]
fn run_engine(
    cmd_rx: std_mpsc::Receiver<AudioCommand>,
    handle: Arc<AudioHandle>,
    pcm_tx: tokio_mpsc::UnboundedSender<Vec<f32>>,
) {
    let mut stream: Option<cpal::Stream> = None;
    // The most recently requested (host, device), used to retry the same
    // target automatically — `None` means no `Open` has been requested yet,
    // or an explicit `Close` cancelled any pending retry.
    let mut last_open: Option<(String, String)> = None;
    // Set once an `Open` fails, or a previously-open stream's `err_fn`
    // reports it lost — cleared the moment a (re)connect attempt succeeds.
    let mut retrying = false;

    loop {
        match cmd_rx.recv_timeout(DEVICE_RETRY_INTERVAL) {
            Ok(AudioCommand::Open { host, device }) => {
                stream = None; // close any previous device before opening the next
                last_open = Some((host.clone(), device.clone()));
                retrying = false;
                match try_open(&host, &device, &handle, &pcm_tx, &mut stream) {
                    Ok((channels, rate)) => {
                        tracing::info!(host = %host, device = %device, channels, rate, "capture device opened");
                    }
                    Err(err) => {
                        tracing::warn!(host = %host, device = %device, error = %err, "failed to open capture device; will keep retrying");
                        retrying = true;
                    }
                }
            }
            Ok(AudioCommand::Close) => {
                stream = None;
                last_open = None;
                retrying = false;
                handle.running.store(false, Ordering::Relaxed);
            }
            Err(std_mpsc::RecvTimeoutError::Disconnected) => return,
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                // A stream that was open can go silently not-running between
                // wakeups if its `err_fn` fired (device lost) — start
                // retrying against the same host/device rather than leaving
                // capture dead until the operator notices and manually
                // reopens (e.g. the OB site's mixer losing power).
                if stream.is_some() && !handle.running.load(Ordering::Relaxed) {
                    stream = None;
                    retrying = true;
                    tracing::warn!("capture device disconnected, will attempt to reconnect");
                }
                if retrying {
                    if let Some((host, device)) = last_open.clone() {
                        match try_open(&host, &device, &handle, &pcm_tx, &mut stream) {
                            Ok((channels, rate)) => {
                                tracing::info!(host = %host, device = %device, channels, rate, "capture device reconnected");
                                retrying = false;
                            }
                            Err(err) => {
                                tracing::debug!(host = %host, device = %device, error = %err, "capture device still unavailable, will retry");
                            }
                        }
                    }
                }
            }
        }
    }
}

fn open_stream(
    host_name: &str,
    name: &str,
    handle: Arc<AudioHandle>,
    pcm_tx: tokio_mpsc::UnboundedSender<Vec<f32>>,
) -> Result<(cpal::Stream, u16, u32, String)> {
    let host = resolve_host(host_name);
    let device = if name.is_empty() || name == "default" {
        host.default_input_device()
    } else {
        host.input_devices()
            .ok()
            .and_then(|mut it| it.find(|d| device_name(d) == name))
    }
    .ok_or_else(|| anyhow!("no matching input device found"))?;

    let opened_name = device_name(&device);
    let supported = device
        .default_input_config()
        .context("device has no default input config")?;
    let channels = supported.channels();
    let sample_rate = supported.sample_rate();
    let sample_format = supported.sample_format();
    let config: cpal::StreamConfig = supported.into();

    // Beyond logging, record the error onto the handle and flip `running`
    // false — previously this only logged, so a mid-session device loss
    // (unplugged, exclusive-mode contention, etc.) left the GUI's device
    // panel reporting a healthy, running device forever. Cloned per format
    // arm below since the closure captures a non-`Copy` `Arc`.
    let make_err_fn = |handle: Arc<AudioHandle>| {
        move |err: cpal::Error| {
            tracing::error!(error = %err, "capture stream error");
            *handle.last_error.write().unwrap() = Some(err.to_string());
            handle.running.store(false, Ordering::Relaxed);
        }
    };

    // Ballistic filter state lives here and is moved into the callback so it
    // persists across callbacks — the whole point of the 300 ms VU / 650 ms
    // PPM time constants is that they span many blocks. Only the audio thread
    // ever touches it, so no locking is involved.
    let mut meters = MeterState::new(sample_rate);
    let ch = channels as usize;

    let stream = match sample_format {
        cpal::SampleFormat::F32 => {
            let h = handle.clone();
            let tx = pcm_tx.clone();
            device.build_input_stream(
                config,
                move |data: &[f32], _| process_block(data, ch, sample_rate, &h, &tx, &mut meters),
                make_err_fn(handle.clone()),
                None,
            )?
        }
        cpal::SampleFormat::I16 => {
            let h = handle.clone();
            let tx = pcm_tx.clone();
            device.build_input_stream(
                config,
                move |data: &[i16], _| {
                    let f: Vec<f32> = data.iter().map(|&s| s as f32 / 32768.0).collect();
                    process_block(&f, ch, sample_rate, &h, &tx, &mut meters)
                },
                make_err_fn(handle.clone()),
                None,
            )?
        }
        cpal::SampleFormat::U16 => {
            let h = handle.clone();
            let tx = pcm_tx.clone();
            device.build_input_stream(
                config,
                move |data: &[u16], _| {
                    let f: Vec<f32> = data
                        .iter()
                        .map(|&s| (s as f32 - 32768.0) / 32768.0)
                        .collect();
                    process_block(&f, ch, sample_rate, &h, &tx, &mut meters)
                },
                make_err_fn(handle.clone()),
                None,
            )?
        }
        other => return Err(anyhow!("unsupported input sample format: {other:?}")),
    };

    stream.play().context("failed to start capture stream")?;
    Ok((stream, channels, sample_rate, opened_name))
}

/// Persistent per-channel meter ballistics, owned by the audio callback so
/// its envelope state survives across callbacks. `scratch_l`/`scratch_r` are
/// reused each block to hand the ballistics a contiguous per-channel slice
/// without allocating.
struct MeterState {
    vu_l: Vu,
    ppm_l: Ppm,
    vu_r: Vu,
    ppm_r: Ppm,
    peak_l: Peak,
    peak_r: Peak,
    /// ITU-R BS.1770-4 LUFS, combined across L/R — unlike the per-channel
    /// ballistics above, this is baked to the device's sample rate at
    /// construction (K-weighting is a fixed filter design, not something
    /// re-derived per callback), so a device swap rebuilds `MeterState`
    /// (see `open_stream`) rather than just resetting envelope state.
    loudness: Loudness,
    scratch_l: Vec<f32>,
    scratch_r: Vec<f32>,
    /// Set once the callback has tried to raise its own OS thread priority
    /// (see `elevate_audio_thread_priority`) — checked on the first call
    /// only, since it's a syscall we don't want to repeat every block.
    priority_elevated: bool,
}

impl MeterState {
    fn new(sample_rate: u32) -> Self {
        Self {
            vu_l: Vu::new(),
            ppm_l: Ppm::new(),
            vu_r: Vu::new(),
            ppm_r: Ppm::new(),
            peak_l: Peak::new(),
            peak_r: Peak::new(),
            loudness: Loudness::new(OUT_CHANNELS, sample_rate),
            scratch_l: Vec::new(),
            scratch_r: Vec::new(),
            priority_elevated: false,
        }
    }
}

/// Raises the *calling* OS thread to the platform's highest scheduling
/// priority. Must be called from the real cpal callback thread itself (not
/// the engine thread that builds the stream) — `cpal` creates that thread
/// internally, so there's no `JoinHandle` to set this on ahead of time; the
/// callback's first invocation is the earliest point we're actually running
/// on it. Best-effort: an unprivileged process can't raise its priority on
/// Linux without `CAP_SYS_NICE` (the same permission wrinkle pro-audio apps
/// like JACK solve with an `/etc/security/limits.d` audio-group entry, not
/// something this binary can grant itself), so failure is logged once and
/// capture keeps running at normal priority rather than treating it as
/// fatal.
fn elevate_audio_thread_priority() {
    use thread_priority::{set_current_thread_priority, ThreadPriority};
    if let Err(err) = set_current_thread_priority(ThreadPriority::Max) {
        tracing::warn!(
            ?err,
            "failed to raise capture thread priority, continuing at normal priority"
        );
    }
}

/// Runs on the audio callback: pick L/R out of the device's native
/// channels, apply gain, meter, and (if live) forward to the encoder.
/// Never blocks and never allocates more than the output `Vec` per callback.
fn process_block(
    raw: &[f32],
    channels: usize,
    sample_rate: u32,
    handle: &AudioHandle,
    pcm_tx: &tokio_mpsc::UnboundedSender<Vec<f32>>,
    meters: &mut MeterState,
) {
    if !meters.priority_elevated {
        meters.priority_elevated = true;
        elevate_audio_thread_priority();
    }

    if channels == 0 {
        return;
    }
    let frames = raw.len() / channels;

    // Per-channel monitoring meters (pre-gain), best-effort — skip this
    // frame under lock contention rather than stalling the audio thread.
    if let Ok(mut peaks) = handle.channel_peaks.try_write() {
        if peaks.len() != channels {
            peaks.resize(channels, 0.0);
        }
        for (ch, slot) in peaks.iter_mut().enumerate() {
            let mut m = 0.0f32;
            for f in 0..frames {
                m = m.max(raw[f * channels + ch].abs());
            }
            *slot = m;
        }
    }

    let mono = handle.mono.load(Ordering::Relaxed);
    let l_idx = (handle.left_ch.load(Ordering::Relaxed) as usize).min(channels - 1);
    let r_idx = (handle.right_ch.load(Ordering::Relaxed) as usize).min(channels - 1);
    let gain = 10f32.powf(handle.gain_db() / 20.0);

    let mut out = Vec::with_capacity(frames * OUT_CHANNELS);
    meters.scratch_l.clear();
    meters.scratch_r.clear();

    for f in 0..frames {
        let base = f * channels;
        let (mut l, mut r) = if mono {
            let v = raw[base + l_idx] * gain;
            (v, v)
        } else {
            (raw[base + l_idx] * gain, raw[base + r_idx] * gain)
        };

        if l.abs() >= CLIP_THRESHOLD {
            handle.clip_l.store(true, Ordering::Relaxed);
        }
        if r.abs() >= CLIP_THRESHOLD {
            handle.clip_r.store(true, Ordering::Relaxed);
        }
        l = l.clamp(-1.0, 1.0);
        r = r.clamp(-1.0, 1.0);

        meters.scratch_l.push(l);
        meters.scratch_r.push(r);
        out.push(l);
        out.push(r);
    }

    // Feed the post-gain, post-clamp block through the IEC ballistics
    // sample-accurately (they step internally), then publish dBFS readings.
    meters.vu_l.process(&meters.scratch_l, sample_rate);
    meters.ppm_l.process(&meters.scratch_l, sample_rate);
    meters.peak_l.process(&meters.scratch_l, sample_rate);
    meters.vu_r.process(&meters.scratch_r, sample_rate);
    meters.ppm_r.process(&meters.scratch_r, sample_rate);
    meters.peak_r.process(&meters.scratch_r, sample_rate);

    if handle.reset_integrated_lufs.swap(false, Ordering::Relaxed) {
        meters.loudness.reset_integrated();
    }
    meters
        .loudness
        .process(&[&meters.scratch_l, &meters.scratch_r]);

    handle
        .vu_l_bits
        .store(meters.vu_l.value_db().to_bits(), Ordering::Relaxed);
    handle
        .ppm_l_bits
        .store(meters.ppm_l.value_db().to_bits(), Ordering::Relaxed);
    handle
        .peak_l_bits
        .store(meters.peak_l.value_db().to_bits(), Ordering::Relaxed);
    handle
        .vu_r_bits
        .store(meters.vu_r.value_db().to_bits(), Ordering::Relaxed);
    handle
        .ppm_r_bits
        .store(meters.ppm_r.value_db().to_bits(), Ordering::Relaxed);
    handle
        .peak_r_bits
        .store(meters.peak_r.value_db().to_bits(), Ordering::Relaxed);
    handle.momentary_lufs_bits.store(
        meters.loudness.momentary_lufs().to_bits(),
        Ordering::Relaxed,
    );
    handle.short_term_lufs_bits.store(
        meters.loudness.short_term_lufs().to_bits(),
        Ordering::Relaxed,
    );
    handle.integrated_lufs_bits.store(
        meters.loudness.integrated_lufs().to_bits(),
        Ordering::Relaxed,
    );

    if handle.live.load(Ordering::Relaxed) {
        let _ = pcm_tx.send(out);
    }
}
