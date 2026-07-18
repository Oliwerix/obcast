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

use anyhow::{anyhow, Context, Result};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use obcast_proto::meter::{Ppm, Vu};
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

/// Enumerate input devices on the default host. Works the same way on
/// Windows (WASAPI), macOS (CoreAudio) and Linux (ALSA/JACK/PulseAudio,
/// whichever cpal picks as the default host) — no platform branching here.
pub fn list_input_devices() -> Vec<DeviceInfo> {
    let host = cpal::default_host();
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
    Open(String),
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
    clip_l: AtomicBool,
    clip_r: AtomicBool,

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

    pub fn open(&self, device_name: &str) {
        let _ = self
            .cmd_tx
            .send(AudioCommand::Open(device_name.to_string()));
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
        clip_l: AtomicBool::new(false),
        clip_r: AtomicBool::new(false),
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

// `stream` below is held only for its `Drop` (which stops the device) —
// it's intentionally never read, just reassigned to swap/close devices.
#[allow(unused_assignments, unused_variables)]
fn run_engine(
    cmd_rx: std_mpsc::Receiver<AudioCommand>,
    handle: Arc<AudioHandle>,
    pcm_tx: tokio_mpsc::UnboundedSender<Vec<f32>>,
) {
    let mut stream: Option<cpal::Stream> = None;

    while let Ok(cmd) = cmd_rx.recv() {
        match cmd {
            AudioCommand::Open(name) => {
                stream = None; // close any previous device before opening the next
                match open_stream(&name, handle.clone(), pcm_tx.clone()) {
                    Ok((s, channels, rate, opened_name)) => {
                        handle.device_channels.store(channels, Ordering::Relaxed);
                        handle.sample_rate.store(rate, Ordering::Relaxed);
                        *handle.device_name.write().unwrap() = opened_name;
                        *handle.channel_peaks.write().unwrap() = vec![0.0; channels as usize];
                        handle.running.store(true, Ordering::Relaxed);
                        *handle.last_error.write().unwrap() = None;
                        stream = Some(s);
                        tracing::info!(device = %name, channels, rate, "capture device opened");
                    }
                    Err(err) => {
                        handle.running.store(false, Ordering::Relaxed);
                        *handle.last_error.write().unwrap() = Some(err.to_string());
                        tracing::warn!(device = %name, error = %err, "failed to open capture device");
                    }
                }
            }
            AudioCommand::Close => {
                stream = None;
                handle.running.store(false, Ordering::Relaxed);
            }
        }
    }
}

fn open_stream(
    name: &str,
    handle: Arc<AudioHandle>,
    pcm_tx: tokio_mpsc::UnboundedSender<Vec<f32>>,
) -> Result<(cpal::Stream, u16, u32, String)> {
    let host = cpal::default_host();
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

    let err_fn = |err| tracing::error!(error = %err, "capture stream error");

    // Ballistic filter state lives here and is moved into the callback so it
    // persists across callbacks — the whole point of the 300 ms VU / 650 ms
    // PPM time constants is that they span many blocks. Only the audio thread
    // ever touches it, so no locking is involved.
    let mut meters = MeterState::new();
    let ch = channels as usize;

    let stream = match sample_format {
        cpal::SampleFormat::F32 => {
            let h = handle.clone();
            let tx = pcm_tx.clone();
            device.build_input_stream(
                config,
                move |data: &[f32], _| process_block(data, ch, sample_rate, &h, &tx, &mut meters),
                err_fn,
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
                err_fn,
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
                err_fn,
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
    scratch_l: Vec<f32>,
    scratch_r: Vec<f32>,
}

impl MeterState {
    fn new() -> Self {
        Self {
            vu_l: Vu::new(),
            ppm_l: Ppm::new(),
            vu_r: Vu::new(),
            ppm_r: Ppm::new(),
            scratch_l: Vec::new(),
            scratch_r: Vec::new(),
        }
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
    meters.vu_r.process(&meters.scratch_r, sample_rate);
    meters.ppm_r.process(&meters.scratch_r, sample_rate);

    handle
        .vu_l_bits
        .store(meters.vu_l.value_db().to_bits(), Ordering::Relaxed);
    handle
        .ppm_l_bits
        .store(meters.ppm_l.value_db().to_bits(), Ordering::Relaxed);
    handle
        .vu_r_bits
        .store(meters.vu_r.value_db().to_bits(), Ordering::Relaxed);
    handle
        .ppm_r_bits
        .store(meters.ppm_r.value_db().to_bits(), Ordering::Relaxed);

    if handle.live.load(Ordering::Relaxed) {
        let _ = pcm_tx.send(out);
    }
}
