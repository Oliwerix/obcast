//! Hardware playout: decode DVR segments and stream them to a `cpal` output
//! device, with start/stop/seek. `plan_uploads` in `obcast-proto` only knows
//! about the playout *head* (a `Seq`); this module is what actually advances
//! it, so every seek here immediately reshapes the encoder's upload plan on
//! the next `ServerState` publish.
//!
//! `symphonia` has no MPEG-TS demuxer, so segments are decoded to raw PCM via
//! a single long-lived `ffmpeg` subprocess (already a hard runtime dependency
//! for the encoder) and fed into cpal through a lock-free ring buffer, in
//! playout order. Segment `.ts` bytes are written to that one process's
//! stdin as each becomes needed and its PCM is read continuously off stdout
//! by a dedicated reader thread — MPEG-TS is splice-friendly, so concatenating
//! segments into one continuous stream keeps the AAC decoder's state alive
//! across segment boundaries (previously each segment spawned and tore down
//! its own ffmpeg process; besides the repeated spawn overhead, that reset
//! the decoder every 2s, which is a well-known source of audible pops from
//! AAC priming/padding samples reappearing at every segment edge). The
//! session is only torn down and respawned on `Start`/`Seek`, where the
//! discontinuity means the old process's buffered state is no longer wanted
//! anyway. The `cpal::Stream` is `!Send`, so the whole engine — decode
//! session, ring buffer, and the stream itself — lives on one dedicated OS
//! thread (plus its reader thread) and is driven by commands over a channel.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::process::{Child, ChildStdin, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use ringbuf::traits::{Consumer, Producer, Split};
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
    /// `skip_ms`: intra-segment offset to skip past before audio from
    /// `position` starts draining — 0 for a plain segment-boundary
    /// start/seek, nonzero for `PlayoutPosition::MillisBehindLive`'s
    /// sub-segment precision. Converted from ms to samples inside the
    /// engine, which is the only place `sample_rate` is known.
    Start {
        position: Seq,
        skip_ms: u32,
    },
    Stop,
    Pause,
    Resume,
    Seek {
        position: Seq,
        skip_ms: u32,
    },
    SetVolume {
        gain: f32,
    },
    SetTestTone {
        enabled: bool,
    },
}

/// Which channel(s) a test-tone pattern section outputs to.
#[derive(Clone, Copy)]
enum ToneChannels {
    Both,
    Left,
    Right,
    Silent,
}

/// 1kHz sine test-tone pattern (CLAUDE.md web remote request): 2s both
/// channels, 0.5s silence, 0.5s left, 0.5s silence, 0.5s right, 0.5s
/// silence, looping. Every section duration is a whole number of 1kHz
/// periods (1ms each), so the sine's phase is exactly zero at every section
/// boundary and at the loop point — no clicks from gating the amplitude.
const TEST_TONE_HZ: f64 = 1000.0;
/// -18 dBFS: matches the meters' 0 VU reference point (see stream.html's
/// METER_VU_REF_DBFS) so the tone reads as "0 VU" on the level meters.
const TEST_TONE_AMPLITUDE: f32 = 0.125_892_5;
const TEST_TONE_PATTERN: &[(f64, ToneChannels)] = &[
    (2.0, ToneChannels::Both),
    (0.5, ToneChannels::Silent),
    (0.5, ToneChannels::Left),
    (0.5, ToneChannels::Silent),
    (0.5, ToneChannels::Right),
    (0.5, ToneChannels::Silent),
];

/// Cumulative end-sample boundaries for `TEST_TONE_PATTERN` at a given
/// sample rate, plus the total pattern length in samples — computed once at
/// stream setup (sample rate is fixed for the stream's lifetime) rather than
/// per-sample in the real-time audio callback.
fn build_test_tone_pattern(sample_rate: u32) -> (Vec<(u64, ToneChannels)>, u64) {
    let sr = sample_rate as f64;
    let mut end = 0u64;
    let bounds = TEST_TONE_PATTERN
        .iter()
        .map(|(secs, ch)| {
            end += (secs * sr).round() as u64;
            (end, *ch)
        })
        .collect();
    (bounds, end.max(1))
}

fn test_tone_channels_at(pos: u64, bounds: &[(u64, ToneChannels)]) -> ToneChannels {
    bounds
        .iter()
        .find(|(end, _)| pos < *end)
        .map(|(_, ch)| *ch)
        .unwrap_or(ToneChannels::Silent)
}

/// One interleaved (L, R) sample pair of the test tone at pattern position
/// `pos` (already wrapped into `[0, total_samples)`).
fn test_tone_sample(pos: u64, sample_rate: u32, bounds: &[(u64, ToneChannels)]) -> (f32, f32) {
    let phase = 2.0 * std::f64::consts::PI * TEST_TONE_HZ * pos as f64 / sample_rate as f64;
    let s = (phase.sin() as f32) * TEST_TONE_AMPLITUDE;
    match test_tone_channels_at(pos, bounds) {
        ToneChannels::Both => (s, s),
        ToneChannels::Left => (s, 0.0),
        ToneChannels::Right => (0.0, s),
        ToneChannels::Silent => (0.0, 0.0),
    }
}

/// Shared with the ingest/control layer so `ServerState` always reflects the
/// playout engine's true position, even between control commands.
pub struct PlayoutHandle {
    running: AtomicBool,
    paused: AtomicBool,
    position_seq: AtomicI64,  // -1 = none
    volume_millis: AtomicU32, // gain * 1000, fixed-point for atomic access
    /// True while the test-tone pattern is overriding the hardware output
    /// (see `EngineCommand::SetTestTone`), independent of `running`/`paused`.
    test_tone: AtomicBool,
    /// Nominal segment duration — immutable for the life of the handle
    /// (set once at `spawn`), used by `ms_into_current_segment` to convert
    /// `pending`'s remaining-sample count into a millisecond offset.
    segment_ms: u32,
    /// Output sample rate, known only once the engine thread opens the
    /// device — 0 until then. `ms_into_current_segment` reports 0 rather
    /// than dividing by zero while this is still unset.
    sample_rate: AtomicU32,
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
    /// FIFO of `(seq, rung fed for it, remaining raw f32 samples not yet
    /// drained)` for segments currently sitting in the ring buffer, oldest
    /// first — pushed by the engine loop whenever it queues a segment's
    /// decoded PCM (or a `(seq, None, 0)` entry for a decode
    /// failure/abandoned/stall-timeout skip, which never puts any audio in
    /// the ring at all), drained by the realtime output callback as it
    /// actually consumes samples. This is what lets `position_seq` reflect
    /// the segment whose audio is truly coming out of the speaker right now
    /// rather than the segment most recently pushed into the ring — without
    /// it, `position_seq` (and everything derived from it:
    /// `ServerState.frontier_seq`/`lead_ms`/`coverage`, plus the client's
    /// on-air-quality/buffer readouts) reads up to a full ring capacity (3
    /// segments) ahead of what a listener can actually hear, since a whole
    /// segment gets pushed to the ring well before the callback has drained
    /// through it. The rung riding alongside each entry is what makes
    /// `playing_rung` ground truth rather than a live DVR-index lookup: the
    /// engine feeds bytes to the decoder many segments ahead of real-time
    /// playback (see `RING_SEGMENTS`), so the rung it picked for a seq is
    /// locked in well before that seq's audio is actually heard — if a
    /// quality upgrade for that same seq lands on disk in the meantime (a
    /// routine race, not an edge case, since uploads are far faster than the
    /// ring's depth in playback time), a live "best rung for this seq" query
    /// would report the new, higher rung even though the decoder already
    /// committed to the old one, which is exactly the "webui/GUI say HD,
    /// speaker is playing low" bug this field exists to prevent.
    pending: std::sync::Mutex<VecDeque<(Seq, Option<RungId>, usize)>>,
    /// The rung of whichever entry is at the front of `pending` — i.e.
    /// currently draining — as of the last `drain_pending` call. -1 means
    /// "unknown" (nothing confirmed draining yet: stopped, just
    /// started/sought, or the ring is genuinely empty). See `playing_rung`.
    playing_rung: AtomicI32,
    /// Highest seq the engine loop has fed to the decoder (or skipped
    /// without feeding — either way, its rung is locked in). -1 means
    /// "nothing fed yet at the current position" (stopped, or just
    /// started/sought). See `fed_seq()` and `ServerState::PlayoutStatus::fed_seq`
    /// for why the upgrade scheduler needs this rather than `position_seq`.
    fed_seq: AtomicI64,
    /// Bumped by the engine command loop on every `Start`/`Seek` to ask the
    /// output callback to drop whatever's currently sitting in the ring
    /// buffer — see `flush_ring_and_pending`'s doc comment for why a seek
    /// needs this in addition to restarting the decoder session (killing the
    /// decoder stops *new* stale writes, but doesn't touch bytes it already
    /// pushed before the jump, which would otherwise keep draining out of
    /// the speaker for up to `RING_SEGMENTS` worth of audio after a seek).
    flush_generation: AtomicU64,
    /// Set by the output callback to the last `flush_generation` it actually
    /// cleared the ring for — `flush_ring_and_pending` polls this to know
    /// when it's safe to let a freshly spawned decoder session start writing
    /// again without its first samples getting wiped by a late flush.
    flushed_generation: AtomicU64,
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

    /// The rung actually reaching the speaker for `position()` right now —
    /// ground truth, not a live re-lookup of "best rung for this seq" (see
    /// `pending`'s doc comment for why those two can and do disagree).
    /// `None` while stopped, just started/sought (nothing confirmed draining
    /// yet), or the segment at the head was skipped without ever producing
    /// audio.
    pub fn playing_rung(&self) -> Option<RungId> {
        let v = self.playing_rung.load(Ordering::Relaxed);
        (v >= 0).then_some(v as RungId)
    }

    /// Highest seq already fed into the decode pipeline — see the field
    /// doc comment on `fed_seq` and on `PlayoutStatus::fed_seq`.
    pub fn fed_seq(&self) -> Option<Seq> {
        let v = self.fed_seq.load(Ordering::Relaxed);
        (v >= 0).then_some(v as Seq)
    }

    /// Milliseconds already drained into the segment at `position()`,
    /// against `segment_ms` — see `PlayoutStatus::position_ms_into_segment`.
    /// Best-effort against `pending`'s front entry rather than sample-exact:
    /// a segment's `remaining` count starts at `segment_samples` (or less,
    /// for the first segment after a sub-segment seek — see
    /// `EngineCommand::Seek`'s `skip_ms`), so `segment_samples - remaining`
    /// is always "how far into this segment's nominal span we are,"
    /// skip included. `0` before the sample rate is known or nothing is
    /// queued yet (stopped, just started/sought).
    pub fn ms_into_current_segment(&self) -> u32 {
        let sample_rate = self.sample_rate.load(Ordering::Relaxed);
        if sample_rate == 0 {
            return 0;
        }
        let remaining = self.pending.lock().unwrap().front().map(|&(_, _, r)| r);
        let Some(remaining) = remaining else {
            return 0;
        };
        let segment_samples =
            (self.segment_ms as u64 * sample_rate as u64 / 1000) as usize * CHANNELS;
        elapsed_ms_from_remaining(remaining, segment_samples, self.segment_ms)
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

    pub fn test_tone(&self) -> bool {
        self.test_tone.load(Ordering::Relaxed)
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

    /// Called from the engine (decode) loop once it has decided what to do
    /// about `seq`: `rung`/`samples` are the rung actually fed and the raw
    /// f32 sample count just pushed to the ring for real audio, or
    /// `(None, 0)` for a decode failure/abandoned/stall-timeout skip that
    /// never put anything in the ring. See `pending`'s doc comment.
    fn enqueue_pending(&self, seq: Seq, rung: Option<RungId>, samples: usize) {
        self.pending.lock().unwrap().push_back((seq, rung, samples));
        self.fed_seq.store(seq as i64, Ordering::Relaxed);
    }

    /// Called from the realtime output callback after it has drained
    /// `filled` real samples this tick (excluding any zero-padded underrun
    /// tail — there's no decoded audio behind that padding to attribute to
    /// a seq). Advances `position_seq`/`playing_rung` to whichever segment
    /// is now at the front of what's left un-drained.
    fn drain_pending(&self, filled: usize) {
        let mut pending = self.pending.lock().unwrap();
        if let Some((next, rung)) = advance_pending(&mut pending, filled) {
            self.position_seq.store(next as i64, Ordering::Relaxed);
            self.playing_rung
                .store(rung.map(|r| r as i32).unwrap_or(-1), Ordering::Relaxed);
        }
    }
}

/// Pure core of `PlayoutHandle::drain_pending`, factored out so it's testable
/// without a real cpal device/thread: walks `pending`'s front entries given
/// `filled` real samples just consumed, popping any that are now fully
/// drained (returning the seq *after* the last one popped — i.e. the new
/// reported position — paired with the rung of whichever entry is now at
/// the front, if any, so `playing_rung` reflects what's actually queued to
/// drain next rather than lagging a stale value — or `None` if nothing
/// changed). Zero-length entries are popped immediately regardless of
/// `filled`, since there's nothing to wait for — they represent a seq that
/// was skipped without ever producing audio (decode failure / abandoned /
/// stall-timeout).
fn advance_pending(
    pending: &mut VecDeque<(Seq, Option<RungId>, usize)>,
    mut filled: usize,
) -> Option<(Seq, Option<RungId>)> {
    let mut new_position = None;
    while let Some(front) = pending.front_mut() {
        if front.2 == 0 {
            let next = front.0 + 1;
            pending.pop_front();
            new_position = Some(next);
            continue;
        }
        if filled == 0 {
            break;
        }
        if front.2 <= filled {
            filled -= front.2;
            let next = front.0 + 1;
            pending.pop_front();
            new_position = Some(next);
        } else {
            front.2 -= filled;
            filled = 0;
        }
    }
    new_position.map(|next| {
        let rung = pending.front().and_then(|&(_, r, _)| r);
        (next, rung)
    })
}

/// Pure core of `PlayoutHandle::ms_into_current_segment`: converts a
/// pending entry's `remaining` sample count into "ms already elapsed against
/// this segment's nominal span." Works unmodified for a sub-segment-seek
/// entry too — such an entry's `remaining` already starts at
/// `segment_samples - skip_samples` rather than the full `segment_samples`
/// (see `EngineCommand::Seek`'s `skip_ms` handling), so `segment_samples -
/// remaining` is `skip_samples + drained-so-far`, i.e. still exactly "how far
/// into the nominal segment," skip included, with no separate case needed.
fn elapsed_ms_from_remaining(remaining: usize, segment_samples: usize, segment_ms: u32) -> u32 {
    if segment_samples == 0 {
        return 0;
    }
    let elapsed = segment_samples
        .saturating_sub(remaining)
        .min(segment_samples);
    (elapsed as u64 * segment_ms as u64 / segment_samples as u64) as u32
}

/// Once a segment has been neither playable nor confirmed-abandoned for this
/// long, playout skips it anyway rather than freezing the head forever — a
/// backstop for when the encoder never calls `/abandon` at all (crashed,
/// disconnected). Generous relative to a normal short outage (CLAUDE.md §5),
/// but bounded: `3 * segment_ms` gives the encoder several retry ticks first.
fn stall_timeout(segment_ms: u32) -> Duration {
    Duration::from_millis(segment_ms.max(1) as u64 * 3)
}

/// Converts a sub-segment seek's `skip_ms` (from `EngineCommand::Start`/
/// `Seek`) into interleaved samples to drop from the next segment fed to the
/// decoder (`spawn_decoder_session`'s `skip_samples`). Clamped below one
/// full segment's worth — a `skip_ms >= segment_ms` would mean "skip the
/// whole segment," which isn't this mechanism's job (the caller should have
/// picked the next segment as `position` instead).
fn skip_samples(skip_ms: u32, sample_rate: u32, segment_ms: u32) -> usize {
    let segment_samples =
        (segment_ms.max(1) as u64 * sample_rate as u64 / 1000) as usize * CHANNELS;
    let requested = (skip_ms as u64 * sample_rate as u64 / 1000) as usize * CHANNELS;
    requested.min(segment_samples.saturating_sub(CHANNELS))
}

/// A live decode pipeline: one `ffmpeg` process reading a continuous MPEG-TS
/// byte stream on stdin and writing continuous interleaved f32 PCM on
/// stdout, plus the dedicated thread draining that stdout into the playout
/// ring buffer. Torn down and replaced wholesale on `Start`/`Seek` (see
/// module docs); left alone across `Pause`/`Resume`, where ffmpeg just blocks
/// on its next stdin read with no CPU cost.
struct DecoderSession {
    child: Child,
    stdin: ChildStdin,
    reader: std::thread::JoinHandle<ringbuf::HeapProd<f32>>,
}

/// Spawns a fresh decode session and hands the ring's producer half to its
/// reader thread. `producer` is always returned to the caller — inside the
/// `Ok` session on success, or alongside the error on failure (spawning a
/// subprocess is the only fallible step, before `producer` is touched at
/// all) — so a failed spawn never silently drops the only handle to the
/// ring and leaves playout permanently mute.
/// `skip_samples`: interleaved samples to silently discard from the front of
/// the decoded PCM stream before any of it reaches the ring — the mechanism
/// behind sub-segment seeking (`EngineCommand::Start`/`Seek`'s `skip_ms`,
/// converted to samples by the caller). `0` for a plain segment-boundary
/// start/seek or a mid-stream decoder restart, where nothing should be
/// dropped.
fn spawn_decoder_session(
    sample_rate: u32,
    mut producer: ringbuf::HeapProd<f32>,
    skip_samples: usize,
) -> Result<DecoderSession, (std::io::Error, ringbuf::HeapProd<f32>)> {
    let child = std::process::Command::new("ffmpeg")
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        // Each segment is encoded with -reset_timestamps 1, so splicing many
        // of them onto one long-lived demuxer's stdin produces small backward
        // DTS steps at segment boundaries; +genpts regenerates monotonic
        // PTS/DTS from decoded frame durations to stop libavformat's muxer
        // sanity check from spamming stderr. Cosmetic only — nothing in this
        // pipeline reads ffmpeg's own timestamps.
        .arg("-fflags")
        .arg("+genpts")
        .arg("-f")
        .arg("mpegts")
        .arg("-i")
        .arg("pipe:0")
        .arg("-f")
        .arg("f32le")
        .arg("-ac")
        .arg(CHANNELS.to_string())
        .arg("-ar")
        .arg(sample_rate.to_string())
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn();
    let mut child = match child {
        Ok(c) => c,
        Err(err) => return Err((err, producer)),
    };
    let stdin = child.stdin.take().expect("piped stdin");
    let mut stdout = child.stdout.take().expect("piped stdout");

    let reader = std::thread::spawn(move || {
        let mut buf = [0u8; 65536];
        // Bytes read but not yet forming a complete f32 (4 bytes); carried
        // over to the front of the next read since `read()` gives no
        // alignment guarantee against our sample boundaries.
        let mut leftover: Vec<u8> = Vec::with_capacity(4);
        // Counts down as leading samples are dropped for a sub-segment seek
        // (see `skip_samples`'s doc comment); once it hits 0 every sample is
        // pushed to the ring as normal for the rest of this session's life.
        let mut skip_remaining = skip_samples;
        loop {
            match stdout.read(&mut buf) {
                Ok(0) | Err(_) => break, // ffmpeg exited or the pipe broke
                Ok(n) => {
                    leftover.extend_from_slice(&buf[..n]);
                    let complete = leftover.len() - leftover.len() % 4;
                    if complete == 0 {
                        continue;
                    }
                    let mut samples: Vec<f32> = leftover[..complete]
                        .chunks_exact(4)
                        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                        .collect();
                    if skip_remaining > 0 {
                        let drop_n = skip_remaining.min(samples.len());
                        samples.drain(..drop_n);
                        skip_remaining -= drop_n;
                    }
                    push_all(&mut producer, &samples);
                    leftover.drain(..complete);
                }
            }
        }
        producer
    });

    Ok(DecoderSession {
        child,
        stdin,
        reader,
    })
}

/// Kills the decoder process and joins its reader thread to reclaim the
/// ring's producer half. Blocking, but brief: killing the process closes its
/// stdout, so the reader's `read()` returns promptly.
fn teardown_decoder_session(session: DecoderSession) -> ringbuf::HeapProd<f32> {
    let DecoderSession {
        mut child, reader, ..
    } = session;
    let _ = child.kill();
    let _ = child.wait();
    reader.join().unwrap_or_else(|_| {
        // The reader thread only panics if `push_all` does, which never
        // allocates or indexes out of bounds — practically unreachable, but
        // an empty ring (rather than propagating the panic) keeps a single
        // bad decode from taking the whole playout engine down.
        tracing::error!("playout decoder reader thread panicked");
        ringbuf::HeapRb::<f32>::new(1).split().0
    })
}

/// Clears the ring buffer's currently-queued audio plus the matching
/// `pending` seq-tracking queue, for `Start`/`Seek`. Killing the old decoder
/// session (see `teardown_decoder_session`, called by the caller just before
/// this) stops any *new* stale writes, but does nothing about bytes it
/// already pushed before the jump — those would otherwise sit in the ring
/// and keep draining out to the speaker for up to `RING_SEGMENTS` worth of
/// audio after the jump, which is exactly the "seek starts playing the new
/// position, then jumps back to the old one" bug this exists to fix (the
/// stale entries in `pending` would also keep overwriting `position_seq`
/// back to the pre-seek value as that stale audio drains, since
/// `drain_pending` runs off whatever's at the front of the queue).
///
/// The ring's consumer half lives only inside the cpal output callback
/// closure (see `run_engine`), so this can't clear it directly from the
/// engine command loop — it bumps `flush_generation` and the callback
/// performs the actual `consumer.clear()` on its next invocation, acking via
/// `flushed_generation`. Blocks briefly (bounded by `deadline`, since a
/// paused/stalled output device would otherwise never invoke the callback
/// at all) so the caller's subsequent fresh decoder session can't race the
/// flush and have its own first samples wiped along with the stale ones.
fn flush_ring_and_pending(handle: &Arc<PlayoutHandle>) {
    handle.pending.lock().unwrap().clear();
    let target = handle.flush_generation.fetch_add(1, Ordering::Relaxed) + 1;
    let deadline = Instant::now() + Duration::from_millis(200);
    while handle.flushed_generation.load(Ordering::Relaxed) < target && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// Tears down any existing decoder session and starts a fresh one — used by
/// `Start`/`Seek` (a discontinuity the old session's buffered state can't
/// straddle) and by the feed loop when a stdin write fails (the decoder
/// process died underneath it). On a failed spawn this reports the same way
/// a failed output-device open does (`device_error`/`PlayoutState::Error`) —
/// playout can't produce audio without a working decoder any more than
/// without a device — while leaving `producer_holder` populated so the next
/// `Start`/`Seek` (or the feed loop's own retry) can still use it.
/// `skip_samples` is forwarded to the fresh session (see
/// `spawn_decoder_session`'s doc comment) — pass 0 for a mid-stream restart
/// (self-heal, failed feed) where nothing should be dropped.
fn restart_decoder_session(
    session: &mut Option<DecoderSession>,
    producer_holder: &mut Option<ringbuf::HeapProd<f32>>,
    sample_rate: u32,
    skip_samples: usize,
    handle: &Arc<PlayoutHandle>,
) {
    if let Some(old) = session.take() {
        *producer_holder = Some(teardown_decoder_session(old));
    }
    let producer = producer_holder
        .take()
        .expect("producer_holder is always repopulated by teardown or a prior failed spawn");
    match spawn_decoder_session(sample_rate, producer, skip_samples) {
        Ok(s) => {
            *session = Some(s);
            handle.device_error.write().unwrap().take();
        }
        Err((err, producer)) => {
            *producer_holder = Some(producer);
            let msg = format!("failed to start audio decoder: {err}");
            tracing::error!("{msg}");
            handle.push_log(LogLevel::Error, msg.clone());
            *handle.device_error.write().unwrap() = Some(msg);
        }
    }
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
        test_tone: AtomicBool::new(false),
        segment_ms,
        sample_rate: AtomicU32::new(0),
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
        pending: std::sync::Mutex::new(VecDeque::new()),
        playing_rung: AtomicI32::new(-1),
        fed_seq: AtomicI64::new(-1),
        flush_generation: AtomicU64::new(0),
        flushed_generation: AtomicU64::new(0),
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
    handle.sample_rate.store(sample_rate, Ordering::Relaxed);
    let config = cpal::StreamConfig {
        channels: CHANNELS as u16,
        sample_rate,
        buffer_size: cpal::BufferSize::Default,
    };

    // Sized in segments: with the persistent decode session (see module
    // docs and `DecoderSession`), how far ahead of playback ffmpeg can work
    // is governed by pipe backpressure through this ring rather than a
    // per-segment gate — feeding segments to its stdin blocks once the ring
    // (and ffmpeg's own internal buffers) fill up. A deeper ring means more
    // bytes can be decoded and queued ahead of the cpal callback's real-time
    // consumption, which is what actually absorbs a transient slowdown (a
    // loaded machine, a slow disk read for one segment) without an audible
    // underrun — previously this was only 3 segments' worth (6s at the 2s
    // default) and still not enough headroom to survive every hiccup, which
    // surfaced as spurious "buffer underrun" stalls that recovered on their
    // own once decode caught back up (confirmed live: both the client and
    // server still had every segment on disk throughout, so it was never a
    // data gap).
    const RING_SEGMENTS: usize = 8;
    let segment_samples = (segment_ms as u64 * sample_rate as u64 / 1000) as usize * CHANNELS;
    let ring = HeapRb::<f32>::new(segment_samples * RING_SEGMENTS);
    let (producer, mut consumer) = ring.split();
    // Held by whichever side currently doesn't have a live decoder: the
    // engine loop between sessions, or a `DecoderSession`'s reader thread
    // while one is running. `teardown_decoder_session` always hands it back.
    let mut producer_holder = Some(producer);
    let mut session: Option<DecoderSession> = None;

    let volume_handle = handle.clone();
    let (test_tone_bounds, test_tone_total_samples) = build_test_tone_pattern(sample_rate);
    // Only ever touched by this callback, so a plain (non-atomic) counter is
    // enough; reset to 0 whenever test tone is off so re-enabling it always
    // starts the pattern fresh from "both channels."
    let mut test_tone_pos: u64 = 0;
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
    // Last `flush_generation` this callback has already acted on — see
    // `flush_ring_and_pending`. Checked unconditionally (even while the test
    // tone is active) so the ack always stays in sync with what the engine
    // command loop asked for, regardless of which branch below runs.
    let mut last_flush_gen: u64 = 0;
    let stream = device.build_output_stream(
        config,
        move |data: &mut [f32], _| {
            let want_flush = volume_handle.flush_generation.load(Ordering::Relaxed);
            if want_flush != last_flush_gen {
                consumer.clear();
                last_flush_gen = want_flush;
                volume_handle
                    .flushed_generation
                    .store(want_flush, Ordering::Relaxed);
            }
            if volume_handle.test_tone.load(Ordering::Relaxed) {
                // Test tone bypasses the segment ring buffer entirely — it's
                // a hardware-output wiring check, independent of DVR/decode.
                volume_handle.underrun.store(false, Ordering::Relaxed);
                for frame in data.chunks_exact_mut(CHANNELS) {
                    let (l, r) = test_tone_sample(test_tone_pos, sample_rate, &test_tone_bounds);
                    frame[0] = l;
                    frame[1] = r;
                    test_tone_pos = (test_tone_pos + 1) % test_tone_total_samples;
                }
            } else {
                test_tone_pos = 0;
                let gain = volume_handle.volume();
                let filled = consumer.pop_slice(data);
                // A partial fill means the ring ran dry this callback — real
                // audio, but not all of it; still an underrun, since some of
                // `data` below gets zero-padded either way.
                volume_handle
                    .underrun
                    .store(filled < data.len(), Ordering::Relaxed);
                // Advance the reported position from what's actually
                // draining out right now — only `filled` real samples, never
                // the zero-padded underrun tail below (there's no decoded
                // audio behind that padding to attribute to any seq).
                volume_handle.drain_pending(filled);
                for s in &mut data[..filled] {
                    *s *= gain;
                }
                for s in &mut data[filled..] {
                    *s = 0.0;
                }
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
    // Set by `Start`/`Seek` to the sub-segment offset (in interleaved
    // samples) to drop from whatever segment is fed next — see
    // `EngineCommand::Start`'s `skip_ms`. Consumed once, by the very next
    // successful feed, then reset to 0: only the first segment after a
    // discontinuity can have a nonzero skip, since the decoder session
    // itself only drops leading samples once per restart (see
    // `spawn_decoder_session`).
    let mut pending_skip_samples: usize = 0;

    loop {
        match cmd_rx.try_recv() {
            Ok(EngineCommand::Start { position, skip_ms }) => {
                current = Some(position);
                stall_since = None;
                *handle.stall_reason.write().unwrap() = None;
                handle
                    .position_seq
                    .store(position as i64, Ordering::Relaxed);
                // No rung confirmed draining yet for the new position —
                // `drain_pending` sets this for real once audio for it
                // actually reaches the output callback.
                handle.playing_rung.store(-1, Ordering::Relaxed);
                // Nothing fed yet at the new position either.
                handle.fed_seq.store(-1, Ordering::Relaxed);
                handle.running.store(true, Ordering::Relaxed);
                handle.paused.store(false, Ordering::Relaxed);
                // No audio has actually reached the ring yet; the output
                // callback will clear this on its first full buffer.
                handle.underrun.store(true, Ordering::Relaxed);
                pending_skip_samples = skip_samples(skip_ms, sample_rate, segment_ms);
                if let Some(old) = session.take() {
                    producer_holder = Some(teardown_decoder_session(old));
                }
                // `flush_ring_and_pending` needs the output callback to
                // actually run in order to ack the flush — a Start right
                // after Stop finds the stream paused, so `play()` first.
                let _ = stream.play();
                flush_ring_and_pending(&handle);
                restart_decoder_session(
                    &mut session,
                    &mut producer_holder,
                    sample_rate,
                    pending_skip_samples,
                    &handle,
                );
                tracing::info!(seq = position, skip_ms, "playout started");
            }
            Ok(EngineCommand::Stop) => {
                current = None;
                stall_since = None;
                *handle.stall_reason.write().unwrap() = None;
                handle.running.store(false, Ordering::Relaxed);
                handle.position_seq.store(-1, Ordering::Relaxed);
                handle.playing_rung.store(-1, Ordering::Relaxed);
                handle.fed_seq.store(-1, Ordering::Relaxed);
                if let Some(old) = session.take() {
                    producer_holder = Some(teardown_decoder_session(old));
                }
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
            Ok(EngineCommand::Seek { position, skip_ms }) => {
                current = Some(position);
                stall_since = None;
                *handle.stall_reason.write().unwrap() = None;
                handle
                    .position_seq
                    .store(position as i64, Ordering::Relaxed);
                handle.playing_rung.store(-1, Ordering::Relaxed);
                handle.fed_seq.store(-1, Ordering::Relaxed);
                // A seek is a discontinuity in the byte stream fed to the
                // decoder, so the old session (mid-decode of the pre-seek
                // position) must go — restart fresh rather than let stale
                // audio for the old position keep draining out of it. The
                // ring buffer and `pending` need the same treatment (see
                // `flush_ring_and_pending`): otherwise audio already decoded
                // for the pre-seek position, still sitting in the ring,
                // keeps draining out *after* the jump — audibly playing the
                // new position for an instant and then "jumping back" to
                // the old one as that stale backlog (and `pending`'s
                // matching seq entries, which `drain_pending` reads
                // `position_seq` off of) works its way through.
                pending_skip_samples = skip_samples(skip_ms, sample_rate, segment_ms);
                if let Some(old) = session.take() {
                    producer_holder = Some(teardown_decoder_session(old));
                }
                flush_ring_and_pending(&handle);
                restart_decoder_session(
                    &mut session,
                    &mut producer_holder,
                    sample_rate,
                    pending_skip_samples,
                    &handle,
                );
                tracing::info!(seq = position, skip_ms, "playout seek");
            }
            Ok(EngineCommand::SetVolume { gain }) => {
                handle
                    .volume_millis
                    .store((gain.max(0.0) * 1000.0) as u32, Ordering::Relaxed);
            }
            Ok(EngineCommand::SetTestTone { enabled }) => {
                handle.test_tone.store(enabled, Ordering::Relaxed);
                if enabled {
                    // The stream may not be playing at all yet (nothing ever
                    // `Start`ed) — the test tone works independent of normal
                    // playout, so it must (re)start the stream itself.
                    let _ = stream.play();
                } else if !handle.running.load(Ordering::Relaxed) {
                    // Only pause if normal playout isn't also active; don't
                    // stop real audio just because the tone was turned off.
                    let _ = stream.pause();
                }
                handle.push_log(
                    LogLevel::Info,
                    format!("test tone {}", if enabled { "enabled" } else { "disabled" }),
                );
                tracing::info!(enabled, "playout test tone toggled");
            }
            Err(mpsc::error::TryRecvError::Disconnected) => return,
            Err(mpsc::error::TryRecvError::Empty) => {}
        }

        let should_feed =
            handle.running.load(Ordering::Relaxed) && !handle.paused.load(Ordering::Relaxed);
        if should_feed && session.is_none() {
            // Self-heal after a failed spawn (see `restart_decoder_session`)
            // instead of leaving playout permanently stuck — retried every
            // tick, which is fine for a rare/exceptional failure (e.g.
            // `ffmpeg` briefly unavailable) and self-limiting since a
            // successful respawn clears this branch immediately. Forwards
            // whatever sub-segment skip is still pending from the most
            // recent Start/Seek in case that one never got a chance to
            // spawn successfully at all.
            restart_decoder_session(
                &mut session,
                &mut producer_holder,
                sample_rate,
                pending_skip_samples,
                &handle,
            );
        }
        if should_feed && session.is_some() {
            if let Some(seq) = current {
                // No per-segment gate on ring space here (unlike the old
                // per-segment decode): feeding bytes to the persistent
                // decoder's stdin naturally blocks once the ring — and
                // ffmpeg's own internal buffers — fill up, since nothing
                // downstream is draining fast enough. That backpressure
                // chain paces feeding to playback on its own.
                let (path_and_rung, abandoned) = rt.block_on(async {
                    let store = store.lock().await;
                    (
                        best_available_path(&store, &rungs, seq),
                        store.is_abandoned(seq),
                    )
                });

                let mut advance = false;
                if let Some((path, rung)) = path_and_rung {
                    // Playable — the common case. A segment not yet on
                    // disk holds the head in place rather than advancing
                    // past it (see the `abandoned`/timeout branches
                    // below for when it stops waiting), protecting "no
                    // audio lost to a short outage" per CLAUDE.md §5.
                    let fed = std::fs::read(&path)
                        .and_then(|bytes| session.as_mut().unwrap().stdin.write_all(&bytes));
                    match fed {
                        Ok(()) => {
                            // Bytes hit the decoder's stdin — whatever
                            // stalled things before is no longer why. The
                            // reader thread clears `underrun` once PCM
                            // actually reaches the ring.
                            *handle.stall_reason.write().unwrap() = None;
                            // The persistent decoder demuxes one continuous
                            // stream (see module docs), so there's no exact
                            // per-segment PCM length to read off the way the
                            // old per-segment decode had — `segment_samples`
                            // (the nominal segment duration) stands in for
                            // `pending`'s purpose of tracking roughly which
                            // segment is audible right now, not sample-exact
                            // position. Shortened by whatever sub-segment
                            // offset a Start/Seek asked to skip past for
                            // *this* segment specifically — see
                            // `pending_skip_samples`'s doc comment — so
                            // `ms_into_current_segment` reads the skip as
                            // already-elapsed rather than the seek looking
                            // like it snapped back to the segment's start.
                            handle.enqueue_pending(
                                seq,
                                Some(rung),
                                segment_samples.saturating_sub(pending_skip_samples),
                            );
                            pending_skip_samples = 0;
                            advance = true;
                        }
                        Err(err) => {
                            // Most likely the decoder process died (broken
                            // pipe) rather than this one segment being bad —
                            // restart the session and retry the same seq
                            // next tick instead of skipping past it.
                            tracing::warn!(seq, error = %err, "failed to feed decoder, restarting decode session");
                            handle.push_log(
                                LogLevel::Warn,
                                format!(
                                    "segment {seq} failed to decode ({err}), restarting decoder"
                                ),
                            );
                            *handle.stall_reason.write().unwrap() =
                                Some(format!("segment {seq} failed to decode: {err}"));
                            restart_decoder_session(
                                &mut session,
                                &mut producer_holder,
                                sample_rate,
                                pending_skip_samples,
                                &handle,
                            );
                        }
                    }
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
                    handle.enqueue_pending(seq, None, 0);
                    // No audio at all for this seq, so any skip requested
                    // for it is moot — don't let it leak into whichever
                    // segment plays next.
                    pending_skip_samples = 0;
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
                        handle.enqueue_pending(seq, None, 0);
                        pending_skip_samples = 0;
                        advance = true;
                    }
                }

                if advance {
                    current = Some(seq + 1);
                    stall_since = None;
                    // `position_seq` no longer advances here — it now
                    // tracks real drain progress via `pending`/
                    // `drain_pending`, driven by the output callback (see
                    // `PlayoutHandle::pending`'s doc comment for why:
                    // advancing it the instant a segment is fed to the
                    // decoder would report a position up to a full ring's
                    // worth ahead of what's actually audible).
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

fn best_available_path(
    store: &DvrStore,
    rungs: &[RungId],
    seq: Seq,
) -> Option<(std::path::PathBuf, RungId)> {
    let rung = *rungs
        .iter()
        .rev()
        .find(|&&r| store.has_rung(seq, r))
        .or_else(|| rungs.first())?;
    let path = store.segment_path(rung, seq);
    path.exists().then_some((path, rung))
}
#[cfg(test)]
mod test_tone_tests {
    use super::*;

    const SR: u32 = 48_000;

    #[test]
    fn pattern_totals_four_point_five_seconds() {
        let (_, total) = build_test_tone_pattern(SR);
        assert_eq!(total, (4.5 * SR as f64) as u64);
    }

    #[test]
    fn sections_land_on_the_documented_boundaries() {
        let (bounds, _) = build_test_tone_pattern(SR);
        let both_end = (2.0 * SR as f64) as u64;
        let pause1_end = both_end + (0.5 * SR as f64) as u64;
        let left_end = pause1_end + (0.5 * SR as f64) as u64;
        let pause2_end = left_end + (0.5 * SR as f64) as u64;
        let right_end = pause2_end + (0.5 * SR as f64) as u64;
        let pause3_end = right_end + (0.5 * SR as f64) as u64;

        assert!(matches!(
            test_tone_channels_at(0, &bounds),
            ToneChannels::Both
        ));
        assert!(matches!(
            test_tone_channels_at(both_end - 1, &bounds),
            ToneChannels::Both
        ));
        assert!(matches!(
            test_tone_channels_at(both_end, &bounds),
            ToneChannels::Silent
        ));
        assert!(matches!(
            test_tone_channels_at(pause1_end, &bounds),
            ToneChannels::Left
        ));
        assert!(matches!(
            test_tone_channels_at(left_end, &bounds),
            ToneChannels::Silent
        ));
        assert!(matches!(
            test_tone_channels_at(pause2_end, &bounds),
            ToneChannels::Right
        ));
        assert!(matches!(
            test_tone_channels_at(right_end, &bounds),
            ToneChannels::Silent
        ));
        assert_eq!(pause3_end, (4.5 * SR as f64) as u64);
    }

    #[test]
    fn left_and_right_sections_zero_the_other_channel() {
        let (bounds, _) = build_test_tone_pattern(SR);
        // A quarter-period offset into the "left" section so the sample
        // itself is non-zero (not just correctly gated at a zero crossing).
        let left_start = (2.5 * SR as f64) as u64;
        let quarter_period = (SR as f64 / TEST_TONE_HZ / 4.0) as u64;
        let (l, r) = test_tone_sample(left_start + quarter_period, SR, &bounds);
        assert!(l.abs() > 0.01);
        assert_eq!(r, 0.0);

        let right_start = (3.5 * SR as f64) as u64;
        let (l, r) = test_tone_sample(right_start + quarter_period, SR, &bounds);
        assert_eq!(l, 0.0);
        assert!(r.abs() > 0.01);
    }

    #[test]
    fn both_section_is_identical_on_both_channels_and_at_amplitude() {
        let (bounds, _) = build_test_tone_pattern(SR);
        let quarter_period = (SR as f64 / TEST_TONE_HZ / 4.0) as u64;
        let (l, r) = test_tone_sample(quarter_period, SR, &bounds);
        assert_eq!(l, r);
        assert!((l - TEST_TONE_AMPLITUDE).abs() < 1e-4);
    }

    #[test]
    fn silence_sections_are_exactly_zero() {
        let (bounds, _) = build_test_tone_pattern(SR);
        let pause_mid = (2.25 * SR as f64) as u64;
        assert_eq!(test_tone_sample(pause_mid, SR, &bounds), (0.0, 0.0));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_pending_reports_nothing_drained_yet() {
        let mut pending = VecDeque::from([(5, Some(2), 100)]);
        assert_eq!(advance_pending(&mut pending, 40), None);
        assert_eq!(pending.front(), Some(&(5, Some(2), 60)));
    }

    #[test]
    fn advance_pending_stays_on_a_segment_until_fully_drained() {
        // A whole segment (100 samples) was queued at once, well ahead of
        // playback — this is exactly the bug: without draining-based
        // tracking, position would jump to 6 the instant it was queued,
        // not once the audio callback has actually consumed it.
        let mut pending = VecDeque::from([(5, Some(2), 100)]);
        assert_eq!(advance_pending(&mut pending, 60), None);
        assert_eq!(advance_pending(&mut pending, 39), None);
        assert_eq!(advance_pending(&mut pending, 1), Some((6, None)));
        assert!(pending.is_empty());
    }

    #[test]
    fn advance_pending_can_cross_multiple_segments_in_one_callback() {
        let mut pending = VecDeque::from([(5, Some(0), 10), (6, Some(1), 10), (7, Some(2), 10)]);
        // One callback drains enough for segments 5 and 6, and part of 7 —
        // the new front is 7, so its rung (hd) is what's reported now.
        assert_eq!(advance_pending(&mut pending, 25), Some((7, Some(2))));
        assert_eq!(pending.front(), Some(&(7, Some(2), 5)));
    }

    #[test]
    fn advance_pending_pops_zero_length_entries_immediately_without_needing_filled() {
        // A decode failure/abandoned/stall-timeout skip never puts audio in
        // the ring, so it must not wait on `filled` samples to be reported
        // as passed — even with 0 filled this callback, it advances.
        let mut pending = VecDeque::from([(5, None, 0), (6, None, 0), (7, Some(1), 10)]);
        assert_eq!(advance_pending(&mut pending, 0), Some((7, Some(1))));
        assert_eq!(pending.front(), Some(&(7, Some(1), 10)));
    }

    #[test]
    fn advance_pending_ignores_the_zero_padded_underrun_tail() {
        // Only real drained samples (`filled`) move the position — a caller
        // must never pass the full underrun-padded buffer length, or a gap
        // would silently skip ahead past audio that hasn't played yet.
        let mut pending = VecDeque::from([(5, Some(0), 100)]);
        assert_eq!(advance_pending(&mut pending, 0), None);
        assert_eq!(pending.front(), Some(&(5, Some(0), 100)));
    }

    #[test]
    fn advance_pending_on_an_empty_queue_reports_nothing() {
        let mut pending: VecDeque<(Seq, Option<RungId>, usize)> = VecDeque::new();
        assert_eq!(advance_pending(&mut pending, 50), None);
    }

    #[test]
    fn advance_pending_reports_none_rung_when_nothing_is_queued_for_the_new_head() {
        // The head has fully drained past seq 5 but nothing has been fed
        // for seq 6 yet (e.g. the decoder hasn't caught up) — the reported
        // position still advances optimistically (existing behaviour), but
        // the rung must read `None` ("unknown") rather than reusing seq 5's
        // rung, since nothing is confirmed to be playing yet.
        let mut pending = VecDeque::from([(5, Some(2), 10)]);
        assert_eq!(advance_pending(&mut pending, 10), Some((6, None)));
        assert!(pending.is_empty());
    }

    #[test]
    fn advance_pending_reflects_a_lower_rung_actually_fed_even_though_disk_now_has_hd() {
        // This is the core bug this field exists to prevent: the decoder
        // committed to the low rung for seq 6 (all that was available when
        // it was fed, many segments ahead of real-time playback); an HD
        // upload for that same seq can land on disk afterward, but what's
        // actually queued to drain — and thus what `playing_rung` must
        // report — stays the rung that was fed, not whatever the DVR index
        // says is best right now.
        let mut pending = VecDeque::from([(5, Some(2), 10), (6, Some(0), 10)]);
        assert_eq!(advance_pending(&mut pending, 10), Some((6, Some(0))));
    }

    #[test]
    fn elapsed_ms_is_zero_for_a_freshly_fed_segment() {
        assert_eq!(elapsed_ms_from_remaining(100, 100, 2000), 0);
    }

    #[test]
    fn elapsed_ms_is_full_segment_once_fully_drained() {
        assert_eq!(elapsed_ms_from_remaining(0, 100, 2000), 2000);
    }

    #[test]
    fn elapsed_ms_is_proportional_partway_through() {
        // 25 of 100 samples drained -> a quarter of the way through.
        assert_eq!(elapsed_ms_from_remaining(75, 100, 2000), 500);
    }

    #[test]
    fn elapsed_ms_counts_a_sub_segment_seek_skip_as_already_elapsed() {
        // A seek that skipped the first half of the segment starts this
        // entry's `remaining` at half the nominal sample count (see
        // `skip_samples`) — even with nothing drained yet, that should read
        // as already halfway into the segment, not 0.
        assert_eq!(elapsed_ms_from_remaining(50, 100, 2000), 1000);
    }

    #[test]
    fn elapsed_ms_is_zero_with_no_sample_rate_known() {
        assert_eq!(elapsed_ms_from_remaining(0, 0, 2000), 0);
    }

    #[test]
    fn skip_samples_converts_ms_to_interleaved_samples() {
        // 48kHz stereo, 250ms -> 12000 frames * 2 channels.
        assert_eq!(skip_samples(250, 48_000, 2000), 24_000);
    }

    #[test]
    fn skip_samples_clamps_below_one_full_segment() {
        // Requesting the whole segment (or more) clamps just under it —
        // "skip the whole segment" isn't this mechanism's job (see the
        // function's doc comment).
        let segment_samples = 2000 * 48_000 / 1000 * CHANNELS;
        assert_eq!(skip_samples(2000, 48_000, 2000), segment_samples - CHANNELS);
        assert_eq!(
            skip_samples(10_000, 48_000, 2000),
            segment_samples - CHANNELS
        );
    }

    #[test]
    fn skip_samples_zero_ms_is_zero_samples() {
        assert_eq!(skip_samples(0, 48_000, 2000), 0);
    }
}
