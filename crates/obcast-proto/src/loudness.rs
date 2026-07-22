//! ITU-R BS.1770-4 K-weighted loudness measurement (the algorithm behind
//! EBU R128 LUFS): momentary (400 ms), short-term (3 s) and integrated
//! (gated, whole-programme) readings, all in LUFS.
//!
//! Unlike [`crate::meter::Vu`]/[`crate::meter::Ppm`]/[`crate::meter::Peak`],
//! which are genuinely independent per-channel ballistics, loudness is a
//! single programme-wide reading across all channels — [`Loudness::process`]
//! takes one slice per channel (e.g. `[left, right]`) and combines them per
//! the standard's channel weighting (1.0 for every channel this app ever
//! feeds it: mono-duplicated or true stereo, never a surround/LFE layout).
//!
//! Pipeline per BS.1770 Annex 1:
//! 1. K-weight each channel (a high-shelf "pre-filter" cascaded with the
//!    RLB high-pass), sample by sample.
//! 2. Accumulate mean square per channel over successive 100 ms partitions.
//! 3. Momentary = the last 400 ms (4 partitions) of mean square, ungated.
//!    Short-term = the last 3 s (30 partitions), ungated.
//! 4. Integrated = a two-pass *gated* average over 400 ms blocks spanning
//!    the whole programme (75% overlap, i.e. one new block per partition):
//!    an absolute gate at -70 LUFS, then a relative gate 10 LU below the
//!    resulting mean. See [`LoudnessHistogram`] for how this stays
//!    constant-memory across an arbitrarily long OB session.

use std::collections::VecDeque;

/// Reported LUFS floor for silence / not-yet-measurable, matching
/// `meter::linear_to_dbfs`'s -100 dBFS silence convention.
const SILENCE_LUFS: f32 = -100.0;

const PARTITION_MS: u64 = 100;
const MOMENTARY_PARTITIONS: usize = 4; // 400 ms / 100 ms
const SHORT_TERM_PARTITIONS: usize = 30; // 3 s / 100 ms

const ABSOLUTE_GATE_LUFS: f64 = -70.0;
const RELATIVE_GATE_LU: f64 = -10.0;

fn mean_square_to_lufs(mean_square: f64) -> f64 {
    if mean_square <= 0.0 {
        f64::NEG_INFINITY
    } else {
        -0.691 + 10.0 * mean_square.log10()
    }
}

fn floor_lufs(v: f64) -> f32 {
    if !v.is_finite() || v < SILENCE_LUFS as f64 {
        SILENCE_LUFS
    } else {
        v as f32
    }
}

/// Direct Form I biquad, `f64` throughout — loudness gating is sensitive to
/// small errors compounding over a long-running broadcast, so this doesn't
/// cut corners with `f32` the way the ballistic meters do.
#[derive(Debug, Clone, Copy)]
struct Biquad {
    b0: f64,
    b1: f64,
    b2: f64,
    a1: f64,
    a2: f64,
    x1: f64,
    x2: f64,
    y1: f64,
    y2: f64,
}

impl Biquad {
    fn new(b0: f64, b1: f64, b2: f64, a1: f64, a2: f64) -> Self {
        Self {
            b0,
            b1,
            b2,
            a1,
            a2,
            x1: 0.0,
            x2: 0.0,
            y1: 0.0,
            y2: 0.0,
        }
    }

    #[inline]
    fn process(&mut self, x0: f64) -> f64 {
        let y0 = self.b0 * x0 + self.b1 * self.x1 + self.b2 * self.x2
            - self.a1 * self.y1
            - self.a2 * self.y2;
        self.x2 = self.x1;
        self.x1 = x0;
        self.y2 = self.y1;
        self.y1 = y0;
        y0
    }
}

/// BS.1770 Annex 1 publishes fixed biquad coefficients for the K-weighting
/// filter, but only at 48 kHz. Both stages here are instead re-derived from
/// their analog prototype (shelf / high-pass, given as `f0`/`Q`/gain) via
/// the standard bilinear transform at *this stream's* sample rate — the
/// same approach reference implementations (e.g. libebur128) use — so
/// K-weighting stays correct at 44.1 kHz and any other rate this app's ABR
/// ladder might run.
fn design_high_shelf(sample_rate: f64) -> Biquad {
    // Pre-filter: a high shelf, +4 dB (approx) above ~1.7 kHz.
    let f0 = 1681.974450955533;
    let g_db = 3.999843853973347;
    let q = 0.7071752369554196;

    let k = (std::f64::consts::PI * f0 / sample_rate).tan();
    let vh = 10f64.powf(g_db / 20.0);
    let vb = vh.powf(0.4996667741545416);
    let a0 = 1.0 + k / q + k * k;

    Biquad::new(
        (vh + vb * k / q + k * k) / a0,
        2.0 * (k * k - vh) / a0,
        (vh - vb * k / q + k * k) / a0,
        2.0 * (k * k - 1.0) / a0,
        (1.0 - k / q + k * k) / a0,
    )
}

fn design_rlb_highpass(sample_rate: f64) -> Biquad {
    // RLB weighting filter: high-pass, ~-3 dB around 38 Hz.
    let f0 = 38.13547087602444;
    let q = 0.5003270373238773;

    let k = (std::f64::consts::PI * f0 / sample_rate).tan();
    let a0 = 1.0 + k / q + k * k;

    Biquad::new(
        1.0 / a0,
        -2.0 / a0,
        1.0 / a0,
        2.0 * (k * k - 1.0) / a0,
        (1.0 - k / q + k * k) / a0,
    )
}

/// The full "K" weighting curve for one channel: pre-filter cascaded with
/// the RLB high-pass.
#[derive(Debug, Clone)]
struct KWeightingFilter {
    shelf: Biquad,
    highpass: Biquad,
}

impl KWeightingFilter {
    fn new(sample_rate: u32) -> Self {
        let sr = (sample_rate.max(1)) as f64;
        Self {
            shelf: design_high_shelf(sr),
            highpass: design_rlb_highpass(sr),
        }
    }

    #[inline]
    fn process_sample(&mut self, x: f32) -> f64 {
        self.highpass.process(self.shelf.process(x as f64))
    }
}

/// Bounded-memory histogram of gated 400 ms block loudness, used for the
/// two-pass BS.1770 integrated measurement. A live OB session can run for
/// hours or days, so this deliberately doesn't keep a growing `Vec` of every
/// block ever measured — instead it buckets blocks by loudness at the same
/// 0.1 LU resolution EBU R128 itself requires for logged values, and keeps
/// per-bucket count + summed mean-square (not just a representative value),
/// so the gated average is still computed exactly, only quantized in *which*
/// blocks a given 0.1 LU step groups together.
struct LoudnessHistogram {
    counts: Vec<u64>,
    sums: Vec<f64>,
}

impl LoudnessHistogram {
    const MIN_LUFS: f64 = ABSOLUTE_GATE_LUFS;
    const STEP_LU: f64 = 0.1;
    const BUCKETS: usize = 750; // (5.0 - MIN_LUFS) / STEP_LU, i.e. -70..5 LUFS

    fn new() -> Self {
        Self {
            counts: vec![0; Self::BUCKETS],
            sums: vec![0.0; Self::BUCKETS],
        }
    }

    fn bucket_index(loudness_lufs: f64) -> Option<usize> {
        if loudness_lufs < Self::MIN_LUFS {
            return None;
        }
        let idx = ((loudness_lufs - Self::MIN_LUFS) / Self::STEP_LU) as usize;
        Some(idx.min(Self::BUCKETS - 1))
    }

    /// Adds one 400 ms block's combined (channel-summed) mean square.
    /// Blocks below the absolute gate (-70 LUFS) are dropped here, at the
    /// source, rather than stored and filtered later — they can never
    /// contribute to an integrated reading, gated or not.
    fn add_block(&mut self, mean_square: f64) {
        let loudness = mean_square_to_lufs(mean_square);
        if let Some(idx) = Self::bucket_index(loudness) {
            self.counts[idx] += 1;
            self.sums[idx] += mean_square;
        }
    }

    /// Two-pass BS.1770 gated integration. `None` if no block has ever
    /// cleared the absolute gate — the standard's own "unmeasurable" case
    /// for a silent or not-yet-started programme.
    fn integrated_lufs(&self) -> Option<f64> {
        let total_count: u64 = self.counts.iter().sum();
        if total_count == 0 {
            return None;
        }
        let total_sum: f64 = self.sums.iter().sum();
        let pass1_mean = total_sum / total_count as f64;
        let gamma_r = mean_square_to_lufs(pass1_mean) + RELATIVE_GATE_LU;

        let mut count2 = 0u64;
        let mut sum2 = 0.0f64;
        for (i, &count) in self.counts.iter().enumerate() {
            if count == 0 {
                continue;
            }
            // Bucket lower edge — a consistent, conservative comparison at
            // the histogram's own 0.1 LU resolution.
            let bucket_lufs = Self::MIN_LUFS + i as f64 * Self::STEP_LU;
            if bucket_lufs >= gamma_r {
                count2 += count;
                sum2 += self.sums[i];
            }
        }

        if count2 == 0 {
            Some(mean_square_to_lufs(pass1_mean))
        } else {
            Some(mean_square_to_lufs(sum2 / count2 as f64))
        }
    }
}

/// Momentary / short-term / integrated LUFS over one programme (e.g. one
/// live OB session). Feed it the same post-gain, linear PCM the VU/PPM
/// ballistics see — see `process`.
pub struct Loudness {
    filters: Vec<KWeightingFilter>,
    partition_len: usize,
    partition_acc: Vec<f64>,
    partition_pos: usize,
    /// Completed partitions' combined (channel-summed) mean square, oldest
    /// first, capped at `SHORT_TERM_PARTITIONS` — this is all momentary and
    /// short-term ever need to look back over.
    partitions: VecDeque<f64>,
    histogram: LoudnessHistogram,
}

impl Loudness {
    pub fn new(channels: usize, sample_rate: u32) -> Self {
        let channels = channels.max(1);
        let sr = sample_rate.max(1);
        Self {
            filters: (0..channels).map(|_| KWeightingFilter::new(sr)).collect(),
            partition_len: ((sr as u64 * PARTITION_MS) / 1000).max(1) as usize,
            partition_acc: vec![0.0; channels],
            partition_pos: 0,
            partitions: VecDeque::with_capacity(SHORT_TERM_PARTITIONS),
            histogram: LoudnessHistogram::new(),
        }
    }

    /// Feeds one block of post-gain, linear PCM: one slice per channel (e.g.
    /// `[left, right]`), all the same length, at the sample rate passed to
    /// `new`. Steps the K-weighting filters and the 100 ms partition
    /// accumulator sample by sample, same pattern as `Vu`/`Ppm::process`.
    /// A channel-count mismatch against `new` is a no-op rather than a
    /// panic — the caller (the audio thread) must never be made to fault on
    /// a metering bug.
    pub fn process(&mut self, channel_samples: &[&[f32]]) {
        if channel_samples.len() != self.filters.len() {
            return;
        }
        let Some(frames) = channel_samples.first().map(|s| s.len()) else {
            return;
        };
        if channel_samples.iter().any(|s| s.len() != frames) {
            return;
        }

        // Indexed rather than iterator-chained: `f` walks all `channels`
        // slices in lockstep (frame-major), which isn't a single-slice
        // pattern clippy's `needless_range_loop` rewrite handles.
        #[allow(clippy::needless_range_loop)]
        for f in 0..frames {
            for (ch, filt) in self.filters.iter_mut().enumerate() {
                let y = filt.process_sample(channel_samples[ch][f]);
                self.partition_acc[ch] += y * y;
            }
            self.partition_pos += 1;
            if self.partition_pos >= self.partition_len {
                self.finish_partition();
            }
        }
    }

    fn finish_partition(&mut self) {
        let n = self.partition_pos.max(1) as f64;
        // BS.1770 channel weighting G_i = 1.0 for every channel this app
        // ever feeds through (mono-duplicated or true L/R stereo) — no
        // surround/LFE weighting applies.
        let combined_ms: f64 = self.partition_acc.iter().map(|&sum_sq| sum_sq / n).sum();
        for acc in &mut self.partition_acc {
            *acc = 0.0;
        }
        self.partition_pos = 0;

        if self.partitions.len() == SHORT_TERM_PARTITIONS {
            self.partitions.pop_front();
        }
        self.partitions.push_back(combined_ms);

        if self.partitions.len() >= MOMENTARY_PARTITIONS {
            let block_ms = self.window_mean_square(MOMENTARY_PARTITIONS);
            self.histogram.add_block(block_ms);
        }
    }

    /// Mean of the last `n` completed partitions' combined mean square
    /// (equivalent to the true mean square over that window, since
    /// partitions are equal-length).
    fn window_mean_square(&self, n: usize) -> f64 {
        let n = n.min(self.partitions.len()).max(1);
        self.partitions.iter().rev().take(n).sum::<f64>() / n as f64
    }

    /// Momentary loudness: last 400 ms, ungated. Floored at -100.0 LUFS
    /// until at least 400 ms of audio has been processed.
    pub fn momentary_lufs(&self) -> f32 {
        if self.partitions.len() < MOMENTARY_PARTITIONS {
            return SILENCE_LUFS;
        }
        floor_lufs(mean_square_to_lufs(
            self.window_mean_square(MOMENTARY_PARTITIONS),
        ))
    }

    /// Short-term loudness: last 3 s (or however much audio has been
    /// processed so far, if less), ungated.
    pub fn short_term_lufs(&self) -> f32 {
        if self.partitions.is_empty() {
            return SILENCE_LUFS;
        }
        floor_lufs(mean_square_to_lufs(
            self.window_mean_square(SHORT_TERM_PARTITIONS),
        ))
    }

    /// Integrated (gated, whole-programme-so-far) loudness. See
    /// `LoudnessHistogram` for the gating and memory-bound details.
    pub fn integrated_lufs(&self) -> f32 {
        self.histogram
            .integrated_lufs()
            .map(floor_lufs)
            .unwrap_or(SILENCE_LUFS)
    }

    /// Resets only the integrated measurement's gated history — momentary/
    /// short-term windows and the K-weighting filters' internal state are
    /// untouched. Mirrors a hardware LUFS meter's "reset integrated" action
    /// at the start of a new programme/segment, without needing to reopen
    /// the audio device.
    pub fn reset_integrated(&mut self) {
        self.histogram = LoudnessHistogram::new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SR: u32 = 48_000;

    fn sine(freq: f64, amp: f32, seconds: f64, sr: u32) -> Vec<f32> {
        let n = (sr as f64 * seconds) as usize;
        (0..n)
            .map(|i| {
                let t = i as f64 / sr as f64;
                (amp as f64 * (2.0 * std::f64::consts::PI * freq * t).sin()) as f32
            })
            .collect()
    }

    fn silence(seconds: f64, sr: u32) -> Vec<f32> {
        vec![0.0; (sr as f64 * seconds) as usize]
    }

    #[test]
    fn silence_reads_floor_before_any_data() {
        let m = Loudness::new(1, SR);
        assert_eq!(m.momentary_lufs(), SILENCE_LUFS);
        assert_eq!(m.short_term_lufs(), SILENCE_LUFS);
        assert_eq!(m.integrated_lufs(), SILENCE_LUFS);
    }

    #[test]
    fn doubling_amplitude_reads_about_6db_louder_momentary() {
        // A differential test rather than an absolute-calibration one: the
        // K-weighting filter's exact gain at 1 kHz doesn't need to be known
        // precisely, since it cancels out between the two identical-
        // frequency measurements. Doubling amplitude doubles mean square in
        // dB terms (+6.02 dB), which the loudness formula's `10*log10`
        // must reproduce exactly.
        let quiet = sine(1000.0, 0.25, 1.0, SR);
        let loud = sine(1000.0, 0.5, 1.0, SR);

        let mut m_quiet = Loudness::new(1, SR);
        m_quiet.process(&[&quiet]);
        let mut m_loud = Loudness::new(1, SR);
        m_loud.process(&[&loud]);

        let diff = m_loud.momentary_lufs() - m_quiet.momentary_lufs();
        assert!((diff - 6.02).abs() < 0.3, "diff={diff}");
    }

    #[test]
    fn short_term_averages_over_3s_while_momentary_tracks_400ms() {
        let mut m = Loudness::new(1, SR);
        // 2 s of a loud tone, then 0.6 s of true silence: the silence
        // dominates the last 400 ms (momentary), but the last 3 s (short-
        // term) is still mostly the loud tone.
        m.process(&[&sine(1000.0, 0.8, 2.0, SR)]);
        m.process(&[&silence(0.6, SR)]);

        let momentary = m.momentary_lufs();
        let short_term = m.short_term_lufs();
        assert_eq!(momentary, SILENCE_LUFS, "momentary={momentary}");
        assert!(
            short_term > momentary + 20.0,
            "momentary={momentary} short_term={short_term}"
        );
    }

    #[test]
    fn integrated_gates_out_near_silence() {
        let mut m = Loudness::new(1, SR);
        m.process(&[&sine(1000.0, 0.5, 2.0, SR)]);
        let before = m.integrated_lufs();

        // True digital silence is far below the -70 LUFS absolute gate, so
        // none of these blocks should ever enter the histogram.
        m.process(&[&silence(5.0, SR)]);
        let after = m.integrated_lufs();

        assert!(
            (after - before).abs() < 1.0,
            "gating should exclude near-silent blocks: before={before} after={after}"
        );
    }

    #[test]
    fn integrated_pulls_toward_a_sustained_level_change() {
        let mut m = Loudness::new(1, SR);
        m.process(&[&sine(1000.0, 0.25, 3.0, SR)]);
        let quiet_integrated = m.integrated_lufs();

        // A long louder passage should pull the gated integrated average up
        // toward it, not leave it pinned at the quiet passage's level.
        m.process(&[&sine(1000.0, 0.9, 10.0, SR)]);
        let after_loud = m.integrated_lufs();

        assert!(
            after_loud > quiet_integrated + 3.0,
            "quiet={quiet_integrated} after_loud={after_loud}"
        );
    }

    #[test]
    fn reset_integrated_clears_gated_history_but_not_windows() {
        let mut m = Loudness::new(1, SR);
        m.process(&[&sine(1000.0, 0.7, 2.0, SR)]);
        assert!(m.integrated_lufs() > SILENCE_LUFS);

        m.reset_integrated();
        assert_eq!(m.integrated_lufs(), SILENCE_LUFS);
        // Short-term/momentary still reflect the audio already processed —
        // only the gated integrated history was reset.
        assert!(m.short_term_lufs() > SILENCE_LUFS);
    }

    #[test]
    fn channel_count_mismatch_is_a_no_op_not_a_panic() {
        let mut m = Loudness::new(2, SR);
        let one_channel = sine(1000.0, 0.5, 1.0, SR);
        m.process(&[&one_channel]); // wrong arity vs. `new(2, ..)`
        assert_eq!(m.momentary_lufs(), SILENCE_LUFS);
    }
}
