//! Meter ballistics shared by the encoder client's capture meter and the
//! server's playout meter, so both follow the same standards-defined
//! dynamics regardless of who is sampling the audio.
//!
//! Two envelope followers run over the same linear signal, superimposed on
//! one meter:
//!
//! - [`Vu`] — IEC 60268-17 "standard volume indicator" (VU meter): a
//!   symmetric, average-reading ballistic. A suddenly-applied steady tone
//!   must reach 99% of its final deflection in 300 ms, and release uses the
//!   same time constant. This is the slow "loudness" bar.
//! - [`Ppm`] — IEC 60268-10 Type I (DIN) peak programme meter: fast,
//!   asymmetric, peak-catching — a "flying" needle superimposed on the VU
//!   bar. The standard's 5 ms Type I integration time is defined as the
//!   burst duration that reads 2 dB below the steady-state reading, which
//!   fixes the attack time constant; decay is specified as -20 dB in 1.5 s.
//!
//! Both track **linear** rectified amplitude and expose a one-pole envelope;
//! convert to dBFS at read time with [`linear_to_dbfs`]. Feed whole audio
//! blocks through `process` — it steps sample-by-sample internally — so the
//! ballistics are accurate regardless of the callback's block size.

/// dBFS for a linear amplitude, floored to keep the log finite on silence.
pub fn linear_to_dbfs(linear: f32) -> f32 {
    if linear <= 0.00001 {
        -100.0
    } else {
        20.0 * linear.log10()
    }
}

/// IEC 60268-17 standard volume indicator ballistic: symmetric attack and
/// release, 300 ms integration time.
#[derive(Debug, Clone, Copy)]
pub struct Vu {
    env: f32,
}

impl Vu {
    /// A one-pole step response `1 - e^(-t/tau)` reaches 99% of its final
    /// value at `t = tau * ln(100)`; solving for `tau` at `t = 300 ms`
    /// gives the standard's integration time constant.
    const TAU_SECS: f32 = 0.065_144; // 0.3 / ln(100)

    pub fn new() -> Self {
        Self { env: 0.0 }
    }

    /// Runs one audio block through the ballistic, sample by sample, and
    /// returns the resulting envelope (linear).
    pub fn process(&mut self, samples: &[f32], sample_rate: u32) -> f32 {
        let dt = 1.0 / sample_rate.max(1) as f32;
        let alpha = 1.0 - (-dt / Self::TAU_SECS).exp();
        for &s in samples {
            self.env += (s.abs() - self.env) * alpha;
        }
        self.env
    }

    pub fn value_linear(&self) -> f32 {
        self.env
    }

    pub fn value_db(&self) -> f32 {
        linear_to_dbfs(self.env)
    }
}

impl Default for Vu {
    fn default() -> Self {
        Self::new()
    }
}

/// IEC 60268-10 Type I (DIN) peak programme meter ballistic: fast attack,
/// slow decay — the "flying PPM" needle.
#[derive(Debug, Clone, Copy)]
pub struct Ppm {
    env: f32,
}

impl Ppm {
    /// Attack time constant derived from the standard's Type I integration
    /// time definition: a 5 ms tone burst must read 2 dB below the
    /// steady-state reading. For a one-pole step response, the level
    /// reached at `t = T` is `1 - e^(-T/tau)`; solving
    /// `20*log10(1 - e^(-T/tau)) = -2 dB` at `T = 5 ms` gives `tau ≈ 3.16 ms`.
    const ATTACK_TAU_SECS: f32 = 0.003_16;
    /// Decay time constant: the standard calls for a fall of 20 dB in
    /// 1.5 s, which a one-pole decay of `tau ≈ 650 ms` satisfies
    /// (`20*log10(e^(-1.5/0.65)) ≈ -20 dB`).
    const DECAY_TAU_SECS: f32 = 0.650;

    pub fn new() -> Self {
        Self { env: 0.0 }
    }

    /// Runs one audio block through the ballistic, sample by sample, and
    /// returns the resulting envelope (linear). Attack and decay use
    /// different time constants, so unlike [`Vu::process`] the per-sample
    /// step can't be reduced to one shared coefficient.
    pub fn process(&mut self, samples: &[f32], sample_rate: u32) -> f32 {
        let dt = 1.0 / sample_rate.max(1) as f32;
        let alpha_attack = 1.0 - (-dt / Self::ATTACK_TAU_SECS).exp();
        let alpha_decay = 1.0 - (-dt / Self::DECAY_TAU_SECS).exp();
        for &s in samples {
            let target = s.abs();
            let alpha = if target > self.env {
                alpha_attack
            } else {
                alpha_decay
            };
            self.env += (target - self.env) * alpha;
        }
        self.env
    }

    pub fn value_linear(&self) -> f32 {
        self.env
    }

    pub fn value_db(&self) -> f32 {
        linear_to_dbfs(self.env)
    }
}

impl Default for Ppm {
    fn default() -> Self {
        Self::new()
    }
}

/// True digital sample peak, with a "flying" hold/decay so it reads like a
/// meter needle rather than jumping straight back to the signal on every
/// sample. Unlike [`Ppm`], which is deliberately *not* instantaneous (its
/// 5 ms integration time is part of the IEC standard), this catches the
/// exact dBFS ceiling with zero attack — the alternate reading operators can
/// switch a meter's flying peak marker to when they want the raw digital
/// peak instead of the broadcast-standard PPM. Decays on the same -20 dB in
/// 1.5 s ballistic as `Ppm` so swapping between the two only changes what
/// the needle catches, not how it falls.
#[derive(Debug, Clone, Copy)]
pub struct Peak {
    env: f32,
}

impl Peak {
    const DECAY_TAU_SECS: f32 = 0.650;

    pub fn new() -> Self {
        Self { env: 0.0 }
    }

    /// Runs one audio block through the ballistic, sample by sample, and
    /// returns the resulting envelope (linear).
    pub fn process(&mut self, samples: &[f32], sample_rate: u32) -> f32 {
        let dt = 1.0 / sample_rate.max(1) as f32;
        let alpha_decay = 1.0 - (-dt / Self::DECAY_TAU_SECS).exp();
        for &s in samples {
            let target = s.abs();
            if target >= self.env {
                self.env = target;
            } else {
                self.env += (target - self.env) * alpha_decay;
            }
        }
        self.env
    }

    pub fn value_linear(&self) -> f32 {
        self.env
    }

    pub fn value_db(&self) -> f32 {
        linear_to_dbfs(self.env)
    }
}

impl Default for Peak {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SR: u32 = 48_000;

    fn full_scale(n: usize) -> Vec<f32> {
        vec![1.0; n]
    }

    fn silence(n: usize) -> Vec<f32> {
        vec![0.0; n]
    }

    #[test]
    fn vu_reaches_99_percent_in_300ms() {
        let mut vu = Vu::new();
        vu.process(&full_scale((SR as f32 * 0.3) as usize), SR);
        assert!(vu.value_linear() >= 0.99, "env={}", vu.value_linear());
        assert!(vu.value_linear() < 0.995, "env={}", vu.value_linear());
    }

    #[test]
    fn vu_release_is_symmetric_with_attack() {
        let mut vu = Vu::new();
        // Settle near full scale first (well beyond the integration time).
        vu.process(&full_scale(SR as usize), SR);
        assert!(vu.value_linear() > 0.999);
        // Releasing for the same 300 ms should fall back to ~1%.
        vu.process(&silence((SR as f32 * 0.3) as usize), SR);
        assert!(vu.value_linear() <= 0.012, "env={}", vu.value_linear());
    }

    #[test]
    fn ppm_5ms_burst_reads_about_2db_down() {
        let mut ppm = Ppm::new();
        ppm.process(&full_scale((SR as f32 * 0.005) as usize), SR);
        let db = ppm.value_db();
        assert!((db - (-2.0)).abs() < 0.3, "db={db}");
    }

    #[test]
    fn ppm_decays_about_20db_in_1_5s() {
        let mut ppm = Ppm::new();
        // Drive to full scale first (many attack time constants).
        ppm.process(&full_scale(SR as usize / 10), SR);
        assert!(ppm.value_db() > -0.5, "db={}", ppm.value_db());
        // Then silence for 1.5 s.
        ppm.process(&silence((SR as f32 * 1.5) as usize), SR);
        assert!(
            (ppm.value_db() - (-20.0)).abs() < 1.0,
            "db={}",
            ppm.value_db()
        );
    }

    #[test]
    fn ppm_attacks_faster_than_vu() {
        // Over a short burst well under the VU integration time, the PPM
        // ballistic must read closer to true peak than the VU ballistic —
        // that's the entire point of superimposing a "flying" peak needle
        // on the slower loudness bar.
        let mut vu = Vu::new();
        let mut ppm = Ppm::new();
        let burst = full_scale((SR as f32 * 0.005) as usize);
        vu.process(&burst, SR);
        ppm.process(&burst, SR);
        assert!(ppm.value_linear() > vu.value_linear());
    }

    #[test]
    fn peak_catches_a_single_sample_instantly() {
        // Zero attack: even a single full-scale sample must be caught, unlike
        // Ppm's 5 ms integration time which would still be reading well
        // below full scale after just one sample.
        let mut peak = Peak::new();
        peak.process(&[1.0], SR);
        assert!(peak.value_linear() > 0.999, "env={}", peak.value_linear());
    }

    #[test]
    fn peak_decays_about_20db_in_1_5s() {
        let mut peak = Peak::new();
        peak.process(&full_scale(SR as usize / 10), SR);
        assert!(peak.value_db() > -0.5, "db={}", peak.value_db());
        peak.process(&silence((SR as f32 * 1.5) as usize), SR);
        assert!(
            (peak.value_db() - (-20.0)).abs() < 1.0,
            "db={}",
            peak.value_db()
        );
    }

    #[test]
    fn peak_catches_higher_than_ppm_on_a_short_burst() {
        // Peak's zero attack must read the true burst level; Ppm's 5 ms
        // integration time reads ~2 dB below it by definition.
        let mut peak = Peak::new();
        let mut ppm = Ppm::new();
        let burst = full_scale((SR as f32 * 0.005) as usize);
        peak.process(&burst, SR);
        ppm.process(&burst, SR);
        assert!(peak.value_linear() > ppm.value_linear());
    }
}
