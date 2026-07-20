//! Operator settings persisted between runs of the GUI: which device/
//! channels/gain were last used, and where to send the stream. Stored as
//! TOML in the OS-appropriate config directory (`directories` gives us
//! that cross-platform: `%APPDATA%` / `~/Library/Application Support` /
//! `~/.config`) so the client remembers a 32-channel snake's L/R picks
//! across restarts instead of making the operator re-find them.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use obcast_proto::state::RungId;

/// Ids of `StreamProfile::default_ladder`'s rungs, in ascending order — used
/// as the persisted default for `AppConfig::enabled_rungs` so a fresh install
/// starts with every rung on.
fn all_rung_ids() -> Vec<RungId> {
    obcast_proto::state::StreamProfile::default_ladder(0)
        .rungs
        .iter()
        .map(|r| r.id)
        .collect()
}

/// Which reading the meter's flying peak marker shows: the broadcast-standard
/// IEC 60268-10 PPM ballistic, or the raw true digital sample peak
/// (`obcast_proto::meter::Peak`). See `gui::meter`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeakMode {
    #[default]
    Ppm,
    DigitalPeak,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub server: String,
    pub stream: String,
    pub ingest_token: String,
    pub segment_ms: u32,
    pub out_dir: String,

    /// Audio subsystem (cpal host) to open `device_name` from, e.g. "ALSA",
    /// "JACK", "PulseAudio", "PipeWire", "WASAPI", "ASIO", "CoreAudio".
    /// Empty = platform default.
    pub audio_host: String,
    pub device_name: String,
    pub mono: bool,
    pub left_channel: u16,
    pub right_channel: u16,
    pub gain_db: f32,
    pub peak_mode: PeakMode,

    /// When enabled, the server starts playout on its own once
    /// `auto_start_buffer_secs` of contiguous buffer has accumulated —
    /// instead of waiting for a web operator to press Start. Irrelevant
    /// once playout has been started by any means. See
    /// `obcast_proto::state::EncoderState::auto_start_buffer_ms`.
    pub auto_start: bool,
    pub auto_start_buffer_secs: u32,

    /// Which rungs of `StreamProfile::default_ladder` this session encodes
    /// and uploads. Any rung, including the lowest, may be disabled — the
    /// scheduler derives its "survival" rung from whatever's actually left
    /// in the filtered profile (see `obcast_proto::state::StreamProfile::filtered`),
    /// not a hardcoded id. Must never be empty in practice; `normalize()`
    /// repairs a stale/hand-edited config that violates that.
    pub enabled_rungs: Vec<RungId>,
    /// The rung the uploader assumes for its bootstrap bandwidth guess
    /// before real throughput/`ServerState` feedback arrives (a few hundred
    /// ms at most) — purely a starting point, not a cap; the closed-loop
    /// scheduler takes over immediately once real data is flowing. Resolved
    /// against whatever's actually enabled via
    /// `StreamProfile::nearest_enabled_or_low` if this rung has since been
    /// disabled.
    pub default_rung: RungId,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: "http://127.0.0.1:8080".into(),
            stream: "obshow".into(),
            ingest_token: String::new(),
            segment_ms: 2000,
            out_dir: "./client-buffer".into(),
            audio_host: String::new(),
            device_name: String::new(),
            mono: false,
            left_channel: 0,
            right_channel: 1,
            gain_db: 0.0,
            peak_mode: PeakMode::Ppm,
            auto_start: false,
            auto_start_buffer_secs: 300,
            enabled_rungs: all_rung_ids(),
            default_rung: 0,
        }
    }
}

fn config_path() -> Option<PathBuf> {
    let dirs = directories::ProjectDirs::from("com", "obcast", "obcast-client")?;
    Some(dirs.config_dir().join("config.toml"))
}

impl AppConfig {
    pub fn load() -> Self {
        let Some(path) = config_path() else {
            return Self::default();
        };
        let Ok(text) = std::fs::read_to_string(&path) else {
            return Self::default();
        };
        let mut cfg: Self = toml::from_str(&text).unwrap_or_else(|err| {
            tracing::warn!(error = %err, ?path, "failed to parse config, using defaults");
            Self::default()
        });
        cfg.normalize();
        cfg
    }

    /// Repairs an `enabled_rungs` left empty by a stale or hand-edited
    /// config file — an empty ladder has no cheap continuity option at all,
    /// so this is a hard floor rather than something left to
    /// `StreamProfile::filtered`'s own (looser) empty-set fallback.
    fn normalize(&mut self) {
        if self.enabled_rungs.is_empty() {
            self.enabled_rungs = vec![0];
        }
    }

    pub fn save(&self) {
        let Some(path) = config_path() else { return };
        if let Some(parent) = path.parent() {
            if let Err(err) = std::fs::create_dir_all(parent) {
                tracing::warn!(error = %err, ?parent, "failed to create config dir");
                return;
            }
        }
        match toml::to_string_pretty(self) {
            Ok(text) => {
                if let Err(err) = std::fs::write(&path, text) {
                    tracing::warn!(error = %err, ?path, "failed to write config");
                }
            }
            Err(err) => tracing::warn!(error = %err, "failed to serialize config"),
        }
    }
}
