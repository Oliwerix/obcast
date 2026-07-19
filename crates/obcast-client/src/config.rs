//! Operator settings persisted between runs of the GUI: which device/
//! channels/gain were last used, and where to send the stream. Stored as
//! TOML in the OS-appropriate config directory (`directories` gives us
//! that cross-platform: `%APPDATA%` / `~/Library/Application Support` /
//! `~/.config`) so the client remembers a 32-channel snake's L/R picks
//! across restarts instead of making the operator re-find them.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub server: String,
    pub stream: String,
    pub ingest_token: String,
    pub segment_ms: u32,
    pub out_dir: String,

    /// Audio subsystem (cpal host) to open `device_name` from, e.g. "ALSA",
    /// "JACK", "PulseAudio", "PipeWire", "WASAPI", "CoreAudio". Empty =
    /// platform default.
    pub audio_host: String,
    pub device_name: String,
    pub mono: bool,
    pub left_channel: u16,
    pub right_channel: u16,
    pub gain_db: f32,
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
        toml::from_str(&text).unwrap_or_else(|err| {
            tracing::warn!(error = %err, ?path, "failed to parse config, using defaults");
            Self::default()
        })
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
