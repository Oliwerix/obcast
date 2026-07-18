//! Server-side config file. Everything else here is still env-var driven
//! (see `docs/getting-started.md`) — this currently covers only the
//! hardware playout audio subsystem/device, since that's the one setting an
//! operator needs to pin per-machine rather than per-run.

use serde::Deserialize;

/// Which cpal host backend (audio subsystem) and device to open for
/// hardware playout. Both empty = cpal's platform default host and its
/// default output device.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct AudioConfig {
    /// e.g. "ALSA", "JACK", "PulseAudio", "WASAPI", "CoreAudio".
    pub host: String,
    pub device: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    #[serde(default)]
    pub audio: AudioConfig,
}

impl ServerConfig {
    /// Reads `OBCAST_CONFIG_FILE` (default `obcast-server.toml` in the
    /// working directory); a missing file is not an error, it just means
    /// defaults for everything in it.
    pub fn load() -> Self {
        let path = std::env::var("OBCAST_CONFIG_FILE")
            .unwrap_or_else(|_| "obcast-server.toml".to_string());
        let Ok(text) = std::fs::read_to_string(&path) else {
            return Self::default();
        };
        toml::from_str(&text).unwrap_or_else(|err| {
            tracing::warn!(error = %err, %path, "failed to parse server config, using defaults");
            Self::default()
        })
    }
}
