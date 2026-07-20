//! Control plane types used by the web remote to drive the server's playout and
//! to read overall stream health.

use serde::{Deserialize, Serialize};

use crate::state::{EncoderState, RungId, Seq, ServerState};

/// Where playout should start / seek to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum PlayoutPosition {
    /// The live edge (newest available segment).
    Live,
    /// A specific segment.
    Seq(Seq),
    /// N seconds behind the live edge (DVR time-shift).
    SecondsBehindLive(u32),
}

/// Commands the web remote can issue to the server's playout engine.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "cmd")]
pub enum PlayoutCommand {
    /// Start hardware output at `position` (the "start on demand" action).
    Start {
        position: PlayoutPosition,
    },
    Stop,
    Pause,
    Resume,
    /// Jump to another point within the DVR window.
    Seek {
        position: PlayoutPosition,
    },
    /// Snap forward to the live edge.
    GoLive,
    SetDevice {
        device_id: String,
    },
    /// Linear gain.
    SetVolume {
        gain: f32,
    },
    /// Toggle a 1kHz sine test tone on the hardware output, for checking
    /// wiring/routing independent of the encoder link: 2s both channels,
    /// 0.5s silence, 0.5s left, 0.5s silence, 0.5s right, 0.5s silence,
    /// looping. Overrides normal playout audio while enabled; does not
    /// change `start`/`stop`/`seek` state.
    SetTestTone {
        enabled: bool,
    },
}

/// A selectable hardware audio output on the server.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioDevice {
    pub id: String,
    pub name: String,
    pub is_default: bool,
}

/// Link-level health summary for operator dashboards.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkHealth {
    pub connected: bool,
    /// Age of the newest received segment.
    pub last_segment_age_ms: u32,
    /// The rung actually reaching the playout head right now (looked up at
    /// `ServerState.playout.position_seq` in `ServerState.coverage`), i.e.
    /// on-air ground truth for the web remote's "Current rung" readout —
    /// mirrors what the client GUI's own on-air quality estimate shows (see
    /// `SharedState::playing_quality`). Deliberately *not* the newest/live
    /// segment's rung: the playout head lags the live edge by design (DVR),
    /// so during a bandwidth dip that recovers, the newest segment can be HD
    /// while the head is still seconds behind, playing out low-rung audio
    /// recorded during the dip — showing the live-edge rung here reads as a
    /// straight quality lie to whoever is watching this to know what's on
    /// air right now. `None` while stopped or if the head's seq has fallen
    /// out of the bounded `coverage` window.
    pub current_rung: Option<RungId>,
    pub throughput_kbps: u32,
    /// Count of permanent gaps in the DVR window.
    pub gaps: u32,
}

/// Severity of a logged status/error message. Mirrors `tracing`'s levels
/// (only the three an operator actually needs to triage from a UI).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Info,
    Warn,
    Error,
}

/// One human-readable status/error line, timestamped for display. Used both
/// by the server (surfaced to the web remote over `recent_log` / the WS
/// `log` event) and by the encoder client (surfaced in its own GUI) — a
/// shared shape so both UIs render the same way, even though a client log
/// never crosses the wire.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Wall-clock, milliseconds since the Unix epoch.
    pub at_ms: u64,
    pub level: LogLevel,
    pub message: String,
}

/// Response to `GET /api/status`: everything a web client needs to render the
/// player, the scrub bar, and the health panel.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ControlStatus {
    pub stream: String,
    pub server: ServerState,
    /// Last known encoder telemetry (may be stale if the link is down).
    pub encoder: Option<EncoderState>,
    pub link: LinkHealth,
    /// Backlog of recent status/error log lines, oldest first, capped to a
    /// fixed window — lets a freshly-opened web remote show history instead
    /// of waiting for the next live `log` event.
    #[serde(default)]
    pub recent_log: Vec<LogEntry>,
}

/// Envelope pushed over the control WebSocket (`/api/ws`) for live updates.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ControlEvent {
    /// Full status snapshot (sent on connect and on significant change).
    Status(Box<ControlStatus>),
    /// Playout head advanced (frequent, lightweight).
    Position { seq: Seq },
    /// Per-channel VU/PPM/Peak meters for the playout bus, dBFS — see
    /// `obcast_proto::meter` for the IEC 60268-17 (VU), IEC 60268-10 Type I
    /// (PPM), and true digital sample `Peak` ballistics these are computed
    /// with. `peak_db_{l,r}` is the alternate flying-peak reading a client
    /// can show instead of `ppm_db_{l,r}`.
    Meters {
        vu_db_l: f32,
        vu_db_r: f32,
        ppm_db_l: f32,
        ppm_db_r: f32,
        #[serde(default)]
        peak_db_l: f32,
        #[serde(default)]
        peak_db_r: f32,
    },
    /// A command was accepted/rejected.
    Ack {
        command: String,
        ok: bool,
        detail: Option<String>,
    },
    /// A new status/error line was logged — append to the client's local
    /// history (already included once in `Status.recent_log`'s backlog).
    Log(LogEntry),
}
