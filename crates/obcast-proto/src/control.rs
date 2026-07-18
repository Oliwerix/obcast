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
    /// Rung the encoder is currently prioritizing, if known.
    pub current_rung: Option<RungId>,
    pub throughput_kbps: u32,
    /// Count of permanent gaps in the DVR window.
    pub gaps: u32,
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
}

/// Envelope pushed over the control WebSocket (`/api/ws`) for live updates.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ControlEvent {
    /// Full status snapshot (sent on connect and on significant change).
    Status(Box<ControlStatus>),
    /// Playout head advanced (frequent, lightweight).
    Position { seq: Seq },
    /// Peak/RMS meters for the playout bus, dBFS.
    Meters { peak_db: f32, rms_db: f32 },
    /// A command was accepted/rejected.
    Ack {
        command: String,
        ok: bool,
        detail: Option<String>,
    },
}
