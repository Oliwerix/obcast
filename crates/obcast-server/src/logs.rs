//! Per-stream ring buffer + live broadcast for server-side status/error
//! messages, surfaced to the web remote via `ControlStatus.recent_log` (REST
//! poll / initial WS snapshot) and `ControlEvent::Log` (live push). Additive
//! to `tracing` — call sites keep their existing `tracing::warn!`/`error!`
//! and also call `LogSink::push` for the human-facing feed; see CLAUDE.md §9.
//!
//! Uses a plain `std::sync::Mutex`, not the async `tokio::sync::Mutex` most
//! of this crate uses elsewhere: the playout engine's dedicated OS thread
//! (`playout.rs`) needs to push log entries without going through
//! `rt.block_on` at every one of its several call sites, and the critical
//! section here is O(capacity) at worst and never held across an `.await`,
//! so a std mutex is safe (and cheap) from the async call sites too.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use obcast_proto::control::{LogEntry, LogLevel};
use tokio::sync::broadcast;

/// Cap on the retained backlog per stream — bounds both server memory and
/// what a status/WS client needs to render. The web remote mirrors this to
/// cap its own DOM list length.
pub const CAPACITY: usize = 200;

pub struct LogSink {
    backlog: Mutex<VecDeque<LogEntry>>,
    tx: broadcast::Sender<LogEntry>,
}

impl LogSink {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(64);
        Self {
            backlog: Mutex::new(VecDeque::with_capacity(CAPACITY)),
            tx,
        }
    }

    /// Records a log entry in the backlog and broadcasts it live. Ignored if
    /// there are no current subscribers — same fire-and-forget pattern as
    /// `StreamHandle::tx`'s `ServerState` broadcast.
    pub fn push(&self, level: LogLevel, message: impl Into<String>) {
        let at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let entry = LogEntry {
            at_ms,
            level,
            message: message.into(),
        };
        {
            let mut backlog = self.backlog.lock().unwrap();
            if backlog.len() >= CAPACITY {
                backlog.pop_front();
            }
            backlog.push_back(entry.clone());
        }
        let _ = self.tx.send(entry);
    }

    /// Oldest-first snapshot of the retained backlog.
    pub fn recent(&self) -> Vec<LogEntry> {
        self.backlog.lock().unwrap().iter().cloned().collect()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<LogEntry> {
        self.tx.subscribe()
    }
}

impl Default for LogSink {
    fn default() -> Self {
        Self::new()
    }
}
