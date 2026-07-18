//! The encoder's model of `ServerState`, updated by both the SSE feed and
//! every upload response (the piggyback path is usually the fresher one).
//! Also carries small upload telemetry for the GUI status panel — read via
//! `try_lock`/atomics so the GUI's per-frame poll never blocks on network
//! tasks.

use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};

use obcast_proto::state::ServerState;
use tokio::sync::Mutex;

pub struct SharedState {
    pub server: Mutex<ServerState>,
    last_uploaded_seq: AtomicI64,
    throughput_kbps: AtomicU32,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            server: Mutex::new(ServerState::unknown()),
            last_uploaded_seq: AtomicI64::new(-1),
            throughput_kbps: AtomicU32::new(0),
        }
    }

    /// Discard stale/out-of-order feedback per the link-plane contract.
    pub async fn update(&self, new_state: ServerState) {
        let mut cur = self.server.lock().await;
        if new_state.rev >= cur.rev {
            *cur = new_state;
        }
    }

    pub fn note_upload(&self, seq: u64, throughput_kbps: u32) {
        self.last_uploaded_seq.store(seq as i64, Ordering::Relaxed);
        self.throughput_kbps
            .store(throughput_kbps, Ordering::Relaxed);
    }

    pub fn last_uploaded_seq(&self) -> Option<u64> {
        let v = self.last_uploaded_seq.load(Ordering::Relaxed);
        (v >= 0).then_some(v as u64)
    }

    pub fn throughput_kbps(&self) -> u32 {
        self.throughput_kbps.load(Ordering::Relaxed)
    }
}
