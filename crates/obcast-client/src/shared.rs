//! The encoder's model of `ServerState`, updated by both the SSE feed and
//! every upload response (the piggyback path is usually the fresher one).
//! Also carries small upload telemetry for the GUI status panel — read via
//! `try_lock`/atomics so the GUI's per-frame poll never blocks on network
//! tasks.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

use obcast_proto::state::ServerState;
use tokio::sync::Mutex;

/// How long a held `ServerState` is trusted before the feedback loop degrades
/// to `ServerState::unknown()` (low rung, protect the live edge) per
/// CLAUDE.md §5. A silently stalled SSE connection or upload-reply feed
/// otherwise pins the encoder on arbitrarily old data forever — there was
/// previously no staleness check at all. Matches the server's own
/// link-down window (`STALE_AFTER` in `obcast-server/src/api.rs`).
const STALE_AFTER: Duration = Duration::from_secs(5);

pub struct SharedState {
    pub server: Mutex<ServerState>,
    /// When `server` was last refreshed by `update()`. `None` until the first
    /// state ever arrives.
    server_updated_at: Mutex<Option<Instant>>,
    last_uploaded_seq: AtomicI64,
    throughput_kbps: AtomicU32,
    /// Last fatal error from the encoder pipeline (ffmpeg exit / stdin write
    /// failure). Set by the controller when it tears the pipeline down, read
    /// by the GUI each frame so a dead "live" session surfaces instead of
    /// silently producing nothing.
    encoder_error: RwLock<Option<String>>,
    /// Latched one-shot: the pipeline died since the GUI last checked, so the
    /// GUI should drop its own `live` state back to idle.
    encoder_failed: AtomicBool,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            server: Mutex::new(ServerState::unknown()),
            server_updated_at: Mutex::new(None),
            last_uploaded_seq: AtomicI64::new(-1),
            throughput_kbps: AtomicU32::new(0),
            encoder_error: RwLock::new(None),
            encoder_failed: AtomicBool::new(false),
        }
    }

    /// Record that the encoder pipeline died and latch the failure flag.
    pub fn set_encoder_error(&self, msg: String) {
        *self.encoder_error.write().unwrap() = Some(msg);
        self.encoder_failed.store(true, Ordering::Relaxed);
    }

    /// Clear both the message and the latch — called when going live afresh.
    pub fn clear_encoder_error(&self) {
        *self.encoder_error.write().unwrap() = None;
        self.encoder_failed.store(false, Ordering::Relaxed);
    }

    pub fn encoder_error(&self) -> Option<String> {
        self.encoder_error.read().unwrap().clone()
    }

    /// Returns true exactly once after a pipeline death, so the GUI can flip
    /// itself out of "live" without repeatedly fighting the operator.
    pub fn take_encoder_failed(&self) -> bool {
        self.encoder_failed.swap(false, Ordering::Relaxed)
    }

    /// Discard stale/out-of-order feedback per the link-plane contract.
    pub async fn update(&self, new_state: ServerState) {
        let mut cur = self.server.lock().await;
        if new_state.rev >= cur.rev {
            *cur = new_state;
            *self.server_updated_at.lock().await = Some(Instant::now());
        }
    }

    /// The held `ServerState`, or `ServerState::unknown()` if it hasn't been
    /// refreshed within `STALE_AFTER` — the scheduler then plays safe (low
    /// rung, protect the live edge) instead of trusting a feed that may have
    /// gone silent, per CLAUDE.md §5 ("the feedback loop degrades safely").
    pub async fn server_state_or_unknown(&self) -> ServerState {
        let fresh = self
            .server_updated_at
            .lock()
            .await
            .is_some_and(|t| t.elapsed() < STALE_AFTER);
        if fresh {
            self.server.lock().await.clone()
        } else {
            ServerState::unknown()
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
