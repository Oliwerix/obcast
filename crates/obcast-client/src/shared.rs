//! The encoder's model of `ServerState`, updated by both the SSE feed and
//! every upload response (the piggyback path is usually the fresher one).

use obcast_proto::state::ServerState;
use tokio::sync::Mutex;

pub struct SharedState {
    pub server: Mutex<ServerState>,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            server: Mutex::new(ServerState::unknown()),
        }
    }

    /// Discard stale/out-of-order feedback per the link-plane contract.
    pub async fn update(&self, new_state: ServerState) {
        let mut cur = self.server.lock().await;
        if new_state.rev >= cur.rev {
            *cur = new_state;
        }
    }
}
