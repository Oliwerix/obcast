//! Test-only hook to simulate playout without the real M5 hardware playout
//! engine. Not part of `docs/protocol.md` — replace with the real control
//! API (M6) once playout exists; nothing downstream should depend on this
//! path staying stable.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;

use obcast_proto::state::{PlayoutState, Seq, ServerState};

use crate::AppState;

#[derive(Deserialize)]
pub struct SetPlayoutRequest {
    pub state: PlayoutState,
    pub position_seq: Option<Seq>,
}

pub async fn set_playout(
    State(app): State<Arc<AppState>>,
    Path(stream): Path<String>,
    Json(body): Json<SetPlayoutRequest>,
) -> Result<Json<ServerState>, StatusCode> {
    let handle = app.stream(&stream).await;
    {
        let mut playout = handle.playout.lock().await;
        playout.state = body.state;
        playout.position_seq = body.position_seq;
    }
    let state = {
        let store = handle.store.lock().await;
        let playout = handle.playout.lock().await.clone();
        store.build_server_state(playout)
    };
    let _ = handle.tx.send(state.clone());
    Ok(Json(state))
}
