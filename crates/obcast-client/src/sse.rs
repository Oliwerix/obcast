//! Link-plane SSE listener. Reconnects on drop so feedback survives upload
//! stalls, per `docs/protocol.md` §3.

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use obcast_proto::control::LogLevel;
use obcast_proto::state::ServerState;

use crate::shared::SharedState;

pub async fn run(
    client: reqwest::Client,
    base_url: String,
    stream: String,
    shared: Arc<SharedState>,
) {
    loop {
        if let Err(err) = connect_and_stream(&client, &base_url, &stream, &shared).await {
            tracing::warn!(error = %err, "state feed disconnected, reconnecting");
            shared.push_log(
                LogLevel::Warn,
                format!("state feed disconnected ({err}), reconnecting"),
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn connect_and_stream(
    client: &reqwest::Client,
    base_url: &str,
    stream: &str,
    shared: &Arc<SharedState>,
) -> reqwest::Result<()> {
    let resp = client
        .get(format!("{base_url}/ingest/{stream}/state"))
        .send()
        .await?;
    let mut bytes_stream = resp.bytes_stream();
    let mut buf = String::new();

    while let Some(chunk) = bytes_stream.next().await {
        buf.push_str(&String::from_utf8_lossy(&chunk?));
        while let Some(idx) = buf.find("\n\n") {
            let frame = buf[..idx].to_string();
            buf.drain(..idx + 2);
            if let Some(data) = frame.lines().find_map(|l| l.strip_prefix("data: ")) {
                if let Ok(state) = serde_json::from_str::<ServerState>(data) {
                    shared.update(state).await;
                }
            }
        }
    }
    Ok(())
}
