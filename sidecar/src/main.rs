mod config;
mod protocol;
mod store;

use std::{os::unix::fs::PermissionsExt, sync::Arc};

use anyhow::{bail, Context, Result};
use config::Config;
use matrix_sdk::{
    authentication::{matrix::MatrixSession, SessionTokens},
    config::SyncSettings,
    ruma::{
        events::room::message::{
            MessageType, OriginalSyncRoomMessageEvent, RoomMessageEventContent,
        },
        OwnedTransactionId,
    },
    Client, Room, RoomState, SessionMeta,
};
use protocol::{Request, Response};
use sha2::{Digest, Sha256};
use store::StateStore;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
};

const MAX_REQUEST_BYTES: u64 = 65_536;
const MAX_MESSAGE_CHARS: usize = 16_000;

struct App {
    config: Config,
    state: StateStore,
    client: Client,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .compact()
        .init();

    let config = Config::from_env()?;
    let access_token = read_secret(&config.access_token_file).await?;
    let store_passphrase = read_secret(&config.store_passphrase_file).await?;
    let state = StateStore::open(&config.state_db)?;

    let client = Client::builder()
        .homeserver_url(&config.homeserver)
        .sqlite_store(&config.store_path, Some(&store_passphrase))
        .build()
        .await?;
    client
        .restore_session(MatrixSession {
            meta: SessionMeta {
                user_id: config.user_id.clone(),
                device_id: config.device_id.clone(),
            },
            tokens: SessionTokens {
                access_token,
                refresh_token: None,
            },
        })
        .await?;

    let response = client.sync_once(SyncSettings::default()).await?;
    let room = client
        .get_room(&config.room_id)
        .context("configured canary room is unavailable")?;
    if room.state() != RoomState::Joined {
        bail!("configured canary room is not joined");
    }
    if !room.latest_encryption_state().await?.is_encrypted() {
        bail!("configured canary room is not encrypted");
    }

    let app = Arc::new(App {
        config,
        state,
        client: client.clone(),
    });
    client.add_event_handler({
        let app = Arc::clone(&app);
        move |event: OriginalSyncRoomMessageEvent, room: Room, client: Client| {
            let app = Arc::clone(&app);
            async move { accept_inbound(app, event, room, client).await }
        }
    });

    let listener = bind_socket(&app.config).await?;
    tracing::info!("XO Matrix sidecar ready");

    let sync = client.sync(SyncSettings::default().token(response.next_batch));
    tokio::pin!(sync);
    tokio::select! {
        result = &mut sync => result.context("Matrix sync stopped"),
        result = serve(listener, Arc::clone(&app)) => result,
        result = tokio::signal::ctrl_c() => {
            result.context("wait for shutdown signal")?;
            Ok(())
        }
    }
}

async fn read_secret(path: &std::path::Path) -> Result<String> {
    let value = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("read secret file {}", path.display()))?;
    let value = value.trim().to_owned();
    if value.is_empty() {
        bail!("secret file is empty");
    }
    Ok(value)
}

async fn bind_socket(config: &Config) -> Result<UnixListener> {
    if let Some(parent) = config.socket.parent() {
        tokio::fs::create_dir_all(parent).await?;
        tokio::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700)).await?;
    }
    if tokio::fs::try_exists(&config.socket).await? {
        tokio::fs::remove_file(&config.socket).await?;
    }
    let listener = UnixListener::bind(&config.socket)
        .with_context(|| format!("bind Unix socket {}", config.socket.display()))?;
    tokio::fs::set_permissions(&config.socket, std::fs::Permissions::from_mode(0o600)).await?;
    Ok(listener)
}

async fn accept_inbound(
    app: Arc<App>,
    event: OriginalSyncRoomMessageEvent,
    room: Room,
    client: Client,
) {
    if room.room_id() != app.config.room_id
        || room.state() != RoomState::Joined
        || event.sender != app.config.sender_id
        || event.sender == client.user_id().expect("restored session has user id")
        || event.content.relates_to.is_some()
    {
        return;
    }
    match room.latest_encryption_state().await {
        Ok(state) if state.is_encrypted() => {}
        _ => return,
    }
    let MessageType::Text(text) = event.content.msgtype else {
        return;
    };
    let body = text.body.trim();
    if body.is_empty() || body.chars().count() > MAX_MESSAGE_CHARS {
        return;
    }
    match app.state.enqueue(event.event_id.as_str(), body) {
        Ok(true) => tracing::info!("accepted one Matrix text event"),
        Ok(false) => tracing::debug!("ignored duplicate Matrix event"),
        Err(_) => tracing::error!("failed to persist Matrix event"),
    }
}

async fn serve(listener: UnixListener, app: Arc<App>) -> Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let app = Arc::clone(&app);
        tokio::spawn(async move {
            if handle_connection(stream, app).await.is_err() {
                tracing::warn!("local Matrix IPC request failed");
            }
        });
    }
}

async fn handle_connection(stream: UnixStream, app: Arc<App>) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut line = String::new();
    let mut reader = BufReader::new(read_half).take(MAX_REQUEST_BYTES + 1);
    reader.read_line(&mut line).await?;
    if line.len() as u64 > MAX_REQUEST_BYTES || !line.ends_with('\n') {
        write_response(&mut write_half, &Response::error("invalid_request")).await?;
        return Ok(());
    }
    let request: Request = match serde_json::from_str(line.trim_end()) {
        Ok(request) => request,
        Err(_) => {
            write_response(&mut write_half, &Response::error("invalid_request")).await?;
            return Ok(());
        }
    };
    let response = match process_request(&app, request).await {
        Ok(response) => response,
        Err(_) => Response::error("operation_failed"),
    };
    write_response(&mut write_half, &response).await?;
    Ok(())
}

async fn write_response(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    response: &Response,
) -> Result<()> {
    let mut payload = serde_json::to_vec(response)?;
    payload.push(b'\n');
    writer.write_all(&payload).await?;
    writer.shutdown().await?;
    Ok(())
}

async fn process_request(app: &App, request: Request) -> Result<Response> {
    match request {
        Request::Status => {
            let (queued, claimed, completed) = app.state.counts()?;
            Ok(Response::status(queued, claimed, completed))
        }
        Request::Claim => Ok(Response::claim(app.state.claim()?)),
        Request::Release { event_id } => Ok(Response::done(
            if app.state.release(&event_id)? {
                "released"
            } else {
                "unchanged"
            },
            None,
        )),
        Request::Send {
            event_id,
            idempotency_key,
            body,
        } => {
            if idempotency_key.trim().is_empty()
                || body.trim().is_empty()
                || body.chars().count() > MAX_MESSAGE_CHARS
            {
                bail!("invalid outbound request");
            }
            if let Some(matrix_event_id) = app.state.outbound_event(&idempotency_key)? {
                return Ok(Response::done("duplicate", Some(matrix_event_id)));
            }
            let room = app
                .client
                .get_room(&app.config.room_id)
                .context("canary room unavailable")?;
            if room.state() != RoomState::Joined
                || !room.latest_encryption_state().await?.is_encrypted()
            {
                bail!("canary room is not joined and encrypted");
            }
            let transaction_id = deterministic_transaction_id(&idempotency_key);
            let sent = room
                .send(RoomMessageEventContent::text_plain(body.trim()))
                .with_transaction_id(transaction_id)
                .await?;
            let matrix_event_id = sent.event_id.to_string();
            app.state
                .complete(&event_id, &idempotency_key, &matrix_event_id)?;
            Ok(Response::done("sent", Some(matrix_event_id)))
        }
    }
}

fn deterministic_transaction_id(idempotency_key: &str) -> OwnedTransactionId {
    let digest = Sha256::digest(idempotency_key.as_bytes());
    OwnedTransactionId::from(format!("xo_{digest:x}"))
}

#[cfg(test)]
mod tests {
    use super::deterministic_transaction_id;

    #[test]
    fn transaction_id_is_stable_and_opaque() {
        let first = deterministic_transaction_id("reply:$private-event");
        let second = deterministic_transaction_id("reply:$private-event");
        assert_eq!(first, second);
        assert!(!first.as_str().contains("private"));
    }
}
