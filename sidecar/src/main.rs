mod config;
mod protocol;
mod store;

use std::{os::unix::fs::PermissionsExt, path::Path, process::Stdio, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use config::Config;
use matrix_sdk::{
    attachment::AttachmentConfig,
    authentication::{matrix::MatrixSession, SessionTokens},
    config::SyncSettings,
    media::{MediaFormat, MediaRequestParameters},
    ruma::{
        events::room::message::{
            AudioMessageEventContent, MessageType, OriginalSyncRoomMessageEvent,
            RoomMessageEventContent,
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
    process::Command,
    time::timeout,
};

const MAX_REQUEST_BYTES: u64 = 65_536;
const MAX_MESSAGE_CHARS: usize = 16_000;
const MAX_AUDIO_BYTES: usize = 25 * 1024 * 1024;
const MAX_AUDIO_DURATION: Duration = Duration::from_secs(5 * 60);
const TRANSCRIBE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const TTS_TIMEOUT: Duration = Duration::from_secs(2 * 60);
const MAX_TRANSCRIPT_BYTES: u64 = 65_536;
const VOICE_PROCESSING_ERROR_MESSAGE: &str =
    "I could not transcribe this Matrix voice message. Please ask Kai to resend it.";

struct App {
    config: Config,
    state: StateStore,
    client: Client,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("pi_matrix_transport_sidecar=info")
        .with_target(false)
        .with_level(true)
        .compact()
        .init();

    let config = Config::from_env()?;
    let access_token = read_secret(&config.access_token_file).await?;
    let store_passphrase = read_secret(&config.store_passphrase_file).await?;
    let state = StateStore::open(&config.state_db)?;
    secure_directory(&config.store_path).await?;
    secure_directory(&config.media_temp_path).await?;
    clear_private_temp_directory(&config.media_temp_path).await?;

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

    // Register the handler before the initial sync so events received while the
    // sidecar was offline are durably queued instead of skipped by next_batch.
    let response = client.sync_once(SyncSettings::default()).await?;
    let room = client
        .get_room(&app.config.room_id)
        .context("configured canary room is unavailable")?;
    if room.state() != RoomState::Joined {
        bail!("configured canary room is not joined");
    }
    if !room.latest_encryption_state().await?.is_encrypted() {
        bail!("configured canary room is not encrypted");
    }

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

async fn secure_directory(path: &std::path::Path) -> Result<()> {
    let existed = tokio::fs::try_exists(path).await?;
    tokio::fs::create_dir_all(path).await?;
    let metadata = tokio::fs::metadata(path).await?;
    if !metadata.is_dir() {
        bail!("private runtime path is not a directory");
    }
    if existed && metadata.permissions().mode() & 0o077 != 0 {
        bail!("private runtime directory must not be accessible by group or other users");
    }
    if !existed {
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).await?;
    }
    Ok(())
}

async fn read_secret(path: &std::path::Path) -> Result<String> {
    let metadata = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("inspect secret file {}", path.display()))?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
        bail!("secret file must be a user-only regular file");
    }
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
    let event_id = event.event_id.to_string();
    match app.state.contains_event(&event_id) {
        Ok(true) => {
            tracing::debug!("ignored duplicate Matrix event");
            return;
        }
        Ok(false) => {}
        Err(_) => {
            tracing::error!("failed to inspect Matrix event state");
            return;
        }
    }
    let accepted = match event.content.msgtype {
        MessageType::Text(text) => {
            let body = text.body.trim();
            if body.is_empty() || body.chars().count() > MAX_MESSAGE_CHARS {
                return;
            }
            app.state.enqueue(&event_id, body, "text")
        }
        MessageType::Audio(audio) => {
            let transcript = match transcribe_audio(&app, audio).await {
                Ok(Some(transcript)) => transcript,
                Ok(None) => {
                    tracing::warn!("rejected one unsupported Matrix audio event");
                    VOICE_PROCESSING_ERROR_MESSAGE.to_owned()
                }
                Err(_) => {
                    tracing::warn!("failed to process one Matrix audio event");
                    VOICE_PROCESSING_ERROR_MESSAGE.to_owned()
                }
            };
            app.state.enqueue(&event_id, &transcript, "voice")
        }
        _ => return,
    };
    match accepted {
        Ok(true) => tracing::info!("accepted one Matrix event"),
        Ok(false) => tracing::debug!("ignored duplicate Matrix event"),
        Err(_) => tracing::error!("failed to persist Matrix event"),
    }
}

async fn clear_private_temp_directory(path: &Path) -> Result<()> {
    let mut entries = tokio::fs::read_dir(path).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        if !file_name.starts_with("matrix-media-") {
            continue;
        }
        let file_type = entry.file_type().await?;
        if file_type.is_dir() {
            tokio::fs::remove_dir_all(entry.path()).await?;
        } else if file_type.is_file() || file_type.is_symlink() {
            tokio::fs::remove_file(entry.path()).await?;
        }
    }
    Ok(())
}

fn private_temp_directory(root: &Path) -> Result<tempfile::TempDir> {
    let directory = tempfile::Builder::new()
        .prefix("matrix-media-")
        .tempdir_in(root)?;
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
    Ok(directory)
}

async fn transcribe_audio(app: &App, audio: AudioMessageEventContent) -> Result<Option<String>> {
    if let Some(info) = &audio.info {
        if info
            .duration
            .is_some_and(|duration| duration > MAX_AUDIO_DURATION)
            || info
                .size
                .is_some_and(|size| u64::from(size) > MAX_AUDIO_BYTES as u64)
            || info
                .mimetype
                .as_deref()
                .is_some_and(|mimetype| !mimetype.starts_with("audio/"))
        {
            return Ok(None);
        }
    }

    let request = MediaRequestParameters {
        source: audio.source,
        format: MediaFormat::File,
    };
    let bytes = app
        .client
        .media()
        .get_media_content(&request, false)
        .await?;
    if bytes.is_empty() || bytes.len() > MAX_AUDIO_BYTES {
        return Ok(None);
    }

    let temp_directory = private_temp_directory(&app.config.media_temp_path)?;
    let path = temp_directory.path().join("inbound.audio");
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await?;
    file.write_all(&bytes).await?;
    file.sync_all().await?;
    tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).await?;
    drop(file);

    let mut child = Command::new(&app.config.transcribe_command)
        .arg(&path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let mut stdout = child
        .stdout
        .take()
        .context("transcriber stdout unavailable")?;
    let (status, transcript_bytes) = timeout(TRANSCRIBE_TIMEOUT, async {
        let mut transcript_bytes = Vec::new();
        (&mut stdout)
            .take(MAX_TRANSCRIPT_BYTES + 1)
            .read_to_end(&mut transcript_bytes)
            .await?;
        let status = child.wait().await?;
        Ok::<_, std::io::Error>((status, transcript_bytes))
    })
    .await
    .context("Matrix audio transcription timed out")??;
    if !status.success() || transcript_bytes.len() as u64 > MAX_TRANSCRIPT_BYTES {
        bail!("Matrix audio transcription failed");
    }
    let transcript = String::from_utf8(transcript_bytes)?.trim().to_owned();
    if transcript.is_empty() || transcript.chars().count() > MAX_MESSAGE_CHARS {
        return Ok(None);
    }
    Ok(Some(transcript))
}

async fn send_audio_reply(
    app: &App,
    room: &Room,
    body: &str,
    transaction_id: OwnedTransactionId,
) -> Result<matrix_sdk::ruma::api::client::message::send_message_event::v3::Response> {
    let temp_directory = private_temp_directory(&app.config.media_temp_path)?;
    let path = temp_directory.path().join("outbound.mp3");
    let mut child = Command::new(&app.config.tts_command)
        .args(["--stdin", "--out"])
        .arg(&path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    child
        .stdin
        .take()
        .context("TTS stdin unavailable")?
        .write_all(body.as_bytes())
        .await?;
    let status = timeout(TTS_TIMEOUT, child.wait())
        .await
        .context("Matrix reply synthesis timed out")??;
    if !status.success() {
        bail!("Matrix reply synthesis failed");
    }
    let data = tokio::fs::read(&path).await?;
    if data.is_empty() || data.len() > MAX_AUDIO_BYTES {
        bail!("Matrix reply audio size is invalid");
    }
    let content_type: mime::Mime = "audio/mpeg".parse()?;
    Ok(room
        .send_attachment(
            "xo-reply.mp3",
            &content_type,
            data,
            AttachmentConfig::new().txn_id(transaction_id),
        )
        .await?)
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
            let kind = app
                .state
                .inbound_kind(&event_id)?
                .context("source event unavailable")?;
            let sent = if kind == "voice" {
                match send_audio_reply(app, &room, body.trim(), transaction_id.clone()).await {
                    Ok(sent) => sent,
                    Err(_) => {
                        tracing::warn!("audio reply failed; using encrypted text fallback");
                        room.send(RoomMessageEventContent::text_plain(body.trim()))
                            .with_transaction_id(transaction_id)
                            .await?
                    }
                }
            } else {
                room.send(RoomMessageEventContent::text_plain(body.trim()))
                    .with_transaction_id(transaction_id)
                    .await?
            };
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
