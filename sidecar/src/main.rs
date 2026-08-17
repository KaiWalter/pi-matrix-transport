mod config;
mod protocol;
mod store;

use std::{
    os::unix::fs::PermissionsExt,
    path::Path,
    process::Stdio,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{bail, Context, Result};
use chrono::Utc;
use config::Config;
use matrix_sdk::ruma::api::client::room::create_room;
use matrix_sdk::{
    attachment::AttachmentConfig,
    authentication::{matrix::MatrixSession, SessionTokens},
    config::SyncSettings,
    media::{MediaFormat, MediaRequestParameters},
    room::edit::EditedContent,
    ruma::{
        events::room::message::{
            AudioMessageEventContent, MessageType, OriginalSyncRoomMessageEvent,
            RoomMessageEventContent, RoomMessageEventContentWithoutRelation,
        },
        OwnedEventId, OwnedTransactionId,
    },
    Client, Room, RoomState, SessionMeta,
};
use protocol::{ActivityOutcome, Request, Response};
use sha2::{Digest, Sha256};
use store::{SourceEventContext, StateStore};
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
    "I could not transcribe this Matrix voice message. Please resend it.";
static OUTBOUND_AUDIO_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Builds a Matrix `m.text` event with the original text as the interoperable
/// fallback and Matrix-safe HTML generated from Markdown for capable clients.
fn rich_text_reply(body: &str) -> RoomMessageEventContent {
    RoomMessageEventContent::text_markdown(body.trim())
}

/// Produces a plain, speakable rendering of Markdown for the TTS boundary.
/// It deliberately retains human-readable content (including link labels and
/// code) while removing presentation-only Markdown syntax and link targets.
fn speech_text(body: &str) -> String {
    let mut in_fenced_code = false;
    let mut lines = Vec::new();

    for raw_line in body.replace('\r', "").lines() {
        let trimmed = raw_line.trim();
        if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            in_fenced_code = !in_fenced_code;
            continue;
        }
        if !in_fenced_code && is_horizontal_rule(trimmed) {
            continue;
        }

        let line = if in_fenced_code {
            trimmed
        } else {
            strip_block_markers(trimmed)
        };
        let spoken = strip_inline_markdown(line);
        if !spoken.is_empty() {
            lines.push(spoken);
        }
    }

    lines.join("\n")
}

fn is_horizontal_rule(line: &str) -> bool {
    let markers = line
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();
    markers.len() >= 3
        && markers
            .chars()
            .all(|character| matches!(character, '-' | '*' | '_'))
}

fn strip_block_markers(line: &str) -> &str {
    let without_quote = line.strip_prefix('>').map(str::trim_start).unwrap_or(line);
    let without_heading = without_quote.trim_start_matches('#').trim_start();
    let without_bullet = without_heading
        .strip_prefix("- ")
        .or_else(|| without_heading.strip_prefix("* "))
        .or_else(|| without_heading.strip_prefix("+ "))
        .unwrap_or(without_heading);

    let ordered_prefix_len = without_bullet
        .char_indices()
        .take_while(|(_, character)| character.is_ascii_digit())
        .last()
        .and_then(|(index, _)| {
            without_bullet[index + 1..]
                .strip_prefix(". ")
                .or_else(|| without_bullet[index + 1..].strip_prefix(") "))
                .map(|_| index + 3)
        });
    ordered_prefix_len
        .and_then(|index| without_bullet.get(index..))
        .unwrap_or(without_bullet)
}

fn strip_inline_markdown(line: &str) -> String {
    let characters = line.chars().collect::<Vec<_>>();
    let mut output = String::new();
    let mut index = 0;

    while index < characters.len() {
        if characters[index] == '\\' && index + 1 < characters.len() {
            output.push(characters[index + 1]);
            index += 2;
            continue;
        }
        if characters[index] == '`'
            || characters[index] == '*'
            || characters[index] == '_'
            || characters[index] == '~'
        {
            index += 1;
            continue;
        }
        if characters[index] == '['
            || (characters[index] == '!' && characters.get(index + 1) == Some(&'['))
        {
            let label_start = if characters[index] == '!' {
                index + 2
            } else {
                index + 1
            };
            if let Some(label_end) = characters[label_start..]
                .iter()
                .position(|character| *character == ']')
            {
                let label_end = label_start + label_end;
                if characters.get(label_end + 1) == Some(&'(') {
                    if let Some(destination_end) = matching_paren(&characters, label_end + 1) {
                        output.push_str(&strip_inline_markdown(
                            &characters[label_start..label_end]
                                .iter()
                                .collect::<String>(),
                        ));
                        index = destination_end + 1;
                        continue;
                    }
                }
            }
        }
        if characters[index] == '<' {
            if let Some(end) = characters[index + 1..]
                .iter()
                .position(|character| *character == '>')
            {
                let end = index + 1 + end;
                let candidate = characters[index + 1..end].iter().collect::<String>();
                if candidate.starts_with("http://")
                    || candidate.starts_with("https://")
                    || candidate.starts_with("mailto:")
                {
                    index = end + 1;
                    continue;
                }
            }
        }
        output.push(characters[index]);
        index += 1;
    }

    output.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn matching_paren(characters: &[char], open_index: usize) -> Option<usize> {
    let mut depth = 0;
    for (index, character) in characters.iter().enumerate().skip(open_index) {
        match character {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

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
    let allowed_rooms = config
        .room_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let state = StateStore::open(&config.state_db, config.room_id.as_str(), &allowed_rooms)?;
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
    enforce_device_trust(&client, &config).await?;

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
        .context("configured room is unavailable")?;
    if room.state() != RoomState::Joined {
        bail!("configured room is not joined");
    }
    if !room.latest_encryption_state().await?.is_encrypted() {
        bail!("configured room is not encrypted");
    }

    let listener = bind_socket(&app.config).await?;
    tracing::info!("Matrix transport sidecar ready");

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

async fn enforce_device_trust(client: &Client, config: &Config) -> Result<()> {
    if !config.require_verified_device {
        tracing::warn!("Matrix device-trust enforcement is disabled");
        return Ok(());
    }

    // This creates and signs a cross-signing identity only for a new account.
    // If the server already has an identity, the SDK leaves it intact; a missing
    // local private identity is therefore a recovery failure, not a reason to
    // rotate the account's remote cross-signing keys automatically.
    client
        .encryption()
        .bootstrap_cross_signing_if_needed(None)
        .await
        .context("bootstrap Matrix cross-signing identity when absent")?;

    let cross_signing = client
        .encryption()
        .cross_signing_status()
        .await
        .context("Matrix encryption is unavailable after session restoration")?;
    let own_device = client
        .encryption()
        .get_own_device()
        .await
        .context("load configured Matrix device")?
        .context("configured Matrix device was not returned by key query")?;

    if !cross_signing.is_complete() || !own_device.is_verified_with_cross_signing() {
        if !config.allow_cross_signing_repair {
            bail!("Matrix device trust is incomplete; restore the crypto store or explicitly authorize cross-signing repair");
        }
        if cross_signing.is_complete() {
            // The identity already exists locally; sign only this device. This
            // deliberately avoids re-uploading device/one-time keys.
            own_device
                .verify()
                .await
                .context("self-sign the configured Matrix device")?;
        } else {
            // This is an explicitly deployment-authorized recovery operation.
            // If local identity material is absent, the homeserver's UIAA policy
            // controls whether new keys may be created.
            client
                .encryption()
                .bootstrap_cross_signing(None)
                .await
                .context("perform explicitly authorized Matrix cross-signing repair")?;
        }
    }

    let cross_signing = client
        .encryption()
        .cross_signing_status()
        .await
        .context("recheck Matrix cross-signing status")?;
    let own_device = client
        .encryption()
        .get_own_device()
        .await
        .context("recheck configured Matrix device")?
        .context("configured Matrix device disappeared after trust repair")?;
    if !cross_signing.is_complete() || !own_device.is_verified_with_cross_signing() {
        bail!("Matrix device-trust repair did not produce a complete self-cross-signed device");
    }

    tracing::info!(
        cross_signing_complete = true,
        current_device_verified = true,
        "Matrix device-trust gate passed"
    );
    Ok(())
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
    if room.state() != RoomState::Joined
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
    let room_id = room.room_id().to_string();
    match app.state.room_enabled(&room_id) {
        Ok(true) => {}
        Ok(false) => return,
        Err(_) => {
            tracing::error!("failed to inspect room binding state");
            return;
        }
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
            app.state.enqueue(&event_id, &room_id, body, "text")
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
            app.state.enqueue(&event_id, &room_id, &transcript, "voice")
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

fn spoken_overview(body: &str) -> String {
    let speech = speech_text(body);
    if speech.is_empty() {
        return String::new();
    }

    const MAX_OVERVIEW_CHARS: usize = 420;
    let mut overview = String::new();
    for word in speech.split_whitespace() {
        let needed = if overview.is_empty() {
            word.len()
        } else {
            word.len() + 1
        };
        if overview.chars().count() + needed > MAX_OVERVIEW_CHARS {
            break;
        }
        if !overview.is_empty() {
            overview.push(' ');
        }
        overview.push_str(word);
    }

    if overview.is_empty() {
        speech
    } else {
        overview
    }
}

fn outbound_audio_filename() -> String {
    let timestamp = Utc::now().format("%Y%m%dT%H%M%S%9fZ");
    let sequence = OUTBOUND_AUDIO_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("matrix-reply-{timestamp}-{sequence:08}.mp3")
}

async fn send_audio_reply(
    app: &App,
    room: &Room,
    speech: &str,
    transaction_id: OwnedTransactionId,
) -> Result<matrix_sdk::ruma::api::client::message::send_message_event::v3::Response> {
    if speech.is_empty() {
        bail!("Matrix reply has no speakable content");
    }
    let temp_directory = private_temp_directory(&app.config.media_temp_path)?;
    let path = temp_directory.path().join("outbound.mp3");
    let mut child = Command::new(&app.config.tts_command)
        .args(["--stdin", "--out"])
        .arg(&path)
        .args(["--voice", &app.config.tts_voice])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    child
        .stdin
        .take()
        .context("TTS stdin unavailable")?
        .write_all(speech.as_bytes())
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
            outbound_audio_filename(),
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
        Request::Claim { room_id } => Ok(Response::claim(app.state.claim(room_id.as_deref())?)),
        Request::ActivityStart { event_id } => {
            let source = ensure_source_event(app, &event_id)?;
            let room = room_for_source(app, &source).await?;
            room.typing_notice(true).await?;
            let sent = room
                .send(RoomMessageEventContent::notice_plain("Processing…"))
                .with_transaction_id(deterministic_transaction_id(&format!(
                    "activity-start:{event_id}"
                )))
                .await?;
            Ok(Response::done(
                "activity_started",
                Some(sent.event_id.to_string()),
            ))
        }
        Request::ActivityHeartbeat {
            event_id,
            status_event_id,
            long_running,
        } => {
            let source = ensure_source_event(app, &event_id)?;
            let room = room_for_source(app, &source).await?;
            room.typing_notice(true).await?;
            if long_running {
                if let Some(status_event_id) = status_event_id {
                    edit_activity_notice(&room, &status_event_id, "Still working…", "working")
                        .await?;
                }
            }
            Ok(Response::done("activity_refreshed", None))
        }
        Request::ActivityStop {
            event_id,
            status_event_id,
            outcome,
        } => {
            let source = ensure_source_event(app, &event_id)?;
            let room = room_for_source(app, &source).await?;
            room.typing_notice(false).await?;
            if let Some(status_event_id) = status_event_id {
                let (body, phase) = match outcome {
                    ActivityOutcome::Done => ("Done.", "done"),
                    ActivityOutcome::Stopped => ("Stopped.", "stopped"),
                };
                edit_activity_notice(&room, &status_event_id, body, phase).await?;
            }
            Ok(Response::done("activity_stopped", None))
        }
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
            let source = ensure_source_event(app, &event_id)?;
            let room = room_for_source(app, &source).await?;
            let matrix_event_id = if source.kind == "voice" {
                let text_sent = room
                    .send(rich_text_reply(&body))
                    .with_transaction_id(deterministic_transaction_id(&format!(
                        "{idempotency_key}:text"
                    )))
                    .await?;
                let speech = spoken_overview(body.trim());
                match send_audio_reply(
                    app,
                    &room,
                    &speech,
                    deterministic_transaction_id(&format!("{idempotency_key}:audio")),
                )
                .await
                {
                    Ok(_audio_sent) => text_sent.event_id.to_string(),
                    Err(_) => {
                        tracing::warn!(
                            "audio overview reply failed; retained encrypted text detail reply"
                        );
                        text_sent.event_id.to_string()
                    }
                }
            } else {
                room.send(rich_text_reply(&body))
                    .with_transaction_id(deterministic_transaction_id(&idempotency_key))
                    .await?
                    .event_id
                    .to_string()
            };
            app.state
                .complete(&event_id, &idempotency_key, &matrix_event_id)?;
            Ok(Response::done("sent", Some(matrix_event_id)))
        }
        Request::ProjectRoomAdd {
            project_slug,
            display_name,
        } => {
            let project_slug = normalize_project_slug(&project_slug)?;
            if let Some(room_id) = app.state.room_for_project(&project_slug)? {
                return Ok(Response::room("project_room_exists", room_id));
            }
            let room_name = display_name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| format!("Project · {}", project_slug.replace('-', " ")));
            let room = create_project_room(app, &room_name).await?;
            let room_id = room.room_id().to_string();
            app.state
                .upsert_room_binding(&room_id, Some(&project_slug))?;
            Ok(Response::room("project_room_created", room_id))
        }
        Request::ProjectRoomRemove { project_slug } => {
            let project_slug = normalize_project_slug(&project_slug)?;
            let Some(room_id) = app.state.room_for_project(&project_slug)? else {
                return Ok(Response::done("project_room_absent", None));
            };
            app.state.remove_room_binding(&room_id)?;
            leave_room_best_effort(app, &room_id).await?;
            Ok(Response::room("project_room_removed", room_id))
        }
        Request::ProjectRoomList => Ok(Response::project_rooms(app.state.list_project_rooms()?)),
    }
}

fn ensure_source_event(app: &App, event_id: &str) -> Result<SourceEventContext> {
    app.state
        .source_context(event_id)?
        .context("source event unavailable")
}

async fn room_for_source(app: &App, source: &SourceEventContext) -> Result<Room> {
    let room_id: matrix_sdk::ruma::OwnedRoomId = source
        .room_id
        .parse()
        .context("source event has invalid room id")?;
    let room = app
        .client
        .get_room(&room_id)
        .context("source room unavailable")?;
    if room.state() != RoomState::Joined || !room.latest_encryption_state().await?.is_encrypted() {
        bail!("source room is not joined and encrypted");
    }
    Ok(room)
}

fn normalize_project_slug(value: &str) -> Result<String> {
    let slug = value.trim().to_ascii_lowercase();
    if slug.is_empty()
        || !slug.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
        || slug.starts_with('-')
        || slug.ends_with('-')
    {
        bail!("project slug must be lowercase kebab-case");
    }
    Ok(slug)
}

async fn create_project_room(app: &App, room_name: &str) -> Result<Room> {
    let mut request = create_room::v3::Request::new();
    request.name = Some(room_name.to_owned());
    request.preset = Some(create_room::v3::RoomPreset::PrivateChat);
    request.is_direct = false;
    request.invite = vec![app.config.sender_id.clone()];

    let room = app.client.create_room(request).await?;
    Ok(room)
}

async fn leave_room_best_effort(app: &App, room_id: &str) -> Result<()> {
    let room_id: matrix_sdk::ruma::OwnedRoomId = room_id.parse().context("invalid room id")?;
    if let Some(room) = app.client.get_room(&room_id) {
        if room.state() == RoomState::Joined {
            room.leave().await?;
        }
    }
    Ok(())
}

async fn edit_activity_notice(
    room: &Room,
    status_event_id: &str,
    body: &str,
    phase: &str,
) -> Result<()> {
    let event_id = OwnedEventId::try_from(status_event_id).context("invalid activity event id")?;
    let edit = room
        .make_edit_event(
            &event_id,
            EditedContent::RoomMessage(RoomMessageEventContentWithoutRelation::notice_plain(body)),
        )
        .await?;
    room.send(edit)
        .with_transaction_id(deterministic_transaction_id(&format!(
            "activity-edit:{status_event_id}:{phase}"
        )))
        .await?;
    Ok(())
}

fn deterministic_transaction_id(idempotency_key: &str) -> OwnedTransactionId {
    let digest = Sha256::digest(idempotency_key.as_bytes());
    OwnedTransactionId::from(format!("matrix_{digest:x}"))
}

#[cfg(test)]
mod tests {
    use super::{
        deterministic_transaction_id, outbound_audio_filename, rich_text_reply, speech_text,
        spoken_overview,
    };

    #[test]
    fn outbound_audio_filenames_are_punctuation_free_and_unique() {
        let first = outbound_audio_filename();
        let second = outbound_audio_filename();

        assert_ne!(first, second);
        assert!(first.starts_with("matrix-reply-"));
        assert!(first.ends_with(".mp3"));
        let timestamp = first
            .strip_prefix("matrix-reply-")
            .and_then(|value| value.split_once('-'))
            .expect("filename contains timestamp")
            .0;
        assert_eq!(timestamp.len(), 25);
        assert_eq!(&timestamp[8..9], "T");
        assert!(timestamp.ends_with('Z'));
        assert!(timestamp[..24]
            .chars()
            .all(|character| character.is_ascii_digit() || character == 'T'));
    }

    #[test]
    fn transaction_id_is_stable_and_opaque() {
        let first = deterministic_transaction_id("reply:$private-event");
        let second = deterministic_transaction_id("reply:$private-event");
        assert_eq!(first, second);
        assert!(!first.as_str().contains("private"));
    }

    #[test]
    fn speech_text_removes_markdown_syntax_and_link_targets() {
        let source = "# Status\n\n- **ready** — see [the guide](https://example.test/guide).\n- `cargo test`\n> _No_ raw URL: <https://example.test>.\n\n![diagram](https://example.test/diagram.png)";
        assert_eq!(
            speech_text(source),
            "Status\nready — see the guide.\ncargo test\nNo raw URL: .\ndiagram",
        );
    }

    #[test]
    fn speech_text_preserves_plain_text_and_strips_fenced_code_markers() {
        assert_eq!(
            speech_text("1. First item\n2. Second item\n\n```text\nlet value = 1;\n```"),
            "First item\nSecond item\nlet value = 1;",
        );
    }

    #[test]
    fn spoken_overview_is_bounded_and_non_empty_for_text() {
        let source = "Status update with several details that should be shortened for spoken output while keeping the key meaning available for quick listening.";
        let overview = spoken_overview(source);
        assert!(!overview.is_empty());
        assert!(overview.chars().count() <= 420);
    }

    #[test]
    fn rich_text_reply_preserves_plain_fallback_and_adds_matrix_html() {
        let body = "# Status\n\n- **ready**\n- [details](https://example.test)";
        let content = rich_text_reply(body);
        let value = serde_json::to_value(content).expect("message content serializes");

        assert_eq!(value["body"], body);
        assert_eq!(value["format"], "org.matrix.custom.html");
        let formatted = value["formatted_body"]
            .as_str()
            .expect("Markdown produces Matrix HTML");
        assert!(formatted.contains("<h1>Status</h1>"));
        assert!(formatted.contains("<strong>ready</strong>"));
        assert!(formatted.contains("<a href=\"https://example.test\">details</a>"));
    }
}
