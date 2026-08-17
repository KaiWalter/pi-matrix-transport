use std::{collections::BTreeSet, env, path::PathBuf};

use anyhow::{bail, Context, Result};
use matrix_sdk::ruma::{OwnedDeviceId, OwnedRoomId, OwnedUserId};

#[derive(Clone, Debug)]
pub struct Config {
    pub homeserver: String,
    pub user_id: OwnedUserId,
    pub device_id: OwnedDeviceId,
    pub access_token_file: PathBuf,
    pub store_path: PathBuf,
    pub store_passphrase_file: PathBuf,
    pub state_db: PathBuf,
    pub socket: PathBuf,
    pub media_temp_path: PathBuf,
    pub transcribe_command: PathBuf,
    pub tts_command: PathBuf,
    pub tts_voice: String,
    pub room_id: OwnedRoomId,
    pub room_ids: Vec<OwnedRoomId>,
    pub sender_id: OwnedUserId,
    pub require_verified_device: bool,
    pub allow_cross_signing_repair: bool,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        if env::var("MATRIX_AGENT_ENABLED").as_deref() != Ok("1") {
            bail!("Matrix transport is disabled; set MATRIX_AGENT_ENABLED=1 explicitly");
        }

        let state_db = required_path("MATRIX_AGENT_STATE_DB")?;
        let media_temp_path = required_path("MATRIX_AGENT_MEDIA_TEMP_PATH")?;
        let expected_media_path = state_db
            .parent()
            .context("MATRIX_AGENT_STATE_DB must have a parent directory")?
            .join("media-tmp");
        if media_temp_path != expected_media_path {
            bail!(
                "MATRIX_AGENT_MEDIA_TEMP_PATH must be the media-tmp sibling of MATRIX_AGENT_STATE_DB"
            );
        }

        let room_id: OwnedRoomId = required("MATRIX_AGENT_ROOM_ID")?
            .try_into()
            .context("invalid MATRIX_AGENT_ROOM_ID")?;
        let mut room_ids = BTreeSet::new();
        room_ids.insert(room_id.clone());
        if let Ok(additional_rooms) = env::var("MATRIX_AGENT_ROOM_IDS") {
            for candidate in additional_rooms
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                let parsed: OwnedRoomId = candidate
                    .try_into()
                    .with_context(|| format!("invalid MATRIX_AGENT_ROOM_IDS entry: {candidate}"))?;
                room_ids.insert(parsed);
            }
        }

        Ok(Self {
            homeserver: required("MATRIX_AGENT_HOMESERVER")?,
            user_id: required("MATRIX_AGENT_USER_ID")?
                .try_into()
                .context("invalid MATRIX_AGENT_USER_ID")?,
            device_id: OwnedDeviceId::from(required("MATRIX_AGENT_DEVICE_ID")?),
            access_token_file: required_path("MATRIX_AGENT_ACCESS_TOKEN_FILE")?,
            store_path: required_path("MATRIX_AGENT_STORE_PATH")?,
            store_passphrase_file: required_path("MATRIX_AGENT_STORE_PASSPHRASE_FILE")?,
            state_db,
            socket: required_path("MATRIX_AGENT_SOCKET")?,
            media_temp_path,
            transcribe_command: required_executable("MATRIX_AGENT_TRANSCRIBE_COMMAND")?,
            tts_command: required_executable("MATRIX_AGENT_TTS_COMMAND")?,
            tts_voice: required("MATRIX_AGENT_TTS_VOICE")?,
            room_id,
            room_ids: room_ids.into_iter().collect(),
            sender_id: required("MATRIX_AGENT_SENDER_ID")?
                .try_into()
                .context("invalid MATRIX_AGENT_SENDER_ID")?,
            require_verified_device: env::var("MATRIX_AGENT_REQUIRE_VERIFIED_DEVICE").as_deref()
                == Ok("1"),
            allow_cross_signing_repair: env::var("MATRIX_AGENT_ALLOW_CROSS_SIGNING_REPAIR")
                .as_deref()
                == Ok("1"),
        })
    }
}

fn required(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("{name} missing"))?;
    if value.trim().is_empty() {
        bail!("{name} is empty");
    }
    Ok(value)
}

fn required_path(name: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(required(name)?))
}

fn required_executable(name: &str) -> Result<PathBuf> {
    use std::os::unix::fs::PermissionsExt;

    let path = required_path(name)?;
    let metadata = std::fs::metadata(&path).with_context(|| format!("inspect {name}"))?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        bail!("{name} must name an executable regular file");
    }
    Ok(path)
}
