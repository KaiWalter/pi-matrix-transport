use std::{env, path::PathBuf};

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
    pub room_id: OwnedRoomId,
    pub sender_id: OwnedUserId,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        if env::var("MATRIX_XO_ENABLED").as_deref() != Ok("1") {
            bail!("XO Matrix transport is disabled; set MATRIX_XO_ENABLED=1 explicitly");
        }

        Ok(Self {
            homeserver: required("MATRIX_XO_HOMESERVER")?,
            user_id: required("MATRIX_XO_USER_ID")?
                .try_into()
                .context("invalid MATRIX_XO_USER_ID")?,
            device_id: OwnedDeviceId::from(required("MATRIX_XO_DEVICE_ID")?),
            access_token_file: required_path("MATRIX_XO_ACCESS_TOKEN_FILE")?,
            store_path: required_path("MATRIX_XO_STORE_PATH")?,
            store_passphrase_file: required_path("MATRIX_XO_STORE_PASSPHRASE_FILE")?,
            state_db: required_path("MATRIX_XO_STATE_DB")?,
            socket: required_path("MATRIX_XO_SOCKET")?,
            room_id: required("MATRIX_XO_ROOM_ID")?
                .try_into()
                .context("invalid MATRIX_XO_ROOM_ID")?,
            sender_id: required("MATRIX_XO_SENDER_ID")?
                .try_into()
                .context("invalid MATRIX_XO_SENDER_ID")?,
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
