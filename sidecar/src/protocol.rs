use serde::{Deserialize, Serialize};

use crate::store::InboundEvent;

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Status,
    Claim,
    Release {
        event_id: String,
    },
    Send {
        event_id: String,
        idempotency_key: String,
        body: String,
    },
}

#[derive(Debug, Serialize)]
pub struct Response {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<InboundEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queued: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claimed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matrix_event_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<&'static str>,
}

impl Response {
    pub fn status(queued: u64, claimed: u64, completed: u64) -> Self {
        Self {
            ok: true,
            status: Some("ready"),
            event: None,
            queued: Some(queued),
            claimed: Some(claimed),
            completed: Some(completed),
            matrix_event_id: None,
            error: None,
        }
    }

    pub fn claim(event: Option<InboundEvent>) -> Self {
        Self {
            ok: true,
            status: Some(if event.is_some() { "claimed" } else { "empty" }),
            event,
            queued: None,
            claimed: None,
            completed: None,
            matrix_event_id: None,
            error: None,
        }
    }

    pub fn done(status: &'static str, matrix_event_id: Option<String>) -> Self {
        Self {
            ok: true,
            status: Some(status),
            event: None,
            queued: None,
            claimed: None,
            completed: None,
            matrix_event_id,
            error: None,
        }
    }

    pub fn error(error: &'static str) -> Self {
        Self {
            ok: false,
            status: None,
            event: None,
            queued: None,
            claimed: None,
            completed: None,
            matrix_event_id: None,
            error: Some(error),
        }
    }
}
