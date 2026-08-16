use serde::{Deserialize, Serialize};

use crate::store::InboundEvent;

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Status,
    Claim,
    ActivityStart {
        event_id: String,
    },
    ActivityHeartbeat {
        event_id: String,
        status_event_id: Option<String>,
        long_running: bool,
    },
    ActivityStop {
        event_id: String,
        status_event_id: Option<String>,
        outcome: ActivityOutcome,
    },
    Release {
        event_id: String,
    },
    Send {
        event_id: String,
        idempotency_key: String,
        body: String,
    },
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActivityOutcome {
    Done,
    Stopped,
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

#[cfg(test)]
mod tests {
    use super::{ActivityOutcome, Request};

    #[test]
    fn activity_requests_decode_with_fixed_fields() {
        let start: Request =
            serde_json::from_str(r#"{"op":"activity_start","event_id":"$source"}"#).unwrap();
        assert!(matches!(start, Request::ActivityStart { event_id } if event_id == "$source"));

        let heartbeat: Request = serde_json::from_str(
            r#"{"op":"activity_heartbeat","event_id":"$source","status_event_id":"$status","long_running":true}"#,
        )
        .unwrap();
        assert!(matches!(
            heartbeat,
            Request::ActivityHeartbeat { event_id, status_event_id: Some(status_event_id), long_running: true }
                if event_id == "$source" && status_event_id == "$status"
        ));

        let stop: Request =
            serde_json::from_str(r#"{"op":"activity_stop","event_id":"$source","outcome":"done"}"#)
                .unwrap();
        assert!(matches!(
            stop,
            Request::ActivityStop {
                outcome: ActivityOutcome::Done,
                ..
            }
        ));
    }

    #[test]
    fn activity_requests_reject_arbitrary_progress_content() {
        let request = r#"{"op":"activity_start","event_id":"$source","body":"private reasoning"}"#;
        assert!(serde_json::from_str::<Request>(request).is_err());
    }
}
