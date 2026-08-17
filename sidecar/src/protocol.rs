use serde::{Deserialize, Serialize};

use crate::store::{InboundEvent, ProjectRoomBinding};

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Status,
    Claim {
        #[serde(default)]
        room_id: Option<String>,
    },
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
    ProjectRoomAdd {
        project_slug: String,
        #[serde(default)]
        display_name: Option<String>,
    },
    ProjectRoomRemove {
        project_slug: String,
    },
    ProjectRoomList,
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
    pub room_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_rooms: Option<Vec<ProjectRoomBinding>>,
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
            room_id: None,
            project_rooms: None,
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
            room_id: None,
            project_rooms: None,
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
            room_id: None,
            project_rooms: None,
            error: None,
        }
    }

    pub fn room(status: &'static str, room_id: String) -> Self {
        Self {
            ok: true,
            status: Some(status),
            event: None,
            queued: None,
            claimed: None,
            completed: None,
            matrix_event_id: None,
            room_id: Some(room_id),
            project_rooms: None,
            error: None,
        }
    }

    pub fn project_rooms(bindings: Vec<ProjectRoomBinding>) -> Self {
        Self {
            ok: true,
            status: Some("project_rooms"),
            event: None,
            queued: None,
            claimed: None,
            completed: None,
            matrix_event_id: None,
            room_id: None,
            project_rooms: Some(bindings),
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
            room_id: None,
            project_rooms: None,
            error: Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ActivityOutcome, Request};

    #[test]
    fn activity_requests_decode_with_fixed_fields() {
        let claim: Request = serde_json::from_str(r#"{"op":"claim"}"#).unwrap();
        assert!(matches!(claim, Request::Claim { room_id: None }));

        let scoped_claim: Request =
            serde_json::from_str(r#"{"op":"claim","room_id":"!ea:example.org"}"#).unwrap();
        assert!(matches!(
            scoped_claim,
            Request::Claim {
                room_id: Some(room_id)
            } if room_id == "!ea:example.org"
        ));

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
