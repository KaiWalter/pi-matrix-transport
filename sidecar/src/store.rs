use std::{
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct InboundImage {
    pub media_type: String,
    pub data: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct InboundEvent {
    pub event_id: String,
    pub room_id: String,
    pub body: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<InboundImage>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProjectRoomBinding {
    pub project_slug: String,
    pub room_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceEventContext {
    pub room_id: String,
    pub kind: String,
}

#[derive(Debug)]
pub struct StateStore {
    connection: Mutex<Connection>,
}

impl StateStore {
    pub fn open(path: &Path, default_room_id: &str, allowed_rooms: &[String]) -> Result<Self> {
        if let Some(parent) = path.parent() {
            let existed = parent.exists();
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create state directory {}", parent.display()))?;
            if existed {
                let mode = std::fs::metadata(parent)?.permissions().mode();
                if mode & 0o077 != 0 {
                    bail!("state directory must not be accessible by group or other users");
                }
            } else {
                std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
                    .with_context(|| format!("secure state directory {}", parent.display()))?;
            }
        }
        if path.is_symlink() {
            bail!("state database must not be a symbolic link");
        }
        let connection = Connection::open(path)
            .with_context(|| format!("open state database {}", path.display()))?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("secure state database {}", path.display()))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             CREATE TABLE IF NOT EXISTS inbound (
               sequence INTEGER PRIMARY KEY AUTOINCREMENT,
               event_id TEXT NOT NULL UNIQUE,
               body TEXT NOT NULL,
               state TEXT NOT NULL CHECK(state IN ('queued','claimed','completed')),
               received_at INTEGER NOT NULL,
               claimed_at INTEGER,
               outbound_event_id TEXT
             );
             CREATE TABLE IF NOT EXISTS outbound (
               idempotency_key TEXT PRIMARY KEY,
               source_event_id TEXT NOT NULL,
               matrix_event_id TEXT NOT NULL,
               sent_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS room_bindings (
               room_id TEXT PRIMARY KEY,
               enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
               project_slug TEXT UNIQUE,
               created_at INTEGER NOT NULL,
               updated_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS proactive_outbound (
               idempotency_key TEXT PRIMARY KEY,
               matrix_event_id TEXT NOT NULL,
               sent_at INTEGER NOT NULL
             );",
        )?;

        let has_kind = connection
            .prepare("PRAGMA table_info(inbound)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .iter()
            .any(|column| column == "kind");
        if !has_kind {
            connection.execute(
                "ALTER TABLE inbound ADD COLUMN kind TEXT NOT NULL DEFAULT 'text' CHECK(kind IN ('text','voice'))",
                [],
            )?;
        }

        let has_room_id = connection
            .prepare("PRAGMA table_info(inbound)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .iter()
            .any(|column| column == "room_id");
        if !has_room_id {
            connection.execute(
                "ALTER TABLE inbound ADD COLUMN room_id TEXT NOT NULL DEFAULT ''",
                [],
            )?;
        }
        connection.execute(
            "UPDATE inbound SET room_id=?1 WHERE room_id IS NULL OR room_id=''",
            [default_room_id],
        )?;

        let inbound_sql: String = connection.query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='inbound'",
            [],
            |row| row.get(0),
        )?;
        if !inbound_sql.contains("'image'") {
            connection.execute_batch(
                "BEGIN IMMEDIATE;
                 ALTER TABLE inbound RENAME TO inbound_before_images;
                 CREATE TABLE inbound (
                   sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                   event_id TEXT NOT NULL UNIQUE,
                   body TEXT NOT NULL,
                   state TEXT NOT NULL CHECK(state IN ('queued','claimed','completed')),
                   received_at INTEGER NOT NULL,
                   claimed_at INTEGER,
                   outbound_event_id TEXT,
                   kind TEXT NOT NULL DEFAULT 'text' CHECK(kind IN ('text','voice','image')),
                   room_id TEXT NOT NULL DEFAULT '',
                   image_mime TEXT,
                   image_data BLOB
                 );
                 INSERT INTO inbound(sequence, event_id, body, state, received_at, claimed_at, outbound_event_id, kind, room_id)
                   SELECT sequence, event_id, body, state, received_at, claimed_at, outbound_event_id, kind, room_id
                   FROM inbound_before_images;
                 DROP TABLE inbound_before_images;
                 COMMIT;",
            )?;
        }

        let store = Self {
            connection: Mutex::new(connection),
        };

        for room_id in allowed_rooms {
            store.upsert_room_binding(room_id, None)?;
        }
        store.requeue_claims()?;
        Ok(store)
    }

    pub fn enqueue(&self, event_id: &str, room_id: &str, body: &str, kind: &str) -> Result<bool> {
        if !matches!(kind, "text" | "voice") {
            bail!("non-image event kind must be text or voice");
        }
        self.enqueue_inner(event_id, room_id, body, kind, None, None)
    }

    pub fn enqueue_image(
        &self,
        event_id: &str,
        room_id: &str,
        body: &str,
        media_type: &str,
        data: &[u8],
    ) -> Result<bool> {
        if data.is_empty() || media_type.is_empty() {
            bail!("image media type and data must be non-empty");
        }
        self.enqueue_inner(
            event_id,
            room_id,
            body,
            "image",
            Some(media_type),
            Some(data),
        )
    }

    fn enqueue_inner(
        &self,
        event_id: &str,
        room_id: &str,
        body: &str,
        kind: &str,
        image_mime: Option<&str>,
        image_data: Option<&[u8]>,
    ) -> Result<bool> {
        if event_id.is_empty()
            || room_id.is_empty()
            || body.trim().is_empty()
            || !matches!(kind, "text" | "voice" | "image")
            || (kind == "image") != (image_mime.is_some() && image_data.is_some())
        {
            bail!("event id, room id, body, kind, and image fields must be valid");
        }
        if !self.room_enabled(room_id)? {
            return Ok(false);
        }
        let changed = self
            .connection
            .lock()
            .expect("state database mutex poisoned")
            .execute(
                "INSERT OR IGNORE INTO inbound(event_id, room_id, body, kind, image_mime, image_data, state, received_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'queued', ?7)",
                params![event_id, room_id, body, kind, image_mime, image_data, now_epoch()?],
            )?;
        Ok(changed == 1)
    }

    pub fn claim(&self, room_id: Option<&str>) -> Result<Option<InboundEvent>> {
        let mut connection = self
            .connection
            .lock()
            .expect("state database mutex poisoned");
        let transaction = connection.transaction()?;
        let now = now_epoch()?;
        // Reclaim only very stale claims to avoid stealing long-running turns
        // from active workers in multi-room setups.
        transaction.execute(
            "UPDATE inbound SET state='queued', claimed_at=NULL WHERE state='claimed' AND claimed_at <= ?1",
            [now - 7_200],
        )?;

        let event = if let Some(room_id) = room_id {
            transaction
                .query_row(
                    "SELECT event_id, room_id, body, kind, image_mime, image_data FROM inbound WHERE state='queued' AND room_id=?1 ORDER BY sequence LIMIT 1",
                    [room_id],
                    inbound_event_from_row,
                )
                .optional()?
        } else {
            transaction
                .query_row(
                    "SELECT event_id, room_id, body, kind, image_mime, image_data FROM inbound WHERE state='queued' ORDER BY sequence LIMIT 1",
                    [],
                    inbound_event_from_row,
                )
                .optional()?
        };

        if let Some(event) = &event {
            transaction.execute(
                "UPDATE inbound SET state='claimed', claimed_at=?2 WHERE event_id=?1 AND state='queued'",
                params![event.event_id, now],
            )?;
        }
        transaction.commit()?;
        Ok(event)
    }

    pub fn release(&self, event_id: &str) -> Result<bool> {
        let changed = self
            .connection
            .lock()
            .expect("state database mutex poisoned")
            .execute(
                "UPDATE inbound SET state='queued', claimed_at=NULL WHERE event_id=?1 AND state='claimed'",
                [event_id],
            )?;
        Ok(changed == 1)
    }

    pub fn contains_event(&self, event_id: &str) -> Result<bool> {
        Ok(self
            .connection
            .lock()
            .expect("state database mutex poisoned")
            .query_row(
                "SELECT 1 FROM inbound WHERE event_id=?1",
                [event_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    pub fn source_context(&self, event_id: &str) -> Result<Option<SourceEventContext>> {
        Ok(self
            .connection
            .lock()
            .expect("state database mutex poisoned")
            .query_row(
                "SELECT room_id, kind FROM inbound WHERE event_id=?1",
                [event_id],
                |row| {
                    Ok(SourceEventContext {
                        room_id: row.get(0)?,
                        kind: row.get(1)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn outbound_event(&self, idempotency_key: &str) -> Result<Option<String>> {
        Ok(self
            .connection
            .lock()
            .expect("state database mutex poisoned")
            .query_row(
                "SELECT matrix_event_id FROM outbound WHERE idempotency_key=?1",
                [idempotency_key],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub fn proactive_outbound_event(&self, idempotency_key: &str) -> Result<Option<String>> {
        Ok(self
            .connection
            .lock()
            .expect("state database mutex poisoned")
            .query_row(
                "SELECT matrix_event_id FROM proactive_outbound WHERE idempotency_key=?1",
                [idempotency_key],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub fn complete_proactive(&self, idempotency_key: &str, matrix_event_id: &str) -> Result<()> {
        self.connection
            .lock()
            .expect("state database mutex poisoned")
            .execute(
                "INSERT OR IGNORE INTO proactive_outbound(idempotency_key, matrix_event_id, sent_at) VALUES (?1, ?2, ?3)",
                params![idempotency_key, matrix_event_id, now_epoch()?],
            )?;
        Ok(())
    }

    pub fn complete(
        &self,
        source_event_id: &str,
        idempotency_key: &str,
        matrix_event_id: &str,
    ) -> Result<()> {
        let mut connection = self
            .connection
            .lock()
            .expect("state database mutex poisoned");
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT OR IGNORE INTO outbound(idempotency_key, source_event_id, matrix_event_id, sent_at) VALUES (?1, ?2, ?3, ?4)",
            params![idempotency_key, source_event_id, matrix_event_id, now_epoch()?],
        )?;
        let changed = transaction.execute(
            "UPDATE inbound SET state='completed', claimed_at=NULL, outbound_event_id=?2, image_mime=NULL, image_data=NULL WHERE event_id=?1 AND state IN ('claimed','completed')",
            params![source_event_id, matrix_event_id],
        )?;
        if changed != 1 {
            bail!("source event is not claimed: {source_event_id}");
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn counts(&self) -> Result<(u64, u64, u64)> {
        let connection = self
            .connection
            .lock()
            .expect("state database mutex poisoned");
        let count = |state: &str| -> Result<u64> {
            Ok(connection.query_row(
                "SELECT COUNT(*) FROM inbound WHERE state=?1",
                [state],
                |row| row.get(0),
            )?)
        };
        Ok((count("queued")?, count("claimed")?, count("completed")?))
    }

    pub fn upsert_room_binding(&self, room_id: &str, project_slug: Option<&str>) -> Result<()> {
        let now = now_epoch()?;
        self.connection
            .lock()
            .expect("state database mutex poisoned")
            .execute(
                "INSERT INTO room_bindings(room_id, enabled, project_slug, created_at, updated_at)
                 VALUES (?1, 1, ?2, ?3, ?3)
                 ON CONFLICT(room_id) DO UPDATE SET enabled=1, project_slug=excluded.project_slug, updated_at=excluded.updated_at",
                params![room_id, project_slug, now],
            )?;
        Ok(())
    }

    pub fn remove_room_binding(&self, room_id: &str) -> Result<bool> {
        let changed = self
            .connection
            .lock()
            .expect("state database mutex poisoned")
            .execute("DELETE FROM room_bindings WHERE room_id=?1", [room_id])?;
        Ok(changed == 1)
    }

    pub fn room_for_project(&self, project_slug: &str) -> Result<Option<String>> {
        Ok(self
            .connection
            .lock()
            .expect("state database mutex poisoned")
            .query_row(
                "SELECT room_id FROM room_bindings WHERE project_slug=?1 AND enabled=1",
                [project_slug],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub fn list_project_rooms(&self) -> Result<Vec<ProjectRoomBinding>> {
        let connection = self
            .connection
            .lock()
            .expect("state database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT project_slug, room_id
             FROM room_bindings
             WHERE enabled=1 AND project_slug IS NOT NULL
             ORDER BY project_slug ASC",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(ProjectRoomBinding {
                project_slug: row.get(0)?,
                room_id: row.get(1)?,
            })
        })?;
        let mut bindings = Vec::new();
        for row in rows {
            bindings.push(row?);
        }
        Ok(bindings)
    }

    pub fn room_enabled(&self, room_id: &str) -> Result<bool> {
        Ok(self
            .connection
            .lock()
            .expect("state database mutex poisoned")
            .query_row(
                "SELECT enabled FROM room_bindings WHERE room_id=?1",
                [room_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .is_some_and(|enabled| enabled == 1))
    }

    fn requeue_claims(&self) -> Result<()> {
        self.connection
            .lock()
            .expect("state database mutex poisoned")
            .execute(
                "UPDATE inbound SET state='queued', claimed_at=NULL WHERE state='claimed'",
                [],
            )?;
        Ok(())
    }
}

fn inbound_event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<InboundEvent> {
    let media_type = row.get::<_, Option<String>>(4)?;
    let image_data = row.get::<_, Option<Vec<u8>>>(5)?;
    let image = match (media_type, image_data) {
        (Some(media_type), Some(data)) => Some(InboundImage {
            media_type,
            data: BASE64.encode(data),
        }),
        _ => None,
    };
    Ok(InboundEvent {
        event_id: row.get(0)?,
        room_id: row.get(1)?,
        body: row.get(2)?,
        kind: row.get(3)?,
        image,
    })
}

fn now_epoch() -> Result<i64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64)
}

#[cfg(test)]
mod tests {
    use std::{os::unix::fs::PermissionsExt, path::PathBuf};

    use rusqlite::Connection;

    use super::StateStore;

    fn temporary_db(name: &str) -> PathBuf {
        std::env::temp_dir()
            .join(format!("pi-matrix-transport-{name}-{}", std::process::id()))
            .join("state.sqlite")
    }

    fn cleanup(path: &std::path::Path) {
        if let Some(parent) = path.parent() {
            let _ = std::fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn deduplicates_and_orders_inbound_events() {
        let path = temporary_db("dedup");
        cleanup(&path);
        let store = StateStore::open(
            &path,
            "!default:example.org",
            &["!default:example.org".to_owned()],
        )
        .unwrap();
        assert_eq!(
            std::fs::metadata(path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(store
            .enqueue("$z-first", "!default:example.org", "first", "text")
            .unwrap());
        assert!(!store
            .enqueue("$z-first", "!default:example.org", "duplicate", "text")
            .unwrap());
        assert!(store.contains_event("$z-first").unwrap());
        assert!(!store.contains_event("$missing").unwrap());
        assert!(store
            .enqueue("$a-second", "!default:example.org", "second", "voice")
            .unwrap());
        let first = store.claim(None).unwrap().unwrap();
        assert_eq!(first.event_id, "$z-first");
        assert_eq!(first.kind, "text");
        assert_eq!(first.image, None);
        let second = store.claim(None).unwrap().unwrap();
        assert_eq!(second.event_id, "$a-second");
        assert_eq!(second.kind, "voice");
        assert!(store.claim(None).unwrap().is_none());
        cleanup(&path);
    }

    #[test]
    fn migrates_existing_text_voice_queue_before_accepting_images() {
        let path = temporary_db("image-migration");
        cleanup(&path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::set_permissions(
            path.parent().unwrap(),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE inbound (
                   sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                   event_id TEXT NOT NULL UNIQUE,
                   body TEXT NOT NULL,
                   state TEXT NOT NULL CHECK(state IN ('queued','claimed','completed')),
                   received_at INTEGER NOT NULL,
                   claimed_at INTEGER,
                   outbound_event_id TEXT,
                   kind TEXT NOT NULL DEFAULT 'text' CHECK(kind IN ('text','voice')),
                   room_id TEXT NOT NULL DEFAULT ''
                 );
                 INSERT INTO inbound(event_id, body, state, received_at, kind, room_id)
                   VALUES ('$legacy', 'legacy voice', 'queued', 1, 'voice', '!default:example.org');",
            )
            .unwrap();
        drop(connection);

        let store = StateStore::open(
            &path,
            "!default:example.org",
            &["!default:example.org".to_owned()],
        )
        .unwrap();
        let legacy = store.claim(None).unwrap().unwrap();
        assert_eq!(legacy.event_id, "$legacy");
        assert_eq!(legacy.kind, "voice");
        assert!(store.release("$legacy").unwrap());
        assert!(store
            .enqueue_image(
                "$image",
                "!default:example.org",
                "diagram.png",
                "image/png",
                b"png-bytes",
            )
            .unwrap());
        cleanup(&path);
    }

    #[test]
    fn persists_and_claims_image_bytes_as_base64() {
        let path = temporary_db("image");
        cleanup(&path);
        let store = StateStore::open(
            &path,
            "!default:example.org",
            &["!default:example.org".to_owned()],
        )
        .unwrap();
        assert!(store
            .enqueue_image(
                "$image",
                "!default:example.org",
                "diagram.png",
                "image/png",
                b"png-bytes",
            )
            .unwrap());
        let event = store.claim(None).unwrap().unwrap();
        assert_eq!(event.kind, "image");
        let image = event.image.unwrap();
        assert_eq!(image.media_type, "image/png");
        assert_eq!(image.data, "cG5nLWJ5dGVz");
        store.complete("$image", "reply:$image", "$out").unwrap();
        let connection = Connection::open(&path).unwrap();
        let retained_bytes: Option<Vec<u8>> = connection
            .query_row(
                "SELECT image_data FROM inbound WHERE event_id='$image'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retained_bytes, None);
        cleanup(&path);
    }

    #[test]
    fn room_scoped_claims_are_isolated() {
        let path = temporary_db("room-scope");
        cleanup(&path);
        let store = StateStore::open(
            &path,
            "!default:example.org",
            &[
                "!default:example.org".to_owned(),
                "!ea:example.org".to_owned(),
            ],
        )
        .unwrap();
        store
            .enqueue("$one", "!default:example.org", "general", "text")
            .unwrap();
        store
            .enqueue("$two", "!ea:example.org", "enterprise", "text")
            .unwrap();

        let scoped = store.claim(Some("!ea:example.org")).unwrap().unwrap();
        assert_eq!(scoped.event_id, "$two");
        assert_eq!(store.claim(Some("!ea:example.org")).unwrap(), None);

        let general = store.claim(Some("!default:example.org")).unwrap().unwrap();
        assert_eq!(general.event_id, "$one");

        cleanup(&path);
    }

    #[test]
    fn releases_and_completes_idempotently() {
        let path = temporary_db("complete");
        cleanup(&path);
        let store = StateStore::open(
            &path,
            "!default:example.org",
            &["!default:example.org".to_owned()],
        )
        .unwrap();
        store
            .enqueue("$one", "!default:example.org", "first", "text")
            .unwrap();
        store.claim(None).unwrap();
        assert!(store.release("$one").unwrap());
        store.claim(None).unwrap();
        store.complete("$one", "reply:$one", "$out").unwrap();
        assert_eq!(
            store.outbound_event("reply:$one").unwrap().as_deref(),
            Some("$out")
        );
        assert_eq!(store.counts().unwrap(), (0, 0, 1));
        cleanup(&path);
    }

    #[test]
    fn startup_requeues_claimed_event() {
        let path = temporary_db("restart");
        cleanup(&path);
        {
            let store = StateStore::open(
                &path,
                "!default:example.org",
                &["!default:example.org".to_owned()],
            )
            .unwrap();
            store
                .enqueue("$one", "!default:example.org", "first", "voice")
                .unwrap();
            store.claim(None).unwrap();
        }
        let reopened = StateStore::open(
            &path,
            "!default:example.org",
            &["!default:example.org".to_owned()],
        )
        .unwrap();
        assert_eq!(reopened.claim(None).unwrap().unwrap().event_id, "$one");
        cleanup(&path);
    }
}
