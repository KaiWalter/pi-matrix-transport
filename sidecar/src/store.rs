use std::{
    path::Path,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct InboundEvent {
    pub event_id: String,
    pub body: String,
}

#[derive(Debug)]
pub struct StateStore {
    connection: Mutex<Connection>,
}

impl StateStore {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create state directory {}", parent.display()))?;
        }
        let connection = Connection::open(path)
            .with_context(|| format!("open state database {}", path.display()))?;
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
             );",
        )?;
        let store = Self {
            connection: Mutex::new(connection),
        };
        store.requeue_claims()?;
        Ok(store)
    }

    pub fn enqueue(&self, event_id: &str, body: &str) -> Result<bool> {
        if event_id.is_empty() || body.trim().is_empty() {
            bail!("event id and body must be non-empty");
        }
        let changed = self.connection.lock().expect("state database mutex poisoned").execute(
            "INSERT OR IGNORE INTO inbound(event_id, body, state, received_at) VALUES (?1, ?2, 'queued', ?3)",
            params![event_id, body, now_epoch()?],
        )?;
        Ok(changed == 1)
    }

    pub fn claim(&self) -> Result<Option<InboundEvent>> {
        let mut connection = self
            .connection
            .lock()
            .expect("state database mutex poisoned");
        let transaction = connection.transaction()?;
        let now = now_epoch()?;
        transaction.execute(
            "UPDATE inbound SET state='queued', claimed_at=NULL WHERE state='claimed' AND claimed_at <= ?1",
            [now - 300],
        )?;
        let event = transaction
            .query_row(
                "SELECT event_id, body FROM inbound WHERE state='queued' ORDER BY sequence LIMIT 1",
                [],
                |row| {
                    Ok(InboundEvent {
                        event_id: row.get(0)?,
                        body: row.get(1)?,
                    })
                },
            )
            .optional()?;
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
        let changed = self.connection.lock().expect("state database mutex poisoned").execute(
            "UPDATE inbound SET state='queued', claimed_at=NULL WHERE event_id=?1 AND state='claimed'",
            [event_id],
        )?;
        Ok(changed == 1)
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
            "UPDATE inbound SET state='completed', claimed_at=NULL, outbound_event_id=?2 WHERE event_id=?1 AND state IN ('claimed','completed')",
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

fn now_epoch() -> Result<i64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::StateStore;

    fn temporary_db(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "pi-matrix-transport-{name}-{}.sqlite",
            std::process::id()
        ))
    }

    #[test]
    fn deduplicates_and_orders_inbound_events() {
        let path = temporary_db("dedup");
        let _ = std::fs::remove_file(&path);
        let store = StateStore::open(&path).unwrap();
        assert!(store.enqueue("$z-first", "first").unwrap());
        assert!(!store.enqueue("$z-first", "duplicate").unwrap());
        assert!(store.enqueue("$a-second", "second").unwrap());
        assert_eq!(store.claim().unwrap().unwrap().event_id, "$z-first");
        assert_eq!(store.claim().unwrap().unwrap().event_id, "$a-second");
        assert!(store.claim().unwrap().is_none());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn releases_and_completes_idempotently() {
        let path = temporary_db("complete");
        let _ = std::fs::remove_file(&path);
        let store = StateStore::open(&path).unwrap();
        store.enqueue("$one", "first").unwrap();
        store.claim().unwrap();
        assert!(store.release("$one").unwrap());
        store.claim().unwrap();
        store.complete("$one", "reply:$one", "$out").unwrap();
        assert_eq!(
            store.outbound_event("reply:$one").unwrap().as_deref(),
            Some("$out")
        );
        assert_eq!(store.counts().unwrap(), (0, 0, 1));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn startup_requeues_claimed_event() {
        let path = temporary_db("restart");
        let _ = std::fs::remove_file(&path);
        {
            let store = StateStore::open(&path).unwrap();
            store.enqueue("$one", "first").unwrap();
            store.claim().unwrap();
        }
        let reopened = StateStore::open(&path).unwrap();
        assert_eq!(reopened.claim().unwrap().unwrap().event_id, "$one");
        let _ = std::fs::remove_file(path);
    }
}
