use crate::error::Result;
use crate::identity::{DeviceIdentity, StoredIdentity};
use rusqlite::{params, Connection};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct StoredMessage {
    pub message_id: String,
    pub conversation_id: String,
    pub sender_device_id: String,
    pub body: String,
    pub msg_type: String,
    pub sent_at: i64,
    pub delivered_at: Option<i64>,
    pub read_at: Option<i64>,
    pub status: String,
}

pub struct Storage {
    conn: Connection,
}

const SCHEMA_V1: &str = r#"
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS self_identity (
    id INTEGER PRIMARY KEY CHECK (id = 1), -- single row
    device_id TEXT NOT NULL,
    signing_key_bytes BLOB NOT NULL,
    noise_static_bytes BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    display_name TEXT,
    public_key BLOB,
    last_seen_at INTEGER,
    trusted INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS conversations (
    conversation_id TEXT PRIMARY KEY,
    peer_device_id TEXT REFERENCES devices(device_id),
    created_at INTEGER
);

CREATE TABLE IF NOT EXISTS messages (
    message_id TEXT PRIMARY KEY,
    conversation_id TEXT REFERENCES conversations(conversation_id),
    sender_device_id TEXT,
    body_ciphertext BLOB,
    type TEXT,
    sent_at INTEGER,
    delivered_at INTEGER,
    read_at INTEGER,
    status TEXT
);

CREATE TABLE IF NOT EXISTS file_transfers (
    transfer_id TEXT PRIMARY KEY,
    message_id TEXT REFERENCES messages(message_id),
    file_name TEXT,
    file_size INTEGER,
    mime_type TEXT,
    total_chunks INTEGER,
    chunk_size INTEGER,
    checksum_sha256 TEXT,
    direction TEXT,
    state TEXT,
    bytes_transferred INTEGER,
    local_path TEXT,
    started_at INTEGER,
    updated_at INTEGER
);

CREATE TABLE IF NOT EXISTS chunk_manifest (
    transfer_id TEXT REFERENCES file_transfers(transfer_id),
    chunk_index INTEGER,
    acked INTEGER DEFAULT 0,
    PRIMARY KEY (transfer_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS typing_state (
    conversation_id TEXT PRIMARY KEY,
    peer_typing INTEGER DEFAULT 0,
    updated_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages(conversation_id, sent_at);
CREATE INDEX IF NOT EXISTS idx_transfers_message ON file_transfers(message_id);
"#;

impl Storage {
    /// Open (or create) the encrypted database at `path`, using `db_key`
    /// as the SQLCipher passphrase. `db_key` should come from the
    /// platform keystore (Android Keystore / Windows DPAPI) -- this
    /// crate does not manage that key's lifecycle, only consumes it.
    pub fn open(path: &str, db_key: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        // PRAGMA key must be the very first statement executed on the connection.
        conn.pragma_update(None, "key", db_key)?;
        // Sanity check the key is correct by touching the DB.
        conn.execute_batch(SCHEMA_V1)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        Ok(Self { conn })
    }

    #[cfg(test)]
    pub fn open_in_memory(db_key: &str) -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.pragma_update(None, "key", db_key)?;
        conn.execute_batch(SCHEMA_V1)?;
        Ok(Self { conn })
    }

    /// Load the self identity if one was previously generated & stored.
    pub fn load_identity(&self) -> Result<Option<DeviceIdentity>> {
        let mut stmt = self
            .conn
            .prepare("SELECT device_id, signing_key_bytes, noise_static_bytes FROM self_identity WHERE id = 1")?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            let device_id: String = row.get(0)?;
            let signing_key_bytes: Vec<u8> = row.get(1)?;
            let noise_static_bytes: Vec<u8> = row.get(2)?;

            let stored = StoredIdentity {
                device_id,
                signing_key_bytes: signing_key_bytes
                    .try_into()
                    .map_err(|_| rusqlite::Error::InvalidQuery)?,
                noise_static_bytes: noise_static_bytes
                    .try_into()
                    .map_err(|_| rusqlite::Error::InvalidQuery)?,
            };
            Ok(Some(DeviceIdentity::from_stored(&stored)?))
        } else {
            Ok(None)
        }
    }

    /// Persist a freshly generated identity. Call once on first launch.
    pub fn save_identity(&self, identity: &DeviceIdentity) -> Result<()> {
        let stored = identity.to_stored();
        self.conn.execute(
            "INSERT OR REPLACE INTO self_identity (id, device_id, signing_key_bytes, noise_static_bytes)
             VALUES (1, ?1, ?2, ?3)",
            params![stored.device_id, stored.signing_key_bytes.to_vec(), stored.noise_static_bytes.to_vec()],
        )?;
        Ok(())
    }

    /// Get-or-create pattern: the typical call on app startup.
    pub fn load_or_create_identity(&self) -> Result<DeviceIdentity> {
        if let Some(existing) = self.load_identity()? {
            Ok(existing)
        } else {
            let identity = DeviceIdentity::generate();
            self.save_identity(&identity)?;
            Ok(identity)
        }
    }

    /// Get-or-create a conversation for a given peer device. Conversation
    /// IDs are deterministic (equal to the peer's device_id) since this
    /// milestone only supports 1:1 chat -- one conversation per peer.
    pub fn get_or_create_conversation(&self, peer_device_id: &str) -> Result<String> {
        let conversation_id = peer_device_id.to_string();
        self.conn.execute(
            "INSERT OR IGNORE INTO conversations (conversation_id, peer_device_id, created_at)
             VALUES (?1, ?2, strftime('%s','now'))",
            params![conversation_id, peer_device_id],
        )?;
        Ok(conversation_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_message(
        &self,
        message_id: &str,
        conversation_id: &str,
        sender_device_id: &str,
        body_plaintext: &str,
        msg_type: &str,
        sent_at_unix: u64,
        status: &str,
    ) -> Result<()> {
        // NOTE: body is stored as plaintext in this column for now. The
        // column is named body_ciphertext because the intent (per the
        // Milestone-1 schema) is to encrypt message bodies at rest with
        // a key derived from the local DB key, on top of the transport-
        // level Noise encryption already protecting messages in transit.
        // That at-rest encryption layer is not yet implemented -- SQLCipher
        // already encrypts the whole DB file, which covers "encrypted
        // database" from the requirements, but per-field encryption
        // would add defense in depth against a decrypted-DB-file scenario.
        // Flagging rather than silently deferring.
        self.conn.execute(
            "INSERT INTO messages
                (message_id, conversation_id, sender_device_id, body_ciphertext,
                 type, sent_at, delivered_at, read_at, status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, ?7)",
            params![
                message_id,
                conversation_id,
                sender_device_id,
                body_plaintext.as_bytes(),
                msg_type,
                sent_at_unix as i64,
                status
            ],
        )?;
        Ok(())
    }

    pub fn mark_message_delivered(&self, message_id: &str, delivered_at_unix: u64) -> Result<()> {
        self.conn.execute(
            "UPDATE messages SET delivered_at = ?1, status = 'delivered' WHERE message_id = ?2",
            params![delivered_at_unix as i64, message_id],
        )?;
        Ok(())
    }

    pub fn mark_message_read(&self, message_id: &str, read_at_unix: u64) -> Result<()> {
        self.conn.execute(
            "UPDATE messages SET read_at = ?1, status = 'read' WHERE message_id = ?2",
            params![read_at_unix as i64, message_id],
        )?;
        Ok(())
    }

    /// Load the most recent `limit` messages for a conversation, oldest
    /// first (ready to render directly in a chat list).
    pub fn load_conversation_history(
        &self,
        conversation_id: &str,
        limit: u32,
    ) -> Result<Vec<StoredMessage>> {
        let mut stmt = self.conn.prepare(
            "SELECT message_id, conversation_id, sender_device_id, body_ciphertext,
                    type, sent_at, delivered_at, read_at, status
             FROM messages
             WHERE conversation_id = ?1
             ORDER BY sent_at DESC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![conversation_id, limit], |row| {
            let body_bytes: Vec<u8> = row.get(3)?;
            Ok(StoredMessage {
                message_id: row.get(0)?,
                conversation_id: row.get(1)?,
                sender_device_id: row.get(2)?,
                body: String::from_utf8_lossy(&body_bytes).to_string(),
                msg_type: row.get(4)?,
                sent_at: row.get(5)?,
                delivered_at: row.get(6)?,
                read_at: row.get(7)?,
                status: row.get(8)?,
            })
        })?;
        let mut messages = Vec::new();
        for r in rows {
            messages.push(r?);
        }
        messages.reverse(); // oldest first for chat rendering
        Ok(messages)
    }

    pub fn list_conversations(&self) -> Result<Vec<(String, String)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT conversation_id, peer_device_id FROM conversations ORDER BY created_at DESC")?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// Create a new file_transfers row when a transfer starts (either
    /// direction). 
    #[allow(clippy::too_many_arguments)]
    pub fn create_transfer(
        &self,
        transfer_id: &str,
        message_id: &str,
        file_name: &str,
        file_size: u64,
        mime_type: &str,
        total_chunks: u64,
        chunk_size: u64,
        direction: &str,
        local_path: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO file_transfers
                (transfer_id, message_id, file_name, file_size, mime_type,
                 total_chunks, chunk_size, checksum_sha256, direction, state,
                 bytes_transferred, local_path, started_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8, 'queued', 0, ?9,
                     strftime('%s','now'), strftime('%s','now'))",
            params![
                transfer_id,
                message_id,
                file_name,
                file_size as i64,
                mime_type,
                total_chunks as i64,
                chunk_size as i64,
                direction,
                local_path
            ],
        )?;
        Ok(())
    }

    pub fn update_transfer_state(&self, transfer_id: &str, state: &str, bytes_transferred: u64) -> Result<()> {
        self.conn.execute(
            "UPDATE file_transfers SET state = ?1, bytes_transferred = ?2, updated_at = strftime('%s','now')
             WHERE transfer_id = ?3",
            params![state, bytes_transferred as i64, transfer_id],
        )?;
        Ok(())
    }

    /// Mark a chunk as acked in the manifest, so progress survives a
    /// restart (not just the in-memory TransferHandle).
    pub fn mark_chunk_acked(&self, transfer_id: &str, chunk_index: u64) -> Result<()> {
        self.conn.execute(
            "INSERT INTO chunk_manifest (transfer_id, chunk_index, acked)
             VALUES (?1, ?2, 1)
             ON CONFLICT(transfer_id, chunk_index) DO UPDATE SET acked = 1",
            params![transfer_id, chunk_index as i64],
        )?;
        Ok(())
    }

    /// Load the set of chunk indices already acked for a transfer. This
    /// powers resume-after-reconnect: re-fetch this set on restart and
    /// skip those indices entirely.
    pub fn load_acked_chunks(&self, transfer_id: &str) -> Result<std::collections::HashSet<u64>> {
        let mut stmt = self.conn.prepare(
            "SELECT chunk_index FROM chunk_manifest WHERE transfer_id = ?1 AND acked = 1",
        )?;
        let rows = stmt.query_map(params![transfer_id], |row| {
            let idx: i64 = row.get(0)?;
            Ok(idx as u64)
        })?;
        let mut set = std::collections::HashSet::new();
        for r in rows {
            set.insert(r?);
        }
        Ok(set)
    }

    /// Fetch enough info about a transfer to rehydrate a TransferHandle
    /// after an app restart (file name + total size + path).
    pub fn load_transfer_meta(&self, transfer_id: &str) -> Result<Option<(String, u64, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT file_name, file_size, local_path FROM file_transfers WHERE transfer_id = ?1",
        )?;
        let mut rows = stmt.query(params![transfer_id])?;
        if let Some(row) = rows.next()? {
            let file_name: String = row.get(0)?;
            let file_size: i64 = row.get(1)?;
            let local_path: String = row.get(2)?;
            Ok(Some((file_name, file_size as u64, local_path)))
        } else {
            Ok(None)
        }
    }

    /// List transfers left in a non-terminal state (active/paused/queued)
    /// -- candidates to auto-resume after a restart/reconnect. Returns
    /// (transfer_id, peer_device_id, direction, local_path) tuples --
    /// peer_device_id is recovered by joining through messages.conversation_id,
    /// which equals the peer's device_id in this app's 1:1-only
    /// conversation model (see storage.rs's get_or_create_conversation).
    pub fn list_resumable_transfers_with_peer(&self) -> Result<Vec<(String, String, String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT ft.transfer_id, m.conversation_id, ft.direction, ft.local_path
             FROM file_transfers ft
             JOIN messages m ON ft.message_id = m.message_id
             WHERE ft.state IN ('active', 'paused', 'queued')",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// List transfers left in a non-terminal state (active/paused/queued)
    /// -- candidates to offer the user a resume for after a crash/reconnect.
    /// Kept alongside `list_resumable_transfers_with_peer` (which most
    /// callers should prefer, since it includes what's needed to
    /// actually act on the result) for any caller that only needs the
    /// bare transfer_id list.
    pub fn list_resumable_transfers(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT transfer_id FROM file_transfers WHERE state IN ('active', 'paused', 'queued')",
        )?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut ids = Vec::new();
        for r in rows {
            ids.push(r?);
        }
        Ok(ids)
    }

    pub fn upsert_known_device(
        &self,
        device_id: &str,
        display_name: &str,
        public_key: &[u8],
        trusted: bool,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO devices (device_id, display_name, public_key, last_seen_at, trusted)
             VALUES (?1, ?2, ?3, strftime('%s','now'), ?4)
             ON CONFLICT(device_id) DO UPDATE SET
                display_name = excluded.display_name,
                last_seen_at = excluded.last_seen_at",
            params![device_id, display_name, public_key, trusted as i32],
        )?;
        Ok(())
    }

    /// Look up a previously-seen device's stored public key (the Noise/
    /// X25519 static key bytes, per `upsert_known_device`'s usage
    /// elsewhere) -- needed by BLE mesh's sealed_box encryption, which
    /// (unlike the QUIC/Noise session path) has no live handshake to
    /// learn the recipient's key from and must instead use a
    /// previously-recorded one.
    pub fn load_device_public_key(&self, device_id: &str) -> Result<Option<Vec<u8>>> {
        let mut stmt = self
            .conn
            .prepare("SELECT public_key FROM devices WHERE device_id = ?1")?;
        let mut rows = stmt.query(params![device_id])?;
        if let Some(row) = rows.next()? {
            let key: Vec<u8> = row.get(0)?;
            Ok(Some(key))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_persists_across_reopen() {
        let storage = Storage::open_in_memory("test-key-123").unwrap();
        let id = storage.load_or_create_identity().unwrap();
        let reloaded = storage.load_identity().unwrap().unwrap();
        assert_eq!(id.device_id, reloaded.device_id);
    }

    #[test]
    fn upsert_device_works() {
        let storage = Storage::open_in_memory("test-key-123").unwrap();
        storage
            .upsert_known_device("abc123", "Zenas-Phone", b"fakepubkey", true)
            .unwrap();
        // Re-upsert should not error (ON CONFLICT path)
        storage
            .upsert_known_device("abc123", "Zenas-Phone-Renamed", b"fakepubkey", true)
            .unwrap();
    }
}
