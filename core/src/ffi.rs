//! Public API surface of zao-transfer-core.
//!
//! Two consumers:
//! 1. Tauri (Windows) - calls these functions directly as normal Rust,
//!    since Tauri apps are themselves Rust binaries. No FFI marshaling needed.
//! 2. Android (Kotlin) - calls through the `android` module below, which
//!    wraps these same functions with JNI signatures.
//!
//! Keeping a plain-Rust API here (instead of putting logic inside the
//! JNI functions) means the Tauri app and the Android bridge share
//! identical behavior -- the JNI layer is a thin translation shim only.

use crate::connection_manager::{ConnectionManager, EventSink};
use crate::discovery::Discovery;
use crate::identity::DeviceIdentity;
use crate::protocol::{FileOffer, ProtocolMessage, TextMessage};
use crate::storage::Storage;
use crate::transport::QuicTransport;
use once_cell::sync::{Lazy, OnceCell};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Process-wide async runtime. Both Android (via JNI, which is called
/// from Java threads with no Tokio context) and the Tauri shell need a
/// runtime to actually drive QUIC/mDNS futures. One shared multi-thread
/// runtime for the whole app's lifetime is simplest and avoids spinning
/// up/tearing down runtimes per call.
static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime")
});

/// Every event the connection manager can raise, flattened into one enum
/// for simple JSON polling from either shell. Both Kotlin and the Tauri
/// UI already poll `discoverPeers` on a timer (Milestone 2); this follows
/// the same pattern rather than introducing a native callback/listener
/// bridge, which would need separate implementations for JNI vs Tauri's
/// event emitter. Simpler to keep one polling model for both.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", content = "data")]
pub enum AppEvent {
    TextMessage {
        from_device_id: String,
        message_id: String,
        conversation_id: String,
        body: String,
        sent_at_unix: u64,
    },
    FileOffer {
        from_device_id: String,
        transfer_id: String,
        file_name: String,
        file_size: u64,
        mime_type: String,
    },
    FileAccept {
        from_device_id: String,
        transfer_id: String,
    },
    FileReject {
        from_device_id: String,
        transfer_id: String,
        reason: String,
    },
    FolderOffer {
        from_device_id: String,
        folder_batch_id: String,
        folder_name: String,
        total_files: u64,
        total_size: u64,
        files: Vec<crate::protocol::FolderFileEntry>,
    },
    FolderAccept {
        from_device_id: String,
        folder_batch_id: String,
    },
    FolderReject {
        from_device_id: String,
        folder_batch_id: String,
        reason: String,
    },
    TransferProgress(crate::transfer::TransferProgress),
    TransferComplete {
        transfer_id: String,
    },
    TransferCancelled {
        transfer_id: String,
        by_device_id: String,
    },
    Typing {
        conversation_id: String,
        is_typing: bool,
    },
    ReadReceipt {
        message_id: String,
        read_at_unix: u64,
    },
    DeliveryAck {
        message_id: String,
        delivered_at_unix: u64,
    },
    Presence {
        device_id: String,
        online: bool,
    },
    PeerConnected {
        device_id: String,
    },
    PeerDisconnected {
        device_id: String,
    },
}

/// Simple in-memory queue EventSink. Events accumulate here until the UI
/// polls and drains them via `drain_events`. Bounded implicitly by how
/// often the UI polls -- fine for chat/status-scale event volume; would
/// need a real cap if this ever queued raw chunk data (it doesn't --
/// FileChunk payloads are written straight to disk in connection_manager
/// and never routed through this queue).
struct QueueSink {
    events: std::sync::Mutex<Vec<AppEvent>>,
}

impl QueueSink {
    fn new() -> Self {
        Self {
            events: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn drain(&self) -> Vec<AppEvent> {
        let mut guard = self.events.lock().unwrap();
        std::mem::take(&mut *guard)
    }

    fn push(&self, event: AppEvent) {
        self.events.lock().unwrap().push(event);
    }
}

impl EventSink for QueueSink {
    fn on_text_message(&self, from_device_id: &str, msg: &TextMessage) {
        self.push(AppEvent::TextMessage {
            from_device_id: from_device_id.to_string(),
            message_id: msg.message_id.clone(),
            conversation_id: msg.conversation_id.clone(),
            body: msg.body.clone(),
            sent_at_unix: msg.sent_at_unix,
        });
    }

    fn on_file_offer(&self, from_device_id: &str, offer: &FileOffer) {
        self.push(AppEvent::FileOffer {
            from_device_id: from_device_id.to_string(),
            transfer_id: offer.transfer_id.clone(),
            file_name: offer.file_name.clone(),
            file_size: offer.file_size,
            mime_type: offer.mime_type.clone(),
        });
    }

    fn on_file_accept(&self, from_device_id: &str, transfer_id: &str) {
        self.push(AppEvent::FileAccept {
            from_device_id: from_device_id.to_string(),
            transfer_id: transfer_id.to_string(),
        });
    }

    fn on_file_reject(&self, from_device_id: &str, transfer_id: &str, reason: &str) {
        self.push(AppEvent::FileReject {
            from_device_id: from_device_id.to_string(),
            transfer_id: transfer_id.to_string(),
            reason: reason.to_string(),
        });
    }

    fn on_folder_offer(&self, from_device_id: &str, offer: &crate::protocol::FolderOffer) {
        self.push(AppEvent::FolderOffer {
            from_device_id: from_device_id.to_string(),
            folder_batch_id: offer.folder_batch_id.clone(),
            folder_name: offer.folder_name.clone(),
            total_files: offer.total_files,
            total_size: offer.total_size,
            files: offer.files.clone(),
        });
    }

    fn on_folder_accept(&self, from_device_id: &str, folder_batch_id: &str) {
        self.push(AppEvent::FolderAccept {
            from_device_id: from_device_id.to_string(),
            folder_batch_id: folder_batch_id.to_string(),
        });
    }

    fn on_folder_reject(&self, from_device_id: &str, folder_batch_id: &str, reason: &str) {
        self.push(AppEvent::FolderReject {
            from_device_id: from_device_id.to_string(),
            folder_batch_id: folder_batch_id.to_string(),
            reason: reason.to_string(),
        });
    }

    fn on_transfer_progress(&self, progress: &crate::transfer::TransferProgress) {
        self.push(AppEvent::TransferProgress(progress.clone()));
    }

    fn on_transfer_complete(&self, transfer_id: &str) {
        self.push(AppEvent::TransferComplete {
            transfer_id: transfer_id.to_string(),
        });
    }

    fn on_transfer_cancelled(&self, transfer_id: &str, by_device_id: &str) {
        self.push(AppEvent::TransferCancelled {
            transfer_id: transfer_id.to_string(),
            by_device_id: by_device_id.to_string(),
        });
    }

    fn on_typing(&self, conversation_id: &str, is_typing: bool) {
        self.push(AppEvent::Typing {
            conversation_id: conversation_id.to_string(),
            is_typing,
        });
    }

    fn on_read_receipt(&self, message_id: &str, read_at_unix: u64) {
        self.push(AppEvent::ReadReceipt {
            message_id: message_id.to_string(),
            read_at_unix,
        });
    }

    fn on_delivery_ack(&self, message_id: &str, delivered_at_unix: u64) {
        self.push(AppEvent::DeliveryAck {
            message_id: message_id.to_string(),
            delivered_at_unix,
        });
    }

    fn on_presence(&self, from_device_id: &str, online: bool) {
        self.push(AppEvent::Presence {
            device_id: from_device_id.to_string(),
            online,
        });
    }

    fn on_peer_connected(&self, device_id: &str) {
        self.push(AppEvent::PeerConnected {
            device_id: device_id.to_string(),
        });
    }

    fn on_peer_disconnected(&self, device_id: &str) {
        self.push(AppEvent::PeerDisconnected {
            device_id: device_id.to_string(),
        });
    }
}

/// Holds all live process-wide state: discovery, QUIC transport, the
/// connection manager (Noise sessions + message routing), the event
/// queue, and the encrypted storage handle. This is necessarily global
/// state: JNI calls are independent, stateless function invocations from
/// the Kotlin side, so anything that must persist between calls (e.g.
/// "start discovery" then later "send text") has to live somewhere the
/// FFI boundary can reach back into.
struct Session {
    storage: Arc<Mutex<Storage>>,
    discovery: Discovery,
    connection_manager: Arc<ConnectionManager>,
    sink: Arc<QueueSink>,
}

static SESSION: OnceCell<Session> = OnceCell::new();

fn session() -> Result<&'static Session, String> {
    SESSION
        .get()
        .ok_or_else(|| "session not started; call start_networking first".to_string())
}

#[derive(Serialize)]
pub struct IdentityInfo {
    pub device_id: String,
    pub public_key_hex: String,
}

impl From<&DeviceIdentity> for IdentityInfo {
    fn from(id: &DeviceIdentity) -> Self {
        IdentityInfo {
            device_id: id.device_id.clone(),
            public_key_hex: hex::encode(id.verifying_key().as_bytes()),
        }
    }
}

/// Open (or create) the encrypted local database and ensure a device
/// identity exists, generating one on first run. Returns identity info
/// as JSON for the calling shell (Kotlin/Tauri) to display/store as needed.
pub fn init_app(db_path: &str, db_key: &str) -> Result<String, String> {
    let storage = Storage::open(db_path, db_key).map_err(|e| e.to_string())?;
    let identity = storage
        .load_or_create_identity()
        .map_err(|e| e.to_string())?;
    let info = IdentityInfo::from(&identity);
    serde_json::to_string(&info).map_err(|e| e.to_string())
}

/// Return the current device's identity info without mutating anything.
/// Fails if init_app has not been called yet (no identity row present).
pub fn get_identity(db_path: &str, db_key: &str) -> Result<String, String> {
    let storage = Storage::open(db_path, db_key).map_err(|e| e.to_string())?;
    let identity = storage
        .load_identity()
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "no identity found; call init_app first".to_string())?;
    let info = IdentityInfo::from(&identity);
    serde_json::to_string(&info).map_err(|e| e.to_string())
}

/// Start LAN discovery (mDNS + UDP broadcast), bind the QUIC listener,
/// and spin up the connection manager (Noise session handling + message
/// routing). Must be called once after init_app, before discover_peers/
/// connect_to_peer/send_text are used. Idempotent: calling twice is a
/// no-op returning success, since OnceCell only allows first-write-wins.
///
/// `downloads_dir` is where incoming file chunks are written -- pass a
/// platform-appropriate writable directory (app-private files dir on
/// Android, %APPDATA%\ZaoP2P\downloads on Windows).
pub fn start_networking(db_path: &str, db_key: &str, display_name: &str, downloads_dir: &str) -> Result<String, String> {
    if SESSION.get().is_some() {
        return Ok(r#"{"status":"already_started"}"#.to_string());
    }

    let storage = Storage::open(db_path, db_key).map_err(|e| e.to_string())?;
    let identity = storage
        .load_or_create_identity()
        .map_err(|e| e.to_string())?;
    let device_id = identity.device_id.clone();
    let storage = Arc::new(Mutex::new(storage));

    std::fs::create_dir_all(downloads_dir).map_err(|e| e.to_string())?;

    // Bind QUIC first so we know the real port to advertise via discovery.
    let transport = QuicTransport::bind("0.0.0.0:0").map_err(|e| e.to_string())?;
    let quic_port = transport.local_addr.port();

    let discovery =
        Discovery::start(&device_id, display_name, quic_port).map_err(|e| e.to_string())?;

    let sink = Arc::new(QueueSink::new());
    let connection_manager = Arc::new(ConnectionManager::new(
        identity,
        Arc::new(transport),
        sink.clone(),
        PathBuf::from(downloads_dir),
        storage.clone(),
    ));
    connection_manager.spawn_accept_loop();

    let session = Session {
        storage,
        discovery,
        connection_manager,
        sink,
    };

    // If another thread raced us and already set it, that's fine --
    // treat it the same as "already started".
    if SESSION.set(session).is_err() {
        return Ok(r#"{"status":"already_started"}"#.to_string());
    }

    serde_json::to_string(&serde_json::json!({
        "status": "started",
        "quic_port": quic_port,
        "device_id": device_id
    }))
    .map_err(|e| e.to_string())
}

/// Return the current list of discovered LAN peers as a JSON array.
/// Safe to call frequently (e.g. every 1-2s from a UI polling loop) --
/// this only reads an in-memory map, no network I/O happens here.
pub fn discover_peers() -> Result<String, String> {
    let sess = session()?;
    let peers = sess.discovery.known_peers();
    serde_json::to_string(&peers).map_err(|e| e.to_string())
}

/// Connect to a peer at the given address (from a DiscoveredPeer entry)
/// and perform the Noise handshake. Safe to call even if already
/// connected -- connection_manager treats it as a no-op in that case.
/// Runs synchronously from the caller's perspective (blocks this call
/// on the shared runtime) since Kotlin/Tauri call sites expect a
/// synchronous-looking function that returns once the outcome is known.
pub fn connect_to_peer(addr: &str, expected_device_id: &str) -> Result<String, String> {
    let sess = session()?;
    let socket_addr: std::net::SocketAddr = addr.parse().map_err(|e| format!("bad addr: {e}"))?;
    let cm = sess.connection_manager.clone();
    let expected = expected_device_id.to_string();

    RUNTIME
        .block_on(async move { cm.connect_to_peer(socket_addr, &expected).await })
        .map_err(|e| e.to_string())?;

    Ok(r#"{"status":"connected"}"#.to_string())
}

/// Connect to a signaling server for internet-mode connection
/// establishment. Optional -- LAN-only usage never needs to call this.
/// `url` should be a `wss://host/path` endpoint (or `ws://` for local
/// testing against a self-hosted server); no signaling server is
/// bundled with this app, see README for what to deploy.
pub fn connect_signaling_server(url: &str) -> Result<String, String> {
    let sess = session()?;
    let cm = sess.connection_manager.clone();
    let url = url.to_string();
    RUNTIME
        .block_on(async move { cm.connect_signaling_server(&url).await })
        .map_err(|e| e.to_string())?;
    Ok(r#"{"status":"connected"}"#.to_string())
}

/// Attempt to reach a peer that was NOT discovered on the local network
/// (i.e. they're on a different network/over the internet), via STUN +
/// signaling-relayed candidates. Requires connect_signaling_server to
/// have succeeded first. This call returns once candidates have been
/// offered -- the actual connection, if it succeeds, arrives
/// asynchronously as a PeerConnected event (same event used for LAN
/// connections), since hole-punch timing depends on both sides'
/// signaling round-trips completing.
pub fn connect_to_peer_via_internet(peer_device_id: &str) -> Result<String, String> {
    let sess = session()?;
    let cm = sess.connection_manager.clone();
    let peer_id = peer_device_id.to_string();
    RUNTIME
        .block_on(async move { cm.connect_to_peer_via_internet(&peer_id).await })
        .map_err(|e| e.to_string())?;
    Ok(r#"{"status":"candidates_offered"}"#.to_string())
}

/// Send a plain text message to a connected peer, persisting it locally
/// first (so it shows in the sender's own chat history immediately,
/// with status "pending" until a DeliveryAck event arrives) then
/// transmitting it over the peer's live Noise/QUIC session.
pub fn send_text_message(peer_device_id: &str, body: &str) -> Result<String, String> {
    let sess = session()?;
    let conversation_id = {
        let storage = sess.storage.blocking_lock();
        storage
            .get_or_create_conversation(peer_device_id)
            .map_err(|e| e.to_string())?
    };

    let message_id = uuid::Uuid::new_v4().to_string();
    let sent_at_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    {
        let storage = sess.storage.blocking_lock();
        storage
            .insert_message(
                &message_id,
                &conversation_id,
                "self", // sender_device_id "self" marks messages authored locally
                body,
                "text",
                sent_at_unix,
                "pending",
            )
            .map_err(|e| e.to_string())?;
    }

    let protocol_msg = ProtocolMessage::Text(TextMessage {
        message_id: message_id.clone(),
        conversation_id: conversation_id.clone(),
        body: body.to_string(),
        sent_at_unix,
    });

    let cm = sess.connection_manager.clone();
    let peer_id = peer_device_id.to_string();
    RUNTIME
        .block_on(async move { cm.send_to(&peer_id, &protocol_msg).await })
        .map_err(|e| e.to_string())?;

    serde_json::to_string(&serde_json::json!({
        "status": "sent",
        "message_id": message_id
    }))
    .map_err(|e| e.to_string())
}

/// Drain and return all events accumulated since the last call (new
/// incoming messages, typing indicators, receipts, presence changes,
/// transfer progress). Intended to be polled on a timer from the UI,
/// same pattern as discover_peers.
pub fn poll_events() -> Result<String, String> {
    let sess = session()?;
    let events = sess.sink.drain();
    serde_json::to_string(&events).map_err(|e| e.to_string())
}

/// Load persisted chat history for a conversation (peer_device_id doubles
/// as conversation_id in this 1:1-only milestone), oldest message first.
pub fn get_conversation_history(peer_device_id: &str, limit: u32) -> Result<String, String> {
    let sess = session()?;
    let messages = {
        let storage = sess.storage.blocking_lock();
        storage
            .load_conversation_history(peer_device_id, limit)
            .map_err(|e| e.to_string())?
    };
    serde_json::to_string(&messages).map_err(|e| e.to_string())
}

/// Send a typing indicator to a peer. Fire-and-forget: failures (e.g.
/// peer not connected) are swallowed since a missed typing indicator
/// is not worth surfacing as an error to the UI.
pub fn send_typing_indicator(peer_device_id: &str, conversation_id: &str, is_typing: bool) -> Result<String, String> {
    let sess = session()?;
    let cm = sess.connection_manager.clone();
    let peer_id = peer_device_id.to_string();
    let conv_id = conversation_id.to_string();
    let msg = ProtocolMessage::TypingIndicator {
        conversation_id: conv_id,
        is_typing,
    };
    let _ = RUNTIME.block_on(async move { cm.send_to(&peer_id, &msg).await });
    Ok(r#"{"status":"ok"}"#.to_string())
}

/// Mark a message read locally and notify the sender via a ReadReceipt.
pub fn mark_message_read(peer_device_id: &str, message_id: &str) -> Result<String, String> {
    let sess = session()?;
    let read_at_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    {
        let storage = sess.storage.blocking_lock();
        storage
            .mark_message_read(message_id, read_at_unix)
            .map_err(|e| e.to_string())?;
    }

    let cm = sess.connection_manager.clone();
    let peer_id = peer_device_id.to_string();
    let msg_id = message_id.to_string();
    let msg = ProtocolMessage::ReadReceipt {
        message_id: msg_id,
        read_at_unix,
    };
    let _ = RUNTIME.block_on(async move { cm.send_to(&peer_id, &msg).await });
    Ok(r#"{"status":"ok"}"#.to_string())
}

/// Report which peer device_ids currently have a live, authenticated
/// session -- this is what "online status" is derived from.
pub fn connected_peers() -> Result<String, String> {
    let sess = session()?;
    let cm = sess.connection_manager.clone();
    let ids = RUNTIME.block_on(async move { cm.connected_device_ids().await });
    serde_json::to_string(&ids).map_err(|e| e.to_string())
}

/// Report this device's own QUIC listen port + known/connected peer
/// counts, useful for diagnostics/UI display ("your device is
/// discoverable as ...").
pub fn networking_status() -> Result<String, String> {
    let sess = session()?;
    let cm = sess.connection_manager.clone();
    let connected_count = RUNTIME.block_on(async move { cm.connected_device_ids().await.len() });
    serde_json::to_string(&serde_json::json!({
        "quic_local_addr": sess.connection_manager.local_addr().to_string(),
        "known_peer_count": sess.discovery.known_peers().len(),
        "connected_peer_count": connected_count,
    }))
    .map_err(|e| e.to_string())
}

/// Offer a file to a connected peer, reading it from a real filesystem
/// path. The file at `file_path` must exist and be readable. Persists
/// a local message row (type "file") so it shows in chat history
/// immediately, alongside the outgoing offer.
/// Returns JSON: {"status":"offered","transfer_id":"..."}
pub fn send_file(peer_device_id: &str, file_path: &str, conversation_id: &str) -> Result<String, String> {
    let sess = session()?;
    let message_id = uuid::Uuid::new_v4().to_string();
    let sent_at_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let path = std::path::Path::new(file_path);
    let file_name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| file_path.to_string());
    let mime_type = crate::connection_manager::mime_guess_from_extension(path);
    let file_size = std::fs::metadata(path)
        .map_err(|e| format!("could not read file metadata: {e}"))?
        .len();

    {
        let storage = sess.storage.blocking_lock();
        storage
            .insert_message(
                &message_id,
                conversation_id,
                "self",
                &file_name,
                "file",
                sent_at_unix,
                "pending",
            )
            .map_err(|e| e.to_string())?;
    }

    let cm = sess.connection_manager.clone();
    let peer_id = peer_device_id.to_string();
    let source = crate::transfer::FileSource::Path(PathBuf::from(file_path));
    let msg_id = message_id.clone();
    let conv_id = conversation_id.to_string();
    let fname = file_name.clone();
    let mtype = mime_type.clone();

    let transfer_id = RUNTIME
        .block_on(async move {
            cm.send_file(&peer_id, source, fname, file_size, mtype, msg_id, conv_id, String::new(), None)
                .await
        })
        .map_err(|e| e.to_string())?;

    serde_json::to_string(&serde_json::json!({
        "status": "offered",
        "transfer_id": transfer_id,
        "message_id": message_id
    }))
    .map_err(|e| e.to_string())
}

/// Offer a file to a connected peer, reading it through an already-open
/// raw file descriptor -- the Android path for content:// URIs (e.g.
/// files picked via ACTION_OPEN_DOCUMENT), which have no real
/// filesystem path Rust can open directly. The Kotlin side is
/// responsible for opening the fd (via
/// `ContentResolver.openFileDescriptor(uri, "r")`) BEFORE calling this,
/// and MUST keep that ParcelFileDescriptor open for the entire duration
/// of the transfer -- closing it early will cause chunk reads to start
/// failing partway through, since each chunk worker reopens the fd via
/// `/proc/self/fd/{fd}` on demand rather than holding one persistent
/// handle (see `transfer::read_chunk_from_source`'s doc comment for why).
///
/// `file_name`/`file_size`/`mime_type` must be supplied explicitly since
/// a raw fd has no filename to derive them from -- the Kotlin side
/// already has these from `ContentResolver.query`/`openAssetFileDescriptor`
/// before this is called.
#[allow(clippy::too_many_arguments)]
pub fn send_file_fd(
    peer_device_id: &str,
    fd: i32,
    file_name: &str,
    file_size: u64,
    mime_type: &str,
    conversation_id: &str,
) -> Result<String, String> {
    let sess = session()?;
    let message_id = uuid::Uuid::new_v4().to_string();
    let sent_at_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    {
        let storage = sess.storage.blocking_lock();
        storage
            .insert_message(
                &message_id,
                conversation_id,
                "self",
                file_name,
                "file",
                sent_at_unix,
                "pending",
            )
            .map_err(|e| e.to_string())?;
    }

    let cm = sess.connection_manager.clone();
    let peer_id = peer_device_id.to_string();
    let source = crate::transfer::FileSource::Fd(fd);
    let msg_id = message_id.clone();
    let conv_id = conversation_id.to_string();
    let fname = file_name.to_string();
    let mtype = mime_type.to_string();

    let transfer_id = RUNTIME
        .block_on(async move {
            cm.send_file(&peer_id, source, fname, file_size, mtype, msg_id, conv_id, String::new(), None)
                .await
        })
        .map_err(|e| e.to_string())?;

    serde_json::to_string(&serde_json::json!({
        "status": "offered",
        "transfer_id": transfer_id,
        "message_id": message_id
    }))
    .map_err(|e| e.to_string())
}

/// Offer an entire folder to a peer. Walks `folder_path` recursively
/// and sends one FileOffer per file found, all sharing one
/// folder_batch_id so the UI can group them. Also persists one local
/// message row per file (type "file"), same as send_file, so folder
/// contents show up in chat history individually.
pub fn send_folder(peer_device_id: &str, folder_path: &str, conversation_id: &str) -> Result<String, String> {
    let sess = session()?;
    let cm = sess.connection_manager.clone();
    let peer_id = peer_device_id.to_string();
    let path = PathBuf::from(folder_path);
    let conv_id = conversation_id.to_string();

    let results = RUNTIME
        .block_on(async move { cm.send_folder(&peer_id, path, conv_id).await })
        .map_err(|e| e.to_string())?;

    let sent_at_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    {
        let storage = sess.storage.blocking_lock();
        for (relative_path, _transfer_id) in &results {
            let message_id = uuid::Uuid::new_v4().to_string();
            if let Err(e) = storage.insert_message(
                &message_id,
                conversation_id,
                "self",
                relative_path,
                "file",
                sent_at_unix,
                "pending",
            ) {
                eprintln!("[zao] failed to persist folder file message row: {e}");
            }
        }
    }

    let files_json: Vec<serde_json::Value> = results
        .into_iter()
        .map(|(relative_path, transfer_id)| {
            serde_json::json!({ "relative_path": relative_path, "transfer_id": transfer_id })
        })
        .collect();

    serde_json::to_string(&serde_json::json!({
        "status": "offered",
        "files": files_json
    }))
    .map_err(|e| e.to_string())
}

/// Accept an incoming file offer (from a FileOffer AppEvent) and begin
/// receiving it. Returns the local destination path once accepted.
pub fn accept_file(
    from_device_id: &str,
    transfer_id: &str,
    message_id: &str,
    file_name: &str,
    file_size: u64,
    mime_type: &str,
) -> Result<String, String> {
    let sess = session()?;
    let cm = sess.connection_manager.clone();
    let offer = FileOffer {
        transfer_id: transfer_id.to_string(),
        message_id: message_id.to_string(),
        conversation_id: from_device_id.to_string(),
        file_name: file_name.to_string(),
        file_size,
        mime_type: mime_type.to_string(),
        total_chunks: file_size.div_ceil(crate::transfer::CHUNK_SIZE),
        chunk_size: crate::transfer::CHUNK_SIZE,
        relative_path: String::new(),
        folder_batch_id: None,
    };
    let from_id = from_device_id.to_string();

    let dest_path = RUNTIME
        .block_on(async move { cm.accept_file(&from_id, &offer).await })
        .map_err(|e| e.to_string())?;

    serde_json::to_string(&serde_json::json!({
        "status": "accepted",
        "dest_path": dest_path.to_string_lossy()
    }))
    .map_err(|e| e.to_string())
}

pub fn reject_file(from_device_id: &str, transfer_id: &str, reason: &str) -> Result<String, String> {
    let sess = session()?;
    let cm = sess.connection_manager.clone();
    let from_id = from_device_id.to_string();
    let tid = transfer_id.to_string();
    let r = reason.to_string();
    RUNTIME
        .block_on(async move { cm.reject_file(&from_id, &tid, &r).await })
        .map_err(|e| e.to_string())?;
    Ok(r#"{"status":"rejected"}"#.to_string())
}

/// Accept a batched folder offer -- sends one FolderAccept covering
/// every file in the batch. Individual FileOffers will still arrive
/// afterward for each file; the UI is expected to auto-accept those
/// (via accept_file) since the user already made the batch-level
/// decision here.
pub fn accept_folder(from_device_id: &str, folder_batch_id: &str) -> Result<String, String> {
    let sess = session()?;
    let cm = sess.connection_manager.clone();
    let from_id = from_device_id.to_string();
    let batch_id = folder_batch_id.to_string();
    RUNTIME
        .block_on(async move { cm.accept_folder(&from_id, &batch_id).await })
        .map_err(|e| e.to_string())?;
    Ok(r#"{"status":"accepted"}"#.to_string())
}

pub fn reject_folder(from_device_id: &str, folder_batch_id: &str, reason: &str) -> Result<String, String> {
    let sess = session()?;
    let cm = sess.connection_manager.clone();
    let from_id = from_device_id.to_string();
    let batch_id = folder_batch_id.to_string();
    let r = reason.to_string();
    RUNTIME
        .block_on(async move { cm.reject_folder(&from_id, &batch_id, &r).await })
        .map_err(|e| e.to_string())?;
    Ok(r#"{"status":"rejected"}"#.to_string())
}

/// Pause an active outgoing transfer by ID. No-op if not currently
/// tracked (e.g. already completed, or app restarted since it began --
/// resume-after-restart for an in-progress send isn't supported in this
/// milestone; only receiver-side resume via the chunk manifest is).
pub fn pause_transfer(transfer_id: &str) -> Result<String, String> {
    let sess = session()?;
    let cm = sess.connection_manager.clone();
    let tid = transfer_id.to_string();
    RUNTIME.block_on(async move { cm.pause_outgoing_transfer(&tid).await });
    Ok(r#"{"status":"ok"}"#.to_string())
}

pub fn resume_transfer(transfer_id: &str) -> Result<String, String> {
    let sess = session()?;
    let cm = sess.connection_manager.clone();
    let tid = transfer_id.to_string();
    RUNTIME.block_on(async move { cm.resume_outgoing_transfer(&tid).await });
    Ok(r#"{"status":"ok"}"#.to_string())
}

/// Cancel a transfer (sender or receiver side) and notify the peer.
pub fn cancel_transfer(peer_device_id: &str, transfer_id: &str) -> Result<String, String> {
    let sess = session()?;
    let cm = sess.connection_manager.clone();
    let peer_id = peer_device_id.to_string();
    let tid = transfer_id.to_string();
    RUNTIME
        .block_on(async move { cm.cancel_transfer(&peer_id, &tid).await })
        .map_err(|e| e.to_string())?;
    Ok(r#"{"status":"ok"}"#.to_string())
}

/// Poll progress for a transfer from the DB (authoritative across
/// restarts, unlike an in-memory-only handle). Returns a JSON object
/// with total/transferred bytes and derived percent, or an error if the
/// transfer_id has no row at all.
pub fn get_transfer_progress(transfer_id: &str) -> Result<String, String> {
    let sess = session()?;
    let storage = sess.storage.blocking_lock();
    let (file_name, total_bytes, _local_path) = storage
        .load_transfer_meta(transfer_id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("transfer {transfer_id} not found"))?;
    let acked = storage
        .load_acked_chunks(transfer_id)
        .map_err(|e| e.to_string())?;
    let bytes_transferred = (acked.len() as u64 * crate::transfer::CHUNK_SIZE).min(total_bytes);
    let percent = if total_bytes == 0 {
        100.0
    } else {
        (bytes_transferred as f64 / total_bytes as f64 * 100.0) as f32
    };
    serde_json::to_string(&serde_json::json!({
        "transfer_id": transfer_id,
        "file_name": file_name,
        "total_bytes": total_bytes,
        "bytes_transferred": bytes_transferred,
        "percent": percent
    }))
    .map_err(|e| e.to_string())
}

/// Expose the shared runtime so higher-level send/receive orchestration
/// (wired up in Milestone 3 alongside the chat UI, once we have a
/// concrete message protocol on top of raw chunk frames) can spawn onto
/// it. Not exposed over JNI directly -- Rust-only, for Tauri and future
/// internal use.
pub fn runtime() -> &'static tokio::runtime::Runtime {
    &RUNTIME
}

/// Encrypt a plaintext payload for BLE mesh transmission to a specific
/// recipient, using the stateless sealed_box scheme (see
/// identity.rs::sealed_box for why this differs from the QUIC path's
/// stateful Noise session -- a flooding mesh can't rely on ordered
/// delivery). `recipient_public_key_hex` should be a previously-known
/// peer's public key, looked up via `get_known_device_public_key` or
/// obtained during an earlier LAN/internet pairing.
pub fn ble_seal_message(recipient_public_key_hex: &str, plaintext: &str) -> Result<String, String> {
    let key_bytes = hex::decode(recipient_public_key_hex).map_err(|e| e.to_string())?;
    let key_array: [u8; 32] = key_bytes
        .try_into()
        .map_err(|_| "recipient public key must be exactly 32 bytes".to_string())?;
    let sealed = crate::identity::sealed_box::seal(&key_array, plaintext.as_bytes())
        .map_err(|e| e.to_string())?;
    Ok(hex::encode(sealed))
}

/// Decrypt a BLE mesh message using this device's own identity (loaded
/// from the encrypted local DB, same as init_app/get_identity).
pub fn ble_open_message(db_path: &str, db_key: &str, sealed_hex: &str) -> Result<String, String> {
    let storage = Storage::open(db_path, db_key).map_err(|e| e.to_string())?;
    let identity = storage
        .load_identity()
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "no identity found; call init_app first".to_string())?;
    let sealed_bytes = hex::decode(sealed_hex).map_err(|e| e.to_string())?;
    let plaintext = crate::identity::sealed_box::open(&identity.noise_static, &sealed_bytes)
        .map_err(|e| e.to_string())?;
    String::from_utf8(plaintext).map_err(|e| e.to_string())
}

/// Look up a previously-known device's public key (hex-encoded), for
/// use with ble_seal_message. Returns an error if this device hasn't
/// been seen/paired before -- BLE mesh can only securely message peers
/// whose public key is already known from a prior LAN/internet
/// connection (sealed_box has no in-band handshake to learn it fresh,
/// unlike Noise_XX over QUIC).
pub fn get_known_device_public_key(peer_device_id: &str) -> Result<String, String> {
    let sess = session()?;
    let storage = sess.storage.blocking_lock();
    let key_bytes = storage
        .load_device_public_key(peer_device_id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no known public key for device {peer_device_id}"))?;
    Ok(hex::encode(key_bytes))
}

// ---------------------------------------------------------------------
// Android JNI bindings
// ---------------------------------------------------------------------
#[cfg(target_os = "android")]
pub mod android {
    use super::*;
    use jni::objects::{JClass, JString};
    use jni::sys::jstring;
    use jni::JNIEnv;

    fn jstring_to_string(env: &mut JNIEnv, s: &JString) -> String {
        env.get_string(s)
            .expect("invalid UTF-8 from Java string")
            .into()
    }

    fn string_to_jstring(env: &JNIEnv, s: String) -> jstring {
        env.new_string(s)
            .expect("failed to allocate Java string")
            .into_raw()
    }

    /// Java signature:
    /// external fun initApp(dbPath: String, dbKey: String): String
    /// Class expected: com.zao.p2p.core.NativeBridge (adjust package as needed)
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_initApp(
        mut env: JNIEnv,
        _class: JClass,
        db_path: JString,
        db_key: JString,
    ) -> jstring {
        let db_path = jstring_to_string(&mut env, &db_path);
        let db_key = jstring_to_string(&mut env, &db_key);

        let result = match init_app(&db_path, &db_key) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun getIdentity(dbPath: String, dbKey: String): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_getIdentity(
        mut env: JNIEnv,
        _class: JClass,
        db_path: JString,
        db_key: JString,
    ) -> jstring {
        let db_path = jstring_to_string(&mut env, &db_path);
        let db_key = jstring_to_string(&mut env, &db_key);

        let result = match get_identity(&db_path, &db_key) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun startNetworking(dbPath: String, dbKey: String, displayName: String, downloadsDir: String): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_startNetworking(
        mut env: JNIEnv,
        _class: JClass,
        db_path: JString,
        db_key: JString,
        display_name: JString,
        downloads_dir: JString,
    ) -> jstring {
        let db_path = jstring_to_string(&mut env, &db_path);
        let db_key = jstring_to_string(&mut env, &db_key);
        let display_name = jstring_to_string(&mut env, &display_name);
        let downloads_dir = jstring_to_string(&mut env, &downloads_dir);

        let result = match start_networking(&db_path, &db_key, &display_name, &downloads_dir) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun connectToPeer(addr: String, expectedDeviceId: String): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_connectToPeer(
        mut env: JNIEnv,
        _class: JClass,
        addr: JString,
        expected_device_id: JString,
    ) -> jstring {
        let addr = jstring_to_string(&mut env, &addr);
        let expected_device_id = jstring_to_string(&mut env, &expected_device_id);
        let result = match connect_to_peer(&addr, &expected_device_id) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun connectSignalingServer(url: String): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_connectSignalingServer(
        mut env: JNIEnv,
        _class: JClass,
        url: JString,
    ) -> jstring {
        let url = jstring_to_string(&mut env, &url);
        let result = match connect_signaling_server(&url) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun connectToPeerViaInternet(peerDeviceId: String): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_connectToPeerViaInternet(
        mut env: JNIEnv,
        _class: JClass,
        peer_device_id: JString,
    ) -> jstring {
        let peer_device_id = jstring_to_string(&mut env, &peer_device_id);
        let result = match connect_to_peer_via_internet(&peer_device_id) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun bleSealMessage(recipientPublicKeyHex: String, plaintext: String): String
    /// Returns hex-encoded sealed bytes on success, or a JSON error object.
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_bleSealMessage(
        mut env: JNIEnv,
        _class: JClass,
        recipient_public_key_hex: JString,
        plaintext: JString,
    ) -> jstring {
        let recipient_public_key_hex = jstring_to_string(&mut env, &recipient_public_key_hex);
        let plaintext = jstring_to_string(&mut env, &plaintext);
        let result = match ble_seal_message(&recipient_public_key_hex, &plaintext) {
            Ok(hex) => hex,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun bleOpenMessage(dbPath: String, dbKey: String, sealedHex: String): String
    /// Returns the decrypted plaintext string on success, or a JSON error object.
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_bleOpenMessage(
        mut env: JNIEnv,
        _class: JClass,
        db_path: JString,
        db_key: JString,
        sealed_hex: JString,
    ) -> jstring {
        let db_path = jstring_to_string(&mut env, &db_path);
        let db_key = jstring_to_string(&mut env, &db_key);
        let sealed_hex = jstring_to_string(&mut env, &sealed_hex);
        let result = match ble_open_message(&db_path, &db_key, &sealed_hex) {
            Ok(plaintext) => plaintext,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun getKnownDevicePublicKey(peerDeviceId: String): String
    /// Returns a hex-encoded public key string on success, or a JSON error object.
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_getKnownDevicePublicKey(
        mut env: JNIEnv,
        _class: JClass,
        peer_device_id: JString,
    ) -> jstring {
        let peer_device_id = jstring_to_string(&mut env, &peer_device_id);
        let result = match get_known_device_public_key(&peer_device_id) {
            Ok(hex) => hex,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun sendTextMessage(peerDeviceId: String, body: String): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_sendTextMessage(
        mut env: JNIEnv,
        _class: JClass,
        peer_device_id: JString,
        body: JString,
    ) -> jstring {
        let peer_device_id = jstring_to_string(&mut env, &peer_device_id);
        let body = jstring_to_string(&mut env, &body);
        let result = match send_text_message(&peer_device_id, &body) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun pollEvents(): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_pollEvents(
        env: JNIEnv,
        _class: JClass,
    ) -> jstring {
        let result = match poll_events() {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun getConversationHistory(peerDeviceId: String, limit: Int): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_getConversationHistory(
        mut env: JNIEnv,
        _class: JClass,
        peer_device_id: JString,
        limit: jni::sys::jint,
    ) -> jstring {
        let peer_device_id = jstring_to_string(&mut env, &peer_device_id);
        let result = match get_conversation_history(&peer_device_id, limit.max(0) as u32) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun sendTypingIndicator(peerDeviceId: String, conversationId: String, isTyping: Boolean): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_sendTypingIndicator(
        mut env: JNIEnv,
        _class: JClass,
        peer_device_id: JString,
        conversation_id: JString,
        is_typing: jni::sys::jboolean,
    ) -> jstring {
        let peer_device_id = jstring_to_string(&mut env, &peer_device_id);
        let conversation_id = jstring_to_string(&mut env, &conversation_id);
        let result = match send_typing_indicator(&peer_device_id, &conversation_id, is_typing != 0) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun markMessageRead(peerDeviceId: String, messageId: String): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_markMessageRead(
        mut env: JNIEnv,
        _class: JClass,
        peer_device_id: JString,
        message_id: JString,
    ) -> jstring {
        let peer_device_id = jstring_to_string(&mut env, &peer_device_id);
        let message_id = jstring_to_string(&mut env, &message_id);
        let result = match mark_message_read(&peer_device_id, &message_id) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun connectedPeers(): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_connectedPeers(
        env: JNIEnv,
        _class: JClass,
    ) -> jstring {
        let result = match connected_peers() {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun discoverPeers(): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_discoverPeers(
        env: JNIEnv,
        _class: JClass,
    ) -> jstring {
        let result = match discover_peers() {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun networkingStatus(): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_networkingStatus(
        env: JNIEnv,
        _class: JClass,
    ) -> jstring {
        let result = match networking_status() {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun pauseTransfer(transferId: String): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_pauseTransfer(
        mut env: JNIEnv,
        _class: JClass,
        transfer_id: JString,
    ) -> jstring {
        let transfer_id = jstring_to_string(&mut env, &transfer_id);
        let result = match pause_transfer(&transfer_id) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun resumeTransfer(transferId: String): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_resumeTransfer(
        mut env: JNIEnv,
        _class: JClass,
        transfer_id: JString,
    ) -> jstring {
        let transfer_id = jstring_to_string(&mut env, &transfer_id);
        let result = match resume_transfer(&transfer_id) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun cancelTransfer(peerDeviceId: String, transferId: String): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_cancelTransfer(
        mut env: JNIEnv,
        _class: JClass,
        peer_device_id: JString,
        transfer_id: JString,
    ) -> jstring {
        let peer_device_id = jstring_to_string(&mut env, &peer_device_id);
        let transfer_id = jstring_to_string(&mut env, &transfer_id);
        let result = match cancel_transfer(&peer_device_id, &transfer_id) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun sendFile(peerDeviceId: String, filePath: String, conversationId: String): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_sendFile(
        mut env: JNIEnv,
        _class: JClass,
        peer_device_id: JString,
        file_path: JString,
        conversation_id: JString,
    ) -> jstring {
        let peer_device_id = jstring_to_string(&mut env, &peer_device_id);
        let file_path = jstring_to_string(&mut env, &file_path);
        let conversation_id = jstring_to_string(&mut env, &conversation_id);
        let result = match send_file(&peer_device_id, &file_path, &conversation_id) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun sendFileFd(peerDeviceId: String, fd: Int, fileName: String, fileSize: Long, mimeType: String, conversationId: String): String
    ///
    /// `fd` must come from `ParcelFileDescriptor.getFd()` on a
    /// descriptor opened via `ContentResolver.openFileDescriptor(uri, "r")`,
    /// and that ParcelFileDescriptor must stay open (not garbage
    /// collected / closed) for the entire duration of the transfer --
    /// see `send_file_fd`'s Rust doc comment for why.
    #[no_mangle]
    #[allow(clippy::too_many_arguments)]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_sendFileFd(
        mut env: JNIEnv,
        _class: JClass,
        peer_device_id: JString,
        fd: jni::sys::jint,
        file_name: JString,
        file_size: jni::sys::jlong,
        mime_type: JString,
        conversation_id: JString,
    ) -> jstring {
        let peer_device_id = jstring_to_string(&mut env, &peer_device_id);
        let file_name = jstring_to_string(&mut env, &file_name);
        let mime_type = jstring_to_string(&mut env, &mime_type);
        let conversation_id = jstring_to_string(&mut env, &conversation_id);
        let result = match send_file_fd(
            &peer_device_id,
            fd,
            &file_name,
            file_size.max(0) as u64,
            &mime_type,
            &conversation_id,
        ) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun sendFolder(peerDeviceId: String, folderPath: String, conversationId: String): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_sendFolder(
        mut env: JNIEnv,
        _class: JClass,
        peer_device_id: JString,
        folder_path: JString,
        conversation_id: JString,
    ) -> jstring {
        let peer_device_id = jstring_to_string(&mut env, &peer_device_id);
        let folder_path = jstring_to_string(&mut env, &folder_path);
        let conversation_id = jstring_to_string(&mut env, &conversation_id);
        let result = match send_folder(&peer_device_id, &folder_path, &conversation_id) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun acceptFile(fromDeviceId: String, transferId: String, messageId: String, fileName: String, fileSize: Long, mimeType: String): String
    #[no_mangle]
    #[allow(clippy::too_many_arguments)]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_acceptFile(
        mut env: JNIEnv,
        _class: JClass,
        from_device_id: JString,
        transfer_id: JString,
        message_id: JString,
        file_name: JString,
        file_size: jni::sys::jlong,
        mime_type: JString,
    ) -> jstring {
        let from_device_id = jstring_to_string(&mut env, &from_device_id);
        let transfer_id = jstring_to_string(&mut env, &transfer_id);
        let message_id = jstring_to_string(&mut env, &message_id);
        let file_name = jstring_to_string(&mut env, &file_name);
        let mime_type = jstring_to_string(&mut env, &mime_type);
        let result = match accept_file(
            &from_device_id,
            &transfer_id,
            &message_id,
            &file_name,
            file_size.max(0) as u64,
            &mime_type,
        ) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun rejectFile(fromDeviceId: String, transferId: String, reason: String): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_rejectFile(
        mut env: JNIEnv,
        _class: JClass,
        from_device_id: JString,
        transfer_id: JString,
        reason: JString,
    ) -> jstring {
        let from_device_id = jstring_to_string(&mut env, &from_device_id);
        let transfer_id = jstring_to_string(&mut env, &transfer_id);
        let reason = jstring_to_string(&mut env, &reason);
        let result = match reject_file(&from_device_id, &transfer_id, &reason) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun acceptFolder(fromDeviceId: String, folderBatchId: String): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_acceptFolder(
        mut env: JNIEnv,
        _class: JClass,
        from_device_id: JString,
        folder_batch_id: JString,
    ) -> jstring {
        let from_device_id = jstring_to_string(&mut env, &from_device_id);
        let folder_batch_id = jstring_to_string(&mut env, &folder_batch_id);
        let result = match accept_folder(&from_device_id, &folder_batch_id) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun rejectFolder(fromDeviceId: String, folderBatchId: String, reason: String): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_rejectFolder(
        mut env: JNIEnv,
        _class: JClass,
        from_device_id: JString,
        folder_batch_id: JString,
        reason: JString,
    ) -> jstring {
        let from_device_id = jstring_to_string(&mut env, &from_device_id);
        let folder_batch_id = jstring_to_string(&mut env, &folder_batch_id);
        let reason = jstring_to_string(&mut env, &reason);
        let result = match reject_folder(&from_device_id, &folder_batch_id, &reason) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }

    /// Java signature:
    /// external fun getTransferProgress(transferId: String): String
    #[no_mangle]
    pub extern "system" fn Java_com_zao_p2p_core_NativeBridge_getTransferProgress(
        mut env: JNIEnv,
        _class: JClass,
        transfer_id: JString,
    ) -> jstring {
        let transfer_id = jstring_to_string(&mut env, &transfer_id);
        let result = match get_transfer_progress(&transfer_id) {
            Ok(json) => json,
            Err(e) => format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
        };
        string_to_jstring(&env, result)
    }
}
