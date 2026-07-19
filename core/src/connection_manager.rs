use crate::error::{CoreError, Result};
use crate::identity::DeviceIdentity;
use crate::noise_session::{NoiseSession, Role};
use crate::protocol::{FileChunkMessage, FileOffer, ProtocolMessage};
use crate::signaling_client::{SignalingClient, SignalingEvent, SignalingSessionTracker};
use crate::storage::Storage;
use crate::stun_client;
use crate::transfer::{
    preallocate_file, read_chunk_from_source, write_chunk_to_file, ChunkFrame, FileSource,
    TransferHandle, TransferProgress, TransferState, CHUNK_SIZE, DEFAULT_PARALLELISM,
};
use crate::transport::{accept_stream, open_stream, QuicTransport};
use quinn::Connection;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex};

/// Callbacks the app layer (FFI) registers so incoming protocol messages
/// can reach the UI. Kept as a trait rather than a concrete struct so
/// both the Android/JNI event path and Tauri's event emitter can each
/// provide their own implementation without duplicating PeerSession.
pub trait EventSink: Send + Sync {
    fn on_text_message(&self, from_device_id: &str, msg: &crate::protocol::TextMessage);
    fn on_file_offer(&self, from_device_id: &str, offer: &FileOffer);
    fn on_file_accept(&self, from_device_id: &str, transfer_id: &str);
    fn on_file_reject(&self, from_device_id: &str, transfer_id: &str, reason: &str);
    fn on_folder_offer(&self, from_device_id: &str, offer: &crate::protocol::FolderOffer);
    fn on_folder_accept(&self, from_device_id: &str, folder_batch_id: &str);
    fn on_folder_reject(&self, from_device_id: &str, folder_batch_id: &str, reason: &str);
    fn on_transfer_progress(&self, progress: &TransferProgress);
    fn on_transfer_complete(&self, transfer_id: &str);
    fn on_transfer_cancelled(&self, transfer_id: &str, by_device_id: &str);
    fn on_typing(&self, conversation_id: &str, is_typing: bool);
    fn on_read_receipt(&self, message_id: &str, read_at_unix: u64);
    fn on_delivery_ack(&self, message_id: &str, delivered_at_unix: u64);
    fn on_presence(&self, from_device_id: &str, online: bool);
    fn on_peer_connected(&self, device_id: &str);
    fn on_peer_disconnected(&self, device_id: &str);
}

/// Where a PeerSession's bytes actually travel once Noise-encrypted.
/// Two variants exist because a relayed connection (internet mode,
/// when direct hole-punching fails on both sides -- see
/// `connect_to_peer_via_internet` and the signaling relay path) has NO
/// QUIC connection at all; it only has a signaling WebSocket and a
/// session_id the relay server uses to forward bytes to the other
/// device. Everything above this enum (Noise encryption, ProtocolMessage
/// dispatch, chat/transfer logic) is IDENTICAL regardless of which
/// variant is active -- only the final "how do these encrypted bytes
/// actually leave this device" step differs.
enum OutboundPath {
    Quic {
        connection: Connection,
        control_tx: mpsc::UnboundedSender<Vec<u8>>,
        transfer_senders: Mutex<HashMap<String, mpsc::UnboundedSender<ProtocolMessage>>>,
    },
    Relay {
        signaling: Arc<SignalingClient>,
        session_id: String,
    },
}

/// One established, authenticated session with a peer device: either a
/// live QUIC connection or a relay session (see `OutboundPath`), plus
/// the Noise transport state used to encrypt/decrypt every message and
/// chunk sent over it.
///
/// For the QUIC path specifically: a single QUIC connection is reused
/// for all traffic to a peer -- control messages (text, offers,
/// receipts) go over one dedicated bidirectional stream, while each
/// file transfer opens its own additional streams for parallel chunk
/// workers. This keeps chat snappy even while a large transfer is in
/// flight, since QUIC multiplexes streams independently (one slow/
/// congested stream doesn't block another). The relay path has no
/// equivalent stream multiplexing (everything goes through one
/// WebSocket connection to the signaling server), so relayed transfers
/// do not get the same parallel-chunk-stream isolation QUIC provides --
/// an inherent tradeoff of the relay being a last-resort fallback, not
/// a first-class transport with the same performance characteristics.
pub struct PeerSession {
    pub device_id: String,
    // The Noise transport state is mutable per-direction in some Noise
    // implementations; `snow`'s TransportState internally tracks
    // separate send/receive nonces, so a single shared, mutex-guarded
    // instance is sufficient and correct for both directions.
    noise: Arc<Mutex<NoiseSession>>,
    outbound: OutboundPath,
}

/// Handle for pushing chunk messages onto a transfer's dedicated stream.
/// Thin wrapper so call sites don't need to know it's backed by an
/// mpsc channel + Noise encryption underneath.
pub struct TransferStreamSender {
    tx: mpsc::UnboundedSender<ProtocolMessage>,
}

impl TransferStreamSender {
    pub async fn send(&self, msg: ProtocolMessage) -> Result<()> {
        self.tx
            .send(msg)
            .map_err(|_| CoreError::InvalidState("transfer stream closed".into()))
    }
}

impl PeerSession {
    pub async fn send_message(&self, msg: &ProtocolMessage) -> Result<()> {
        let plaintext = msg
            .encode_plaintext()
            .map_err(|e| CoreError::InvalidState(format!("encode failed: {e}")))?;
        let ciphertext = {
            let mut noise = self.noise.lock().await;
            noise.encrypt(&plaintext)?
        };
        match &self.outbound {
            OutboundPath::Quic { control_tx, .. } => control_tx
                .send(ciphertext)
                .map_err(|_| CoreError::InvalidState("control stream closed".into())),
            OutboundPath::Relay {
                signaling,
                session_id,
            } => signaling.send_relay_data(session_id, ciphertext),
        }
    }

    /// The underlying QUIC connection, if this session is QUIC-backed.
    /// Returns None for a relay-backed session (see `OutboundPath`) --
    /// callers that need this (parallel chunk stream setup) already
    /// fall back to sending chunks through `send_message` instead when
    /// this returns None; see `transfer_stream_sender`.
    pub fn quic_connection(&self) -> Option<&Connection> {
        match &self.outbound {
            OutboundPath::Quic { connection, .. } => Some(connection),
            OutboundPath::Relay { .. } => None,
        }
    }

    /// Get (or lazily open) a dedicated QUIC stream for one transfer, OR
    /// -- for a relay-backed session -- a lightweight wrapper that sends
    /// chunk messages through the same relay session as everything else
    /// (see `OutboundPath`'s doc comment on why relayed transfers don't
    /// get the same parallel-stream isolation QUIC provides; this is an
    /// accepted, documented tradeoff of the relay being a last-resort
    /// fallback).
    ///
    /// For the QUIC case: all chunk messages for a given transfer_id are
    /// serialized through this single stream's writer task, keeping
    /// bytes in order without needing per-chunk sequencing logic --
    /// QUIC/TCP-style in-order delivery on one stream already guarantees
    /// that. Multiple *different* transfers each get their own stream,
    /// which is what lets them progress independently and in parallel
    /// over the same underlying connection.
    ///
    /// Reuses the session's shared Noise transport state for encryption
    /// (not a separate handshake per stream) -- Noise's nonce counter is
    /// tracked per NoiseSession object, so all ciphertext for this peer
    /// must go through the same shared, mutex-guarded instance to stay
    /// decryptable in order on the receiving end, regardless of which
    /// stream or transport carries it.
    pub async fn transfer_stream_sender(&self, transfer_id: &str) -> Result<TransferStreamSender> {
        match &self.outbound {
            OutboundPath::Relay { .. } => {
                // No separate stream concept over a relay -- route chunk
                // messages through the same encrypt-and-send path as
                // control messages. TransferStreamSender wraps a channel
                // either way, so this still needs one: spawn a task that
                // forwards each ProtocolMessage to send_message, giving
                // callers the same API shape as the QUIC path.
                let (tx, mut rx) = mpsc::unbounded_channel::<ProtocolMessage>();
                let noise = self.noise.clone();
                let signaling = match &self.outbound {
                    OutboundPath::Relay { signaling, .. } => signaling.clone(),
                    OutboundPath::Quic { .. } => unreachable!(),
                };
                let session_id = match &self.outbound {
                    OutboundPath::Relay { session_id, .. } => session_id.clone(),
                    OutboundPath::Quic { .. } => unreachable!(),
                };
                tokio::spawn(async move {
                    while let Some(msg) = rx.recv().await {
                        let plaintext = match msg.encode_plaintext() {
                            Ok(p) => p,
                            Err(_) => continue,
                        };
                        let ciphertext = {
                            let mut n = noise.lock().await;
                            match n.encrypt(&plaintext) {
                                Ok(c) => c,
                                Err(_) => break,
                            }
                        };
                        if signaling.send_relay_data(&session_id, ciphertext).is_err() {
                            break;
                        }
                    }
                });
                Ok(TransferStreamSender { tx })
            }
            OutboundPath::Quic {
                connection,
                transfer_senders,
                ..
            } => {
                let mut senders = transfer_senders.lock().await;
                if let Some(existing) = senders.get(transfer_id) {
                    return Ok(TransferStreamSender {
                        tx: existing.clone(),
                    });
                }

                let (mut send, _recv) = crate::transport::open_stream(connection).await?;
                let (tx, mut rx) = mpsc::unbounded_channel::<ProtocolMessage>();
                let noise = self.noise.clone();

                tokio::spawn(async move {
                    while let Some(msg) = rx.recv().await {
                        let plaintext = match msg.encode_plaintext() {
                            Ok(p) => p,
                            Err(_) => continue,
                        };
                        let ciphertext = {
                            let mut n = noise.lock().await;
                            match n.encrypt(&plaintext) {
                                Ok(c) => c,
                                Err(_) => break,
                            }
                        };
                        if write_framed(&mut send, &ciphertext).await.is_err() {
                            break;
                        }
                    }
                });

                senders.insert(transfer_id.to_string(), tx.clone());
                Ok(TransferStreamSender { tx })
            }
        }
    }

    /// Drop the dedicated stream sender for a completed/cancelled
    /// transfer, so its background writer task winds down (channel
    /// closes once the sender is dropped and no clones remain) and the
    /// map doesn't grow unbounded across many transfers over a
    /// long-lived session. No-op for a relay-backed session, which has
    /// no per-transfer stream map to clean up.
    pub async fn close_transfer_stream(&self, transfer_id: &str) {
        if let OutboundPath::Quic {
            transfer_senders, ..
        } = &self.outbound
        {
            transfer_senders.lock().await.remove(transfer_id);
        }
    }
}

/// Tracks one file transfer this device is sending: the source path,
/// the handle used for pause/resume/cancel + progress, and a channel
/// that the offer/accept flow uses to signal "peer accepted, go ahead
/// and start sending chunks" (or reject/timeout).
struct OutgoingTransfer {
    file_source: FileSource,
    handle: TransferHandle,
    accept_tx: Option<tokio::sync::oneshot::Sender<bool>>,
}

/// Tracks one file transfer this device is receiving: destination path
/// and the handle used for progress/cancel tracking. Chunks arrive on a
/// dedicated stream (not the control stream) once accepted.
struct IncomingTransfer {
    dest_path: PathBuf,
    handle: TransferHandle,
}

/// Tracks a folder offer this device sent, awaiting the receiver's
/// single accept/reject decision for the whole batch (see
/// ProtocolMessage::FolderOffer's doc comment for why this negotiates
/// once instead of per-file).
struct PendingFolderOffer {
    accept_tx: Option<tokio::sync::oneshot::Sender<bool>>,
}

/// Manages all active peer sessions for this device: performing
/// outbound handshakes, accepting inbound ones, and routing decoded
/// protocol messages to the registered EventSink. One instance per
/// running app, held inside the FFI session registry.
pub struct ConnectionManager {
    identity: DeviceIdentity,
    transport: Arc<QuicTransport>,
    sessions: Arc<Mutex<HashMap<String, Arc<PeerSession>>>>,
    sink: Arc<dyn EventSink>,
    downloads_dir: PathBuf,
    outgoing_transfers: Arc<Mutex<HashMap<String, OutgoingTransfer>>>,
    incoming_transfers: Arc<Mutex<HashMap<String, IncomingTransfer>>>,
    pending_folder_offers: Arc<Mutex<HashMap<String, PendingFolderOffer>>>,
    // Guarded by a tokio Mutex (not a std Mutex) since it's held across
    // await points in some call paths -- rusqlite::Connection is Send
    // but not Sync, so this is the simplest correct way to share one
    // Storage handle across the async tasks spawned throughout this
    // manager (accept loop, per-peer readers, transfer workers).
    storage: Arc<Mutex<Storage>>,
    // Internet mode: set once connect_signaling_server is called.
    // None until then -- LAN-only usage (Milestones 1-4) never touches
    // this and continues to work with no signaling server configured.
    signaling: Mutex<Option<Arc<SignalingClient>>>,
    signaling_sessions: Arc<SignalingSessionTracker>,
    // Routes RelayData frames to an in-progress relay handshake (see
    // run_relay_handshake) before a PeerSession exists for that peer --
    // once the handshake completes, the session_id is removed here and
    // subsequent RelayData frames route through the normal
    // self.sessions lookup in handle_relay_data instead.
    relay_handshake_waiters: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<Vec<u8>>>>>,
}

impl ConnectionManager {
    pub fn new(
        identity: DeviceIdentity,
        transport: Arc<QuicTransport>,
        sink: Arc<dyn EventSink>,
        downloads_dir: PathBuf,
        storage: Arc<Mutex<Storage>>,
    ) -> Self {
        Self {
            identity,
            transport,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            sink,
            downloads_dir,
            outgoing_transfers: Arc::new(Mutex::new(HashMap::new())),
            incoming_transfers: Arc::new(Mutex::new(HashMap::new())),
            pending_folder_offers: Arc::new(Mutex::new(HashMap::new())),
            storage,
            signaling: Mutex::new(None),
            signaling_sessions: Arc::new(SignalingSessionTracker::new()),
            relay_handshake_waiters: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Background task: accept incoming QUIC connections and, for each,
    /// perform the responder side of a Noise_XX handshake before
    /// treating it as a live session. Spawn this once at startup.
    pub fn spawn_accept_loop(self: &Arc<Self>) {
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                match this.transport.accept().await {
                    Some(Ok(conn)) => {
                        let this2 = this.clone();
                        tokio::spawn(async move {
                            if let Err(e) = this2.handle_inbound_connection(conn).await {
                                eprintln!("[zao] inbound connection failed: {e}");
                            }
                        });
                    }
                    Some(Err(e)) => {
                        eprintln!("[zao] accept error: {e}");
                    }
                    None => break, // endpoint closed
                }
            }
        });
    }

    async fn handle_inbound_connection(self: Arc<Self>, conn: Connection) -> Result<()> {
        let (mut send, mut recv) = accept_stream(&conn).await?;
        let mut noise = NoiseSession::new(&self.identity, Role::Responder)?;

        // Noise_XX responder side: read e, write e/ee/s/es, read s/se.
        let msg1 = read_framed(&mut recv).await?;
        noise.read_handshake_message(&msg1)?;

        let msg2 = noise.write_handshake_message(&[])?;
        write_framed(&mut send, &msg2).await?;

        let msg3 = read_framed(&mut recv).await?;
        noise.read_handshake_message(&msg3)?;

        let peer_static = noise
            .peer_static_key()
            .ok_or_else(|| CoreError::Crypto("no remote static key after handshake".into()))?;
        let peer_device_id = crate::identity::DeviceIdentity::device_id_from_public_key(&peer_static);

        let noise = noise.into_transport_mode()?;
        self.register_session(peer_device_id, peer_static, conn, noise, send, recv)
            .await
    }

    /// Connect to a signaling server for internet-mode connection
    /// establishment. Safe to skip entirely for LAN-only usage --
    /// nothing in Milestones 1-4's flows requires this. `url` should be
    /// a `wss://` (TLS) endpoint in any real deployment; `ws://` works
    /// for local testing against a self-hosted server.
    pub async fn connect_signaling_server(self: &Arc<Self>, url: &str) -> Result<()> {
        let client = Arc::new(SignalingClient::connect(url, &self.identity.device_id).await?);
        *self.signaling.lock().await = Some(client.clone());

        let this = self.clone();
        tokio::spawn(async move {
            loop {
                match client.next_event().await {
                    Some(event) => this.handle_signaling_event(event, &client).await,
                    None => break, // signaling connection closed
                }
            }
        });

        Ok(())
    }

    async fn handle_signaling_event(self: &Arc<Self>, event: SignalingEvent, client: &Arc<SignalingClient>) {
        match event {
            SignalingEvent::Registered => {
                // Informational only -- nothing blocks on registration
                // completing since outbound sends queue regardless.
            }
            SignalingEvent::RegisterFailed { error } => {
                eprintln!("[zao] signaling registration failed: {error}");
            }
            SignalingEvent::IncomingCandidates {
                from_device_id,
                session_id,
                candidates,
            } => {
                self.signaling_sessions
                    .register(&session_id, &from_device_id)
                    .await;
                let direct_likely = self
                    .try_candidates(&from_device_id, &candidates)
                    .await
                    .is_ok();
                let _ = client.send(&crate::signaling::SignalingMessage::CandidateResult {
                    session_id,
                    direct_connection_likely: direct_likely,
                });
            }
            SignalingEvent::CandidateResult {
                session_id,
                direct_connection_likely,
            } => {
                if !direct_connection_likely {
                    // Peer couldn't reach us directly either -- fall back
                    // to relay for this session. If our own direct
                    // attempt (made when we sent the offer) already
                    // succeeded, this is a harmless redundant request;
                    // the relay path is only actually used if no
                    // PeerSession exists for this peer by the time data
                    // needs to flow.
                    let _ = client.request_relay(&session_id);
                }
            }
            SignalingEvent::RelayReady { session_id } => {
                self.activate_relay_session(session_id, client).await;
            }
            SignalingEvent::RelayData { session_id, data } => {
                self.handle_relay_data(&session_id, data).await;
            }
            SignalingEvent::RelayClosed { session_id } => {
                self.signaling_sessions.remove(&session_id).await;
            }
            SignalingEvent::Disconnected => {
                *self.signaling.lock().await = None;
            }
        }
    }

    /// Called when the signaling server confirms both sides of a
    /// session_id have requested a relay -- from here, this device acts
    /// as the Noise_XX INITIATOR over the relay (arbitrarily: whichever
    /// side's device_id sorts lexicographically first takes the
    /// initiator role, so both sides independently agree on who leads
    /// without a separate negotiation message -- this mirrors how the
    /// QUIC path's initiator/responder roles are determined by who
    /// dials vs who accepts, but a relay has no equivalent "who called
    /// first" signal, so a deterministic tie-breaker is needed instead).
    async fn activate_relay_session(self: &Arc<Self>, session_id: String, client: &Arc<SignalingClient>) {
        let peer_device_id = match self.signaling_sessions.peer_for_session(&session_id).await {
            Some(id) => id,
            None => {
                eprintln!("[zao] RelayReady for unknown session {session_id}, ignoring");
                return;
            }
        };

        if self.sessions.lock().await.contains_key(&peer_device_id) {
            return;
        }

        let is_initiator = self.identity.device_id < peer_device_id;
        let client = client.clone();
        let this = self.clone();
        let sid = session_id.clone();
        let peer_id = peer_device_id.clone();

        tokio::spawn(async move {
            if let Err(e) = this
                .run_relay_handshake(sid, peer_id, is_initiator, client)
                .await
            {
                eprintln!("[zao] relay handshake failed: {e}");
            }
        });
    }

    /// Performs a full Noise_XX handshake with the relay's forwarded
    /// bytes standing in for what would otherwise be QUIC stream reads/
    /// writes -- the handshake message FORMAT and SEQUENCE are
    /// identical to the QUIC path; only the transport carrying each
    /// handshake message differs (RelayData frames instead of a QUIC
    /// stream). This is what lets a relayed connection reach the exact
    /// same authenticated, encrypted PeerSession state as a direct one.
    async fn run_relay_handshake(
        self: Arc<Self>,
        session_id: String,
        peer_device_id: String,
        is_initiator: bool,
        client: Arc<SignalingClient>,
    ) -> Result<()> {
        let (rx_tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
        self.relay_handshake_waiters
            .lock()
            .await
            .insert(session_id.clone(), rx_tx);

        let mut noise = NoiseSession::new(
            &self.identity,
            if is_initiator {
                Role::Initiator
            } else {
                Role::Responder
            },
        )?;

        async fn recv_next(rx: &mut mpsc::UnboundedReceiver<Vec<u8>>) -> Result<Vec<u8>> {
            rx.recv()
                .await
                .ok_or_else(|| CoreError::InvalidState("relay closed during handshake".into()))
        }

        if is_initiator {
            let msg1 = noise.write_handshake_message(&[])?;
            client.send_relay_data(&session_id, msg1)?;

            let msg2 = recv_next(&mut rx).await?;
            noise.read_handshake_message(&msg2)?;

            let msg3 = noise.write_handshake_message(&[])?;
            client.send_relay_data(&session_id, msg3)?;
        } else {
            let msg1 = recv_next(&mut rx).await?;
            noise.read_handshake_message(&msg1)?;

            let msg2 = noise.write_handshake_message(&[])?;
            client.send_relay_data(&session_id, msg2)?;

            let msg3 = recv_next(&mut rx).await?;
            noise.read_handshake_message(&msg3)?;
        }

        let peer_static = noise
            .peer_static_key()
            .ok_or_else(|| CoreError::Crypto("no remote static key after relay handshake".into()))?;
        let verified_device_id = crate::identity::DeviceIdentity::device_id_from_public_key(&peer_static);
        if verified_device_id != peer_device_id {
            return Err(CoreError::InvalidState(format!(
                "relay peer identity mismatch: expected {peer_device_id}, got {verified_device_id}"
            )));
        }

        self.relay_handshake_waiters.lock().await.remove(&session_id);

        let noise = noise.into_transport_mode()?;
        let session = Arc::new(PeerSession {
            device_id: verified_device_id.clone(),
            noise: Arc::new(Mutex::new(noise)),
            outbound: OutboundPath::Relay {
                signaling: client,
                session_id,
            },
        });

        {
            let storage = self.storage.lock().await;
            if let Err(e) = storage.upsert_known_device(&verified_device_id, &verified_device_id, &peer_static, true) {
                eprintln!("[zao] failed to persist relay peer public key: {e}");
            }
        }

        self.sessions
            .lock()
            .await
            .insert(verified_device_id.clone(), session);
        self.sink.on_peer_connected(&verified_device_id);
        self.clone().auto_resume_transfers_to_peer(&verified_device_id);

        Ok(())
    }

    /// Routes an inbound RelayData frame either to an in-progress
    /// handshake (see `run_relay_handshake`) or, once a relay-backed
    /// PeerSession already exists for this session's peer, decrypts and
    /// dispatches it exactly like a QUIC stream's bytes via the same
    /// `decrypt_and_dispatch` helper -- from the dispatch layer's
    /// perspective, a relayed message is indistinguishable from a
    /// direct one.
    async fn handle_relay_data(self: &Arc<Self>, session_id: &str, data: Vec<u8>) {
        let waiter = self
            .relay_handshake_waiters
            .lock()
            .await
            .get(session_id)
            .cloned();
        if let Some(waiter) = waiter {
            let _ = waiter.send(data);
            return;
        }

        let peer_device_id = match self.signaling_sessions.peer_for_session(session_id).await {
            Some(id) => id,
            None => {
                eprintln!("[zao] RelayData for unknown session {session_id}, dropping");
                return;
            }
        };

        let noise = {
            let sessions = self.sessions.lock().await;
            match sessions.get(&peer_device_id) {
                Some(session) => session.noise.clone(),
                None => {
                    eprintln!(
                        "[zao] RelayData for {peer_device_id} but no active session \
                         (handshake not yet complete or already torn down), dropping"
                    );
                    return;
                }
            }
        };

        decrypt_and_dispatch(self, &peer_device_id, &noise, &data).await;
    }

    /// Try connecting to a peer using the STUN-discovered public address
    /// this device has for itself, offering candidates to `peer_device_id`
    /// through the signaling server, then attempting a direct QUIC
    /// connection to whatever candidates come back. This is the
    /// internet-mode counterpart to `connect_to_peer` (which is for
    /// already-known LAN addresses) -- use this when a peer was NOT
    /// discovered via mDNS/UDP broadcast, i.e. they're on a different
    /// network entirely.
    pub async fn connect_to_peer_via_internet(
        self: &Arc<Self>,
        peer_device_id: &str,
    ) -> Result<()> {
        let signaling = {
            let guard = self.signaling.lock().await;
            guard
                .clone()
                .ok_or_else(|| CoreError::InvalidState("signaling server not connected".into()))?
        };

        let public_addr = stun_client::discover_public_address(&self.stun_probe_socket().await?)
            .await?;
        let local_addr = self.transport.local_addr;

        let session_id = uuid::Uuid::new_v4().to_string();
        self.signaling_sessions
            .register(&session_id, peer_device_id)
            .await;

        // Offer both the public (STUN) and local candidate -- if both
        // devices happen to share a LAN despite not being discovered via
        // mDNS (e.g. mDNS blocked on that network), the local candidate
        // lets them connect directly without touching the internet path
        // at all. Public candidate is listed first since it's the
        // common case for genuinely remote peers.
        let candidates = vec![public_addr.to_string(), local_addr.to_string()];
        signaling.offer_candidates(peer_device_id, &session_id, candidates)?;

        Ok(())
    }

    /// Bind a throwaway UDP socket purely for the STUN query -- NOT the
    /// same socket QUIC listens on, since quinn's Endpoint owns its
    /// socket privately and doesn't expose a way to send arbitrary
    /// (non-QUIC) UDP datagrams through it. This means the STUN-
    /// discovered public port will generally NOT match the QUIC
    /// listener's actual public port unless the NAT in front of this
    /// device preserves port numbers consistently across different
    /// local ports (true for many, not all, consumer NAT
    /// implementations). Full port-preserving STUN-for-QUIC-itself
    /// would need quinn to expose raw datagram send/recv on its bound
    /// socket, which it does not. Flagging this rather than silently
    /// assuming it always works -- see README's Milestone 5 notes.
    async fn stun_probe_socket(&self) -> Result<tokio::net::UdpSocket> {
        tokio::net::UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(CoreError::Io)
    }

    /// Attempt a direct QUIC connection to each candidate in order,
    /// stopping at the first success. Used both when we receive
    /// candidates from a peer (responder side of hole-punching) and
    /// could be used proactively once CandidateResult comes back
    /// affirmatively (not currently re-attempted on that path since the
    /// initial offer-side attempt already covers it -- see
    /// `connect_to_peer_via_internet`).
    async fn try_candidates(self: &Arc<Self>, peer_device_id: &str, candidates: &[String]) -> Result<()> {
        for candidate in candidates {
            let addr: SocketAddr = match candidate.parse() {
                Ok(a) => a,
                Err(_) => continue,
            };
            if self.connect_to_peer(addr, peer_device_id).await.is_ok() {
                return Ok(());
            }
        }
        Err(CoreError::InvalidState(format!(
            "no candidate reachable for peer {peer_device_id}"
        )))
    }

    /// Dial out to a peer discovered via LAN/mDNS or, later, via
    /// signaling for internet mode. Performs the Noise_XX initiator
    /// handshake, then registers the resulting session. `expected_device_id`
    /// is checked against the peer's handshake-revealed identity below --
    /// since the Noise static key is deterministically derived from the
    /// peer's Ed25519 signing key (see identity.rs), a mismatch here means
    /// either a stale/incorrect discovery entry or a spoofing attempt, not
    /// a false positive from unrelated key material.
    pub async fn connect_to_peer(self: &Arc<Self>, addr: SocketAddr, expected_device_id: &str) -> Result<()> {
        {
            let sessions = self.sessions.lock().await;
            if sessions.contains_key(expected_device_id) {
                return Ok(()); // already connected, nothing to do
            }
        }

        let conn = self.transport.connect(addr, expected_device_id).await?;
        let (mut send, mut recv) = open_stream(&conn).await?;
        let mut noise = NoiseSession::new(&self.identity, Role::Initiator)?;

        // Noise_XX initiator side: write e, read e/ee/s/es, write s/se.
        let msg1 = noise.write_handshake_message(&[])?;
        write_framed(&mut send, &msg1).await?;

        let msg2 = read_framed(&mut recv).await?;
        noise.read_handshake_message(&msg2)?;

        let msg3 = noise.write_handshake_message(&[])?;
        write_framed(&mut send, &msg3).await?;

        let peer_static = noise
            .peer_static_key()
            .ok_or_else(|| CoreError::Crypto("no remote static key after handshake".into()))?;
        let actual_device_id = crate::identity::DeviceIdentity::device_id_from_public_key(&peer_static);

        if actual_device_id != expected_device_id {
            return Err(CoreError::InvalidState(format!(
                "peer identity mismatch: expected {expected_device_id}, got {actual_device_id}"
            )));
        }

        let noise = noise.into_transport_mode()?;
        self.register_session(actual_device_id, peer_static, conn, noise, send, recv)
            .await
    }

    async fn register_session(
        self: &Arc<Self>,
        device_id: String,
        peer_static_key: Vec<u8>,
        connection: Connection,
        noise: NoiseSession,
        send: quinn::SendStream,
        mut recv: quinn::RecvStream,
    ) -> Result<()> {
        // Persist the peer's public key now, while we have cryptographic
        // certainty of it (straight from a just-completed Noise
        // handshake) -- this is what lets BLE mesh's sealed_box
        // encryption (which has no in-band handshake of its own) later
        // encrypt to this peer using a key it already trusts, rather
        // than requiring a separate pairing flow specific to BLE.
        {
            let storage = self.storage.lock().await;
            if let Err(e) = storage.upsert_known_device(&device_id, &device_id, &peer_static_key, true) {
                eprintln!("[zao] failed to persist peer public key: {e}");
            }
        }

        let noise = Arc::new(Mutex::new(noise));
        let (control_tx, mut control_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        let session = Arc::new(PeerSession {
            device_id: device_id.clone(),
            noise: noise.clone(),
            outbound: OutboundPath::Quic {
                connection: connection.clone(),
                control_tx,
                transfer_senders: Mutex::new(HashMap::new()),
            },
        });

        self.sessions
            .lock()
            .await
            .insert(device_id.clone(), session.clone());
        self.sink.on_peer_connected(&device_id);

        // Auto-resume: check whether any outgoing transfer to this peer
        // was left in a non-terminal state (app restarted mid-send,
        // connection dropped, etc) and, if so, resume it now that we
        // have a live session again. This is what makes "resume
        // interrupted transfers after reconnecting" true for the
        // SENDER side, not just the receiver side (which already
        // resumed correctly via the chunk manifest since Milestone 4 --
        // see the README's Milestone 4 notes on this asymmetry, now
        // closed).
        self.clone().auto_resume_transfers_to_peer(&device_id);

        // Outbound writer task: serializes all control-stream writes for
        // this peer through one channel, so PeerSession::send_message can
        // be called concurrently from multiple call sites without
        // interleaving writes on the same stream.
        let mut send = send;
        tokio::spawn(async move {
            while let Some(ciphertext) = control_rx.recv().await {
                if write_framed(&mut send, &ciphertext).await.is_err() {
                    break;
                }
            }
        });

        // Inbound reader task: decrypt + dispatch every message arriving
        // on the control stream to the EventSink, and handle file chunks
        // by writing them straight to disk.
        let this = self.clone();
        let device_id_for_reader = device_id.clone();
        let noise_for_control = noise.clone();
        tokio::spawn(async move {
            loop {
                let ciphertext = match read_framed(&mut recv).await {
                    Ok(bytes) => bytes,
                    Err(_) => break, // stream closed / peer disconnected
                };
                if !decrypt_and_dispatch(&this, &device_id_for_reader, &noise_for_control, &ciphertext).await {
                    break;
                }
            }
            this.sessions.lock().await.remove(&device_id_for_reader);
            this.sink.on_peer_disconnected(&device_id_for_reader);
        });

        // Additional-streams accept task: the control stream established
        // above is opened once during the handshake, but each file
        // transfer later opens its OWN dedicated stream (see
        // `PeerSession::transfer_stream_sender`) so large transfers don't
        // share a lane with chat traffic. This task is what catches those
        // additional incoming streams on the receiving side -- without
        // it, a peer's per-transfer stream would arrive at this end with
        // nothing calling `connection.accept_bi()` for it.
        let this2 = self.clone();
        let device_id_for_streams = device_id.clone();
        let noise_for_streams = noise.clone();
        let connection_for_streams = connection.clone();
        tokio::spawn(async move {
            loop {
                let (_send, mut recv) = match accept_stream(&connection_for_streams).await {
                    Ok(pair) => pair,
                    Err(_) => break, // connection closed
                };
                let this3 = this2.clone();
                let device_id3 = device_id_for_streams.clone();
                let noise3 = noise_for_streams.clone();
                tokio::spawn(async move {
                    loop {
                        let ciphertext = match read_framed(&mut recv).await {
                            Ok(bytes) => bytes,
                            Err(_) => break,
                        };
                        if !decrypt_and_dispatch(&this3, &device_id3, &noise3, &ciphertext).await {
                            break;
                        }
                    }
                });
            }
        });

        Ok(())
    }

    async fn dispatch_incoming(&self, from_device_id: &str, message: ProtocolMessage) {
        match message {
            ProtocolMessage::Text(text) => self.sink.on_text_message(from_device_id, &text),
            ProtocolMessage::FileOffer(offer) => self.sink.on_file_offer(from_device_id, &offer),
            ProtocolMessage::FileAccept { transfer_id } => {
                self.handle_file_accept(from_device_id, &transfer_id).await;
            }
            ProtocolMessage::FileReject { transfer_id, reason } => {
                self.handle_file_reject(from_device_id, &transfer_id, &reason).await;
            }
            ProtocolMessage::FolderOffer(offer) => {
                self.sink.on_folder_offer(from_device_id, &offer);
            }
            ProtocolMessage::FolderAccept { folder_batch_id } => {
                self.handle_folder_accept(from_device_id, &folder_batch_id).await;
            }
            ProtocolMessage::FolderReject { folder_batch_id, reason } => {
                self.handle_folder_reject(from_device_id, &folder_batch_id, &reason).await;
            }
            ProtocolMessage::TypingIndicator {
                conversation_id,
                is_typing,
            } => self.sink.on_typing(&conversation_id, is_typing),
            ProtocolMessage::ReadReceipt {
                message_id,
                read_at_unix,
            } => self.sink.on_read_receipt(&message_id, read_at_unix),
            ProtocolMessage::DeliveryAck {
                message_id,
                delivered_at_unix,
            } => self.sink.on_delivery_ack(&message_id, delivered_at_unix),
            ProtocolMessage::Presence { online, .. } => {
                self.sink.on_presence(from_device_id, online)
            }
            ProtocolMessage::FileChunk(chunk_msg) => {
                self.handle_incoming_chunk(from_device_id, chunk_msg).await;
            }
            ProtocolMessage::FileChunkAck { .. } => {
                // Fast-path ack from the receiver's control stream. The
                // sender's authoritative "is this chunk done" state is
                // `TransferHandle::mark_chunk_acked`, called directly by
                // each chunk-sending worker once its stream write
                // succeeds (see `spawn_sender_workers`) rather than
                // waiting on this ack round-trip -- so this event is
                // informational only right now (useful for a future
                // "confirmed received" distinction from "sent"). Not
                // required for correctness in this milestone.
            }
            ProtocolMessage::FileTransferComplete { transfer_id } => {
                self.incoming_transfers.lock().await.remove(&transfer_id);
                self.sink.on_transfer_complete(&transfer_id);
            }
            ProtocolMessage::FileTransferCancelled {
                transfer_id,
                by_device_id,
            } => {
                if let Some(t) = self.outgoing_transfers.lock().await.get(&transfer_id) {
                    t.handle.cancel();
                }
                if let Some(t) = self.incoming_transfers.lock().await.get(&transfer_id) {
                    t.handle.cancel();
                }
                self.sink.on_transfer_cancelled(&transfer_id, &by_device_id);
            }
            ProtocolMessage::Ping => {
                if let Some(session) = self.sessions.lock().await.get(from_device_id) {
                    let _ = session.send_message(&ProtocolMessage::Pong).await;
                }
            }
            ProtocolMessage::Pong => {
                // Keepalive response -- presence/liveness is currently
                // derived from session-connected state, not round-trip
                // latency, so no further action needed here.
            }
        }
    }

    async fn handle_file_accept(&self, from_device_id: &str, transfer_id: &str) {
        let accept_tx = {
            let mut outgoing = self.outgoing_transfers.lock().await;
            outgoing.get_mut(transfer_id).and_then(|t| t.accept_tx.take())
        };
        if let Some(tx) = accept_tx {
            let _ = tx.send(true);
        }
        self.sink.on_file_accept(from_device_id, transfer_id);
    }

    async fn handle_file_reject(&self, from_device_id: &str, transfer_id: &str, reason: &str) {
        let accept_tx = {
            let mut outgoing = self.outgoing_transfers.lock().await;
            outgoing.get_mut(transfer_id).and_then(|t| t.accept_tx.take())
        };
        if let Some(tx) = accept_tx {
            let _ = tx.send(false);
        }
        self.outgoing_transfers.lock().await.remove(transfer_id);
        self.sink.on_file_reject(from_device_id, transfer_id, reason);
    }

    async fn handle_folder_accept(&self, from_device_id: &str, folder_batch_id: &str) {
        let accept_tx = {
            let mut pending = self.pending_folder_offers.lock().await;
            pending
                .get_mut(folder_batch_id)
                .and_then(|p| p.accept_tx.take())
        };
        if let Some(tx) = accept_tx {
            let _ = tx.send(true);
        }
        self.sink.on_folder_accept(from_device_id, folder_batch_id);
    }

    async fn handle_folder_reject(&self, from_device_id: &str, folder_batch_id: &str, reason: &str) {
        let accept_tx = {
            let mut pending = self.pending_folder_offers.lock().await;
            pending
                .get_mut(folder_batch_id)
                .and_then(|p| p.accept_tx.take())
        };
        if let Some(tx) = accept_tx {
            let _ = tx.send(false);
        }
        self.pending_folder_offers.lock().await.remove(folder_batch_id);
        self.sink.on_folder_reject(from_device_id, folder_batch_id, reason);
    }

    async fn handle_incoming_chunk(&self, from_device_id: &str, chunk_msg: FileChunkMessage) {
        let frame = ChunkFrame::new(chunk_msg.chunk_index, chunk_msg.payload);
        if frame.checksum != chunk_msg.checksum_sha256 || !frame.verify() {
            eprintln!(
                "[zao] chunk {} for transfer {} failed integrity check, dropping",
                chunk_msg.chunk_index, chunk_msg.transfer_id
            );
            return;
        }

        let (dest_path, handle) = {
            let incoming = self.incoming_transfers.lock().await;
            match incoming.get(&chunk_msg.transfer_id) {
                Some(t) => (t.dest_path.clone(), t.handle.clone()),
                None => {
                    // Chunk arrived for a transfer we have no record of --
                    // most likely the offer was never accepted on this
                    // device (e.g. app restarted mid-transfer without
                    // persisting in-memory transfer state yet). Drop
                    // silently rather than writing to an unknown path.
                    return;
                }
            }
        };

        if let Err(e) =
            write_chunk_to_file(&dest_path, chunk_msg.chunk_index, &frame.payload).await
        {
            eprintln!("[zao] failed writing chunk to disk: {e}");
            return;
        }

        handle.mark_chunk_acked(chunk_msg.chunk_index, frame.payload.len() as u64);
        {
            let storage = self.storage.lock().await;
            if let Err(e) = storage.mark_chunk_acked(&chunk_msg.transfer_id, chunk_msg.chunk_index) {
                eprintln!("[zao] failed to persist chunk ack to manifest: {e}");
            }
        }
        self.sink
            .on_transfer_progress(&handle.progress(TransferState::Active));

        // Ack back to the sender over the control stream -- informational
        // per the note in dispatch_incoming, but useful for future
        // "confirmed received" UI and for a sender-side retransmit
        // strategy if we add one later.
        if let Some(session) = self.sessions.lock().await.get(from_device_id) {
            let _ = session
                .send_message(&ProtocolMessage::FileChunkAck {
                    transfer_id: chunk_msg.transfer_id.clone(),
                    chunk_index: chunk_msg.chunk_index,
                })
                .await;
        }

        if handle.pending_chunk_indices().is_empty() {
            self.incoming_transfers
                .lock()
                .await
                .remove(&chunk_msg.transfer_id);
            {
                let storage = self.storage.lock().await;
                let progress = handle.progress(TransferState::Completed);
                if let Err(e) = storage.update_transfer_state(
                    &chunk_msg.transfer_id,
                    TransferState::Completed.as_str(),
                    progress.bytes_transferred,
                ) {
                    eprintln!("[zao] failed to persist completed transfer state: {e}");
                }
            }
            self.sink.on_transfer_complete(&chunk_msg.transfer_id);
            if let Some(session) = self.sessions.lock().await.get(from_device_id) {
                let _ = session
                    .send_message(&ProtocolMessage::FileTransferComplete {
                        transfer_id: chunk_msg.transfer_id.clone(),
                    })
                    .await;
            }
        }
    }

    /// Prepare local disk state to receive a file once an offer is
    /// accepted: preallocate the destination file so parallel chunk
    /// writes can land at arbitrary offsets safely.
    pub async fn prepare_to_receive(&self, transfer_id: &str, total_size: u64) -> Result<PathBuf> {
        let path = self.downloads_dir.join(transfer_id);
        preallocate_file(&path, total_size).await?;
        Ok(path)
    }

    /// Offer a file to a connected peer and, if accepted, stream it in
    /// parallel chunks over dedicated QUIC streams (separate from the
    /// control stream used for chat, so a large transfer never blocks
    /// message delivery). Returns once the offer is sent; the actual
    /// send happens in a spawned background task, with progress
    /// reaching the UI via `on_transfer_progress` events.
    ///
    /// `wait_for_accept_secs` bounds how long we wait for the peer to
    /// respond before giving up and emitting a rejection-equivalent
    /// event -- prevents a permanently "sending..." UI state if the
    /// peer's app is backgrounded/unresponsive.
    ///
    /// Takes `file_name`/`file_size`/`mime_type` as explicit parameters
    /// rather than deriving them from `source` -- necessary because
    /// `FileSource::Fd` (Android content:// URIs, read via a raw file
    /// descriptor -- see `transfer::FileSource`'s doc comment) has no
    /// filename or extension to derive anything from; the caller (which
    /// already queried this via Android's ContentResolver, or read it
    /// straight from a real path on Windows) must supply it.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_file(
        self: &Arc<Self>,
        peer_device_id: &str,
        source: FileSource,
        file_name: String,
        file_size: u64,
        mime_type: String,
        message_id: String,
        conversation_id: String,
        relative_path: String,
        folder_batch_id: Option<String>,
    ) -> Result<String> {
        let transfer_id = uuid::Uuid::new_v4().to_string();
        let handle = TransferHandle::new(transfer_id.clone(), file_name.clone(), file_size);

        // local_path persisted to the DB manifest is informational/for
        // resumability bookkeeping only when it's a real path; for an
        // Fd source there is no stable path to persist (the fd number
        // is only valid for this process's lifetime), so auto-resume
        // after a restart is a known limitation for Fd-sourced sends --
        // see the README's Milestone 8 notes.
        let local_path_for_db = match &source {
            FileSource::Path(p) => p.to_string_lossy().to_string(),
            FileSource::Fd(fd) => format!("fd:{fd}"),
        };

        {
            let storage = self.storage.lock().await;
            if let Err(e) = storage.create_transfer(
                &transfer_id,
                &message_id,
                &file_name,
                file_size,
                &mime_type,
                handle.total_chunks(),
                CHUNK_SIZE,
                "send",
                &local_path_for_db,
            ) {
                eprintln!("[zao] failed to persist outgoing transfer row: {e}");
            }
        }

        let (accept_tx, accept_rx) = tokio::sync::oneshot::channel::<bool>();
        self.outgoing_transfers.lock().await.insert(
            transfer_id.clone(),
            OutgoingTransfer {
                file_source: source.clone(),
                handle: handle.clone(),
                accept_tx: Some(accept_tx),
            },
        );

        let offer = FileOffer {
            transfer_id: transfer_id.clone(),
            message_id,
            conversation_id,
            file_name,
            file_size,
            mime_type,
            total_chunks: handle.total_chunks(),
            chunk_size: CHUNK_SIZE,
            relative_path,
            folder_batch_id,
        };
        self.send_to(peer_device_id, &ProtocolMessage::FileOffer(offer))
            .await?;

        let this = self.clone();
        let peer_id = peer_device_id.to_string();
        let tid = transfer_id.clone();
        tokio::spawn(async move {
            const WAIT_FOR_ACCEPT_SECS: u64 = 120;
            let accepted = tokio::time::timeout(
                std::time::Duration::from_secs(WAIT_FOR_ACCEPT_SECS),
                accept_rx,
            )
            .await;

            let accepted = matches!(accepted, Ok(Ok(true)));
            if !accepted {
                this.outgoing_transfers.lock().await.remove(&tid);
                return;
            }

            if let Err(e) = this.run_outgoing_transfer(&peer_id, &tid, source, handle).await {
                eprintln!("[zao] outgoing transfer {tid} failed: {e}");
            }
        });

        Ok(transfer_id)
    }

    /// Offer an entire folder to a peer: sends ONE `FolderOffer` listing
    /// every file up front (see that message's doc comment for why),
    /// waits for the receiver's single accept/reject decision for the
    /// whole batch, and only then walks the folder and calls
    /// `send_file` once per file -- each still negotiated/transferred
    /// as its own independent transfer_id/chunk stream, preserving
    /// per-file resumability, but without forcing forty separate
    /// accept prompts for a forty-file folder.
    ///
    /// Returns the list of (relative_path, transfer_id) pairs for every
    /// file offered, in the order they were found. If some individual
    /// file's offer fails after the batch was accepted (e.g. permission
    /// error reading it), that file is skipped and logged rather than
    /// aborting the whole folder.
    pub async fn send_folder(
        self: &Arc<Self>,
        peer_device_id: &str,
        folder_path: PathBuf,
        conversation_id: String,
    ) -> Result<Vec<(String, String)>> {
        let folder_batch_id = uuid::Uuid::new_v4().to_string();
        let files = collect_files_recursive(&folder_path).await?;

        if files.is_empty() {
            return Err(CoreError::InvalidState(
                "folder contains no files to send".into(),
            ));
        }

        let folder_name = folder_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "folder".to_string());

        // Build the manifest up front -- this needs file sizes, which
        // means a metadata stat per file before any transfer begins.
        // Cheap relative to the actual transfer, and necessary so the
        // receiver's accept/reject decision can be informed by total
        // size, not just file count.
        let mut manifest = Vec::with_capacity(files.len());
        let mut total_size: u64 = 0;
        for file_path in &files {
            let metadata = tokio::fs::metadata(file_path).await?;
            let relative_path = relative_path_string(file_path, &folder_path);
            total_size += metadata.len();
            manifest.push(crate::protocol::FolderFileEntry {
                relative_path,
                file_size: metadata.len(),
                mime_type: mime_guess_from_extension(file_path),
            });
        }

        let (accept_tx, accept_rx) = tokio::sync::oneshot::channel::<bool>();
        self.pending_folder_offers.lock().await.insert(
            folder_batch_id.clone(),
            PendingFolderOffer {
                accept_tx: Some(accept_tx),
            },
        );

        let offer = crate::protocol::FolderOffer {
            folder_batch_id: folder_batch_id.clone(),
            conversation_id: conversation_id.clone(),
            folder_name,
            total_files: files.len() as u64,
            total_size,
            files: manifest.clone(),
        };
        self.send_to(peer_device_id, &ProtocolMessage::FolderOffer(offer))
            .await?;

        // Wait for the receiver's single accept/reject decision, with
        // the same timeout rationale as an individual file offer (see
        // send_file) -- don't hang forever if the peer's app is
        // backgrounded/unresponsive.
        const WAIT_FOR_ACCEPT_SECS: u64 = 120;
        let accepted = tokio::time::timeout(
            std::time::Duration::from_secs(WAIT_FOR_ACCEPT_SECS),
            accept_rx,
        )
        .await;
        let accepted = matches!(accepted, Ok(Ok(true)));

        self.pending_folder_offers
            .lock()
            .await
            .remove(&folder_batch_id);

        if !accepted {
            return Err(CoreError::InvalidState(
                "folder offer was rejected or timed out".into(),
            ));
        }

        let mut results = Vec::new();
        for (file_path, manifest_entry) in files.iter().zip(manifest.iter()) {
            let relative_path = manifest_entry.relative_path.clone();
            let file_name = file_path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "file".to_string());
            let message_id = uuid::Uuid::new_v4().to_string();
            match self
                .send_file(
                    peer_device_id,
                    FileSource::Path(file_path.clone()),
                    file_name,
                    manifest_entry.file_size,
                    manifest_entry.mime_type.clone(),
                    message_id,
                    conversation_id.clone(),
                    relative_path.clone(),
                    Some(folder_batch_id.clone()),
                )
                .await
            {
                Ok(transfer_id) => results.push((relative_path, transfer_id)),
                Err(e) => {
                    eprintln!("[zao] skipping file in folder send ({relative_path}): {e}");
                }
            }
        }

        if results.is_empty() {
            return Err(CoreError::InvalidState(
                "no files in folder could be offered (all failed)".into(),
            ));
        }

        Ok(results)
    }

    /// Check the DB for outgoing transfers to `device_id` left in a
    /// non-terminal state, and re-initiate each one. Runs as a spawned
    /// background task (not awaited by the caller) since connection
    /// setup (register_session) shouldn't block on this. Only handles
    /// the SEND direction -- an interrupted RECEIVE simply waits for the
    /// sender to retry, which the manifest already supports correctly.
    fn auto_resume_transfers_to_peer(self: Arc<Self>, device_id: &str) {
        let device_id = device_id.to_string();
        tokio::spawn(async move {
            let resumable = {
                let storage = self.storage.lock().await;
                match storage.list_resumable_transfers_with_peer() {
                    Ok(list) => list,
                    Err(e) => {
                        eprintln!("[zao] auto-resume: failed to query resumable transfers: {e}");
                        return;
                    }
                }
            };

            for (transfer_id, peer_device_id, direction, local_path) in resumable {
                if peer_device_id != device_id || direction != "send" {
                    continue;
                }
                if self.outgoing_transfers.lock().await.contains_key(&transfer_id) {
                    continue;
                }

                // fd-sourced transfers (Android content:// URIs, see
                // FileSource::Fd's doc comment) cannot be auto-resumed
                // after a process restart -- the raw fd number stored
                // in `local_path` as "fd:N" was only ever valid for the
                // process that originally opened it via
                // ContentResolver.openFileDescriptor, and that fd is
                // gone once the app process dies. This is a real,
                // documented limitation (see README's Milestone 8
                // notes), not silently swallowed: skip with a clear log
                // message rather than attempting to reopen a
                // now-meaningless fd number.
                if local_path.starts_with("fd:") {
                    eprintln!(
                        "[zao] auto-resume: skipping {transfer_id} (fd-sourced transfer, \
                         not resumable across a restart -- original picker selection would \
                         need to be re-made by the user)"
                    );
                    continue;
                }

                let (file_name, total_bytes, _path) = {
                    let storage = self.storage.lock().await;
                    match storage.load_transfer_meta(&transfer_id) {
                        Ok(Some(meta)) => meta,
                        Ok(None) => continue,
                        Err(e) => {
                            eprintln!("[zao] auto-resume: failed to load transfer meta for {transfer_id}: {e}");
                            continue;
                        }
                    }
                };

                let already_acked = {
                    let storage = self.storage.lock().await;
                    storage.load_acked_chunks(&transfer_id).unwrap_or_default()
                };

                let handle = TransferHandle::resume_from_manifest(
                    transfer_id.clone(),
                    file_name,
                    total_bytes,
                    already_acked,
                );

                let source = FileSource::Path(PathBuf::from(&local_path));
                self.outgoing_transfers.lock().await.insert(
                    transfer_id.clone(),
                    OutgoingTransfer {
                        file_source: source.clone(),
                        handle: handle.clone(),
                        accept_tx: None,
                    },
                );

                eprintln!("[zao] auto-resuming outgoing transfer {transfer_id} to {device_id}");
                if let Err(e) = self
                    .run_outgoing_transfer(&device_id, &transfer_id, source, handle)
                    .await
                {
                    eprintln!("[zao] auto-resume of {transfer_id} failed: {e}");
                }
            }
        });
    }

    async fn run_outgoing_transfer(
        self: &Arc<Self>,
        peer_device_id: &str,
        transfer_id: &str,
        source: FileSource,
        handle: TransferHandle,
    ) -> Result<()> {
        let session = {
            let sessions = self.sessions.lock().await;
            sessions
                .get(peer_device_id)
                .cloned()
                .ok_or_else(|| CoreError::NotFound(format!("peer {peer_device_id} not connected")))?
        };

        let pending = handle.pending_chunk_indices();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(DEFAULT_PARALLELISM));
        let mut tasks = Vec::new();

        for chunk_index in pending {
            if handle.is_cancelled() {
                break;
            }
            while handle.is_paused() {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                if handle.is_cancelled() {
                    break;
                }
            }

            let permit = semaphore.clone().acquire_owned().await.map_err(|_| {
                CoreError::InvalidState("semaphore closed unexpectedly".into())
            })?;
            let chunk_source = source.clone();
            let handle_clone = handle.clone();
            let session_clone = session.clone();
            let tid = transfer_id.to_string();
            let storage_clone = self.storage.clone();
            let sink_clone = self.sink.clone();

            // Each chunk is sent as one framed ProtocolMessage over the
            // peer's dedicated per-transfer QUIC stream (opened once per
            // transfer, reused for all its chunks) rather than the
            // shared control stream -- this is what gives file transfer
            // its own congestion/flow-control lane, so a large transfer
            // doesn't stall chat message delivery and vice versa.
            let stream_sender = session.transfer_stream_sender(transfer_id).await?;

            let task = tokio::spawn(async move {
                let _permit = permit;
                let payload = read_chunk_from_source(&chunk_source, chunk_index).await?;
                let chunk_len = payload.len() as u64;
                let frame = ChunkFrame::new(chunk_index, payload);

                let msg = ProtocolMessage::FileChunk(FileChunkMessage {
                    transfer_id: tid.clone(),
                    chunk_index,
                    payload: frame.payload,
                    checksum_sha256: frame.checksum,
                });
                stream_sender.send(msg).await?;
                handle_clone.mark_chunk_acked(chunk_index, chunk_len);
                {
                    let storage = storage_clone.lock().await;
                    if let Err(e) = storage.mark_chunk_acked(&tid, chunk_index) {
                        eprintln!("[zao] failed to persist sender-side chunk ack: {e}");
                    }
                }
                sink_clone.on_transfer_progress(&handle_clone.progress(TransferState::Active));
                let _ = session_clone; // keep session alive for the duration of this task
                Ok::<(), CoreError>(())
            });
            tasks.push(task);
        }

        for task in tasks {
            task.await
                .map_err(|e| CoreError::InvalidState(format!("chunk worker panicked: {e}")))??;
        }

        self.outgoing_transfers.lock().await.remove(transfer_id);

        let final_state = if handle.is_cancelled() {
            let _ = self
                .send_to(
                    peer_device_id,
                    &ProtocolMessage::FileTransferCancelled {
                        transfer_id: transfer_id.to_string(),
                        by_device_id: self.identity.device_id.clone(),
                    },
                )
                .await;
            TransferState::Cancelled
        } else {
            TransferState::Completed
        };

        {
            let storage = self.storage.lock().await;
            let progress = handle.progress(final_state);
            if let Err(e) =
                storage.update_transfer_state(transfer_id, final_state.as_str(), progress.bytes_transferred)
            {
                eprintln!("[zao] failed to persist final transfer state: {e}");
            }
        }
        self.sink.on_transfer_progress(&handle.progress(final_state));

        Ok(())
    }

    /// Accept an incoming file offer: preallocate the destination file,
    /// register an IncomingTransfer so arriving chunks know where to
    /// land, persist the transfer row so it's resumable after a restart,
    /// and notify the sender to begin.
    pub async fn accept_file(&self, from_device_id: &str, offer: &FileOffer) -> Result<PathBuf> {
        let dest_path = self.prepare_to_receive(&offer.transfer_id, offer.file_size).await?;

        // If this transfer_id already has a manifest (e.g. the app
        // restarted mid-transfer and the user is re-accepting the same
        // offer), rehydrate from the already-acked chunk set instead of
        // starting the progress bar from zero -- this is what makes
        // "resume interrupted transfers after reconnecting" actually
        // true across a restart, not just within one running session.
        let already_acked = {
            let storage = self.storage.lock().await;
            storage
                .load_acked_chunks(&offer.transfer_id)
                .unwrap_or_default()
        };
        let handle = if already_acked.is_empty() {
            TransferHandle::new(
                offer.transfer_id.clone(),
                offer.file_name.clone(),
                offer.file_size,
            )
        } else {
            TransferHandle::resume_from_manifest(
                offer.transfer_id.clone(),
                offer.file_name.clone(),
                offer.file_size,
                already_acked,
            )
        };

        {
            let storage = self.storage.lock().await;
            if let Err(e) = storage.create_transfer(
                &offer.transfer_id,
                &offer.message_id,
                &offer.file_name,
                offer.file_size,
                &offer.mime_type,
                offer.total_chunks,
                offer.chunk_size,
                "receive",
                &dest_path.to_string_lossy(),
            ) {
                // Non-fatal: if a row already exists (re-accept after
                // restart) this will error on the primary key -- the
                // transfer can still proceed using the in-memory handle.
                eprintln!("[zao] create_transfer (non-fatal, may already exist): {e}");
            }
        }

        self.incoming_transfers.lock().await.insert(
            offer.transfer_id.clone(),
            IncomingTransfer {
                dest_path: dest_path.clone(),
                handle,
            },
        );
        self.send_to(
            from_device_id,
            &ProtocolMessage::FileAccept {
                transfer_id: offer.transfer_id.clone(),
            },
        )
        .await?;
        Ok(dest_path)
    }

    pub async fn reject_file(&self, from_device_id: &str, transfer_id: &str, reason: &str) -> Result<()> {
        self.send_to(
            from_device_id,
            &ProtocolMessage::FileReject {
                transfer_id: transfer_id.to_string(),
                reason: reason.to_string(),
            },
        )
        .await
    }

    /// Accept a batched folder offer: sends one FolderAccept, which the
    /// sender treats as implicit acceptance for every FileOffer that
    /// follows with this folder_batch_id. Nothing else needs to happen
    /// on the receiver side here -- individual FileOffers will still
    /// arrive and go through the normal accept_file path per file (the
    /// UI layer is expected to auto-accept those, having already shown
    /// the user the folder-level prompt); this method's only job is the
    /// batch-level acknowledgement.
    pub async fn accept_folder(&self, from_device_id: &str, folder_batch_id: &str) -> Result<()> {
        self.send_to(
            from_device_id,
            &ProtocolMessage::FolderAccept {
                folder_batch_id: folder_batch_id.to_string(),
            },
        )
        .await
    }

    pub async fn reject_folder(&self, from_device_id: &str, folder_batch_id: &str, reason: &str) -> Result<()> {
        self.send_to(
            from_device_id,
            &ProtocolMessage::FolderReject {
                folder_batch_id: folder_batch_id.to_string(),
                reason: reason.to_string(),
            },
        )
        .await
    }

    /// Cancel a transfer this device initiated or is receiving. Marks
    /// the local TransferHandle cancelled (workers/writers check this
    /// flag cooperatively) and notifies the peer.
    pub async fn cancel_transfer(&self, peer_device_id: &str, transfer_id: &str) -> Result<()> {
        if let Some(t) = self.outgoing_transfers.lock().await.get(transfer_id) {
            t.handle.cancel();
        }
        if let Some(t) = self.incoming_transfers.lock().await.get(transfer_id) {
            t.handle.cancel();
        }
        self.send_to(
            peer_device_id,
            &ProtocolMessage::FileTransferCancelled {
                transfer_id: transfer_id.to_string(),
                by_device_id: self.identity.device_id.clone(),
            },
        )
        .await
    }

    pub async fn pause_outgoing_transfer(&self, transfer_id: &str) {
        if let Some(t) = self.outgoing_transfers.lock().await.get(transfer_id) {
            t.handle.pause();
        }
    }

    pub async fn resume_outgoing_transfer(&self, transfer_id: &str) {
        if let Some(t) = self.outgoing_transfers.lock().await.get(transfer_id) {
            t.handle.resume();
        }
    }

    pub async fn send_to(&self, device_id: &str, msg: &ProtocolMessage) -> Result<()> {
        let sessions = self.sessions.lock().await;
        let session = sessions
            .get(device_id)
            .ok_or_else(|| CoreError::NotFound(format!("no active session with {device_id}")))?;
        session.send_message(msg).await
    }

    pub async fn is_connected(&self, device_id: &str) -> bool {
        self.sessions.lock().await.contains_key(device_id)
    }

    pub async fn connected_device_ids(&self) -> Vec<String> {
        self.sessions.lock().await.keys().cloned().collect()
    }

    /// This device's own bound QUIC address, for diagnostics/UI display.
    pub fn local_addr(&self) -> SocketAddr {
        self.transport.local_addr
    }
}

/// Shared by both the control-stream reader and each per-transfer-stream
/// reader: decrypt one ciphertext frame with the peer's (shared) Noise
/// session, decode it as a ProtocolMessage, and dispatch it. Returns
/// `false` if the reader loop calling this should stop (decryption
/// failure implies either connection desync or a compromised peer --
/// either way, safer to drop the stream than keep reading garbage).
async fn decrypt_and_dispatch(
    manager: &Arc<ConnectionManager>,
    from_device_id: &str,
    noise: &Arc<Mutex<NoiseSession>>,
    ciphertext: &[u8],
) -> bool {
    let plaintext = {
        let mut n = noise.lock().await;
        match n.decrypt(ciphertext) {
            Ok(p) => p,
            Err(_) => return false,
        }
    };
    match ProtocolMessage::decode_plaintext(&plaintext) {
        Ok(message) => {
            manager.dispatch_incoming(from_device_id, message).await;
            true
        }
        Err(_) => true, // malformed message, ignore and keep reading
    }
}

/// Compute a folder-relative path string with normalized forward
/// slashes, regardless of the host OS's native separator -- this keeps
/// the wire format consistent whether the sender is Windows (backslash
/// paths) or Android/Linux (forward-slash paths already).
fn relative_path_string(file_path: &std::path::Path, folder_root: &std::path::Path) -> String {
    file_path
        .strip_prefix(folder_root)
        .map(|p| p.to_string_lossy().replace('\\', "/"))
        .unwrap_or_else(|_| file_path.to_string_lossy().to_string())
}

/// Recursively walk `dir` and return every regular file found (not
/// directories or symlinks -- symlinks are skipped rather than followed,
/// to avoid an unbounded walk if a symlink cycle exists). Uses an
/// explicit stack instead of recursion so a very deeply nested folder
/// structure doesn't risk stack overflow.
async fn collect_files_recursive(dir: &std::path::Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut stack = vec![dir.to_path_buf()];

    while let Some(current) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&current).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if file_type.is_dir() {
                stack.push(entry.path());
            } else if file_type.is_file() {
                files.push(entry.path());
            }
            // Symlinks (file_type.is_symlink()) are intentionally
            // skipped -- see doc comment above.
        }
    }

    Ok(files)
}

/// Minimal extension-to-MIME mapping covering the file types called out
/// in the requirements (images, videos, documents, APKs). Falls back to
/// application/octet-stream for anything unrecognized -- deliberately
/// not pulling in a full mime-guessing crate for this modest need.
pub(crate) fn mime_guess_from_extension(path: &std::path::Path) -> String {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    match ext.as_str() {
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "mp4" => "video/mp4",
        "mov" => "video/quicktime",
        "mkv" => "video/x-matroska",
        "webm" => "video/webm",
        "pdf" => "application/pdf",
        "doc" => "application/msword",
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xls" => "application/vnd.ms-excel",
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "ppt" => "application/vnd.ms-powerpoint",
        "pptx" => "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "txt" => "text/plain",
        "zip" => "application/zip",
        "apk" => "application/vnd.android.package-archive",
        _ => "application/octet-stream",
    }
    .to_string()
}

/// Frame format for the raw bytes on the wire (before/after Noise
/// encryption, this framing wraps the ciphertext itself): [u32 LE len][bytes]
async fn write_framed<W: tokio::io::AsyncWrite + Unpin>(writer: &mut W, data: &[u8]) -> Result<()> {
    writer.write_all(&(data.len() as u32).to_le_bytes()).await?;
    writer.write_all(data).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_framed<R: tokio::io::AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    // Guard against a malicious/corrupt peer claiming an absurd frame
    // size and forcing a huge allocation -- control-stream messages
    // (text, offers, receipts) are always small; chunk payloads are
    // capped at CHUNK_SIZE plus a small header margin.
    const MAX_FRAME: usize = (CHUNK_SIZE as usize) + 4096;
    if len > MAX_FRAME {
        return Err(CoreError::InvalidState(format!(
            "frame length {len} exceeds max {MAX_FRAME}"
        )));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}
