use crate::error::{CoreError, Result};
use crate::signaling::SignalingMessage;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message as WsMessage;

/// Events the signaling client surfaces to the connection layer above
/// it (ultimately reaching ConnectionManager, which decides what to do
/// with candidates -- attempt hole-punch, fall back to relay, etc).
/// Kept separate from SignalingMessage itself so callers don't need to
/// pattern-match variants that are purely protocol bookkeeping (Ping/
/// Pong, RegisterAck) that this client already handles internally.
#[derive(Debug, Clone)]
pub enum SignalingEvent {
    Registered,
    RegisterFailed { error: String },
    IncomingCandidates {
        from_device_id: String,
        session_id: String,
        candidates: Vec<String>,
    },
    CandidateResult {
        session_id: String,
        direct_connection_likely: bool,
    },
    RelayReady { session_id: String },
    RelayData { session_id: String, data: Vec<u8> },
    RelayClosed { session_id: String },
    Disconnected,
}

/// Client-side connection to a signaling server. Owns the WebSocket and
/// exposes a simple send/receive-events API; the actual decision-making
/// about what to do with candidates (attempt a direct QUIC connection,
/// request a relay, etc) belongs to ConnectionManager, not this client --
/// this type's only job is getting SignalingMessages to and from the
/// server reliably.
pub struct SignalingClient {
    outbound_tx: mpsc::UnboundedSender<WsMessage>,
    events: Arc<Mutex<mpsc::UnboundedReceiver<SignalingEvent>>>,
}

impl SignalingClient {
    /// Connect to a signaling server at `url` (e.g. "wss://signal.example.com/ws")
    /// and register this device's ID. Spawns background tasks for the
    /// read and write halves of the WebSocket; call `next_event` in a
    /// loop to drain incoming signaling events.
    pub async fn connect(url: &str, device_id: &str) -> Result<Self> {
        let (ws_stream, _response) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(|e| CoreError::InvalidState(format!("signaling connect failed: {e}")))?;
        let (mut ws_write, mut ws_read) = ws_stream.split();

        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<WsMessage>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<SignalingEvent>();

        // Writer task: serializes all outbound sends through one channel
        // so multiple call sites (register, offer candidates, relay
        // data) don't race writing to the same WebSocket sink directly.
        tokio::spawn(async move {
            while let Some(msg) = outbound_rx.recv().await {
                if ws_write.send(msg).await.is_err() {
                    break;
                }
            }
        });

        // Reader task: decode each incoming text frame as a
        // SignalingMessage and translate it into the smaller
        // SignalingEvent surface for callers.
        let event_tx_clone = event_tx.clone();
        tokio::spawn(async move {
            while let Some(Ok(msg)) = ws_read.next().await {
                let text = match msg {
                    WsMessage::Text(t) => t,
                    WsMessage::Close(_) => break,
                    _ => continue, // ignore binary/ping/pong frames at this layer
                };
                let parsed = match SignalingMessage::from_json(&text) {
                    Ok(m) => m,
                    Err(_) => continue, // malformed frame from server, skip
                };
                let event = match parsed {
                    SignalingMessage::RegisterAck { success, error } => {
                        if success {
                            SignalingEvent::Registered
                        } else {
                            SignalingEvent::RegisterFailed {
                                error: error.unwrap_or_else(|| "unknown error".to_string()),
                            }
                        }
                    }
                    SignalingMessage::IncomingCandidates {
                        from_device_id,
                        session_id,
                        candidates,
                    } => SignalingEvent::IncomingCandidates {
                        from_device_id,
                        session_id,
                        candidates,
                    },
                    SignalingMessage::CandidateResult {
                        session_id,
                        direct_connection_likely,
                    } => SignalingEvent::CandidateResult {
                        session_id,
                        direct_connection_likely,
                    },
                    SignalingMessage::RelayReady { session_id } => {
                        SignalingEvent::RelayReady { session_id }
                    }
                    SignalingMessage::RelayData { session_id, data } => {
                        SignalingEvent::RelayData { session_id, data }
                    }
                    SignalingMessage::RelayClosed { session_id } => {
                        SignalingEvent::RelayClosed { session_id }
                    }
                    SignalingMessage::Pong => continue, // keepalive, no event needed
                    // Register/OfferCandidates/RequestRelay/Ping are
                    // client-to-server only messages; receiving one back
                    // from the server would indicate a protocol bug on
                    // the server side, not something this client acts on.
                    _ => continue,
                };
                if event_tx_clone.send(event).is_err() {
                    break;
                }
            }
            let _ = event_tx_clone.send(SignalingEvent::Disconnected);
        });

        let client = Self {
            outbound_tx,
            events: Arc::new(Mutex::new(event_rx)),
        };

        client.send(&SignalingMessage::Register {
            device_id: device_id.to_string(),
        })?;

        Ok(client)
    }

    pub fn send(&self, msg: &SignalingMessage) -> Result<()> {
        let json = msg
            .to_json()
            .map_err(|e| CoreError::InvalidState(format!("signaling encode failed: {e}")))?;
        self.outbound_tx
            .send(WsMessage::Text(json))
            .map_err(|_| CoreError::InvalidState("signaling connection closed".into()))
    }

    pub fn offer_candidates(&self, to_device_id: &str, session_id: &str, candidates: Vec<String>) -> Result<()> {
        self.send(&SignalingMessage::OfferCandidates {
            to_device_id: to_device_id.to_string(),
            session_id: session_id.to_string(),
            candidates,
        })
    }

    pub fn request_relay(&self, session_id: &str) -> Result<()> {
        self.send(&SignalingMessage::RequestRelay {
            session_id: session_id.to_string(),
        })
    }

    pub fn send_relay_data(&self, session_id: &str, data: Vec<u8>) -> Result<()> {
        self.send(&SignalingMessage::RelayData {
            session_id: session_id.to_string(),
            data,
        })
    }

    /// Await the next signaling event. Returns `None` once the
    /// connection is closed and no further events will arrive.
    pub async fn next_event(&self) -> Option<SignalingEvent> {
        self.events.lock().await.recv().await
    }
}

/// Tracks in-flight signaling sessions (one per peer we're trying to
/// reach over the internet), so ConnectionManager can correlate
/// IncomingCandidates/CandidateResult/RelayData events with the right
/// ongoing connection attempt. Kept here rather than in
/// ConnectionManager directly since this is signaling-specific
/// bookkeeping, not general peer-session state.
pub struct SignalingSessionTracker {
    sessions: Mutex<HashMap<String, String>>, // session_id -> peer device_id
}

impl SignalingSessionTracker {
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
        }
    }

    pub async fn register(&self, session_id: &str, peer_device_id: &str) {
        self.sessions
            .lock()
            .await
            .insert(session_id.to_string(), peer_device_id.to_string());
    }

    pub async fn peer_for_session(&self, session_id: &str) -> Option<String> {
        self.sessions.lock().await.get(session_id).cloned()
    }

    pub async fn remove(&self, session_id: &str) {
        self.sessions.lock().await.remove(session_id);
    }
}

impl Default for SignalingSessionTracker {
    fn default() -> Self {
        Self::new()
    }
}
