use serde::{Deserialize, Serialize};

/// Messages exchanged with a signaling server over WebSocket. The
/// signaling server's ONLY job is to relay these small JSON messages
/// between two devices that know each other's device_id but not yet
/// each other's network address -- it never sees message content, file
/// data, or anything beyond connection-establishment metadata. This
/// protocol is intentionally the same shape whether the eventual
/// connection ends up direct (hole-punched) or relayed (see
/// `RelayData` below) -- the signaling server doesn't need to know
/// which outcome occurred.
///
/// NOTE: this defines the protocol the client speaks; the server side
/// (which must run this exact protocol to interoperate) is a separate
/// deployable component, not included in this milestone per project
/// scope -- see README's Milestone 5 notes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum SignalingMessage {
    /// Sent immediately after connecting: registers this device_id with
    /// the server so peers can address messages to it. The server is
    /// expected to key connections by device_id for the lifetime of the
    /// WebSocket connection (no persistent server-side account needed).
    Register { device_id: String },

    /// Sent by the server in response to Register, confirming
    /// registration succeeded (or reporting why it didn't, e.g. a
    /// duplicate registration from the same device_id already connected
    /// elsewhere).
    RegisterAck { success: bool, error: Option<String> },

    /// Ask the server to relay a set of connection candidates (this
    /// device's public STUN-discovered address, LAN address if
    /// relevant, and a session nonce) to a specific peer by device_id.
    /// This is the internet-mode equivalent of what mDNS/UDP broadcast
    /// provide on LAN -- "here's how to reach me" -- just relayed
    /// through a rendezvous point instead of broadcast locally.
    OfferCandidates {
        to_device_id: String,
        session_id: String,
        candidates: Vec<String>, // "ip:port" strings, in priority order
    },

    /// Delivered by the server to the target device_id when someone
    /// sends OfferCandidates addressed to them.
    IncomingCandidates {
        from_device_id: String,
        session_id: String,
        candidates: Vec<String>,
    },

    /// Sent back by the receiving device once it has attempted the
    /// candidates -- lets the offering side know whether to expect an
    /// incoming hole-punched connection or whether it should fall back
    /// to requesting a relay for this session_id instead.
    CandidateResult {
        session_id: String,
        direct_connection_likely: bool,
    },

    /// If direct hole-punching fails on both sides, either device can
    /// ask the signaling server to allocate a relay session -- the
    /// server (in its relay role) then forwards raw bytes between both
    /// devices' WebSocket connections for this session_id. This keeps
    /// deployment simple (one server process handles both signaling and
    /// relay) at the cost of relayed traffic being bounded by the
    /// server's own bandwidth -- acceptable as a last-resort fallback,
    /// not the common path.
    RequestRelay { session_id: String },
    RelayReady { session_id: String },
    RelayData { session_id: String, data: Vec<u8> },
    RelayClosed { session_id: String },

    /// Lightweight keepalive so the server can detect and clean up dead
    /// connections without waiting for a TCP-level timeout.
    Ping,
    Pong,
}

impl SignalingMessage {
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn from_json(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_roundtrips_through_json() {
        let msg = SignalingMessage::Register {
            device_id: "abc123".into(),
        };
        let json = msg.to_json().unwrap();
        let decoded = SignalingMessage::from_json(&json).unwrap();
        match decoded {
            SignalingMessage::Register { device_id } => assert_eq!(device_id, "abc123"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn offer_candidates_roundtrips() {
        let msg = SignalingMessage::OfferCandidates {
            to_device_id: "peer1".into(),
            session_id: "sess1".into(),
            candidates: vec!["203.0.113.5:41000".into(), "10.0.0.5:41000".into()],
        };
        let json = msg.to_json().unwrap();
        let decoded = SignalingMessage::from_json(&json).unwrap();
        match decoded {
            SignalingMessage::OfferCandidates { candidates, .. } => {
                assert_eq!(candidates.len(), 2);
            }
            _ => panic!("wrong variant"),
        }
    }
}
