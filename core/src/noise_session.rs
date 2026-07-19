use crate::error::{CoreError, Result};
use crate::identity::DeviceIdentity;
use snow::{Builder, TransportState};

const NOISE_PARAMS: &str = "Noise_XX_25519_ChaChaPoly_SHA256";

/// Wraps a Noise_XX handshake + resulting transport state.
/// Noise_XX is used (not IK/K) because it doesn't require devices to
/// have exchanged static keys ahead of time -- appropriate for a first
/// contact / pairing flow where identity is only verified via the
/// out-of-band safety-code comparison after handshake completes.
pub enum NoiseSession {
    Handshaking(snow::HandshakeState),
    Transport(TransportState),
}

pub enum Role {
    Initiator,
    Responder,
}

impl NoiseSession {
    pub fn new(identity: &DeviceIdentity, role: Role) -> Result<Self> {
        let local_private_key_bytes = identity.noise_static.to_bytes();
        let builder = Builder::new(
            NOISE_PARAMS
                .parse()
                .map_err(|e| CoreError::Crypto(format!("bad noise params: {e:?}")))?,
        )
        .local_private_key(&local_private_key_bytes);

        let handshake = match role {
            Role::Initiator => builder
                .build_initiator()
                .map_err(|e| CoreError::Crypto(e.to_string()))?,
            Role::Responder => builder
                .build_responder()
                .map_err(|e| CoreError::Crypto(e.to_string()))?,
        };

        Ok(Self::Handshaking(handshake))
    }

    /// Write the next handshake message. Returns bytes to send to the peer.
    pub fn write_handshake_message(&mut self, payload: &[u8]) -> Result<Vec<u8>> {
        match self {
            NoiseSession::Handshaking(hs) => {
                let mut buf = vec![0u8; 4096];
                let len = hs
                    .write_message(payload, &mut buf)
                    .map_err(|e| CoreError::Crypto(e.to_string()))?;
                buf.truncate(len);
                Ok(buf)
            }
            NoiseSession::Transport(_) => Err(CoreError::InvalidState(
                "handshake already complete".into(),
            )),
        }
    }

    /// Read an incoming handshake message from the peer.
    pub fn read_handshake_message(&mut self, msg: &[u8]) -> Result<Vec<u8>> {
        match self {
            NoiseSession::Handshaking(hs) => {
                let mut buf = vec![0u8; 4096];
                let len = hs
                    .read_message(msg, &mut buf)
                    .map_err(|e| CoreError::Crypto(e.to_string()))?;
                buf.truncate(len);
                Ok(buf)
            }
            NoiseSession::Transport(_) => Err(CoreError::InvalidState(
                "handshake already complete".into(),
            )),
        }
    }

    /// Once handshake is complete on both sides (is_handshake_finished),
    /// call this to transition into the transport (data) phase.
    pub fn into_transport_mode(self) -> Result<Self> {
        match self {
            NoiseSession::Handshaking(hs) => {
                let transport = hs
                    .into_transport_mode()
                    .map_err(|e| CoreError::Crypto(e.to_string()))?;
                Ok(NoiseSession::Transport(transport))
            }
            NoiseSession::Transport(_) => Ok(self),
        }
    }

    pub fn is_handshake_finished(&self) -> bool {
        match self {
            NoiseSession::Handshaking(hs) => hs.is_handshake_finished(),
            NoiseSession::Transport(_) => true,
        }
    }

    /// Extract the peer's static public key. Used after handshake to
    /// compute their device_id and check it against known/trusted devices.
    pub fn peer_static_key(&self) -> Option<Vec<u8>> {
        match self {
            NoiseSession::Handshaking(hs) => hs.get_remote_static().map(|k| k.to_vec()),
            NoiseSession::Transport(ts) => ts.get_remote_static().map(|k| k.to_vec()),
        }
    }

    /// Encrypt a chunk of plaintext for transport over any wire (QUIC, TCP, BLE).
    pub fn encrypt(&mut self, plaintext: &[u8]) -> Result<Vec<u8>> {
        match self {
            NoiseSession::Transport(ts) => {
                let mut buf = vec![0u8; plaintext.len() + 16]; // +tag
                let len = ts
                    .write_message(plaintext, &mut buf)
                    .map_err(|e| CoreError::Crypto(e.to_string()))?;
                buf.truncate(len);
                Ok(buf)
            }
            NoiseSession::Handshaking(_) => {
                Err(CoreError::InvalidState("not in transport mode".into()))
            }
        }
    }

    pub fn decrypt(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        match self {
            NoiseSession::Transport(ts) => {
                let mut buf = vec![0u8; ciphertext.len()];
                let len = ts
                    .read_message(ciphertext, &mut buf)
                    .map_err(|e| CoreError::Crypto(e.to_string()))?;
                buf.truncate(len);
                Ok(buf)
            }
            NoiseSession::Handshaking(_) => {
                Err(CoreError::InvalidState("not in transport mode".into()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_handshake_and_encrypted_roundtrip() {
        let alice_id = DeviceIdentity::generate();
        let bob_id = DeviceIdentity::generate();

        let mut alice = NoiseSession::new(&alice_id, Role::Initiator).unwrap();
        let mut bob = NoiseSession::new(&bob_id, Role::Responder).unwrap();

        // -> e
        let msg1 = alice.write_handshake_message(&[]).unwrap();
        bob.read_handshake_message(&msg1).unwrap();

        // <- e, ee, s, es
        let msg2 = bob.write_handshake_message(&[]).unwrap();
        alice.read_handshake_message(&msg2).unwrap();

        // -> s, se
        let msg3 = alice.write_handshake_message(&[]).unwrap();
        bob.read_handshake_message(&msg3).unwrap();

        assert!(alice.is_handshake_finished());
        assert!(bob.is_handshake_finished());

        let mut alice_t = alice.into_transport_mode().unwrap();
        let mut bob_t = bob.into_transport_mode().unwrap();

        let plaintext = b"hello from alice";
        let ciphertext = alice_t.encrypt(plaintext).unwrap();
        let decrypted = bob_t.decrypt(&ciphertext).unwrap();
        assert_eq!(plaintext.to_vec(), decrypted);
    }
}
