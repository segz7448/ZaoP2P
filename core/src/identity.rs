use crate::error::{CoreError, Result};
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use rand_core::OsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256, Sha512};
use x25519_dalek::{PublicKey as XPublicKey, StaticSecret as XStaticSecret};

/// A device's full identity: a long-term signing key (who you are)
/// plus a Noise/X25519 static key (used for session handshakes).
/// Only the public halves + device_id are ever persisted in plaintext;
/// the private key bytes live only inside the encrypted SQLCipher DB.
#[derive(Clone)]
pub struct DeviceIdentity {
    pub device_id: String, // hex sha256 of the ed25519 public key
    pub signing_key: SigningKey,
    pub noise_static: XStaticSecret,
}

#[derive(Serialize, Deserialize)]
pub struct StoredIdentity {
    pub device_id: String,
    pub signing_key_bytes: [u8; 32],
    pub noise_static_bytes: [u8; 32],
}

impl DeviceIdentity {
    /// Generate a brand new identity. Called once, on first app launch.
    ///
    /// The Noise/X25519 static key is deterministically derived from the
    /// Ed25519 signing key (via SHA-512, matching the standard Ed25519
    /// seed-expansion construction used by X25519 conversion libraries)
    /// rather than generated independently. This matters: `device_id` is
    /// defined as sha256(ed25519 public key), but the Noise_XX handshake
    /// only ever reveals the X25519 static key to the peer -- if the two
    /// keys were unrelated, a peer could never compute the other side's
    /// real device_id from a live connection, and connection_manager.rs's
    /// post-handshake identity check would be comparing unrelated values.
    /// Deriving X25519 deterministically from Ed25519 keeps one identity,
    /// not two.
    pub fn generate() -> Self {
        let signing_key = SigningKey::generate(&mut OsRng);
        let noise_static = Self::derive_noise_static(&signing_key);
        let device_id = Self::derive_device_id(&signing_key.verifying_key());

        Self {
            device_id,
            signing_key,
            noise_static,
        }
    }

    /// Deterministically derive an X25519 static secret from an Ed25519
    /// signing key's seed bytes, via SHA-512 (the same expand-then-clamp
    /// approach used by standard ed25519-to-x25519 conversion). This is
    /// a one-way derivation: knowing the X25519 key does not reveal the
    /// Ed25519 key, so exposing the Noise static key during a handshake
    /// does not weaken the long-term signing identity.
    fn derive_noise_static(signing_key: &SigningKey) -> XStaticSecret {
        let mut hasher = Sha512::new();
        hasher.update(signing_key.to_bytes());
        hasher.update(b"zao-p2p-noise-static-v1"); // domain separation
        let expanded = hasher.finalize();
        let mut scalar_bytes = [0u8; 32];
        scalar_bytes.copy_from_slice(&expanded[..32]);
        XStaticSecret::from(scalar_bytes)
    }

    pub fn verifying_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    pub fn noise_public(&self) -> XPublicKey {
        XPublicKey::from(&self.noise_static)
    }

    /// device_id = sha256(ed25519 public key), hex encoded.
    /// This is the stable, human/QR-shareable identifier used for pairing.
    fn derive_device_id(vk: &VerifyingKey) -> String {
        let mut hasher = Sha256::new();
        hasher.update(vk.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Derive a device_id from a peer's raw Noise/X25519 static public
    /// key bytes, as observed after a completed handshake. Because
    /// `DeviceIdentity::generate` derives the X25519 static key
    /// deterministically from the Ed25519 signing key (see
    /// `derive_noise_static`), this value equals the peer's real
    /// account device_id -- NOT a separate identity space. This is what
    /// lets `connection_manager.rs` verify a peer's identity right after
    /// the handshake, before any application data is exchanged.
    pub fn device_id_from_public_key(public_key_bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(public_key_bytes);
        hex::encode(hasher.finalize())
    }

    /// Sign an arbitrary payload (e.g. a pairing challenge) with the
    /// long-term identity key.
    pub fn sign(&self, message: &[u8]) -> [u8; 64] {
        self.signing_key.sign(message).to_bytes()
    }

    pub fn to_stored(&self) -> StoredIdentity {
        StoredIdentity {
            device_id: self.device_id.clone(),
            signing_key_bytes: self.signing_key.to_bytes(),
            noise_static_bytes: self.noise_static.to_bytes(),
        }
    }

    pub fn from_stored(stored: &StoredIdentity) -> Result<Self> {
        let signing_key = SigningKey::from_bytes(&stored.signing_key_bytes);
        let noise_static = XStaticSecret::from(stored.noise_static_bytes);
        let expected_id = Self::derive_device_id(&signing_key.verifying_key());
        if expected_id != stored.device_id {
            return Err(CoreError::InvalidState(
                "stored device_id does not match derived key".into(),
            ));
        }
        Ok(Self {
            device_id: stored.device_id.clone(),
            signing_key,
            noise_static,
        })
    }
}

/// BLE mesh messages are NOT sent over the stateful Noise transport
/// session used by QUIC (see NoiseSession above). That session assumes
/// an ordered, single-path stream with a monotonic nonce counter --
/// exactly what a flooding mesh does NOT provide (a message can arrive
/// via multiple relay paths, out of order, or be duplicated). Instead,
/// BLE mesh messages use a stateless sealed-box construction: a fresh
/// ephemeral X25519 keypair per message, Diffie-Hellman with the
/// recipient's known static public key, then ChaCha20-Poly1305 with a
/// key derived from that shared secret. This is the same trust model as
/// Noise (recipient's long-term public key must already be known/
/// verified via prior pairing) but without any session/ordering state,
/// which is what a multi-path flood requires.
pub mod sealed_box {
    use super::*;
    use chacha20poly1305::aead::{Aead, KeyInit};
    use chacha20poly1305::{ChaCha20Poly1305, Nonce};

    /// Encrypt `plaintext` for a recipient identified by their X25519
    /// public key bytes (the same bytes exposed via `noise_public()`,
    /// and hashed to their device_id via `device_id_from_public_key`).
    /// Returns `[ephemeral_public_key: 32 bytes][nonce: 12 bytes][ciphertext]`,
    /// self-contained so no separate session setup is needed -- the
    /// recipient can decrypt using only their own static secret key and
    /// the bytes of this message.
    pub fn seal(recipient_public_key_bytes: &[u8; 32], plaintext: &[u8]) -> Result<Vec<u8>> {
        let recipient_public = XPublicKey::from(*recipient_public_key_bytes);
        let ephemeral_secret = XStaticSecret::random_from_rng(OsRng);
        let ephemeral_public = XPublicKey::from(&ephemeral_secret);
        let shared_secret = ephemeral_secret.diffie_hellman(&recipient_public);

        let key = derive_symmetric_key(shared_secret.as_bytes());
        let cipher = ChaCha20Poly1305::new((&key).into());

        let mut nonce_bytes = [0u8; 12];
        rand::RngCore::fill_bytes(&mut OsRng, &mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| CoreError::Crypto(format!("sealed_box encrypt failed: {e}")))?;

        let mut out = Vec::with_capacity(32 + 12 + ciphertext.len());
        out.extend_from_slice(ephemeral_public.as_bytes());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Decrypt a sealed_box message using this device's own Noise
    /// static secret key (the recipient's long-term X25519 key --
    /// deterministically derived from the Ed25519 identity, per
    /// `derive_noise_static`, so this is the same key `noise_public()`
    /// exposes the public half of).
    pub fn open(recipient_secret: &XStaticSecret, sealed: &[u8]) -> Result<Vec<u8>> {
        if sealed.len() < 32 + 12 {
            return Err(CoreError::Crypto("sealed_box message too short".into()));
        }
        let ephemeral_public_bytes: [u8; 32] = sealed[0..32]
            .try_into()
            .map_err(|_| CoreError::Crypto("bad ephemeral key length".into()))?;
        let ephemeral_public = XPublicKey::from(ephemeral_public_bytes);
        let nonce_bytes = &sealed[32..44];
        let ciphertext = &sealed[44..];

        let shared_secret = recipient_secret.diffie_hellman(&ephemeral_public);
        let key = derive_symmetric_key(shared_secret.as_bytes());
        let cipher = ChaCha20Poly1305::new((&key).into());
        let nonce = Nonce::from_slice(nonce_bytes);

        cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| CoreError::Crypto(format!("sealed_box decrypt failed: {e}")))
    }

    /// Derive a symmetric key from a raw X25519 shared secret via
    /// SHA-256 -- a plain hash is sufficient here (not HKDF) since the
    /// shared secret is only ever used once per message (fresh
    /// ephemeral key each time), so there's no multi-message key-reuse
    /// concern that HKDF's context-separation would otherwise address.
    fn derive_symmetric_key(shared_secret_bytes: &[u8]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(shared_secret_bytes);
        hasher.update(b"zao-p2p-sealed-box-v1");
        hasher.finalize().into()
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::identity::DeviceIdentity;

        #[test]
        fn seal_and_open_roundtrip() {
            let recipient = DeviceIdentity::generate();
            let recipient_public_bytes = recipient.noise_public().to_bytes();

            let plaintext = b"hello over BLE mesh";
            let sealed = seal(&recipient_public_bytes, plaintext).unwrap();
            let opened = open(&recipient.noise_static, &sealed).unwrap();

            assert_eq!(opened, plaintext);
        }

        #[test]
        fn wrong_recipient_cannot_decrypt() {
            let recipient = DeviceIdentity::generate();
            let eavesdropper = DeviceIdentity::generate();
            let recipient_public_bytes = recipient.noise_public().to_bytes();

            let sealed = seal(&recipient_public_bytes, b"secret").unwrap();
            let result = open(&eavesdropper.noise_static, &sealed);
            assert!(result.is_err());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_and_roundtrip() {
        let id = DeviceIdentity::generate();
        let stored = id.to_stored();
        let restored = DeviceIdentity::from_stored(&stored).unwrap();
        assert_eq!(id.device_id, restored.device_id);
    }

    #[test]
    fn device_id_is_deterministic_from_key() {
        let id = DeviceIdentity::generate();
        let expected = DeviceIdentity::derive_device_id(&id.verifying_key());
        assert_eq!(id.device_id, expected);
    }

    #[test]
    fn noise_static_key_resolves_back_to_real_device_id() {
        // This is the property connection_manager.rs relies on: after a
        // Noise handshake, hashing the peer's revealed X25519 static key
        // must equal their real (Ed25519-derived) device_id, or identity
        // verification post-handshake would be meaningless.
        let id = DeviceIdentity::generate();
        let noise_public_bytes = id.noise_public().to_bytes();
        let derived = device_id_from_public_key(&noise_public_bytes);
        assert_eq!(derived, id.device_id);
    }

    #[test]
    fn two_identities_derive_different_noise_keys() {
        let a = DeviceIdentity::generate();
        let b = DeviceIdentity::generate();
        assert_ne!(a.noise_static.to_bytes(), b.noise_static.to_bytes());
    }
}
