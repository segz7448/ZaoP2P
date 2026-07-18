use serde::{Deserialize, Serialize};

/// Every logical exchange between two devices -- a chat message, a
/// file-transfer offer, a typing indicator, a read receipt -- is one of
/// these variants, serialized as JSON, encrypted with the session's
/// Noise transport keys, length-prefixed, and sent over a QUIC stream.
///
/// Wire framing (after Noise encryption): [u32 LE length][ciphertext bytes]
/// The plaintext inside the ciphertext is `serde_json::to_vec(&ProtocolMessage)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum ProtocolMessage {
    /// Plain text chat message.
    Text(TextMessage),

    /// Sender announces an incoming file/folder and waits for Accept/Reject.
    FileOffer(FileOffer),
    FileAccept { transfer_id: String },
    FileReject { transfer_id: String, reason: String },

    /// Batched folder negotiation: sent ONCE before any per-file
    /// FileOffers, listing every file in the folder up front so the
    /// receiver can accept or reject the whole batch in a single
    /// decision (e.g. "accept this 40-file, 2GB folder?") rather than
    /// forty separate per-file prompts. Once accepted, the sender
    /// follows up with one FileOffer per entry (each still negotiated
    /// as its own transfer_id/chunk stream underneath, preserving
    /// per-file resumability) -- this message only replaces the
    /// UX-facing negotiation step, not the underlying per-file transfer
    /// mechanics, which are unchanged from a standalone FileOffer.
    FolderOffer(FolderOffer),
    /// Accepting a FolderOffer implicitly accepts every FileOffer that
    /// follows with the same folder_batch_id -- the receiver does not
    /// need to send a separate FileAccept per file (though the sender
    /// still tracks per-file accept state internally via the same
    /// acceptance, to keep one code path for both batched and
    /// standalone transfers).
    FolderAccept { folder_batch_id: String },
    FolderReject { folder_batch_id: String, reason: String },

    /// One chunk of file data. Kept separate from FileOffer so large
    /// files stream incrementally rather than needing to be buffered
    /// into one giant protocol message.
    FileChunk(FileChunkMessage),

    /// Receiver -> sender ack for a specific chunk, enabling the sender
    /// to know what's safe to skip on resume even before checking the
    /// DB manifest (fast path for the common "still connected" case).
    FileChunkAck { transfer_id: String, chunk_index: u64 },

    FileTransferComplete { transfer_id: String },
    FileTransferCancelled { transfer_id: String, by_device_id: String },

    /// Presence / UX signals.
    TypingIndicator { conversation_id: String, is_typing: bool },
    ReadReceipt { message_id: String, read_at_unix: u64 },
    DeliveryAck { message_id: String, delivered_at_unix: u64 },
    Presence { online: bool, unix_time: u64 },

    /// Lightweight keepalive so "online status" can reflect a live
    /// connection rather than only the last message exchanged.
    Ping,
    Pong,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextMessage {
    pub message_id: String,
    pub conversation_id: String,
    pub body: String,
    pub sent_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderOffer {
    pub folder_batch_id: String,
    pub conversation_id: String,
    pub folder_name: String,
    pub total_files: u64,
    pub total_size: u64,
    /// Full manifest of every file in the folder, sent up front so the
    /// receiver's accept/reject decision can be informed by the whole
    /// picture (names, sizes, count) rather than learning about files
    /// one at a time as individual FileOffers trickle in afterward.
    pub files: Vec<FolderFileEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderFileEntry {
    pub relative_path: String,
    pub file_size: u64,
    pub mime_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileOffer {
    pub transfer_id: String,
    pub message_id: String,
    pub conversation_id: String,
    pub file_name: String,
    pub file_size: u64,
    pub mime_type: String,
    pub total_chunks: u64,
    pub chunk_size: u64,
    /// Present for folder transfers: the relative path within the
    /// folder being sent, e.g. "photos/2026/img1.jpg". Empty for a
    /// single standalone file.
    pub relative_path: String,
    /// Groups multiple FileOffers that belong to the same folder
    /// transfer, so the UI can show one aggregate progress bar as well
    /// as per-file detail.
    pub folder_batch_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileChunkMessage {
    pub transfer_id: String,
    pub chunk_index: u64,
    pub payload: Vec<u8>,
    pub checksum_sha256: [u8; 32],
}

impl ProtocolMessage {
    pub fn encode_plaintext(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    pub fn decode_plaintext(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_message_roundtrips_through_json() {
        let msg = ProtocolMessage::Text(TextMessage {
            message_id: "m1".into(),
            conversation_id: "c1".into(),
            body: "hey".into(),
            sent_at_unix: 1000,
        });
        let encoded = msg.encode_plaintext().unwrap();
        let decoded = ProtocolMessage::decode_plaintext(&encoded).unwrap();
        match decoded {
            ProtocolMessage::Text(t) => assert_eq!(t.body, "hey"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn file_offer_roundtrips() {
        let offer = FileOffer {
            transfer_id: "t1".into(),
            message_id: "m2".into(),
            conversation_id: "c1".into(),
            file_name: "video.mp4".into(),
            file_size: 123456,
            mime_type: "video/mp4".into(),
            total_chunks: 15,
            chunk_size: crate::transfer::CHUNK_SIZE,
            relative_path: String::new(),
            folder_batch_id: None,
        };
        let msg = ProtocolMessage::FileOffer(offer);
        let encoded = msg.encode_plaintext().unwrap();
        let decoded = ProtocolMessage::decode_plaintext(&encoded).unwrap();
        match decoded {
            ProtocolMessage::FileOffer(o) => assert_eq!(o.file_name, "video.mp4"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn folder_offer_roundtrips() {
        let offer = FolderOffer {
            folder_batch_id: "batch1".into(),
            conversation_id: "c1".into(),
            folder_name: "vacation_photos".into(),
            total_files: 2,
            total_size: 5000,
            files: vec![
                FolderFileEntry {
                    relative_path: "img1.jpg".into(),
                    file_size: 2000,
                    mime_type: "image/jpeg".into(),
                },
                FolderFileEntry {
                    relative_path: "sub/img2.jpg".into(),
                    file_size: 3000,
                    mime_type: "image/jpeg".into(),
                },
            ],
        };
        let msg = ProtocolMessage::FolderOffer(offer);
        let encoded = msg.encode_plaintext().unwrap();
        let decoded = ProtocolMessage::decode_plaintext(&encoded).unwrap();
        match decoded {
            ProtocolMessage::FolderOffer(o) => {
                assert_eq!(o.files.len(), 2);
                assert_eq!(o.total_size, 5000);
            }
            _ => panic!("wrong variant"),
        }
    }
}
