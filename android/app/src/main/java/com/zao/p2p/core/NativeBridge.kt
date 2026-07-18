package com.zao.p2p.core

/**
 * Thin Kotlin wrapper around zao-transfer-core (Rust), loaded as a .so.
 * Function signatures here must match the #[no_mangle] JNI exports in
 * core/src/ffi.rs exactly (package path is baked into the JNI symbol name:
 * Java_com_zao_p2p_core_NativeBridge_<method>).
 *
 * If you rename this package or class, you MUST update the corresponding
 * Java_com_zao_p2p_core_NativeBridge_* function names in ffi.rs to match,
 * or the dynamic linker will fail to resolve the native methods at runtime.
 */
object NativeBridge {
    init {
        System.loadLibrary("zao_transfer_core")
    }

    /**
     * Opens (or creates) the encrypted local DB and ensures a device
     * identity exists. Call once on app startup.
     * Returns JSON: {"device_id": "...", "public_key_hex": "..."}
     * or {"error": "..."} on failure.
     */
    external fun initApp(dbPath: String, dbKey: String): String

    /**
     * Returns this device's identity info as JSON. Fails with
     * {"error": "..."} if initApp has not been called yet.
     */
    external fun getIdentity(dbPath: String, dbKey: String): String

    /**
     * Starts LAN discovery (mDNS + UDP broadcast fallback), binds the
     * QUIC listener, and starts the connection manager (Noise session
     * handling + message routing). Call once, after initApp. Safe to
     * call more than once (idempotent) -- subsequent calls return
     * {"status":"already_started"}.
     * `downloadsDir` must be a writable directory -- incoming file
     * chunks are written there.
     * Returns JSON: {"status":"started","quic_port":N,"device_id":"..."} or an error.
     */
    external fun startNetworking(dbPath: String, dbKey: String, displayName: String, downloadsDir: String): String

    /**
     * Returns the current list of discovered LAN peers as a JSON array
     * of DiscoveredPeer objects: [{device_id, display_name, addr, via, last_seen_unix}, ...]
     * Cheap to call frequently (e.g. every 1-2s) -- no network I/O, just
     * reads an in-memory map maintained by background discovery threads.
     */
    external fun discoverPeers(): String

    /** Diagnostics: this device's own QUIC address + known/connected peer counts. */
    external fun networkingStatus(): String

    /**
     * Dial a peer at the given "ip:port" address (from a DiscoveredPeer
     * entry) and perform the Noise_XX handshake. Safe to call even if
     * already connected. Blocks the calling thread until the outcome is
     * known -- call from a background thread/coroutine, not the UI thread.
     */
    external fun connectToPeer(addr: String, expectedDeviceId: String): String

    /**
     * Connect to a signaling server for internet-mode connection
     * establishment (reaching peers not on the same LAN). Optional --
     * never required for LAN-only usage. `url` should be `wss://host/path`
     * in production, or `ws://` for local testing. No signaling server
     * ships with this app; you must deploy one yourself (see README).
     */
    external fun connectSignalingServer(url: String): String

    /**
     * Attempt to reach a peer over the internet (not discovered via
     * LAN/mDNS) using STUN + signaling-relayed candidates. Requires
     * connectSignalingServer to have succeeded first. Returns once
     * candidates are offered -- watch for a PeerConnected event (via
     * pollEvents) to know if/when the connection actually succeeds.
     */
    external fun connectToPeerViaInternet(peerDeviceId: String): String

    /**
     * Encrypt a plaintext string for BLE mesh transmission to a specific
     * recipient, using a stateless sealed-box scheme (NOT the same as
     * the QUIC path's session-based Noise encryption -- a flooding mesh
     * can't rely on ordered delivery). `recipientPublicKeyHex` comes
     * from getKnownDevicePublicKey. Returns hex-encoded sealed bytes.
     */
    external fun bleSealMessage(recipientPublicKeyHex: String, plaintext: String): String

    /** Decrypt a BLE mesh message using this device's own identity. */
    external fun bleOpenMessage(dbPath: String, dbKey: String, sealedHex: String): String

    /**
     * Look up a previously-known peer's public key (hex-encoded), needed
     * for bleSealMessage. Only works for peers this device has
     * previously connected to over LAN/internet (BLE mesh has no
     * in-band handshake of its own to learn a key fresh) -- returns a
     * JSON error object if the peer is unknown.
     */
    external fun getKnownDevicePublicKey(peerDeviceId: String): String

    /**
     * Send a text message to a connected peer. Persists it locally first
     * (visible immediately in chat history with status "pending"), then
     * transmits over the peer's live encrypted session.
     * Returns JSON: {"status":"sent","message_id":"..."} or an error.
     */
    external fun sendTextMessage(peerDeviceId: String, body: String): String

    /**
     * Drain and return all events accumulated since the last call --
     * incoming messages, typing indicators, receipts, presence changes,
     * transfer progress -- as a JSON array of tagged AppEvent objects.
     * Intended to be polled on a timer (e.g. every 500ms-1s) from the UI.
     */
    external fun pollEvents(): String

    /**
     * Load persisted chat history for a conversation with a peer
     * (oldest message first), as a JSON array of StoredMessage objects.
     */
    external fun getConversationHistory(peerDeviceId: String, limit: Int): String

    /** Send a typing indicator to a peer. Fire-and-forget. */
    external fun sendTypingIndicator(peerDeviceId: String, conversationId: String, isTyping: Boolean): String

    /** Mark a message as read locally and notify the sender. */
    external fun markMessageRead(peerDeviceId: String, messageId: String): String

    /** List device_ids of peers with a currently live, authenticated session. */
    external fun connectedPeers(): String

    /** Pause an in-flight transfer. No-op if transferId isn't tracked. */
    external fun pauseTransfer(transferId: String): String

    /** Resume a previously paused transfer. */
    external fun resumeTransfer(transferId: String): String

    /** Cancel a transfer (sender or receiver side). Notifies the peer. */
    external fun cancelTransfer(peerDeviceId: String, transferId: String): String

    /**
     * Poll current progress for a transfer as JSON, read authoritatively
     * from the local DB manifest (so it's correct even across an app
     * restart, unlike an in-memory-only handle):
     * {transfer_id, file_name, total_bytes, bytes_transferred, percent}
     */
    external fun getTransferProgress(transferId: String): String

    /**
     * Offer a file at the given local path to a connected peer. Persists
     * a local "file" message row immediately, then sends the offer and
     * (once accepted) streams the file in parallel chunks over a
     * dedicated QUIC stream separate from chat traffic.
     * Returns JSON: {"status":"offered","transfer_id":"...","message_id":"..."}
     */
    external fun sendFile(peerDeviceId: String, filePath: String, conversationId: String): String

    /**
     * Offer a file to a connected peer, reading it through an
     * already-open raw file descriptor -- use this instead of sendFile
     * for content:// URIs (e.g. files picked via ACTION_OPEN_DOCUMENT),
     * which have no real filesystem path. `fd` must come from
     * `ParcelFileDescriptor.getFd()` on a descriptor opened via
     * `ContentResolver.openFileDescriptor(uri, "r")`, and that
     * ParcelFileDescriptor MUST be kept open (a member field, not a
     * local that gets garbage collected) for the entire duration of the
     * transfer -- chunk workers reopen the fd on demand via
     * /proc/self/fd, so closing it early causes reads to start failing
     * partway through, not just at the end.
     */
    external fun sendFileFd(
        peerDeviceId: String,
        fd: Int,
        fileName: String,
        fileSize: Long,
        mimeType: String,
        conversationId: String,
    ): String

    /**
     * Offer an entire folder to a peer. Walks folderPath recursively and
     * sends one FileOffer per file found, sharing one folder_batch_id.
     * Returns JSON: {"status":"offered","files":[{"relative_path":"...","transfer_id":"..."}]}
     */
    external fun sendFolder(peerDeviceId: String, folderPath: String, conversationId: String): String

    /**
     * Accept a batched folder offer (fields come from a FolderOffer
     * AppEvent) -- covers every file in the batch with one call.
     * Individual FileOffers will still arrive afterward for each file;
     * the app should auto-accept those via acceptFile since the user
     * already made the batch-level decision here.
     */
    external fun acceptFolder(fromDeviceId: String, folderBatchId: String): String

    /** Reject a batched folder offer, notifying the sender with a reason. */
    external fun rejectFolder(fromDeviceId: String, folderBatchId: String, reason: String): String

    /**
     * Accept an incoming file offer (fields come from a FileOffer
     * AppEvent) and begin receiving it into the app's downloads
     * directory. Returns JSON: {"status":"accepted","dest_path":"..."}
     */
    external fun acceptFile(
        fromDeviceId: String,
        transferId: String,
        messageId: String,
        fileName: String,
        fileSize: Long,
        mimeType: String,
    ): String

    /** Reject an incoming file offer, notifying the sender with a reason. */
    external fun rejectFile(fromDeviceId: String, transferId: String, reason: String): String
}
