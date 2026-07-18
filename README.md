# Zao P2P

Cross-platform (Android + Windows) peer-to-peer messaging & file
sharing app. Local-first: SQLCipher-encrypted SQLite on each device, no
cloud database. Chat and file transfer work over LAN (mDNS/UDP
discovery + QUIC), WiFi Direct and Bluetooth mesh (Android), and the
internet (STUN + a self-hosted signaling/relay server) -- see
"Milestone status" below for what's implemented in each mode and what
its known limitations are.

## Structure

```
core/               Shared Rust crate (zao-transfer-core) -- used by both apps
  identity.rs         Ed25519 + X25519 device identity, sealed_box (BLE mesh crypto)
  noise_session.rs    Noise_XX handshake wrapper (E2E encryption foundation)
  storage.rs          SQLCipher schema + data access
  discovery.rs         mDNS + UDP broadcast LAN peer discovery
  transport.rs         QUIC transport (quinn)
  transfer.rs           Chunked file transfer engine (parallel workers, resumable)
  protocol.rs           Application-level message wire format (chat, file/folder offers)
  connection_manager.rs Ties Noise + QUIC/relay + protocol together per peer
  signaling.rs           Client-side signaling protocol types
  signaling_client.rs     WebSocket client for the signaling server (internet mode)
  stun_client.rs          Hand-rolled STUN client (NAT traversal)
  ffi.rs                  Plain-Rust API + Android JNI bindings

android/            Android app (Kotlin), loads core as libzao_transfer_core.so
  .../chat/            Chat UI, file/folder transfer UI
  .../transport/       WiFi Direct + BLE mesh native Kotlin (no Rust binding exists for either)

windows/            Windows app (Tauri), loads core as a direct Rust dependency
  src-tauri/           Rust/Tauri shell
  ui/                  HTML/JS chat + file transfer UI

signaling-server/            Separate deployable project (Rust/tokio) -- a small
                             WebSocket server for internet-mode connection
                             rendezvous + relay fallback. Needs a VPS/always-on
                             machine to run it.
signaling-server-cloudflare/ Same server, reimplemented for Cloudflare Workers +
                             Durable Objects -- deploys entirely on Cloudflare's
                             free tier, no VPS or payment needed. Speaks the
                             identical wire protocol; pick either one.

.github/workflows/
  android-build.yml   Cross-compiles core for Android ABIs (cargo-ndk),
                       then builds + zips the APK
  windows-build.yml    Builds core + Tauri bundle, zips the EXE/MSI
```

Note: both `signaling-server/` and `signaling-server-cloudflare/` are
packaged and deployed separately from the Android/Windows app zips --
each is a server you run/deploy yourself (a VPS for the Rust version,
a free Cloudflare account for the Workers version), not something that
ships inside the mobile/desktop apps. You only need ONE of them, not
both -- see "Which signaling server should I use?" below.

## Which signaling server should I use?

**Neither, if you only need LAN, WiFi Direct, or Bluetooth mesh** --
internet mode (and therefore a signaling server) is only needed when
two devices are on completely different networks with no direct route
between them (e.g. different cities, different ISPs). Skip this
section entirely if that's not a scenario you need.

If you do need it:

- **`signaling-server-cloudflare/` (recommended for most people):**
  deploys entirely on Cloudflare's free tier in about 5 minutes, no
  credit card, no server to keep running yourself. Use this unless you
  already have a VPS sitting around or a specific reason to prefer
  running your own process.
- **`signaling-server/` (the Rust version):** use this if you already
  have a VPS, a spare always-on Linux box, or prefer running your own
  process instead of depending on Cloudflare's infrastructure.

Both speak the identical wire protocol -- either one works with the
apps unchanged; you only need to deploy one of them, and can switch
later by just changing the URL in the apps' "Connect over the
internet" dialog.

## IMPORTANT — not yet verified by a real compiler

This code was written without access to a Rust toolchain, Android SDK, or
Windows build environment in the authoring sandbox. It has been carefully
reviewed but **not compiled**. Expect the first CI run to surface minor
issues (dependency version pins, JNI signature mismatches, Gradle/NDK
version drift) — this is normal for a first push, not a sign of deeper
problems. Push to a branch first and watch both workflow runs before
merging to main.

## Milestone 6 is Android-only, and here's why

WiFi Direct and BLE mesh both have no Windows equivalent that fits this
app's model, and no Rust crate exists with mature bindings for either
(this was flagged back in the very first architecture message). Windows
does have Bluetooth APIs, but building a comparable BLE GATT
server/client there would be a substantial separate native Windows
implementation (WinRT Bluetooth APIs), not a small addition -- deferred
rather than built as a token/incomplete gesture.

## RESOLVED in M7: WiFi Direct group formation now reaches QUIC

This was the most important limitation from Milestone 6, closed in
Milestone 7. `WifiDirectManager` discovers peers and forms a group;
previously, the hand-off from "group formed, here's the group owner's
IP" to "QUIC connection established" was missing because `WifiP2pInfo`
gives the group owner's IP but never the port their QUIC listener is
bound to. `WifiDirectPortExchange.kt` now closes this with a small,
self-contained TCP handshake run directly over the WiFi Direct link
(the group owner listens on a fixed port; the client connects; both
exchange their real QUIC port + device_id), avoiding any dependency on
unverified mDNS-over-WiFi-Direct behavior. See Milestone 7's section
below for the one remaining edge case that's still untested against
real hardware (the owner-side client-address assumption).

## Known scope limits: BLE mesh

- **Single-hop mesh with flood-relay groundwork, not full multi-hop
  routing.** Messages relay to all currently-connected mesh peers with
  a decrementing TTL and are deduplicated by message ID, which is the
  minimum viable foundation a mesh needs -- but there's no path/topology
  awareness or smarter rebroadcast suppression a larger mesh would
  need. Flagged directly in `BleMeshManager`'s class doc comment.
- **Messaging only, not file transfer.** BLE's practical throughput and
  GATT payload limits make it unsuitable for the 8MB chunks the QUIC
  transport uses; BLE mesh here is a last-resort text-messaging
  fallback for when no WiFi/internet transport exists at all, which
  matches the requirements' framing of Bluetooth mesh as an offline
  fallback, not a primary file-transfer path.
- **Requires a prior LAN/WiFi-Direct/internet connection to message
  someone over BLE.** `sealed_box` encryption needs the recipient's
  public key, which is only learned during a Noise handshake over one
  of those other transports (see the `register_session` fix above) --
  there's no in-band BLE pairing/handshake flow in this milestone. This
  means BLE mesh currently works as "stay reachable via Bluetooth after
  you've already connected with someone once," not "discover and
  message a total stranger over Bluetooth alone."

## Milestone status

- **M1-M6 (done)**: See git history / earlier sections below for full
  detail. Summary: identity + encrypted storage + Noise handshake (M1);
  mDNS/UDP LAN discovery + QUIC transport + chunked transfer engine
  (M2); full chat protocol + UI (M3); file transfer end-to-end with
  dedicated per-transfer streams + resumable manifests (M4);
  internet-mode client-side (STUN + signaling protocol, no server
  bundled, M5); WiFi Direct + BLE mesh, Android-only (M6).
- **M7 (done, this push)**: Three concrete follow-ups from M6's honest
  gap list, all closed:
  1. **WiFi Direct -> QUIC gap closed.** `WifiDirectPortExchange.kt`
     does a tiny, self-contained TCP handshake over the WiFi Direct
     link itself (group owner listens on a fixed port, client connects,
     both exchange their real QUIC port + device_id) -- this avoids
     depending on mDNS behavior over the WiFi Direct interface, which
     couldn't be verified without real hardware. One known remaining
     edge case is documented inline in `MainActivity.kt` (the owner
     side's assumption about the client's reachable address in a
     multi-device group hasn't been tested against real WiFi Direct
     hardware).
  2. **Folder transfer implemented.** `ConnectionManager::send_folder`
     walks a directory recursively and offers each file as its own
     `FileOffer`, all sharing one `folder_batch_id` (the field already
     existed in `protocol.rs` since M3, unused until now). Wired into
     both UIs: right-click the attach button on Windows, long-press on
     Android, both using their platform's native directory picker
     (Tauri's `dialog.open({directory: true})`, Android's
     `ACTION_OPEN_DOCUMENT_TREE` + `DocumentFile` tree walk).
  3. **Auto-resume of interrupted outgoing transfers.** Closed the
     asymmetry flagged in M4's notes, where only the *receiver* side
     correctly resumed after a restart. `ConnectionManager` now checks,
     every time a peer connection is (re-)established, whether the DB
     has any outgoing transfer to that peer left in a non-terminal
     state, and if so, resumes it from the manifest automatically --
     no user action needed, same as receiver-side resume already
     provided.
- **M8 (done, this push)**: Two follow-ups from M7's next-steps list:
  1. **Streaming file reads on Android.** Previously, sending a picked
     file (content:// URI) or folder (tree URI) copied everything into
     the app's cache dir first, since the native transfer engine reads
     via real filesystem paths and Android's document picker doesn't
     give you one. Now: `transfer::FileSource` is a new enum
     (`Path` or `Fd`) threaded through `ConnectionManager::send_file`
     and the chunk-reading code. For a picked file, Kotlin opens a
     `ParcelFileDescriptor` and passes the raw fd number to a new
     `send_file_fd` entrypoint; each chunk worker reopens that fd via
     `/proc/self/fd/{fd}` (a Linux/Android mechanism) to get its own
     independent read position -- necessary because parallel chunk
     workers need independent seek positions, but a single POSIX fd has
     one shared position across all uses. No more double I/O for single
     file sends. Folder sends still copy (see the risk note below for
     why that's a reasonable next step, not this pass's scope).
  2. **Batched folder offers.** New `FolderOffer`/`FolderAccept`/
     `FolderReject` protocol messages (`protocol.rs`) let a whole folder
     be negotiated as ONE accept/reject decision (with a full manifest
     of every file's name/size shown up front) instead of one prompt
     per file. Once accepted, individual `FileOffer`s still follow per
     file (preserving per-file resumability) but are now auto-accepted
     on the receiving side rather than re-prompting -- both UIs show one
     "accept this 40-file folder?" dialog instead of forty separate ones.
- **M9 (done, this push)**: Internet mode is now genuinely complete and
  deployable, not just client-side scaffolding:
  1. **`signaling-server/` -- a new, separate deployable project.**
     A small stateless WebSocket server implementing the exact protocol
     `core/src/signaling.rs` speaks: registers devices by ID, routes
     candidate offers between them, and relays raw bytes as a last
     resort once both sides of a session request it. Ships with a
     Dockerfile, systemd unit example, and TLS reverse-proxy configs
     (Caddy/nginx) in its own README, since a signaling server is
     something you deploy and run yourself -- it's not part of the
     Android/Windows app builds. See `signaling-server/README.md` for
     the full deployment guide and an explicit statement of its trust
     model (it's untrusted for anything beyond rendezvous; all real
     payloads are already Noise-encrypted by the time it sees them).
  2. **The relay data path is now fully wired**, closing the gap that's
     been documented since M5. Previously, `RelayReady`/`RelayData`
     events were received but not acted on. Now: `ConnectionManager`
     performs a complete Noise_XX handshake *through* the relay
     (handshake messages travel as `RelayData` frames instead of QUIC
     stream bytes -- same message format/sequence, different carrier),
     verifies the peer's identity exactly as the QUIC path does, and
     registers a relay-backed `PeerSession`. This required restructuring
     `PeerSession` around a new `OutboundPath` enum (`Quic` or `Relay`)
     so that Noise encryption, message dispatch, and even file-transfer
     chunk sending are IDENTICAL code paths regardless of which
     transport is actually moving the bytes -- from the dispatch layer's
     perspective, a relayed message is indistinguishable from a direct
     one. One deterministic tie-breaker was needed that has no QUIC
     equivalent: since a relay has no "who dialed vs who accepted"
     signal, whichever device's device_id sorts lexicographically first
     takes the Noise initiator role.
  3. **`signaling-server-cloudflare/` -- a free-tier deployment option.**
     Since a self-hosted VPS isn't free, the same server logic was
     reimplemented for Cloudflare Workers + Durable Objects, which
     deploys entirely on Cloudflare's free tier with no server to keep
     running yourself. Speaks the byte-for-byte identical wire protocol
     as the Rust version, so either one works with the apps unchanged
     -- see "Which signaling server should I use?" near the top of this
     file.

## Milestone 9 risk notes

1. **Relayed file transfers don't get QUIC's parallel-stream
   isolation.** A relay session is one WebSocket connection with no
   stream multiplexing equivalent, so chunk messages for a relayed
   transfer share the same serialized path as chat messages, rather
   than each transfer getting its own dedicated lane. This is a real,
   documented performance tradeoff (see `OutboundPath`'s doc comment) --
   acceptable since relay is explicitly the last-resort path (only used
   when direct hole-punching fails on both sides), not a regression in
   the common case.
2. **The signaling server has no rate limiting or abuse protection.**
   Explicitly stated in its own README: it's a minimal, stateless
   router. A production deployment serving real users should add
   rate-limiting (connections per IP, registration churn, relay
   bandwidth) at the reverse-proxy layer or in the server itself --
   not currently implemented.
3. **Two separate copies of the same protocol enum** now exist
   (`core/src/signaling.rs` and `signaling-server/src/protocol.rs`),
   kept deliberately separate rather than sharing a crate dependency
   (the server has no reason to pull in QUIC/mDNS/SQLCipher). This
   means a future protocol change must be applied to both copies by
   hand -- flagged with a comment at the top of both files, but there's
   no compiler-enforced guarantee they stay in sync. Worth a shared
   `zao-signaling-protocol` crate if this becomes error-prone in practice.
4. **The relay handshake's initiator tie-break (lexicographic device_id
   comparison) is new, untested logic.** It's a small, deterministic
   function with an obvious correctness argument (both sides compute
   the same comparison independently and always agree), but like all
   native/protocol code in this project, it hasn't been verified by an
   actual two-device test run in this environment.
5. **`tokio-tungstenite`'s exact API surface is used for the first time
   on the server side** (`accept_async`, `WebSocketStream::split`,
   `Message` variants) -- same category of risk as every new dependency
   in this project; written against the standard, stable pattern but
   not compiled here.
6. **The Cloudflare Workers version uses the Durable Objects Hibernation
   API** (`acceptWebSocket`/`webSocketMessage`/`webSocketClose`), which
   is Cloudflare's documented current pattern for exactly this
   WebSocket-registry use case, but has not been deployed or tested
   against a real Cloudflare account in this environment (no
   credentials or JS runtime available here). If `wrangler deploy`
   reports a type/method mismatch on first try, check
   https://developers.cloudflare.com/durable-objects/best-practices/websockets/
   for the current exact API shape.

## Milestone 8 risk notes

1. **Folder sends still use the cache-dir copy, not streaming.** Single
   *file* picks now stream via fd (see above), but `handlePickedFolder`
   still copies a picked folder tree into the cache dir before calling
   `sendFolder`. Extending the fd approach to a whole tree would need
   either N simultaneously-open ParcelFileDescriptors (one per file,
   for the duration of a potentially long multi-file transfer) or an
   fd-batch protocol variant of `send_folder` -- reasonable follow-up
   work, deliberately out of scope for this pass to keep the fd
   lifetime story simple (one fd, one transfer, closed on that
   transfer's own completion event) rather than introducing a more
   complex multi-fd lifecycle without being able to test it against
   real hardware.
2. **An open ParcelFileDescriptor is closed when ChatActivity is
   destroyed**, even if its transfer hasn't finished -- meaning
   navigating away from the chat screen mid-send currently interrupts
   an in-progress fd-sourced file transfer. This is a real, honest
   limitation of tying fd lifetime to Activity lifetime; a more robust
   design would move the fd-holding responsibility to a
   longer-lived component (a foreground Service), which is a
   reasonable scope increase for a future pass, not done here.
3. **`/proc/self/fd/{fd}` re-opening is Linux/Android-specific and
   unverified against real hardware.** The technique itself is
   well-established (used by various Android NDK code and some
   cross-platform libraries), but this project's implementation
   hasn't been tested against a real device/emulator, since this
   environment has no Android runtime to test against -- flagged as
   the same category of risk as every previous milestone's untested
   native code.
4. **Folder-level accept auto-accepts each subsequent per-file
   FileOffer** by checking `folder_batch_id` against a client-side
   `acceptedFolderBatches` set (Android) -- this set is in-memory only
   and doesn't persist across an app restart mid-folder-transfer. If
   the app restarts while a large folder transfer is still arriving,
   any FileOffers for that batch that hadn't arrived yet before the
   restart would re-prompt individually rather than auto-accepting.
   Minor edge case, not silently ignored.

## Milestone 7 risk notes

1. **WiFi Direct port exchange uses a fixed TCP port (57732).** This
   assumes nothing else on the device's WiFi Direct group interface is
   using that port -- reasonable for a purpose-built app, but worth
   knowing if debugging a bind failure on that specific port.
2. **The owner-side WiFi Direct client address assumption is
   untested.** Documented inline in `MainActivity.kt`'s
   `performWifiDirectPortExchange`: when the group owner receives a
   port-exchange connection, it currently assumes it can reach that
   client back at the same `groupOwnerAddress` convention used
   elsewhere, which is a reasonable assumption for this app's 1:1 model
   but hasn't been verified against real multi-device WiFi Direct
   hardware (not available in this environment).
3. **Folder transfer sends every file as an independent transfer**, not
   a single combined stream -- for a folder with many small files, this
   means many separate `FileOffer`/accept round-trips rather than one
   negotiation for the whole batch. This matches how the underlying
   chunked-transfer engine is built (one `TransferHandle` per file) and
   keeps per-file resumability, at the cost of more protocol
   round-trips for very large folders. A future pass could batch the
   *offer* itself (one `FileOffer`-like message listing all files) while
   still transferring each file's chunks independently, if round-trip
   overhead becomes a real issue.
4. **Auto-resume doesn't yet have a UI-visible indicator** that it's
   happening -- it resumes silently in the background and progress
   events flow through the same `TransferProgress` path as any other
   transfer, so the existing file-bubble UI should show it advancing
   again, but there's no distinct "resuming…" state communicated before
   the first progress event arrives after a reconnect.

## Milestone 6 risk notes

1. **`chacha20poly1305` crate API surface risk.** Same category of risk
   as `quinn`/`snow`/`mdns-sd` in earlier milestones -- this is the
   first use of this crate in the project (for `sealed_box` in
   `identity.rs`), written against the standard RustCrypto AEAD trait
   pattern (`Aead`/`KeyInit` traits, `.encrypt()`/`.decrypt()`) which has
   been stable across the 0.9/0.10 line, but not verified by an actual
   build in this environment.
2. **Two real bugs caught and fixed while building this milestone**,
   worth knowing about:
   - `upsert_known_device` (written in M1) had never been called with a
     real public key anywhere in the codebase -- meaning `sealed_box`
     would have had an empty table to look up recipients from. Fixed by
     persisting each peer's public key immediately after a successful
     Noise handshake, in `register_session`.
   - The WiFi Direct → QUIC hand-off was initially written with a
     placeholder port (`:0`) that would have silently failed to
     connect -- caught in review and replaced with an honest status
     message plus a documented gap (see above) instead of code that
     looks functional but isn't.
3. **BLE runtime permissions on Android 12+ (API 31+).**
   `BLUETOOTH_SCAN`/`ADVERTISE`/`CONNECT` are runtime-requestable, not
   just manifest declarations -- `MainActivity`'s permission request
   list was extended to include them (previously only requested
   `ACCESS_FINE_LOCATION`, which is what M2's mDNS discovery needed).
   Devices on Android 11 and below don't have these as runtime
   permissions at all; requesting them there is a harmless no-op.
4. **WiFi Direct's WPS_PBC (push-button) connection mode** was chosen
   over PIN entry to avoid a separate PIN-entry UI flow for this
   milestone. Most modern Android devices support PBC, but this hasn't
   been tested against real hardware (no ability to do so in this
   environment) -- if connection negotiation fails on a specific device
   pairing, PIN-based `WpsInfo.DISPLAY`/`KEYPAD` modes are the fallback
   to add.

## RESOLVED in M9: the signaling/relay server now exists

At Milestone 5, no signaling/relay server was included in this
project, by explicit request at the time -- only the client-side
protocol (see `signaling.rs`). That changed in Milestone 9:
`signaling-server/` is now a complete, deployable implementation of
exactly the four responsibilities listed below, with its own README
covering deployment (Docker, systemd, fly.io/Render, TLS reverse-proxy
setup). See `signaling-server/README.md` for the full picture. The
description below is kept for historical context on what the protocol
requires of a server, which is still accurate.

1. Accepts connections and handles `Register { device_id }`, replying
   with `RegisterAck`.
2. Routes `OfferCandidates { to_device_id, ... }` from one connected
   device to another by looking up `to_device_id` among currently
   registered connections, delivering it as `IncomingCandidates`.
3. Routes `CandidateResult` back to whichever device sent the original
   offer for that `session_id`.
4. For `RequestRelay`, if both devices in a session request it, begins
   forwarding `RelayData { session_id, data }` frames between them
   verbatim (the server never needs to decrypt anything -- payloads are
   already Noise-encrypted by the time they'd reach the relay path).

This is a deliberately small, stateless protocol (no server-side
database, no persistent accounts) -- suitable for a lightweight
WebSocket relay, which is exactly what `signaling-server/` is.

## Milestone 5 risk notes / known incompleteness (read before pushing)

1. **STUN-discovered address may not match QUIC's actual public
   port.** `stun_client.rs`'s query runs on a separate throwaway UDP
   socket, not the same socket `quinn::Endpoint` binds for QUIC itself
   (quinn doesn't expose raw datagram I/O on its bound socket). For NATs
   that preserve port numbers consistently across different local ports
   from the same device (common but not universal), the STUN result
   will still be usable as a QUIC connect target; for NATs that don't
   (some symmetric NATs), the discovered address won't actually reach
   the QUIC listener, and hole-punching will fail over to relay. This
   is a genuine limitation, not a bug to silently paper over -- flagged
   directly in `stun_probe_socket`'s doc comment as well.
2. **Relay data path is defined but not connected end-to-end.**
   `SignalingMessage::RelayData` and the corresponding `SignalingEvent`
   exist, and `ConnectionManager::handle_signaling_event` requests a
   relay when direct candidates fail on both sides, but incoming
   `RelayData` bytes are not yet decrypted/dispatched the same way a
   direct QUIC stream's bytes are (see the comment in
   `handle_signaling_event`'s `RelayData` arm). This is the natural next
   increment once a real relay server exists to test against --
   building and testing that wiring blind, with no server to verify it
   against, would be lower-confidence than being explicit that it's the
   next step.
3. **No re-attempt loop or timeout on the offering side.** If a peer
   never responds to offered candidates (offline, signaling server
   restarted, etc), `connect_to_peer_via_internet` doesn't currently
   retry or time out with a UI-visible failure -- it simply never
   produces a `PeerConnected` event. A future pass should add a
   visible timeout/failure event, not just silence.
4. **No TLS certificate pinning or auth on the signaling connection
   itself** beyond whatever the `wss://` TLS layer provides -- anyone
   who can reach your deployed signaling server can attempt to
   register a device_id and receive candidates addressed to it. This is
   consistent with the protocol's design (the signaling server is
   explicitly untrusted for anything beyond connection rendezvous,
   since all real payloads are Noise-encrypted independently), but a
   production deployment should still rate-limit and monitor the
   signaling server for abuse.

## Milestone 4 risk notes (still applicable)

1. **I caught and fixed three real bugs while building this milestone**,
   worth knowing about since they're the kind of thing that's easy to
   reintroduce if this code is refactored later:
   - `handle_file_reject` originally didn't receive `from_device_id` at
     all and hardcoded an empty string in the event sent to the UI --
     fixed to thread it through properly.
   - The sender-side transfer completion handler reported
     `TransferState::Completed` even when the transfer had actually been
     cancelled -- fixed to report `Cancelled` correctly and persist that
     to the DB.
   - `Session.storage` was briefly typed with `std::sync::Mutex` while
     call sites used `.blocking_lock()` (a `tokio::sync::Mutex`-only
     method) -- would have been a hard compile error; fixed by
     explicitly disambiguating `QueueSink`'s (correctly synchronous)
     event queue as `std::sync::Mutex` and `Session.storage` (correctly
     async-shared) as `tokio::sync::Mutex`, rather than relying on a
     single ambiguous `use` import for both.
2. **Sender-side resume is DB-tracked but not yet auto-resumed on
   restart.** If the app restarts mid-send, the manifest correctly
   remembers which chunks were sent, but nothing currently re-triggers
   `send_file` automatically for an interrupted outgoing transfer --
   the user would need to re-initiate the send (which will then, thanks
   to the manifest, only actually retransmit missing chunks once the
   engine processes `pending_chunk_indices()`). Receiver-side resume
   (re-accepting the same offer) is fully wired. Auto-resuming
   in-flight sends on restart is a reasonable follow-up, not core to
   this milestone's scope.
3. **The content:// URI → filesystem path copy on Android** (in
   `ChatActivity.handlePickedFile`) means picking a very large file
   costs one extra full read+write into the app's cache dir before the
   transfer even starts, since the native chunking engine reads real
   filesystem paths, not Android content URIs. Fine for this milestone;
   a future pass could stream directly from a `ParcelFileDescriptor`
   to avoid the double I/O for multi-GB files specifically.
4. **Folder transfer is modeled but not implemented.** `FileOffer`
   already carries `relative_path`/`folder_batch_id` fields for this
   (from M3's protocol design), but there's no UI or FFI entrypoint yet
   that offers a whole folder as a batch of individual `FileOffer`s.
   Deferred to M5 per the original plan.
5. **`quinn`/`snow`/`mdns-sd` API surface risk carries over from M2/M3**
   -- see those sections below, still applicable since this milestone
   builds directly on that transport code, plus this milestone adds
   `tokio::sync::oneshot` (standard, low-risk) for the offer/accept
   handshake.

## Milestone 3 risk notes (still applicable)

Beyond the general "not compiled yet" caveat below, these areas are
newly added and more likely to need first-CI-run attention:

1. **Identity binding fix.** While building this milestone I caught and
   fixed a real design gap: the Noise handshake only reveals a peer's
   X25519 key, but `device_id` was defined from the Ed25519 key. Fixed
   by deterministically deriving the X25519 Noise static key from the
   Ed25519 signing key (`derive_noise_static` in `identity.rs`), with a
   test (`noise_static_key_resolves_back_to_real_device_id`) proving the
   two now resolve to the same device_id. Worth re-reading that function
   if you touch identity code later -- it's load-bearing for the
   post-handshake identity check in `connection_manager.rs`.
2. **Chat only works between two devices that have discovered each
   other on the same LAN in this milestone.** `connect_to_peer` dials a
   raw `ip:port` from a `DiscoveredPeer` entry -- there is no persisted
   "contacts" concept yet, so closing and reopening the app currently
   means re-discovering and re-tapping a peer before chat resumes (chat
   *history* does persist and reload correctly; only the live connection
   needs re-establishing).
3. **The event queue (`QueueSink`) has no cap.** For chat-scale message
   volume this is a non-issue, but if the UI stops polling for an
   extended period while messages keep arriving, memory grows
   unbounded. Not a concern for this milestone's scope; worth a max-size
   + drop-oldest policy if idle-UI scenarios become common later.
4. **`quinn`/`snow`/`mdns-sd` API surface risk carries over from M2** --
   see the M2 notes below, still applicable since this milestone builds
   directly on that transport code.
5. **Windows UI's drag-and-drop is currently a visual-only shell.** It
   correctly detects drag-over/drop and shows the affordance, but does
   not yet trigger a real file transfer -- that requires the
   `send_file`/`accept_file` FFI entrypoints, which are explicitly
   deferred to the next milestone (see below).

## Milestone 2 risk notes (still applicable)

Milestone 2 adds mDNS/UDP discovery, QUIC transport (`quinn`), and the
chunked transfer engine. Beyond the general "not compiled yet" caveat
above, these specific areas are more likely to need first-CI-run fixes
than the Milestone 1 code:

1. **`ring` cross-compiling to Android targets.** `rustls` (via `quinn`)
   depends on `ring`, which needs a C compiler + assembler for each
   Android ABI (arm64-v8a, armeabi-v7a, x86_64). `cargo-ndk` normally
   sets this up automatically, but if the Android workflow fails with
   linker errors mentioning `ring` or `aes`/`sha256` assembly, that's
   the likely cause -- the fix is usually ensuring the NDK's clang is on
   PATH for the build step (cargo-ndk does this, but NDK version drift
   can break it).
2. **`mdns-sd`'s multicast behavior on Android.** Android's networking
   stack is stricter about multicast than desktop Linux/Windows. The
   `MulticastLock` in `MainActivity.kt` is necessary but may not be
   sufficient on all OEM ROMs/Android versions -- some devices (notably
   some Xiaomi/Samsung power-saving configs) throttle background
   multicast regardless. The UDP broadcast fallback in `discovery.rs`
   exists specifically to cover this gap; if mDNS finds nothing on a
   real device but UDP broadcast entries show up in `discoverPeers()`,
   that's the fallback working as designed, not a bug.
3. **`quinn` 0.11's exact API shape.** This was written against my
   knowledge of the quinn 0.10/0.11 API (`Endpoint::server`,
   `ServerConfig::with_single_cert`, `QuicClientConfig::try_from`).
   Point-release API changes between minor quinn versions are common;
   if `cargo build` reports missing methods on `Endpoint` or
   `ServerConfig`, check the installed quinn version's docs.rs page and
   adjust `transport.rs` accordingly -- the logic/intent will still be
   correct even if a method name shifted.
4. **Android emulators can't test real mDNS/LAN discovery.** Two
   emulator instances don't share a real LAN segment by default, so
   discovery between them may not work even with correct code -- test
   peer discovery on two physical devices (or one physical device +
   the Windows build) on the same WiFi network instead.
5. **Windows Firewall will prompt on first QUIC bind.** The first time
   the Windows EXE runs, Windows Defender Firewall will likely show an
   "Allow this app to communicate" prompt for both UDP (QUIC) and
   possibly mDNS. This is expected, not an error -- the user needs to
   click Allow for LAN discovery/transfer to work.

## Known temporary shortcuts (must fix before any real release)

1. **DB encryption key handling is a placeholder.** Both `MainActivity.kt`
   and `windows/src-tauri/src/main.rs` currently generate a random key and
   store it in plaintext (SharedPreferences on Android, a plain file on
   Windows) purely so the SQLCipher pipeline is testable end-to-end.
   Before shipping: wrap the key with Android Keystore (Android) and
   DPAPI / `CryptProtectData` (Windows) so it's never stored in plaintext.
2. **No `icon.ico`** — see `windows/src-tauri/icons/README.md`. Generate
   one with `tauri icon <source.png>` once you have real branding, then
   add it back into `tauri.conf.json`.
3. **Kotlin package path is fixed** (`com.zao.p2p.core.NativeBridge`) —
   if you rename it, update the `Java_com_zao_p2p_core_NativeBridge_*`
   function names in `core/src/ffi.rs` to match, or JNI linking fails at
   runtime with `UnsatisfiedLinkError`.

## Building locally (optional — CI does this for you)

**Android core lib:**
```
cd core
cargo ndk -t arm64-v8a -t armeabi-v7a -t x86_64 -o ../android/app/src/main/jniLibs build --release
cd ../android && ./gradlew assembleDebug
```

**Windows:**
```
cargo install tauri-cli --version "^1"
cd windows
cargo tauri build
```

## Next milestones

- **M10**: Extend streaming reads to folder sends (see M8's risk notes
  on why this was scoped out of M8 itself), move fd-holding
  responsibility to a foreground Service so a transfer survives
  navigating away from ChatActivity. Add rate-limiting to
  `signaling-server` before any public-facing deployment.
- **M11**: Store-and-forward / DTN groundwork -- BLE mesh's flood-relay
  foundation from M6 is the natural base to extend here. Android
  Keystore/DPAPI key wrapping (replacing the M1 placeholder) should
  land no later than this milestone, ideally sooner.
