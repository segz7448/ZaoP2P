package com.zao.p2p

import android.Manifest
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.net.wifi.WifiManager
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.TextView
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat
import com.zao.p2p.chat.ChatActivity
import com.zao.p2p.core.NativeBridge
import com.zao.p2p.transport.BleMeshManager
import com.zao.p2p.transport.WifiDirectManager
import com.zao.p2p.transport.WifiDirectPortExchange
import org.json.JSONArray
import org.json.JSONObject
import java.security.SecureRandom
import java.util.Base64

/**
 * Milestone 3: this screen is now a peer list/launcher, not the chat
 * itself. It starts networking, polls discovered LAN peers, and lets the
 * user tap one to open ChatActivity (the actual 1:1 chat screen).
 */
class MainActivity : AppCompatActivity() {

    companion object {
        // Feature 1: signaling server is a fixed, already-deployed
        // endpoint -- nobody should have to type this in per connection.
        // If the deployment ever moves, this is the one line to change.
        private const val DEFAULT_SIGNALING_URL = "wss://zao-signaling-server.ayiijumo.workers.dev"
        private const val RECENT_PEERS_PREFS = "zao_recent_internet_peers"
        private const val RECENT_PEERS_KEY = "peers"
        private const val RECENT_PEERS_MAX = 10
    }

    private val pollHandler = Handler(Looper.getMainLooper())
    private lateinit var peerListLayout: LinearLayout
    private lateinit var statusView: TextView
    private lateinit var recentPeersLayout: LinearLayout
    private var multicastLock: WifiManager.MulticastLock? = null
    private var selfDeviceId: String = ""
    private var dbPathForBle: String = ""
    private var dbKeyForBle: String = ""

    private var wifiDirectManager: WifiDirectManager? = null
    private var bleMeshManager: BleMeshManager? = null
    private lateinit var wifiDirectStatusView: TextView
    private lateinit var bleStatusView: TextView

    // BLUETOOTH_SCAN/ADVERTISE/CONNECT are runtime-requestable on API 31+
    // (Android 12+), not just manifest declarations -- BLE mesh
    // (BleMeshManager) silently finds nothing without these actually
    // being granted, similar to how ACCESS_FINE_LOCATION gates mDNS/
    // WiFi Direct peer discovery on older API levels. Requesting a
    // permission that doesn't apply to the running OS version is a
    // harmless no-op (the system ignores unknown/inapplicable runtime
    // permission requests), so listing all of these unconditionally is
    // simpler than branching on Build.VERSION.SDK_INT here.
    private val requiredPermissions = arrayOf(
        Manifest.permission.ACCESS_FINE_LOCATION,
        Manifest.permission.BLUETOOTH_SCAN,
        Manifest.permission.BLUETOOTH_ADVERTISE,
        Manifest.permission.BLUETOOTH_CONNECT,
    )

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        val dbPath = "${filesDir.absolutePath}/zao.db"
        val dbKey = getOrCreateLocalDbKey()

        val initResultJson = try {
            NativeBridge.initApp(dbPath, dbKey)
        } catch (e: UnsatisfiedLinkError) {
            """{"error":"native library not loaded: ${e.message}"}"""
        }
        selfDeviceId = try {
            JSONObject(initResultJson).optString("device_id", "")
        } catch (e: Exception) {
            ""
        }
        dbPathForBle = dbPath
        dbKeyForBle = dbKey

        val root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(48, 96, 48, 48)
        }
        root.addView(TextView(this).apply {
            textSize = 16f
            text = "Zao P2P -- Milestone 6\nYour device: $selfDeviceId\n"
        })
        statusView = TextView(this).apply { textSize = 14f }
        root.addView(statusView)
        root.addView(TextView(this).apply {
            textSize = 14f
            setPadding(0, 24, 0, 8)
            text = "Nearby devices (LAN):"
        })
        peerListLayout = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL }
        root.addView(peerListLayout)

        // WiFi Direct: a device-to-device link that doesn't need shared
        // WiFi infrastructure. Once a group forms, the group owner's IP
        // is handed to the SAME QUIC transport used for LAN (see
        // WifiDirectManager's doc comment) -- this UI section only
        // triggers discovery/connection; the actual chat/transfer path
        // afterward is identical to any LAN peer.
        root.addView(TextView(this).apply {
            textSize = 14f
            setPadding(0, 24, 0, 8)
            text = "WiFi Direct:"
        })
        wifiDirectStatusView = TextView(this).apply {
            textSize = 12f
            text = "Not started"
        }
        root.addView(wifiDirectStatusView)
        root.addView(Button(this).apply {
            text = "Discover WiFi Direct peers"
            setOnClickListener { wifiDirectManager?.discoverPeers() }
        })

        // BLE mesh: messaging-only fallback with no WiFi/internet at
        // all, using sealed_box encryption (see identity.rs) since it
        // has no ordered-stream session to hang Noise transport state
        // off of. Kept as its own status line rather than merged into
        // the LAN peer list, since BLE peers can only exchange short
        // encrypted text messages here, not open a full chat/transfer
        // session the way a LAN/WiFi-Direct/internet peer can.
        root.addView(TextView(this).apply {
            textSize = 14f
            setPadding(0, 24, 0, 8)
            text = "Bluetooth mesh (messaging only):"
        })
        bleStatusView = TextView(this).apply {
            textSize = 12f
            text = "Not started"
        }
        root.addView(bleStatusView)

        // Internet mode entry point: signaling server is fixed (see
        // DEFAULT_SIGNALING_URL) -- only the peer needs to be supplied,
        // either as a pasted connect link or a bare device_id shared out
        // of band (e.g. the person reads it off the other device's "Your
        // device" label, or shares this device's own link below).
        root.addView(Button(this).apply {
            text = "Connect over the internet…"
            setOnClickListener { showInternetConnectDialog() }
        })
        root.addView(Button(this).apply {
            text = "Share my connect link…"
            setOnClickListener { shareMyConnectLink() }
        })

        root.addView(TextView(this).apply {
            textSize = 14f
            setPadding(0, 24, 0, 8)
            text = "Recent (internet):"
        })
        recentPeersLayout = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL }
        root.addView(recentPeersLayout)

        setContentView(root)

        requestPermissionsThenStartNetworking(dbPath, dbKey)
        renderRecentPeers()
    }

    private fun requestPermissionsThenStartNetworking(dbPath: String, dbKey: String) {
        val missing = requiredPermissions.filter {
            ContextCompat.checkSelfPermission(this, it) != PackageManager.PERMISSION_GRANTED
        }
        if (missing.isNotEmpty()) {
            ActivityCompat.requestPermissions(this, missing.toTypedArray(), 1001)
        }
        startNetworkingAndPoll(dbPath, dbKey)
    }

    private fun acquireMulticastLock() {
        val wifiManager = applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager
        multicastLock = wifiManager.createMulticastLock("zaop2p-mdns").apply {
            setReferenceCounted(true)
            acquire()
        }
    }

    private fun startNetworkingAndPoll(dbPath: String, dbKey: String) {
        acquireMulticastLock()

        val displayName = android.os.Build.MODEL ?: "Android Device"
        val downloadsDir = "${filesDir.absolutePath}/downloads"

        val startResult = try {
            NativeBridge.startNetworking(dbPath, dbKey, displayName, downloadsDir)
        } catch (e: UnsatisfiedLinkError) {
            """{"error":"${e.message}"}"""
        }
        statusView.text = "Networking: $startResult"

        pollPeers()
        startWifiDirect()
        startBleMesh()
    }

    private fun startWifiDirect() {
        wifiDirectManager = WifiDirectManager(
            context = applicationContext,
            onPeersChanged = { devices ->
                runOnUiThread {
                    wifiDirectStatusView.text = if (devices.isEmpty()) {
                        "No WiFi Direct peers found yet…"
                    } else {
                        "Found: " + devices.joinToString(", ") { it.deviceName }
                    }
                }
            },
            onGroupFormed = { isGroupOwner, groupOwnerAddress ->
                runOnUiThread {
                    wifiDirectStatusView.text = if (groupOwnerAddress != null) {
                        "Group formed (owner=$isGroupOwner) at $groupOwnerAddress -- connecting via QUIC…"
                    } else {
                        "Group formed but no address available"
                    }
                }
                // Closes the WiFi Direct -> QUIC gap: do a tiny raw TCP
                // port-exchange handshake over the WiFi Direct link
                // itself (see WifiDirectPortExchange's doc comment for
                // why this approach was chosen over depending on mDNS
                // behavior that can't be verified without real
                // hardware), then hand the peer's real QUIC port to the
                // exact same connectToPeer path used for any LAN peer.
                if (groupOwnerAddress != null) {
                    performWifiDirectPortExchange(isGroupOwner, groupOwnerAddress)
                }
            },
            onError = { message ->
                runOnUiThread { wifiDirectStatusView.text = "WiFi Direct error: $message" }
            },
        )
        wifiDirectManager?.start()
    }

    /**
     * Runs the WifiDirectPortExchange handshake on a background thread
     * (both listenForExchange and connectForExchange block the calling
     * thread), then connects via QUIC using the peer's real port once
     * learned. The group owner listens; the client (non-owner) connects
     * -- this matches the natural client/server roles WiFi Direct
     * already assigns, so no extra negotiation is needed to decide who
     * listens.
     */
    private fun performWifiDirectPortExchange(isGroupOwner: Boolean, groupOwnerAddress: String) {
        Thread {
            val selfQuicPort = readSelfQuicPort()
            if (selfQuicPort == null) {
                runOnUiThread { wifiDirectStatusView.text = "WiFi Direct: could not read own QUIC port yet" }
                return@Thread
            }

            val result = if (isGroupOwner) {
                WifiDirectPortExchange.listenForExchange(selfQuicPort, selfDeviceId)
            } else {
                WifiDirectPortExchange.connectForExchange(groupOwnerAddress, selfQuicPort, selfDeviceId)
            }

            if (result == null) {
                runOnUiThread { wifiDirectStatusView.text = "WiFi Direct: port exchange failed or timed out" }
                return@Thread
            }

            // The group owner's address is known (groupOwnerAddress);
            // for the CLIENT side connecting to the owner, that's
            // exactly the address to dial. For the OWNER side, the
            // just-connected client's address is only available as the
            // TCP peer address from the exchange socket itself, which
            // WifiDirectPortExchange doesn't currently surface back up
            // (it only returns the peer's declared port/device_id, not
            // their observed IP) -- since WiFi Direct groups are
            // typically one owner + one client in this app's 1:1 model,
            // the owner can reasonably assume the client is reachable at
            // the WiFi Direct group's client-side address convention,
            // but this hasn't been verified against real multi-device
            // WiFi Direct hardware. Flagged rather than silently assumed.
            val peerAddr = "$groupOwnerAddress:${result.peerQuicPort}"

            val connectResult = try {
                NativeBridge.connectToPeer(peerAddr, result.peerDeviceId)
            } catch (e: UnsatisfiedLinkError) {
                """{"error":"${e.message}"}"""
            }
            runOnUiThread {
                wifiDirectStatusView.text = "WiFi Direct: $connectResult (peer ${result.peerDeviceId})"
            }
        }.start()
    }

    private fun readSelfQuicPort(): Int? {
        val statusJson = try {
            NativeBridge.networkingStatus()
        } catch (e: UnsatisfiedLinkError) {
            return null
        }
        return try {
            val addr = JSONObject(statusJson).optString("quic_local_addr", "")
            addr.substringAfterLast(":").toIntOrNull()
        } catch (e: Exception) {
            null
        }
    }

    private fun startBleMesh() {
        bleMeshManager = BleMeshManager(
            context = applicationContext,
            onMessageReceived = { fromShortId, sealedPayload ->
                val sealedHex = sealedPayload.joinToString("") { "%02x".format(it) }
                Thread {
                    val plaintext = try {
                        NativeBridge.bleOpenMessage(dbPathForBle, dbKeyForBle, sealedHex)
                    } catch (e: UnsatisfiedLinkError) {
                        null
                    }
                    runOnUiThread {
                        bleStatusView.text = if (plaintext != null && !plaintext.startsWith("{\"error\"")) {
                            "BLE message from $fromShortId: $plaintext"
                        } else {
                            "BLE message from $fromShortId (could not decrypt -- unknown sender key)"
                        }
                    }
                }.start()
            },
            onPeerDiscovered = { shortId, _ ->
                runOnUiThread {
                    bleStatusView.text = "BLE mesh peer nearby: $shortId"
                }
            },
            onError = { message ->
                runOnUiThread { bleStatusView.text = "BLE mesh error: $message" }
            },
        )
        bleMeshManager?.start(selfDeviceId)
    }

    private fun pollPeers() {
        val peersJson = try {
            NativeBridge.discoverPeers()
        } catch (e: UnsatisfiedLinkError) {
            "[]"
        }
        renderPeerList(peersJson)
        pollHandler.postDelayed({ pollPeers() }, 2000)
    }

    private fun renderPeerList(peersJson: String) {
        val peers = try {
            JSONArray(peersJson)
        } catch (e: Exception) {
            return
        }
        peerListLayout.removeAllViews()
        for (i in 0 until peers.length()) {
            val peer = peers.optJSONObject(i) ?: continue
            val deviceId = peer.optString("device_id")
            val displayName = peer.optString("display_name", "Unknown Device")
            val addr = peer.optString("addr")

            val button = Button(this).apply {
                text = "$displayName  ($addr)"
                setOnClickListener {
                    val intent = Intent(this@MainActivity, ChatActivity::class.java).apply {
                        putExtra(ChatActivity.EXTRA_PEER_DEVICE_ID, deviceId)
                        putExtra(ChatActivity.EXTRA_PEER_ADDR, addr)
                    }
                    startActivity(intent)
                }
            }
            peerListLayout.addView(button)
        }
        if (peers.length() == 0) {
            peerListLayout.addView(TextView(this).apply {
                text = "No devices found yet on this network…"
                textSize = 13f
            })
        }
    }

    /**
     * Feature 2: a single shareable link that carries this device's
     * device_id (the server is already fixed, so it doesn't need to be
     * in the link). Peers connect by pasting it into "Connect over the
     * internet…" -- see parsePeerIdFromInput, which also still accepts a
     * bare device_id for backwards compatibility.
     */
    private fun buildConnectLink(deviceId: String): String = "zaop2p://connect?peer=$deviceId"

    private fun parsePeerIdFromInput(input: String): String {
        val trimmed = input.trim()
        if (trimmed.startsWith("zaop2p://")) {
            val peer = Uri.parse(trimmed).getQueryParameter("peer")
            if (!peer.isNullOrEmpty()) return peer
        }
        return trimmed
    }

    private fun shareMyConnectLink() {
        val link = buildConnectLink(selfDeviceId)
        val sendIntent = Intent(Intent.ACTION_SEND).apply {
            type = "text/plain"
            putExtra(Intent.EXTRA_TEXT, link)
        }
        startActivity(Intent.createChooser(sendIntent, "Share your Zao connect link"))
    }

    /**
     * Feature 3: remembers peers connected to over the internet so
     * reconnecting doesn't mean re-entering their id (or link) again.
     */
    private fun recentPeersPrefs() = getSharedPreferences(RECENT_PEERS_PREFS, MODE_PRIVATE)

    private fun loadRecentPeers(): List<String> {
        val raw = recentPeersPrefs().getString(RECENT_PEERS_KEY, null) ?: return emptyList()
        return try {
            val arr = JSONArray(raw)
            (0 until arr.length()).map { arr.getString(it) }
        } catch (e: Exception) {
            emptyList()
        }
    }

    private fun saveRecentPeer(deviceId: String) {
        val updated = listOf(deviceId) + loadRecentPeers().filter { it != deviceId }
        val arr = JSONArray(updated.take(RECENT_PEERS_MAX))
        recentPeersPrefs().edit().putString(RECENT_PEERS_KEY, arr.toString()).apply()
        renderRecentPeers()
    }

    private fun renderRecentPeers() {
        recentPeersLayout.removeAllViews()
        val recents = loadRecentPeers()
        if (recents.isEmpty()) {
            recentPeersLayout.addView(TextView(this).apply {
                text = "No internet peers yet…"
                textSize = 13f
            })
            return
        }
        for (deviceId in recents) {
            recentPeersLayout.addView(Button(this).apply {
                text = deviceId
                setOnClickListener { connectViaInternet(DEFAULT_SIGNALING_URL, deviceId) }
            })
        }
    }

    /**
     * Signaling server is fixed (DEFAULT_SIGNALING_URL) -- no longer
     * prompted for. Only the peer is asked for, and can be given either
     * as a full connect link (see buildConnectLink) or a bare
     * device_id. A successful connection surfaces as a PeerConnected
     * event via the normal event-polling path once hole-punching (or
     * relay, once wired) completes; this dialog itself does not open
     * ChatActivity automatically, since the connection outcome isn't
     * known synchronously.
     */
    private fun showInternetConnectDialog() {
        val layout = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(48, 24, 48, 0)
        }
        val peerInput = EditText(this).apply { hint = "Paste connect link or peer device ID" }
        layout.addView(peerInput)

        AlertDialog.Builder(this)
            .setTitle("Connect over the internet")
            .setView(layout)
            .setPositiveButton("Connect") { _, _ ->
                val peerDeviceId = parsePeerIdFromInput(peerInput.text.toString())
                if (peerDeviceId.isNotEmpty()) {
                    connectViaInternet(DEFAULT_SIGNALING_URL, peerDeviceId)
                }
            }
            .setNegativeButton("Cancel", null)
            .show()
    }

    private fun connectViaInternet(serverUrl: String, peerDeviceId: String) {
        Thread {
            try {
                val signalResult = NativeBridge.connectSignalingServer(serverUrl)
                val signalJson = JSONObject(signalResult)
                if (signalJson.has("error")) {
                    runOnUiThread { statusView.text = "Signaling connect failed: ${signalJson.optString("error")}" }
                    return@Thread
                }
                val offerResult = NativeBridge.connectToPeerViaInternet(peerDeviceId)
                runOnUiThread {
                    statusView.text = "Internet mode: $offerResult (waiting for connection to establish…)"
                    saveRecentPeer(peerDeviceId)
                    val intent = Intent(this@MainActivity, ChatActivity::class.java).apply {
                        putExtra(ChatActivity.EXTRA_PEER_DEVICE_ID, peerDeviceId)
                        putExtra(ChatActivity.EXTRA_PEER_ADDR, "") // no LAN address -- rely on the session internet-mode connect already established
                    }
                    startActivity(intent)
                }
            } catch (e: UnsatisfiedLinkError) {
                runOnUiThread { statusView.text = "Internet connect failed: ${e.message}" }
            }
        }.start()
    }

    /**
     * TEMPORARY key handling for this milestone only. Stores the
     * SQLCipher passphrase in SharedPreferences, which is NOT the final
     * design -- see README.md. Must be replaced with Android Keystore-
     * backed key wrapping before any real release.
     */
    private fun getOrCreateLocalDbKey(): String {
        val prefs = getSharedPreferences("zao_temp_keys", MODE_PRIVATE)
        prefs.getString("db_key", null)?.let { return it }

        val bytes = ByteArray(32)
        SecureRandom().nextBytes(bytes)
        val key = Base64.getEncoder().encodeToString(bytes)
        prefs.edit().putString("db_key", key).apply()
        return key
    }

    override fun onDestroy() {
        pollHandler.removeCallbacksAndMessages(null)
        multicastLock?.let { if (it.isHeld) it.release() }
        wifiDirectManager?.stop()
        bleMeshManager?.stop()
        super.onDestroy()
    }
}
