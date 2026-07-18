package com.zao.p2p.transport

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.net.wifi.p2p.WifiP2pConfig
import android.net.wifi.p2p.WifiP2pDevice
import android.net.wifi.p2p.WifiP2pDeviceList
import android.net.wifi.p2p.WifiP2pInfo
import android.net.wifi.p2p.WifiP2pManager
import android.util.Log

/**
 * Wraps Android's WifiP2pManager (WiFi Direct) for peer discovery and
 * group formation. This is Android-only by nature, not by choice --
 * WiFi Direct has no equivalent API on Windows and no Rust binding
 * exists, so unlike the QUIC/mDNS transport (which lives in the shared
 * Rust core), this manager is pure Kotlin, native to this platform.
 *
 * WiFi Direct's role in the transport priority order (WiFi Direct > LAN
 * > Internet > BLE mesh, per the project requirements) is to provide a
 * direct device-to-device link WITHOUT requiring both devices to be on
 * the same access-point network -- useful when two phones are near each
 * other but not on shared WiFi (e.g. no router present, or on different
 * WiFi networks/guest VLANs that isolate clients from each other).
 *
 * Once a WiFi Direct group forms, the group owner gets a real IP
 * address other members can reach it at (typically 192.168.49.1) --
 * from that point on, the EXISTING QUIC transport in the Rust core
 * connects over that IP exactly like any LAN connection. This class's
 * job ends at "group formed, here's the group owner's IP" -- it does
 * not reimplement QUIC/Noise/chunking, it just establishes the link
 * that the Rust core's transport then uses.
 */
class WifiDirectManager(
    private val context: Context,
    private val onPeersChanged: (List<WifiP2pDevice>) -> Unit,
    private val onGroupFormed: (isGroupOwner: Boolean, groupOwnerAddress: String?) -> Unit,
    private val onError: (String) -> Unit,
) {
    companion object {
        private const val TAG = "WifiDirectManager"
    }

    private val manager: WifiP2pManager? =
        context.getSystemService(Context.WIFI_P2P_SERVICE) as? WifiP2pManager
    private var channel: WifiP2pManager.Channel? = null
    private var receiver: BroadcastReceiver? = null
    private var isRegistered = false

    private val intentFilter = IntentFilter().apply {
        addAction(WifiP2pManager.WIFI_P2P_STATE_CHANGED_ACTION)
        addAction(WifiP2pManager.WIFI_P2P_PEERS_CHANGED_ACTION)
        addAction(WifiP2pManager.WIFI_P2P_CONNECTION_CHANGED_ACTION)
        addAction(WifiP2pManager.WIFI_P2P_THIS_DEVICE_CHANGED_ACTION)
    }

    /**
     * Registers the broadcast receiver and initializes the WiFi P2P
     * channel. Must be called before discoverPeers(). Requires
     * NEARBY_WIFI_DEVICES (API 33+) or ACCESS_FINE_LOCATION (older) to
     * actually receive peer discovery results -- the manifest already
     * declares both; runtime permission grant is handled by the calling
     * Activity (MainActivity), same as mDNS discovery's requirements.
     */
    fun start() {
        val mgr = manager
        if (mgr == null) {
            onError("WiFi Direct is not supported on this device")
            return
        }
        channel = mgr.initialize(context, context.mainLooper, null)

        receiver = object : BroadcastReceiver() {
            override fun onReceive(ctx: Context, intent: Intent) {
                handleIntent(intent)
            }
        }
        context.registerReceiver(receiver, intentFilter)
        isRegistered = true
    }

    fun stop() {
        if (isRegistered) {
            try {
                context.unregisterReceiver(receiver)
            } catch (e: IllegalArgumentException) {
                // Already unregistered -- safe to ignore.
            }
            isRegistered = false
        }
        manager?.let { mgr ->
            channel?.let { ch ->
                mgr.removeGroup(ch, null) // best-effort cleanup, no callback needed on teardown
            }
        }
    }

    private fun handleIntent(intent: Intent) {
        when (intent.action) {
            WifiP2pManager.WIFI_P2P_PEERS_CHANGED_ACTION -> requestPeerList()
            WifiP2pManager.WIFI_P2P_CONNECTION_CHANGED_ACTION -> requestConnectionInfo()
            WifiP2pManager.WIFI_P2P_STATE_CHANGED_ACTION -> {
                val state = intent.getIntExtra(WifiP2pManager.EXTRA_WIFI_STATE, -1)
                if (state == WifiP2pManager.WIFI_P2P_STATE_DISABLED) {
                    onError("WiFi Direct was disabled")
                }
            }
        }
    }

    /**
     * Start an active peer scan. Results arrive asynchronously via the
     * PEERS_CHANGED broadcast (handled above), not as a direct callback
     * from this call -- this matches how WifiP2pManager's API is
     * designed; discoverPeers() only reports whether the scan REQUEST
     * was accepted, not scan results.
     */
    fun discoverPeers() {
        val mgr = manager ?: return
        val ch = channel ?: return
        mgr.discoverPeers(ch, object : WifiP2pManager.ActionListener {
            override fun onSuccess() {
                Log.d(TAG, "Peer discovery started")
            }

            override fun onFailure(reasonCode: Int) {
                onError("WiFi Direct discovery failed: ${reasonCodeToString(reasonCode)}")
            }
        })
    }

    private fun requestPeerList() {
        val mgr = manager ?: return
        val ch = channel ?: return
        mgr.requestPeers(ch) { peers: WifiP2pDeviceList ->
            onPeersChanged(peers.deviceList.toList())
        }
    }

    private fun requestConnectionInfo() {
        val mgr = manager ?: return
        val ch = channel ?: return
        mgr.requestConnectionInfo(ch) { info: WifiP2pInfo ->
            if (info.groupFormed) {
                val groupOwnerAddress = info.groupOwnerAddress?.hostAddress
                onGroupFormed(info.isGroupOwner, groupOwnerAddress)
            }
        }
    }

    /**
     * Connect to a specific peer discovered via discoverPeers(). On
     * success, a WIFI_P2P_CONNECTION_CHANGED_ACTION broadcast follows,
     * which triggers requestConnectionInfo() above and ultimately
     * onGroupFormed() with the group owner's IP -- that IP is what the
     * Rust core's QUIC transport then dials, same as any LAN address.
     */
    fun connectToPeer(device: WifiP2pDevice) {
        val mgr = manager ?: return
        val ch = channel ?: return
        val config = WifiP2pConfig().apply {
            deviceAddress = device.deviceAddress
            // WPS_PBC (push-button config) avoids needing a PIN-entry
            // UI flow for this milestone; most modern devices support it.
            wps.setup = android.net.wifi.WpsInfo.PBC
        }
        mgr.connect(ch, config, object : WifiP2pManager.ActionListener {
            override fun onSuccess() {
                Log.d(TAG, "Connection initiated to ${device.deviceName}")
            }

            override fun onFailure(reasonCode: Int) {
                onError("WiFi Direct connect failed: ${reasonCodeToString(reasonCode)}")
            }
        })
    }

    fun disconnect() {
        val mgr = manager ?: return
        val ch = channel ?: return
        mgr.removeGroup(ch, object : WifiP2pManager.ActionListener {
            override fun onSuccess() {
                Log.d(TAG, "Disconnected from WiFi Direct group")
            }

            override fun onFailure(reasonCode: Int) {
                onError("WiFi Direct disconnect failed: ${reasonCodeToString(reasonCode)}")
            }
        })
    }

    private fun reasonCodeToString(reasonCode: Int): String = when (reasonCode) {
        WifiP2pManager.P2P_UNSUPPORTED -> "P2P unsupported on this device"
        WifiP2pManager.ERROR -> "internal error"
        WifiP2pManager.BUSY -> "system busy, try again"
        WifiP2pManager.NO_SERVICE_REQUESTS -> "no service requests"
        else -> "unknown ($reasonCode)"
    }
}
