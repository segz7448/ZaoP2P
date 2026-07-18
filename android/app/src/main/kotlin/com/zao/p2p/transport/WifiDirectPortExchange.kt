package com.zao.p2p.transport

import android.util.Log
import org.json.JSONObject
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException

/**
 * Closes the WiFi Direct -> QUIC hand-off gap documented in this
 * project's README: `WifiP2pInfo` (from WifiDirectManager) gives us the
 * group owner's IP address once a group forms, but never the port their
 * QUIC listener is bound to -- that's normally learned via mDNS, and
 * mDNS's behavior over the WiFi Direct network interface specifically
 * (a separate interface from the regular WiFi radio) isn't something
 * this project can verify without real hardware.
 *
 * Rather than depend on that uncertain mDNS behavior, this does a tiny,
 * self-contained TCP handshake directly over the WiFi Direct link: the
 * group owner listens on a FIXED, well-known TCP port for exactly this
 * purpose; the client (non-owner) connects to it, both sides exchange
 * one line of JSON containing their real QUIC port + device_id, then
 * the connection closes. After that, both sides have everything
 * `NativeBridge.connectToPeer(addr, expectedDeviceId)` needs -- the
 * actual chat/transfer session that follows is 100% the same QUIC/
 * Noise path used for any LAN peer; this class's only job is the one
 * missing piece of information.
 */
object WifiDirectPortExchange {
    private const val TAG = "WifiDirectPortExchange"

    /** Fixed port both sides agree on for this handshake specifically --
     *  deliberately NOT the QUIC port itself (which is randomly bound
     *  per the existing `QuicTransport::bind("0.0.0.0:0")` design in
     *  the Rust core), so this can run as a short-lived, one-shot
     *  listener without colliding with anything else. */
    private const val EXCHANGE_PORT = 57732
    private const val SOCKET_TIMEOUT_MS = 10_000

    data class ExchangeResult(val peerQuicPort: Int, val peerDeviceId: String)

    /**
     * Group-owner side: listen once for an incoming exchange connection,
     * reply with our own QUIC port + device_id, and return what the
     * connecting peer sent us. Runs on the calling thread and blocks
     * until a connection arrives or `SOCKET_TIMEOUT_MS` elapses -- call
     * from a background thread, not the UI thread.
     */
    fun listenForExchange(selfQuicPort: Int, selfDeviceId: String): ExchangeResult? {
        return try {
            ServerSocket(EXCHANGE_PORT).use { serverSocket ->
                serverSocket.soTimeout = SOCKET_TIMEOUT_MS
                val client = serverSocket.accept()
                client.use { socket ->
                    exchangeOverSocket(socket, selfQuicPort, selfDeviceId)
                }
            }
        } catch (e: SocketTimeoutException) {
            Log.w(TAG, "No WiFi Direct peer connected for port exchange within timeout")
            null
        } catch (e: Exception) {
            Log.e(TAG, "Port exchange listen failed: ${e.message}")
            null
        }
    }

    /**
     * Client (non-owner) side: connect to the group owner's fixed
     * exchange port, send our own QUIC port + device_id, and read back
     * theirs. `groupOwnerAddress` is the IP WifiDirectManager already
     * gave us from WifiP2pInfo.
     */
    fun connectForExchange(groupOwnerAddress: String, selfQuicPort: Int, selfDeviceId: String): ExchangeResult? {
        return try {
            Socket().use { socket ->
                socket.connect(java.net.InetSocketAddress(groupOwnerAddress, EXCHANGE_PORT), SOCKET_TIMEOUT_MS)
                exchangeOverSocket(socket, selfQuicPort, selfDeviceId)
            }
        } catch (e: Exception) {
            Log.e(TAG, "Port exchange connect failed: ${e.message}")
            null
        }
    }

    /**
     * Both sides run the identical exchange logic once a raw socket is
     * open: write our line, read their line. Order doesn't need to be
     * negotiated separately since TCP full-duplex lets both directions
     * proceed independently -- each side just writes then reads.
     */
    private fun exchangeOverSocket(socket: Socket, selfQuicPort: Int, selfDeviceId: String): ExchangeResult? {
        socket.soTimeout = SOCKET_TIMEOUT_MS
        val writer = PrintWriter(socket.getOutputStream(), true)
        val reader = BufferedReader(InputStreamReader(socket.getInputStream()))

        val outgoing = JSONObject().apply {
            put("quic_port", selfQuicPort)
            put("device_id", selfDeviceId)
        }
        writer.println(outgoing.toString())

        val line = reader.readLine() ?: return null
        return try {
            val incoming = JSONObject(line)
            ExchangeResult(
                peerQuicPort = incoming.getInt("quic_port"),
                peerDeviceId = incoming.getString("device_id"),
            )
        } catch (e: Exception) {
            Log.e(TAG, "Malformed port exchange response: ${e.message}")
            null
        }
    }
}
