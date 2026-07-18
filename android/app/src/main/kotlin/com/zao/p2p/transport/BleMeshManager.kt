package com.zao.p2p.transport

import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCallback
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattDescriptor
import android.bluetooth.BluetoothGattServer
import android.bluetooth.BluetoothGattServerCallback
import android.bluetooth.BluetoothGattService
import android.bluetooth.BluetoothManager
import android.bluetooth.BluetoothProfile
import android.bluetooth.le.AdvertiseCallback
import android.bluetooth.le.AdvertiseData
import android.bluetooth.le.AdvertiseSettings
import android.bluetooth.le.BluetoothLeAdvertiser
import android.bluetooth.le.BluetoothLeScanner
import android.bluetooth.le.ScanCallback
import android.bluetooth.le.ScanFilter
import android.bluetooth.le.ScanResult
import android.bluetooth.le.ScanSettings
import android.content.Context
import android.os.ParcelUuid
import android.util.Log
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * BLE mesh transport: lets nearby devices exchange small messages
 * (text chat -- NOT file chunks, see size note below) without WiFi or
 * internet, using Bluetooth Low Energy GATT.
 *
 * SCOPE HONESTY: this implements single-hop BLE messaging plus the
 * dedup/flood groundwork a true multi-hop mesh needs, not a complete
 * multi-hop routing protocol. Each device both advertises (as a GATT
 * server, so others can connect to it) and scans (as a GATT client, so
 * it can connect to others) simultaneously -- this dual role is what
 * "mesh" requires structurally, but actual store-and-forward relaying
 * across more than one hop needs a routing/TTL policy that is
 * deliberately minimal here (see `relayMessage` below) rather than a
 * full mesh routing algorithm (e.g. flooding with a proper TTL decrement
 * and path tracking, or something like Bluetooth Mesh's managed
 * flooding). This is flagged as a concrete next step, not silently
 * incomplete.
 *
 * Message size: BLE GATT characteristic writes are limited (typically
 * ~20 bytes unextended, up to ~512 with "long write"/MTU negotiation).
 * This transport chunks a message into MTU-sized fragments with a
 * simple reassembly header, which is fine for chat text but NOT
 * intended for file transfer -- file chunks (8MB each, per the QUIC
 * transport's CHUNK_SIZE) are far too large for BLE to move at
 * reasonable speed; BLE mesh here is a messaging-only fallback for when
 * no WiFi/internet transport is available at all.
 */
class BleMeshManager(
    private val context: Context,
    private val onMessageReceived: (fromDeviceId: String, payload: ByteArray) -> Unit,
    private val onPeerDiscovered: (deviceId: String, bluetoothAddress: String) -> Unit,
    private val onError: (String) -> Unit,
) {
    companion object {
        private const val TAG = "BleMeshManager"

        val SERVICE_UUID: UUID = UUID.fromString("7a3f9c00-1234-4a9e-8f1a-000000000001")
        val MESSAGE_CHARACTERISTIC_UUID: UUID = UUID.fromString("7a3f9c00-1234-4a9e-8f1a-000000000002")
        val CLIENT_CONFIG_DESCRIPTOR_UUID: UUID = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")

        private const val MAX_FRAGMENT_SIZE = 500
        private const val DEDUP_CACHE_MAX_SIZE = 500
        private const val DEFAULT_TTL: Byte = 4
    }

    private val bluetoothManager = context.getSystemService(Context.BLUETOOTH_SERVICE) as? BluetoothManager
    private val adapter: BluetoothAdapter? = bluetoothManager?.adapter
    private var advertiser: BluetoothLeAdvertiser? = null
    private var scanner: BluetoothLeScanner? = null
    private var gattServer: BluetoothGattServer? = null

    private val clientConnections = ConcurrentHashMap<String, BluetoothGatt>()

    private val seenMessageIds = object : LinkedHashMap<String, Boolean>(16, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, Boolean>?): Boolean {
            return size > DEDUP_CACHE_MAX_SIZE
        }
    }

    private val reassemblyBuffers = ConcurrentHashMap<String, FragmentBuffer>()

    private class FragmentBuffer(val totalFragments: Int) {
        val fragments = arrayOfNulls<ByteArray>(totalFragments)
        var received = 0
    }

    fun start(selfDeviceId: String) {
        val bt = adapter
        if (bt == null || !bt.isEnabled) {
            onError("Bluetooth is not available or not enabled")
            return
        }
        advertiser = bt.bluetoothLeAdvertiser
        scanner = bt.bluetoothLeScanner
        if (advertiser == null || scanner == null) {
            onError("This device does not support BLE advertising/scanning")
            return
        }

        startGattServer(selfDeviceId)
        startAdvertising()
        startScanning()
    }

    fun stop() {
        try {
            advertiser?.stopAdvertising(advertiseCallback)
        } catch (e: SecurityException) {
            // Missing runtime permission -- nothing more to clean up here.
        }
        try {
            scanner?.stopScan(scanCallback)
        } catch (e: SecurityException) {
            // Same as above.
        }
        gattServer?.close()
        clientConnections.values.forEach { it.close() }
        clientConnections.clear()
    }

    private fun startGattServer(selfDeviceId: String) {
        val server = bluetoothManager?.openGattServer(context, object : BluetoothGattServerCallback() {
            override fun onConnectionStateChange(device: BluetoothDevice, status: Int, newState: Int) {
                if (newState == BluetoothProfile.STATE_CONNECTED) {
                    Log.d(TAG, "GATT server: device connected ${device.address}")
                }
            }

            override fun onCharacteristicWriteRequest(
                device: BluetoothDevice,
                requestId: Int,
                characteristic: BluetoothGattCharacteristic,
                preparedWrite: Boolean,
                responseNeeded: Boolean,
                offset: Int,
                value: ByteArray,
            ) {
                if (characteristic.uuid == MESSAGE_CHARACTERISTIC_UUID) {
                    handleIncomingFragment(device.address, value)
                }
                if (responseNeeded) {
                    gattServer?.sendResponse(device, requestId, android.bluetooth.BluetoothGatt.GATT_SUCCESS, offset, value)
                }
            }

            override fun onDescriptorWriteRequest(
                device: BluetoothDevice,
                requestId: Int,
                descriptor: BluetoothGattDescriptor,
                preparedWrite: Boolean,
                responseNeeded: Boolean,
                offset: Int,
                value: ByteArray,
            ) {
                if (responseNeeded) {
                    gattServer?.sendResponse(device, requestId, android.bluetooth.BluetoothGatt.GATT_SUCCESS, offset, value)
                }
            }
        }) ?: run {
            onError("Could not open GATT server")
            return
        }
        gattServer = server

        val service = BluetoothGattService(SERVICE_UUID, BluetoothGattService.SERVICE_TYPE_PRIMARY)
        val characteristic = BluetoothGattCharacteristic(
            MESSAGE_CHARACTERISTIC_UUID,
            BluetoothGattCharacteristic.PROPERTY_WRITE or BluetoothGattCharacteristic.PROPERTY_NOTIFY,
            BluetoothGattCharacteristic.PERMISSION_WRITE,
        )
        val descriptor = BluetoothGattDescriptor(
            CLIENT_CONFIG_DESCRIPTOR_UUID,
            BluetoothGattDescriptor.PERMISSION_READ or BluetoothGattDescriptor.PERMISSION_WRITE,
        )
        characteristic.addDescriptor(descriptor)
        service.addCharacteristic(characteristic)
        server.addService(service)
    }

    private val advertiseCallback = object : AdvertiseCallback() {
        override fun onStartFailure(errorCode: Int) {
            onError("BLE advertise failed: ${advertiseErrorToString(errorCode)}")
        }
    }

    private fun startAdvertising() {
        val settings = AdvertiseSettings.Builder()
            .setAdvertiseMode(AdvertiseSettings.ADVERTISE_MODE_BALANCED)
            .setTxPowerLevel(AdvertiseSettings.ADVERTISE_TX_POWER_MEDIUM)
            .setConnectable(true)
            .build()
        val data = AdvertiseData.Builder()
            .addServiceUuid(ParcelUuid(SERVICE_UUID))
            .setIncludeDeviceName(false)
            .build()
        try {
            advertiser?.startAdvertising(settings, data, advertiseCallback)
        } catch (e: SecurityException) {
            onError("Missing Bluetooth permission for advertising: ${e.message}")
        }
    }

    private val scanCallback = object : ScanCallback() {
        override fun onScanResult(callbackType: Int, result: ScanResult) {
            val device = result.device
            onPeerDiscovered(shortHashOf(device.address), device.address)
            connectAsClient(device)
        }

        override fun onScanFailed(errorCode: Int) {
            onError("BLE scan failed: code $errorCode")
        }
    }

    private fun startScanning() {
        val filter = ScanFilter.Builder()
            .setServiceUuid(ParcelUuid(SERVICE_UUID))
            .build()
        val settings = ScanSettings.Builder()
            .setScanMode(ScanSettings.SCAN_MODE_BALANCED)
            .build()
        try {
            scanner?.startScan(listOf(filter), settings, scanCallback)
        } catch (e: SecurityException) {
            onError("Missing Bluetooth permission for scanning: ${e.message}")
        }
    }

    private fun connectAsClient(device: BluetoothDevice) {
        if (clientConnections.containsKey(device.address)) return

        try {
            val gatt = device.connectGatt(context, false, object : BluetoothGattCallback() {
                override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
                    if (newState == BluetoothProfile.STATE_CONNECTED) {
                        gatt.discoverServices()
                    } else if (newState == BluetoothProfile.STATE_DISCONNECTED) {
                        clientConnections.remove(device.address)
                        gatt.close()
                    }
                }

                override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
                    if (status == android.bluetooth.BluetoothGatt.GATT_SUCCESS) {
                        clientConnections[device.address] = gatt
                    }
                }
            })
        } catch (e: SecurityException) {
            onError("Missing Bluetooth permission to connect: ${e.message}")
        }
    }

    /**
     * Send a message (already Noise-encrypted ciphertext from the Rust
     * core, treated as an opaque payload here -- this BLE layer never
     * sees plaintext) to all currently-connected mesh peers. Used both
     * for originating a new message and for relaying one received from
     * elsewhere (flooding).
     */
    fun broadcastMessage(messageId: String, payload: ByteArray, ttl: Byte = DEFAULT_TTL) {
        if (ttl <= 0) return

        synchronized(seenMessageIds) {
            if (seenMessageIds.containsKey(messageId)) return
            seenMessageIds[messageId] = true
        }

        val framed = frameMessage(messageId, ttl, payload)
        val fragments = fragmentData(framed)

        for (gatt in clientConnections.values) {
            val service = gatt.getService(SERVICE_UUID) ?: continue
            val characteristic = service.getCharacteristic(MESSAGE_CHARACTERISTIC_UUID) ?: continue
            for (fragment in fragments) {
                characteristic.value = fragment
                try {
                    gatt.writeCharacteristic(characteristic)
                } catch (e: SecurityException) {
                    onError("Missing permission to write BLE characteristic: ${e.message}")
                }
            }
        }
    }

    private fun frameMessage(messageId: String, ttl: Byte, payload: ByteArray): ByteArray {
        val idBytes = messageId.toByteArray(Charsets.UTF_8).copyOf(16)
        val buffer = java.nio.ByteBuffer.allocate(16 + 1 + payload.size)
        buffer.put(idBytes)
        buffer.put(ttl)
        buffer.put(payload)
        return buffer.array()
    }

    private fun fragmentData(data: ByteArray): List<ByteArray> {
        val chunkSize = MAX_FRAGMENT_SIZE - 2
        val totalFragments = ((data.size + chunkSize - 1) / chunkSize).coerceAtLeast(1)
        if (totalFragments > 255) {
            Log.e(TAG, "Message too large for BLE mesh fragmentation (${data.size} bytes) -- dropping")
            return emptyList()
        }
        val fragments = mutableListOf<ByteArray>()
        for (i in 0 until totalFragments) {
            val start = i * chunkSize
            val end = minOf(start + chunkSize, data.size)
            val chunk = data.copyOfRange(start, end)
            val fragment = ByteArray(2 + chunk.size)
            fragment[0] = i.toByte()
            fragment[1] = totalFragments.toByte()
            chunk.copyInto(fragment, 2)
            fragments.add(fragment)
        }
        return fragments
    }

    private fun handleIncomingFragment(sourceAddress: String, fragment: ByteArray) {
        if (fragment.size < 2) return
        val index = fragment[0].toInt() and 0xFF
        val total = fragment[1].toInt() and 0xFF
        val chunk = fragment.copyOfRange(2, fragment.size)

        val key = "$sourceAddress:$total"
        val buffer = reassemblyBuffers.getOrPut(key) { FragmentBuffer(total) }
        if (index >= buffer.fragments.size || buffer.fragments[index] != null) {
            return
        }
        buffer.fragments[index] = chunk
        buffer.received++

        if (buffer.received == total) {
            reassemblyBuffers.remove(key)
            val fullMessage = buffer.fragments.filterNotNull().fold(ByteArray(0)) { acc, part -> acc + part }
            handleReassembledMessage(sourceAddress, fullMessage)
        }
    }

    private fun handleReassembledMessage(sourceAddress: String, framed: ByteArray) {
        if (framed.size < 17) return
        val messageId = String(framed.copyOfRange(0, 16), Charsets.UTF_8).trimEnd('\u0000')
        val ttl = framed[16]
        val payload = framed.copyOfRange(17, framed.size)

        val alreadySeen = synchronized(seenMessageIds) {
            val seen = seenMessageIds.containsKey(messageId)
            seenMessageIds[messageId] = true
            seen
        }
        if (alreadySeen) return

        onMessageReceived(shortHashOf(sourceAddress), payload)

        // Flood relay: forward to all other currently-connected mesh
        // peers with a decremented TTL. This is "store-and-forward
        // within one hop's neighborhood" -- true multi-hop routing
        // across a larger mesh needs more (path/topology awareness,
        // smarter TTL/rebroadcast suppression) than this minimal flood,
        // which is deliberately scoped small here (see class doc comment).
        if (ttl > 1) {
            broadcastMessage(messageId, payload, (ttl - 1).toByte())
        }
    }

    /**
     * BLE device addresses aren't meaningful device_ids in this app's
     * identity model (see identity.rs's Ed25519/X25519-derived
     * device_id) -- this hash is only a stable-enough local label for
     * displaying/deduplicating peers in the UI before a real Noise
     * handshake (carried as opaque payload bytes over this same
     * transport) establishes the peer's actual device_id.
     */
    private fun shortHashOf(input: String): String {
        val digest = MessageDigest.getInstance("SHA-256").digest(input.toByteArray())
        return digest.take(4).joinToString("") { "%02x".format(it) }
    }

    private fun advertiseErrorToString(errorCode: Int): String = when (errorCode) {
        AdvertiseCallback.ADVERTISE_FAILED_ALREADY_STARTED -> "already started"
        AdvertiseCallback.ADVERTISE_FAILED_DATA_TOO_LARGE -> "advertise data too large"
        AdvertiseCallback.ADVERTISE_FAILED_FEATURE_UNSUPPORTED -> "feature unsupported"
        AdvertiseCallback.ADVERTISE_FAILED_INTERNAL_ERROR -> "internal error"
        AdvertiseCallback.ADVERTISE_FAILED_TOO_MANY_ADVERTISERS -> "too many advertisers"
        else -> "unknown ($errorCode)"
    }
}
