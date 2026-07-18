package com.zao.p2p.chat

import android.app.AlertDialog
import android.content.Intent
import android.database.Cursor
import android.net.Uri
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.provider.OpenableColumns
import android.text.Editable
import android.text.TextWatcher
import android.widget.Button
import android.widget.EditText
import android.widget.TextView
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.zao.p2p.R
import com.zao.p2p.core.NativeBridge
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.io.FileOutputStream

/**
 * Milestone 4 chat screen: text chat (from Milestone 3) plus file
 * transfer -- sending via a file picker or receiving an offer, with a
 * live progress bar, pause/resume/cancel, rendered as its own bubble
 * type in ChatAdapter.
 *
 * Two polling loops drive the UI:
 *  1. pollEvents() every 700ms -- messages, typing, receipts, presence,
 *     file offers, and transfer progress.
 *  2. A typing-timeout timer -- stops sending "is_typing=true" after the
 *     user pauses.
 */
class ChatActivity : AppCompatActivity() {

    companion object {
        const val EXTRA_PEER_DEVICE_ID = "peer_device_id"
        const val EXTRA_PEER_ADDR = "peer_addr"
        private const val EVENT_POLL_INTERVAL_MS = 700L
        private const val TYPING_STOP_DELAY_MS = 2000L
    }

    private lateinit var peerDeviceId: String
    private lateinit var peerAddr: String
    private val conversationId get() = peerDeviceId // 1:1-only in this milestone

    private lateinit var recyclerView: RecyclerView
    private lateinit var adapter: ChatAdapter
    private lateinit var statusBar: TextView
    private lateinit var typingIndicator: TextView
    private lateinit var messageInput: EditText
    private lateinit var sendButton: Button
    private lateinit var attachButton: Button

    private val pollHandler = Handler(Looper.getMainLooper())
    private val typingHandler = Handler(Looper.getMainLooper())
    private var isCurrentlyTyping = false
    private var typingStopRunnable: Runnable? = null

    // Incoming offers are queued by transfer_id so accept/reject (fired
    // from a dialog) can look up the full offer details later.
    private val pendingOffers = mutableMapOf<String, FileOfferInfo>()

    // Holds ParcelFileDescriptors opened for outgoing fd-sourced sends
    // (see handlePickedFile), keyed by transfer_id, until the transfer
    // completes/is cancelled/fails -- closed explicitly in handleEvents
    // and onDestroy, never relying on garbage collection to close them
    // (which could happen mid-transfer and break in-progress reads).
    private val openFileDescriptors = mutableMapOf<String, android.os.ParcelFileDescriptor>()

    // Folder batch IDs the user has already accepted, so individual
    // FileOffers belonging to that batch can be auto-accepted instead
    // of re-prompting per file (see the FileOffer event handler).
    private val acceptedFolderBatches = mutableSetOf<String>()

    data class FileOfferInfo(
        val fromDeviceId: String,
        val transferId: String,
        val fileName: String,
        val fileSize: Long,
        val mimeType: String,
    )

    private val filePickerLauncher = registerForActivityResult(
        ActivityResultContracts.OpenDocument()
    ) { uri: Uri? ->
        if (uri != null) handlePickedFile(uri)
    }

    private val folderPickerLauncher = registerForActivityResult(
        ActivityResultContracts.OpenDocumentTree()
    ) { uri: Uri? ->
        if (uri != null) handlePickedFolder(uri)
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_chat)

        peerDeviceId = intent.getStringExtra(EXTRA_PEER_DEVICE_ID)
            ?: error("ChatActivity requires EXTRA_PEER_DEVICE_ID")
        peerAddr = intent.getStringExtra(EXTRA_PEER_ADDR) ?: ""

        recyclerView = findViewById(R.id.messageList)
        statusBar = findViewById(R.id.statusBar)
        typingIndicator = findViewById(R.id.typingIndicator)
        messageInput = findViewById(R.id.messageInput)
        sendButton = findViewById(R.id.sendButton)
        attachButton = findViewById(R.id.attachButton)

        adapter = ChatAdapter(
            mutableListOf(),
            onPauseResume = { msg -> onPauseResumeClicked(msg) },
            onCancel = { msg -> onCancelClicked(msg) },
        )
        recyclerView.layoutManager = LinearLayoutManager(this)
        recyclerView.adapter = adapter

        loadHistory()
        connectIfNeeded()

        sendButton.setOnClickListener { onSendClicked() }
        // Matches the Windows UI's click-for-file / right-click-for-folder
        // pattern: tap sends a single file, long-press offers a folder --
        // keeps the input bar to one button instead of two.
        attachButton.setOnClickListener { filePickerLauncher.launch(arrayOf("*/*")) }
        attachButton.setOnLongClickListener {
            folderPickerLauncher.launch(null)
            true
        }
        messageInput.addTextChangedListener(object : TextWatcher {
            override fun beforeTextChanged(s: CharSequence?, start: Int, count: Int, after: Int) {}
            override fun onTextChanged(s: CharSequence?, start: Int, before: Int, count: Int) {
                onUserTyping()
            }
            override fun afterTextChanged(s: Editable?) {}
        })

        pollEvents()
    }

    private fun loadHistory() {
        val historyJson = try {
            NativeBridge.getConversationHistory(peerDeviceId, 200)
        } catch (e: UnsatisfiedLinkError) {
            "[]"
        }
        // File-type history rows are skipped in this milestone: a fully
        // faithful replay would need to reconstruct transfer progress
        // for a possibly-completed transfer, which needs a DB-backed
        // progress read (get_transfer_progress) per row. Deferred rather
        // than rendered incorrectly as a stuck-at-0% bar.
        val messages = parseStoredMessages(historyJson).filter { it.type != "file" }
        adapter.setMessages(messages)
        scrollToBottom()
    }

    private fun connectIfNeeded() {
        if (peerAddr.isEmpty()) return
        Thread {
            try {
                val result = NativeBridge.connectToPeer(peerAddr, peerDeviceId)
                runOnUiThread { statusBar.text = "Connection: $result" }
            } catch (e: UnsatisfiedLinkError) {
                runOnUiThread { statusBar.text = "Connection failed: ${e.message}" }
            }
        }.start()
    }

    private fun onSendClicked() {
        val body = messageInput.text?.toString()?.trim().orEmpty()
        if (body.isEmpty()) return
        messageInput.setText("")
        stopTypingNow()

        Thread {
            try {
                val resultJson = NativeBridge.sendTextMessage(peerDeviceId, body)
                val result = JSONObject(resultJson)
                val messageId = result.optString("message_id", "unknown")
                runOnUiThread {
                    adapter.appendMessage(
                        ChatMessage(
                            messageId = messageId,
                            body = body,
                            sentAtUnix = System.currentTimeMillis() / 1000,
                            isOutgoing = true,
                            status = "pending",
                        )
                    )
                    scrollToBottom()
                }
            } catch (e: UnsatisfiedLinkError) {
                runOnUiThread { statusBar.text = "Send failed: ${e.message}" }
            }
        }.start()
    }

    /**
     * Android's ACTION_OPEN_DOCUMENT returns a content:// Uri, not a
     * filesystem path -- but the native transfer engine reads files by
     * path (`read_chunk_from_file` opens a std filesystem File). Rather
     * than teach the Rust side to understand content:// Uris (which
     * would need JNI-side ContentResolver calls mid-transfer), the
     * simplest correct approach is to copy the picked file into the
     * app's private cache dir once up front, then hand send_file a real
     * path. For very large files this copy costs one extra full read+
     * write before the transfer even starts -- acceptable for this
     * milestone; a future pass could stream directly from a
     * ParcelFileDescriptor instead to avoid the double I/O.
     */
    /**
     * Uses sendFileFd instead of copying the picked file into the app's
     * cache dir first (Milestone 8: streaming reads, avoiding the
     * double I/O the cache-copy approach cost on every send). The
     * ParcelFileDescriptor MUST stay open for the transfer's full
     * duration -- native chunk workers reopen it on demand via
     * /proc/self/fd/{fd} (see NativeBridge.sendFileFd's doc comment),
     * so this activity holds it in `openFileDescriptors`, keyed by
     * transfer_id, and only closes it once a TransferComplete/
     * TransferCancelled event arrives for that transfer (handled in
     * handleEvents) or the activity is destroyed.
     */
    private fun handlePickedFile(uri: Uri) {
        statusBar.text = "Preparing file…"
        Thread {
            var pfd: android.os.ParcelFileDescriptor? = null
            try {
                val fileName = queryFileName(uri) ?: "file_${System.currentTimeMillis()}"
                val fileSize = queryFileSize(uri)
                val mimeType = contentResolver.getType(uri) ?: "application/octet-stream"

                pfd = contentResolver.openFileDescriptor(uri, "r")
                    ?: throw java.io.IOException("could not open picked file for reading")

                val resultJson = NativeBridge.sendFileFd(
                    peerDeviceId,
                    pfd.fd,
                    fileName,
                    fileSize,
                    mimeType,
                    conversationId,
                )
                val result = JSONObject(resultJson)
                if (result.has("error")) {
                    throw java.io.IOException(result.optString("error"))
                }
                val transferId = result.optString("transfer_id")

                // Keep the fd open, tracked by transfer_id, until the
                // transfer finishes -- see class doc comment on this
                // function for why closing it early would break
                // in-progress chunk reads.
                openFileDescriptors[transferId] = pfd

                runOnUiThread {
                    adapter.appendMessage(
                        ChatMessage(
                            messageId = result.optString("message_id", transferId),
                            body = "",
                            sentAtUnix = System.currentTimeMillis() / 1000,
                            isOutgoing = true,
                            status = "pending",
                            type = "file",
                            transferId = transferId,
                            fileName = fileName,
                            fileSize = fileSize,
                        )
                    )
                    statusBar.text = "Offering $fileName…"
                    scrollToBottom()
                }
            } catch (e: Exception) {
                // Only close on failure here -- the success path's fd
                // stays open, owned by openFileDescriptors now, until
                // the transfer's outcome is known.
                pfd?.close()
                runOnUiThread { statusBar.text = "File send failed: ${e.message}" }
            }
        }.start()
    }

    /**
     * ACTION_OPEN_DOCUMENT_TREE gives a tree Uri, not a filesystem path
     * either -- same fundamental issue as handlePickedFile, but for an
     * entire directory. Recursively copies the picked tree into a
     * subfolder of the app's cache dir (preserving relative structure),
     * then hands send_folder a real path to walk. For a folder with many
     * or large files this front-loads all the copy I/O before any
     * network transfer begins; acceptable for this milestone, same
     * tradeoff noted in handlePickedFile for single files.
     */
    private fun handlePickedFolder(treeUri: Uri) {
        statusBar.text = "Preparing folder…"
        Thread {
            try {
                val rootDoc = androidx.documentfile.provider.DocumentFile.fromTreeUri(this, treeUri)
                    ?: throw java.io.IOException("could not open picked folder")
                val folderName = rootDoc.name ?: "folder_${System.currentTimeMillis()}"
                val destRoot = File(cacheDir, "folder_send_${System.currentTimeMillis()}/$folderName")
                destRoot.mkdirs()

                val fileCount = copyDocumentTreeRecursive(rootDoc, destRoot)
                if (fileCount == 0) {
                    runOnUiThread { statusBar.text = "Folder is empty, nothing to send" }
                    return@Thread
                }

                val resultJson = NativeBridge.sendFolder(peerDeviceId, destRoot.absolutePath, conversationId)
                val result = JSONObject(resultJson)
                val files = result.optJSONArray("files")

                runOnUiThread {
                    if (files != null) {
                        for (i in 0 until files.length()) {
                            val f = files.optJSONObject(i) ?: continue
                            val relativePath = f.optString("relative_path")
                            val transferId = f.optString("transfer_id")
                            adapter.appendMessage(
                                ChatMessage(
                                    messageId = transferId,
                                    body = "",
                                    sentAtUnix = System.currentTimeMillis() / 1000,
                                    isOutgoing = true,
                                    status = "pending",
                                    type = "file",
                                    transferId = transferId,
                                    fileName = "$folderName/$relativePath",
                                    fileSize = 0,
                                )
                            )
                        }
                    }
                    statusBar.text = "Offering folder $folderName (${files?.length() ?: 0} files)…"
                    scrollToBottom()
                }
            } catch (e: Exception) {
                runOnUiThread { statusBar.text = "Folder send failed: ${e.message}" }
            }
        }.start()
    }

    /**
     * Recursively copies a DocumentFile tree to a real filesystem
     * destination, preserving the relative directory structure. Returns
     * the number of files copied. Uses an explicit stack (not
     * recursion) for the same reason as the Rust side's
     * collect_files_recursive -- avoid stack overflow on deeply nested
     * folder structures.
     */
    private fun copyDocumentTreeRecursive(root: androidx.documentfile.provider.DocumentFile, destRoot: File): Int {
        var fileCount = 0
        val stack = ArrayDeque<Pair<androidx.documentfile.provider.DocumentFile, File>>()
        stack.addLast(root to destRoot)

        while (stack.isNotEmpty()) {
            val (currentDoc, currentDest) = stack.removeLast()
            val children = currentDoc.listFiles()
            for (child in children) {
                val childName = child.name ?: continue
                if (child.isDirectory) {
                    val childDest = File(currentDest, childName)
                    childDest.mkdirs()
                    stack.addLast(child to childDest)
                } else if (child.isFile) {
                    val childDest = File(currentDest, childName)
                    contentResolver.openInputStream(child.uri)?.use { input ->
                        FileOutputStream(childDest).use { output ->
                            input.copyTo(output)
                        }
                    }
                    fileCount++
                }
            }
        }
        return fileCount
    }

    private fun queryFileName(uri: Uri): String? {
        var name: String? = null
        val cursor: Cursor? = contentResolver.query(uri, null, null, null, null)
        cursor?.use {
            val nameIndex = it.getColumnIndex(OpenableColumns.DISPLAY_NAME)
            if (it.moveToFirst() && nameIndex >= 0) {
                name = it.getString(nameIndex)
            }
        }
        return name
    }

    private fun queryFileSize(uri: Uri): Long {
        var size = 0L
        val cursor: Cursor? = contentResolver.query(uri, null, null, null, null)
        cursor?.use {
            val sizeIndex = it.getColumnIndex(OpenableColumns.SIZE)
            if (it.moveToFirst() && sizeIndex >= 0 && !it.isNull(sizeIndex)) {
                size = it.getLong(sizeIndex)
            }
        }
        return size
    }

    private fun onPauseResumeClicked(msg: ChatMessage) {
        val transferId = msg.transferId ?: return
        val isPausing = msg.transferState != "paused"
        Thread {
            try {
                if (isPausing) {
                    NativeBridge.pauseTransfer(transferId)
                } else {
                    NativeBridge.resumeTransfer(transferId)
                }
            } catch (e: UnsatisfiedLinkError) { /* non-critical */ }
        }.start()
        adapter.updateTransferProgress(
            transferId,
            msg.transferPercent,
            msg.transferSpeedBytesPerSec,
            msg.transferEtaSeconds,
            if (isPausing) "paused" else "active",
        )
    }

    private fun onCancelClicked(msg: ChatMessage) {
        val transferId = msg.transferId ?: return
        Thread {
            try {
                NativeBridge.cancelTransfer(peerDeviceId, transferId)
            } catch (e: UnsatisfiedLinkError) { /* non-critical */ }
        }.start()
    }

    private fun onUserTyping() {
        if (!isCurrentlyTyping) {
            isCurrentlyTyping = true
            sendTypingSignal(true)
        }
        typingStopRunnable?.let { typingHandler.removeCallbacks(it) }
        val runnable = Runnable { stopTypingNow() }
        typingStopRunnable = runnable
        typingHandler.postDelayed(runnable, TYPING_STOP_DELAY_MS)
    }

    private fun stopTypingNow() {
        if (isCurrentlyTyping) {
            isCurrentlyTyping = false
            sendTypingSignal(false)
        }
    }

    private fun sendTypingSignal(isTyping: Boolean) {
        Thread {
            try {
                NativeBridge.sendTypingIndicator(peerDeviceId, conversationId, isTyping)
            } catch (e: UnsatisfiedLinkError) { /* non-critical */ }
        }.start()
    }

    private fun pollEvents() {
        Thread {
            val eventsJson = try {
                NativeBridge.pollEvents()
            } catch (e: UnsatisfiedLinkError) {
                "[]"
            }
            runOnUiThread { handleEvents(eventsJson) }
        }.start()

        pollHandler.postDelayed({ pollEvents() }, EVENT_POLL_INTERVAL_MS)
    }

    private fun handleEvents(eventsJson: String) {
        val events = try {
            JSONArray(eventsJson)
        } catch (e: Exception) {
            return
        }
        var shouldScroll = false

        for (i in 0 until events.length()) {
            val event = events.optJSONObject(i) ?: continue
            when (event.optString("event")) {
                "TextMessage" -> {
                    val data = event.optJSONObject("data") ?: continue
                    if (data.optString("from_device_id") != peerDeviceId) continue
                    adapter.appendMessage(
                        ChatMessage(
                            messageId = data.optString("message_id"),
                            body = data.optString("body"),
                            sentAtUnix = data.optLong("sent_at_unix"),
                            isOutgoing = false,
                            status = "delivered",
                        )
                    )
                    shouldScroll = true
                    val messageId = data.optString("message_id")
                    Thread {
                        try {
                            NativeBridge.markMessageRead(peerDeviceId, messageId)
                        } catch (e: UnsatisfiedLinkError) { /* non-critical */ }
                    }.start()
                }
                "FileOffer" -> {
                    val data = event.optJSONObject("data") ?: continue
                    if (data.optString("from_device_id") != peerDeviceId) continue
                    val offer = FileOfferInfo(
                        fromDeviceId = data.optString("from_device_id"),
                        transferId = data.optString("transfer_id"),
                        fileName = data.optString("file_name"),
                        fileSize = data.optLong("file_size"),
                        mimeType = data.optString("mime_type"),
                    )
                    pendingOffers[offer.transferId] = offer
                    // Auto-accept individual file offers that belong to
                    // an already-accepted folder batch (see
                    // showFolderOfferDialog) -- the user already made
                    // the batch-level decision, so re-prompting per file
                    // would defeat the point of folder-level negotiation.
                    if (data.optString("folder_batch_id", "") in acceptedFolderBatches) {
                        acceptOffer(offer)
                    } else {
                        showIncomingOfferDialog(offer)
                    }
                }
                "FolderOffer" -> {
                    val data = event.optJSONObject("data") ?: continue
                    if (data.optString("from_device_id") != peerDeviceId) continue
                    showFolderOfferDialog(data)
                }
                "TransferProgress" -> {
                    val data = event.optJSONObject("data") ?: continue
                    adapter.updateTransferProgress(
                        data.optString("transfer_id"),
                        data.optDouble("percent", 0.0).toFloat(),
                        data.optDouble("speed_bytes_per_sec", 0.0),
                        if (data.has("eta_seconds") && !data.isNull("eta_seconds")) data.optDouble("eta_seconds") else null,
                        data.optString("state", "active"),
                    )
                }
                "TransferComplete" -> {
                    val data = event.optJSONObject("data") ?: continue
                    val transferId = data.optString("transfer_id")
                    adapter.updateTransferProgress(transferId, 100f, 0.0, 0.0, "completed")
                    closeFileDescriptorFor(transferId)
                }
                "TransferCancelled" -> {
                    val data = event.optJSONObject("data") ?: continue
                    val transferId = data.optString("transfer_id")
                    adapter.updateTransferProgress(transferId, 0f, 0.0, null, "cancelled")
                    statusBar.text = "Transfer cancelled by ${data.optString("by_device_id")}"
                    closeFileDescriptorFor(transferId)
                }
                "Typing" -> {
                    val data = event.optJSONObject("data") ?: continue
                    if (data.optString("conversation_id") != conversationId) continue
                    val isTyping = data.optBoolean("is_typing")
                    typingIndicator.visibility =
                        if (isTyping) android.view.View.VISIBLE else android.view.View.INVISIBLE
                    typingIndicator.text = if (isTyping) "typing…" else ""
                }
                "DeliveryAck" -> {
                    val data = event.optJSONObject("data") ?: continue
                    adapter.updateStatus(data.optString("message_id"), "delivered")
                }
                "ReadReceipt" -> {
                    val data = event.optJSONObject("data") ?: continue
                    adapter.updateStatus(data.optString("message_id"), "read")
                }
                "PeerConnected" -> {
                    val data = event.optJSONObject("data") ?: continue
                    if (data.optString("device_id") == peerDeviceId) statusBar.text = "Online"
                }
                "PeerDisconnected" -> {
                    val data = event.optJSONObject("data") ?: continue
                    if (data.optString("device_id") == peerDeviceId) statusBar.text = "Offline"
                }
            }
        }

        if (shouldScroll) scrollToBottom()
    }

    private fun showIncomingOfferDialog(offer: FileOfferInfo) {
        AlertDialog.Builder(this)
            .setTitle("Incoming file")
            .setMessage("${offer.fileName}\n${formatBytes(offer.fileSize)}\nFrom: ${offer.fromDeviceId}")
            .setPositiveButton("Accept") { _, _ -> acceptOffer(offer) }
            .setNegativeButton("Decline") { _, _ -> rejectOffer(offer) }
            .setCancelable(false)
            .show()
    }

    /**
     * Shows the batch-level prompt for an incoming FolderOffer -- one
     * decision for the whole folder, matching the sender side's
     * single-negotiation design (see ProtocolMessage::FolderOffer's doc
     * comment in the Rust core). Accepting marks the batch_id as
     * pre-accepted so subsequent individual FileOffers for this folder
     * skip the per-file prompt (see the FileOffer event handler above).
     */
    private fun showFolderOfferDialog(data: JSONObject) {
        val folderBatchId = data.optString("folder_batch_id")
        val folderName = data.optString("folder_name")
        val totalFiles = data.optLong("total_files")
        val totalSize = data.optLong("total_size")
        val fromDeviceId = data.optString("from_device_id")

        AlertDialog.Builder(this)
            .setTitle("Incoming folder")
            .setMessage("$folderName\n$totalFiles files, ${formatBytes(totalSize)}\nFrom: $fromDeviceId")
            .setPositiveButton("Accept") { _, _ ->
                acceptedFolderBatches.add(folderBatchId)
                Thread {
                    try {
                        NativeBridge.acceptFolder(fromDeviceId, folderBatchId)
                    } catch (e: UnsatisfiedLinkError) {
                        runOnUiThread { statusBar.text = "Folder accept failed: ${e.message}" }
                    }
                }.start()
            }
            .setNegativeButton("Decline") { _, _ ->
                Thread {
                    try {
                        NativeBridge.rejectFolder(fromDeviceId, folderBatchId, "Declined by user")
                    } catch (e: UnsatisfiedLinkError) { /* non-critical */ }
                }.start()
            }
            .setCancelable(false)
            .show()
    }

    /** Closes and forgets the ParcelFileDescriptor for a finished/cancelled outgoing transfer. */
    private fun closeFileDescriptorFor(transferId: String) {
        openFileDescriptors.remove(transferId)?.let {
            try {
                it.close()
            } catch (e: java.io.IOException) {
                // Already closed or otherwise unrecoverable -- nothing more to do.
            }
        }
    }

    private fun acceptOffer(offer: FileOfferInfo) {
        Thread {
            try {
                NativeBridge.acceptFile(
                    offer.fromDeviceId,
                    offer.transferId,
                    offer.transferId, // message_id isn't separately tracked client-side for incoming offers in this milestone
                    offer.fileName,
                    offer.fileSize,
                    offer.mimeType,
                )
                runOnUiThread {
                    adapter.appendMessage(
                        ChatMessage(
                            messageId = offer.transferId,
                            body = "",
                            sentAtUnix = System.currentTimeMillis() / 1000,
                            isOutgoing = false,
                            status = "delivered",
                            type = "file",
                            transferId = offer.transferId,
                            fileName = offer.fileName,
                            fileSize = offer.fileSize,
                        )
                    )
                    scrollToBottom()
                }
            } catch (e: UnsatisfiedLinkError) {
                runOnUiThread { statusBar.text = "Accept failed: ${e.message}" }
            }
        }.start()
    }

    private fun rejectOffer(offer: FileOfferInfo) {
        Thread {
            try {
                NativeBridge.rejectFile(offer.fromDeviceId, offer.transferId, "Declined by user")
            } catch (e: UnsatisfiedLinkError) { /* non-critical */ }
        }.start()
    }

    private fun formatBytes(bytes: Long): String {
        if (bytes < 1024) return "$bytes B"
        val units = arrayOf("KB", "MB", "GB", "TB")
        var size = bytes.toDouble()
        var i = -1
        do {
            size /= 1024
            i++
        } while (size >= 1024 && i < units.size - 1)
        return String.format("%.1f %s", size, units[i])
    }

    private fun parseStoredMessages(json: String): List<ChatMessage> {
        val array = try {
            JSONArray(json)
        } catch (e: Exception) {
            return emptyList()
        }
        val result = mutableListOf<ChatMessage>()
        for (i in 0 until array.length()) {
            val obj = array.optJSONObject(i) ?: continue
            val senderId = obj.optString("sender_device_id")
            result.add(
                ChatMessage(
                    messageId = obj.optString("message_id"),
                    body = obj.optString("body"),
                    sentAtUnix = obj.optLong("sent_at"),
                    isOutgoing = senderId == "self",
                    status = obj.optString("status", "sent"),
                    type = obj.optString("type", "text"),
                )
            )
        }
        return result
    }

    private fun scrollToBottom() {
        if (adapter.itemCount > 0) {
            recyclerView.scrollToPosition(adapter.itemCount - 1)
        }
    }

    override fun onDestroy() {
        pollHandler.removeCallbacksAndMessages(null)
        typingHandler.removeCallbacksAndMessages(null)
        openFileDescriptors.values.forEach {
            try {
                it.close()
            } catch (e: java.io.IOException) { /* already closed or unrecoverable */ }
        }
        openFileDescriptors.clear()
        super.onDestroy()
    }
}
