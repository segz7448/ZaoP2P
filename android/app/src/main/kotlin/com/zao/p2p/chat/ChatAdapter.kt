package com.zao.p2p.chat

import android.view.Gravity
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.Button
import android.widget.LinearLayout
import android.widget.ProgressBar
import android.widget.TextView
import androidx.recyclerview.widget.RecyclerView
import com.zao.p2p.R
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import kotlin.math.ceil

private const val VIEW_TYPE_TEXT = 0
private const val VIEW_TYPE_FILE = 1

/**
 * Renders both plain text bubbles and file-transfer bubbles (with a
 * live progress bar + pause/cancel controls) in one chat list.
 * `onPauseResume`/`onCancel` are supplied by ChatActivity so the adapter
 * stays free of any NativeBridge calls -- it only renders state and
 * forwards user taps.
 */
class ChatAdapter(
    private val messages: MutableList<ChatMessage>,
    private val onPauseResume: (ChatMessage) -> Unit,
    private val onCancel: (ChatMessage) -> Unit,
) : RecyclerView.Adapter<RecyclerView.ViewHolder>() {

    private val timeFormat = SimpleDateFormat("HH:mm", Locale.getDefault())

    class TextViewHolder(view: View) : RecyclerView.ViewHolder(view) {
        val row: LinearLayout = view.findViewById(R.id.messageRow)
        val bubble: LinearLayout = view.findViewById(R.id.bubble)
        val body: TextView = view.findViewById(R.id.messageBody)
        val meta: TextView = view.findViewById(R.id.messageMeta)
    }

    class FileViewHolder(view: View) : RecyclerView.ViewHolder(view) {
        val row: LinearLayout = view.findViewById(R.id.messageRow)
        val fileName: TextView = view.findViewById(R.id.fileName)
        val fileSize: TextView = view.findViewById(R.id.fileSize)
        val progressBar: ProgressBar = view.findViewById(R.id.transferProgressBar)
        val percent: TextView = view.findViewById(R.id.transferPercent)
        val speed: TextView = view.findViewById(R.id.transferSpeed)
        val pauseResumeBtn: Button = view.findViewById(R.id.pauseResumeBtn)
        val cancelBtn: Button = view.findViewById(R.id.cancelBtn)
    }

    override fun getItemViewType(position: Int): Int =
        if (messages[position].type == "file") VIEW_TYPE_FILE else VIEW_TYPE_TEXT

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): RecyclerView.ViewHolder {
        return if (viewType == VIEW_TYPE_FILE) {
            val view = LayoutInflater.from(parent.context).inflate(R.layout.item_file_message, parent, false)
            FileViewHolder(view)
        } else {
            val view = LayoutInflater.from(parent.context).inflate(R.layout.item_message, parent, false)
            TextViewHolder(view)
        }
    }

    override fun onBindViewHolder(holder: RecyclerView.ViewHolder, position: Int) {
        val msg = messages[position]
        when (holder) {
            is TextViewHolder -> bindText(holder, msg)
            is FileViewHolder -> bindFile(holder, msg)
        }
    }

    private fun bindText(holder: TextViewHolder, msg: ChatMessage) {
        holder.body.text = msg.body
        val statusLabel = statusLabelFor(msg.status)
        val time = timeFormat.format(Date(msg.sentAtUnix * 1000))
        holder.meta.text = if (msg.isOutgoing) "$time · $statusLabel" else time

        if (msg.isOutgoing) {
            holder.row.gravity = Gravity.END
            holder.bubble.setBackgroundResource(R.drawable.bubble_outgoing)
        } else {
            holder.row.gravity = Gravity.START
            holder.bubble.setBackgroundResource(R.drawable.bubble_incoming)
        }
    }

    private fun bindFile(holder: FileViewHolder, msg: ChatMessage) {
        holder.row.gravity = if (msg.isOutgoing) Gravity.END else Gravity.START
        holder.fileName.text = msg.fileName ?: "File"
        holder.fileSize.text = formatBytes(msg.fileSize)
        holder.progressBar.progress = msg.transferPercent.toInt()
        holder.percent.text = "${msg.transferPercent.toInt()}%"
        holder.speed.text = if (msg.transferSpeedBytesPerSec > 0) {
            formatBytes(msg.transferSpeedBytesPerSec.toLong()) + "/s" +
                (msg.transferEtaSeconds?.let { " · ${ceil(it).toInt()}s left" } ?: "")
        } else ""

        val finished = msg.transferState == "completed" || msg.transferState == "cancelled"
        holder.pauseResumeBtn.visibility = if (finished) View.GONE else View.VISIBLE
        holder.cancelBtn.visibility = if (finished) View.GONE else View.VISIBLE
        holder.pauseResumeBtn.text = if (msg.transferState == "paused") "Resume" else "Pause"

        holder.pauseResumeBtn.setOnClickListener { onPauseResume(msg) }
        holder.cancelBtn.setOnClickListener { onCancel(msg) }
    }

    private fun statusLabelFor(status: String): String = when (status) {
        "pending" -> "sending…"
        "sent" -> "sent"
        "delivered" -> "delivered"
        "read" -> "read"
        "failed" -> "failed"
        else -> status
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

    override fun getItemCount(): Int = messages.size

    fun setMessages(newMessages: List<ChatMessage>) {
        messages.clear()
        messages.addAll(newMessages)
        notifyDataSetChanged()
    }

    fun appendMessage(message: ChatMessage) {
        messages.add(message)
        notifyItemInserted(messages.size - 1)
    }

    /** Update an existing text message's status in place (e.g. pending -> delivered -> read). */
    fun updateStatus(messageId: String, newStatus: String): Boolean {
        val index = messages.indexOfFirst { it.messageId == messageId }
        if (index == -1) return false
        messages[index].status = newStatus
        notifyItemChanged(index)
        return true
    }

    /** Update a file bubble's live progress fields by transfer_id. */
    fun updateTransferProgress(
        transferId: String,
        percent: Float,
        speedBytesPerSec: Double,
        etaSeconds: Double?,
        state: String,
    ): Boolean {
        val index = messages.indexOfFirst { it.transferId == transferId }
        if (index == -1) return false
        val msg = messages[index]
        msg.transferPercent = percent
        msg.transferSpeedBytesPerSec = speedBytesPerSec
        msg.transferEtaSeconds = etaSeconds
        msg.transferState = state
        notifyItemChanged(index)
        return true
    }
}
