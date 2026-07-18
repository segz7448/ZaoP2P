package com.zao.p2p.chat

/**
 * UI-side representation of one chat message, built from either a
 * StoredMessage (loaded history) or a live TextMessage AppEvent
 * (freshly arrived). `isOutgoing` drives bubble alignment/color;
 * `status` drives the delivery-state label (sending/sent/delivered/read).
 *
 * `type` distinguishes a plain text bubble from a file-transfer bubble --
 * file bubbles additionally carry transfer metadata used to render and
 * live-update a progress bar (see ChatAdapter).
 */
data class ChatMessage(
    val messageId: String,
    val body: String,
    val sentAtUnix: Long,
    val isOutgoing: Boolean,
    var status: String, // "pending" | "sent" | "delivered" | "read" | "failed"
    val type: String = "text", // "text" | "file"
    val transferId: String? = null,
    val fileName: String? = null,
    val fileSize: Long = 0,
    var transferPercent: Float = 0f,
    var transferSpeedBytesPerSec: Double = 0.0,
    var transferEtaSeconds: Double? = null,
    var transferState: String = "active", // "active" | "paused" | "completed" | "cancelled" | "failed"
)
