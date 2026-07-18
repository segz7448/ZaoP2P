use crate::error::{CoreError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

/// Chunk size chosen as a balance point: large enough to keep per-chunk
/// overhead (framing, chunk header, DB write) low relative to payload,
/// small enough that a dropped/retried chunk doesn't waste much work.
/// 8MB matches what LocalSend-class tools use for LAN transfer.
pub const CHUNK_SIZE: u64 = 8 * 1024 * 1024;

/// How many chunk workers run concurrently over the same QUIC connection.
/// Each worker is a separate QUIC stream; this is what gives parallel
/// chunk throughput on top of QUIC's own multiplexing/congestion control.
pub const DEFAULT_PARALLELISM: usize = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferState {
    Queued,
    Active,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl TransferState {
    pub fn as_str(&self) -> &'static str {
        match self {
            TransferState::Queued => "queued",
            TransferState::Active => "active",
            TransferState::Paused => "paused",
            TransferState::Completed => "completed",
            TransferState::Failed => "failed",
            TransferState::Cancelled => "cancelled",
        }
    }
}

/// A snapshot of transfer progress, emitted periodically to the UI layer
/// on both sender and receiver. This is what populates the progress bar,
/// speed readout, ETA, and byte counters the requirements call for.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferProgress {
    pub transfer_id: String,
    pub file_name: String,
    pub total_bytes: u64,
    pub bytes_transferred: u64,
    pub percent: f32,
    pub speed_bytes_per_sec: f64,
    pub eta_seconds: Option<f64>,
    pub state: String,
}

/// Shared, thread-safe transfer handle. Cloning this is cheap (just Arc
/// clones) -- pass clones into each chunk worker task and to whatever
/// exposes pause()/resume()/cancel() to the UI.
#[derive(Clone)]
pub struct TransferHandle {
    inner: Arc<TransferInner>,
}

struct TransferInner {
    transfer_id: String,
    file_name: String,
    total_bytes: u64,
    total_chunks: u64,
    bytes_transferred: AtomicU64,
    paused: AtomicBool,
    cancelled: AtomicBool,
    started_at: Instant,
    // Rolling window for speed calculation: (timestamp, cumulative_bytes)
    speed_samples: Mutex<Vec<(Instant, u64)>>,
    acked_chunks: Mutex<HashSet<u64>>,
}

impl TransferHandle {
    pub fn new(transfer_id: String, file_name: String, total_bytes: u64) -> Self {
        let total_chunks = total_bytes.div_ceil(CHUNK_SIZE);
        Self {
            inner: Arc::new(TransferInner {
                transfer_id,
                file_name,
                total_bytes,
                total_chunks,
                bytes_transferred: AtomicU64::new(0),
                paused: AtomicBool::new(false),
                cancelled: AtomicBool::new(false),
                started_at: Instant::now(),
                speed_samples: Mutex::new(Vec::new()),
                acked_chunks: Mutex::new(HashSet::new()),
            }),
        }
    }

    /// Rehydrate a handle for a previously interrupted transfer, given
    /// the set of chunk indices already acked (from `chunk_manifest`).
    /// This is what makes "resume interrupted transfers after
    /// reconnecting" work: workers skip any index already in this set.
    pub fn resume_from_manifest(
        transfer_id: String,
        file_name: String,
        total_bytes: u64,
        already_acked: HashSet<u64>,
    ) -> Self {
        let handle = Self::new(transfer_id, file_name, total_bytes);
        let bytes_done = already_acked.len() as u64 * CHUNK_SIZE;
        handle
            .inner
            .bytes_transferred
            .store(bytes_done.min(total_bytes), Ordering::SeqCst);
        *handle.inner.acked_chunks.lock().unwrap() = already_acked;
        handle
    }

    pub fn transfer_id(&self) -> &str {
        &self.inner.transfer_id
    }

    pub fn total_chunks(&self) -> u64 {
        self.inner.total_chunks
    }

    pub fn is_paused(&self) -> bool {
        self.inner.paused.load(Ordering::SeqCst)
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst)
    }

    pub fn pause(&self) {
        self.inner.paused.store(true, Ordering::SeqCst);
    }

    pub fn resume(&self) {
        self.inner.paused.store(false, Ordering::SeqCst);
    }

    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::SeqCst);
    }

    /// Which chunk indices still need transferring -- i.e. total range
    /// minus whatever the manifest already marked acked. Used both for
    /// a fresh transfer (empty acked set) and a resumed one.
    pub fn pending_chunk_indices(&self) -> Vec<u64> {
        let acked = self.inner.acked_chunks.lock().unwrap();
        (0..self.inner.total_chunks)
            .filter(|i| !acked.contains(i))
            .collect()
    }

    /// Mark a chunk as successfully sent/received and acked by the peer.
    /// Call this after each chunk's transfer + integrity check succeeds.
    pub fn mark_chunk_acked(&self, chunk_index: u64, chunk_len: u64) {
        let mut acked = self.inner.acked_chunks.lock().unwrap();
        if acked.insert(chunk_index) {
            drop(acked);
            let new_total = self
                .inner
                .bytes_transferred
                .fetch_add(chunk_len, Ordering::SeqCst)
                + chunk_len;
            self.record_speed_sample(new_total);
        }
    }

    fn record_speed_sample(&self, cumulative_bytes: u64) {
        let mut samples = self.inner.speed_samples.lock().unwrap();
        let now = Instant::now();
        samples.push((now, cumulative_bytes));
        // Keep only the last 5 seconds of samples for a responsive-but-
        // stable rolling speed estimate.
        samples.retain(|(t, _)| now.duration_since(*t) < Duration::from_secs(5));
    }

    pub fn acked_chunk_set(&self) -> HashSet<u64> {
        self.inner.acked_chunks.lock().unwrap().clone()
    }

    /// Produce a progress snapshot for UI consumption / event emission.
    pub fn progress(&self, state: TransferState) -> TransferProgress {
        let transferred = self.inner.bytes_transferred.load(Ordering::SeqCst);
        let percent = if self.inner.total_bytes == 0 {
            100.0
        } else {
            (transferred as f64 / self.inner.total_bytes as f64 * 100.0) as f32
        };

        let speed = self.current_speed_bytes_per_sec();
        let remaining = self.inner.total_bytes.saturating_sub(transferred);
        let eta = if speed > 0.0 {
            Some(remaining as f64 / speed)
        } else {
            None
        };

        TransferProgress {
            transfer_id: self.inner.transfer_id.clone(),
            file_name: self.inner.file_name.clone(),
            total_bytes: self.inner.total_bytes,
            bytes_transferred: transferred,
            percent,
            speed_bytes_per_sec: speed,
            eta_seconds: eta,
            state: state.as_str().to_string(),
        }
    }

    fn current_speed_bytes_per_sec(&self) -> f64 {
        let samples = self.inner.speed_samples.lock().unwrap();
        if samples.len() < 2 {
            return 0.0;
        }
        let (t_first, b_first) = samples.first().unwrap();
        let (t_last, b_last) = samples.last().unwrap();
        let elapsed = t_last.duration_since(*t_first).as_secs_f64();
        if elapsed <= 0.0 {
            return 0.0;
        }
        (*b_last as f64 - *b_first as f64) / elapsed
    }
}

/// One chunk's worth of framed data as sent over a QUIC stream.
/// Wire format: [chunk_index: u64 LE][chunk_len: u32 LE][sha256: 32 bytes][payload: chunk_len bytes]
pub struct ChunkFrame {
    pub chunk_index: u64,
    pub payload: Vec<u8>,
    pub checksum: [u8; 32],
}

impl ChunkFrame {
    pub fn new(chunk_index: u64, payload: Vec<u8>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(&payload);
        let checksum = hasher.finalize().into();
        Self {
            chunk_index,
            payload,
            checksum,
        }
    }

    pub fn verify(&self) -> bool {
        let mut hasher = Sha256::new();
        hasher.update(&self.payload);
        let computed: [u8; 32] = hasher.finalize().into();
        computed == self.checksum
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + 4 + 32 + self.payload.len());
        buf.extend_from_slice(&self.chunk_index.to_le_bytes());
        buf.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.checksum);
        buf.extend_from_slice(&self.payload);
        buf
    }

    pub async fn read_from<R: tokio::io::AsyncRead + Unpin>(reader: &mut R) -> Result<Self> {
        let mut header = [0u8; 8 + 4 + 32];
        reader.read_exact(&mut header).await?;
        let chunk_index = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let payload_len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        let checksum: [u8; 32] = header[12..44].try_into().unwrap();

        let mut payload = vec![0u8; payload_len];
        reader.read_exact(&mut payload).await?;

        let frame = ChunkFrame {
            chunk_index,
            payload,
            checksum,
        };
        Ok(frame)
    }
}

/// Where a chunk's bytes are read from: either a real filesystem path
/// (the normal case on Windows and most of Android) or an already-open
/// file descriptor number (Android's content:// URI case -- see the
/// doc comment on `read_chunk_from_source` for why this exists).
#[derive(Clone)]
pub enum FileSource {
    Path(PathBuf),
    /// Raw fd number from an Android `ParcelFileDescriptor`, obtained by
    /// the JNI layer via `ContentResolver.openFileDescriptor` and
    /// passed down as a plain integer -- Rust has no direct API for
    /// Android content:// URIs, so the fd number is the only bridge.
    Fd(i32),
}

/// Sender-side: reads one chunk directly from the source file at the
/// correct offset. Memory usage stays flat (one chunk buffer, ~8MB)
/// regardless of total file size, which is what lets this handle
/// multi-gigabyte files without ballooning RAM.
///
/// For `FileSource::Fd`, this re-opens the file via
/// `/proc/self/fd/{fd}` (a Linux/Android-specific mechanism; this path
/// is never hit on Windows since only Android's content:// picker flow
/// produces an `Fd` source) RATHER than reading through the original
/// fd directly. This matters for correctness, not just convenience:
/// parallel chunk workers each need their own independent file
/// position to seek to their own offset concurrently, but a single
/// POSIX file descriptor has ONE shared read/write position across
/// every use of it -- concurrent seeks on the same fd from multiple
/// tasks would race. Re-opening `/proc/self/fd/{fd}` for each read
/// gives every worker its own fresh open-file-description (and thus
/// its own independent offset), while still ultimately reading through
/// the same underlying inode the original `ParcelFileDescriptor`
/// pointed at -- exactly what concurrent chunk reads need.
pub async fn read_chunk_from_source(source: &FileSource, chunk_index: u64) -> Result<Vec<u8>> {
    let offset = chunk_index * CHUNK_SIZE;
    let mut file = match source {
        FileSource::Path(path) => File::open(path).await?,
        FileSource::Fd(fd) => {
            let proc_path = format!("/proc/self/fd/{fd}");
            File::open(&proc_path).await.map_err(|e| {
                CoreError::Io(std::io::Error::new(
                    e.kind(),
                    format!(
                        "failed to reopen fd {fd} via {proc_path} (this path only works on \
                         Linux/Android, and only while the original ParcelFileDescriptor stays \
                         open on the Kotlin side for the duration of the transfer): {e}"
                    ),
                ))
            })?
        }
    };
    file.seek(SeekFrom::Start(offset)).await?;

    let metadata = file.metadata().await?;
    let remaining = metadata.len().saturating_sub(offset);
    let read_len = remaining.min(CHUNK_SIZE) as usize;

    let mut buf = vec![0u8; read_len];
    file.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Legacy path-only entrypoint, kept so existing call sites (Windows,
/// and any Android path that already has a real filesystem path rather
/// than a content:// URI) don't need to wrap every call in
/// `FileSource::Path(..)` explicitly.
pub async fn read_chunk_from_file(path: &PathBuf, chunk_index: u64) -> Result<Vec<u8>> {
    read_chunk_from_source(&FileSource::Path(path.clone()), chunk_index).await
}

/// Receiver-side: writes one chunk directly to its offset in the
/// destination file. The file is pre-allocated to full size before any
/// chunks arrive (see `preallocate_file`), so out-of-order / parallel
/// chunk writes from concurrent workers never conflict or require
/// reassembly -- each worker just seeks and writes its own slice.
pub async fn write_chunk_to_file(path: &PathBuf, chunk_index: u64, payload: &[u8]) -> Result<()> {
    let offset = chunk_index * CHUNK_SIZE;
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .await?;
    file.seek(SeekFrom::Start(offset)).await?;
    file.write_all(payload).await?;
    file.flush().await?;
    Ok(())
}

/// Pre-allocate the destination file to its final size. Sparse on most
/// filesystems (ext4, NTFS), so this doesn't actually consume disk space
/// up front -- it just reserves the layout so parallel chunk writers can
/// seek+write independently without truncation/resize races.
pub async fn preallocate_file(path: &PathBuf, total_size: u64) -> Result<()> {
    let file = File::create(path).await?;
    file.set_len(total_size).await?;
    Ok(())
}

/// Orchestrates parallel chunk workers for one transfer (sender side).
/// `send_fn` is supplied by the caller (wiring in the actual QUIC stream)
/// so this module stays transport-agnostic and testable without a real
/// network connection.
pub async fn run_sender_workers<F, Fut>(
    handle: TransferHandle,
    file_path: PathBuf,
    parallelism: usize,
    send_fn: F,
) -> Result<()>
where
    F: Fn(ChunkFrame) -> Fut + Send + Sync + 'static + Clone,
    Fut: std::future::Future<Output = Result<()>> + Send,
{
    let pending = handle.pending_chunk_indices();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(parallelism));
    let mut tasks = Vec::new();

    for chunk_index in pending {
        if handle.is_cancelled() {
            return Err(CoreError::InvalidState("transfer cancelled".into()));
        }
        while handle.is_paused() {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if handle.is_cancelled() {
                return Err(CoreError::InvalidState("transfer cancelled".into()));
            }
        }

        let permit = semaphore.clone().acquire_owned().await.map_err(|_| {
            CoreError::InvalidState("semaphore closed unexpectedly".into())
        })?;
        let path = file_path.clone();
        let handle_clone = handle.clone();
        let send_fn_clone = send_fn.clone();

        let task = tokio::spawn(async move {
            let _permit = permit; // held until this task completes
            let payload = read_chunk_from_file(&path, chunk_index).await?;
            let chunk_len = payload.len() as u64;
            let frame = ChunkFrame::new(chunk_index, payload);
            send_fn_clone(frame).await?;
            handle_clone.mark_chunk_acked(chunk_index, chunk_len);
            Ok::<(), CoreError>(())
        });
        tasks.push(task);
    }

    for task in tasks {
        task.await
            .map_err(|e| CoreError::InvalidState(format!("worker task panicked: {e}")))??;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile_shim::write_temp_file;

    // Minimal inline temp-file helper so we don't add a tempfile dependency
    // just for tests.
    mod tempfile_shim {
        use std::path::PathBuf;

        pub fn write_temp_file(name: &str, contents: &[u8]) -> PathBuf {
            let mut path = std::env::temp_dir();
            path.push(format!("zao_test_{}", name));
            std::fs::write(&path, contents).unwrap();
            path
        }
    }

    #[test]
    fn chunk_frame_roundtrip_and_checksum() {
        let payload = vec![42u8; 1024];
        let frame = ChunkFrame::new(3, payload.clone());
        assert!(frame.verify());

        let encoded = frame.encode();
        assert_eq!(encoded.len(), 8 + 4 + 32 + payload.len());
    }

    #[test]
    fn tampered_payload_fails_checksum() {
        let payload = vec![1u8; 100];
        let mut frame = ChunkFrame::new(0, payload);
        frame.payload[0] = 0xFF; // corrupt after checksum was computed
        assert!(!frame.verify());
    }

    #[test]
    fn progress_math_is_sane() {
        let handle = TransferHandle::new("t1".into(), "file.bin".into(), 100);
        handle.mark_chunk_acked(0, 50);
        let progress = handle.progress(TransferState::Active);
        assert_eq!(progress.bytes_transferred, 50);
        assert_eq!(progress.percent, 50.0);
    }

    #[test]
    fn resume_skips_already_acked_chunks() {
        let mut acked = HashSet::new();
        acked.insert(0u64);
        acked.insert(1u64);
        let handle = TransferHandle::resume_from_manifest(
            "t2".into(),
            "big.bin".into(),
            CHUNK_SIZE * 5,
            acked,
        );
        let pending = handle.pending_chunk_indices();
        assert_eq!(pending, vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn read_chunk_reads_correct_offset_and_length() {
        let data = vec![7u8; (CHUNK_SIZE + 100) as usize];
        let path = write_temp_file("chunk_read_test.bin", &data);

        let chunk0 = read_chunk_from_file(&path, 0).await.unwrap();
        assert_eq!(chunk0.len(), CHUNK_SIZE as usize);

        let chunk1 = read_chunk_from_file(&path, 1).await.unwrap();
        assert_eq!(chunk1.len(), 100); // remainder, not a full chunk

        std::fs::remove_file(&path).ok();
    }
}
