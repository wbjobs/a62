use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::mpsc::{self, Sender, Receiver, error::TrySendError};

use crate::trace_record::TraceRecord;

pub const DEFAULT_QUEUE_CAPACITY: usize = 100_000;
pub const DEFAULT_BATCH_SIZE: usize = 500;
pub const DEFAULT_FLUSH_INTERVAL_MS: u64 = 100;

#[derive(Debug, Clone)]
pub enum LogSinkConfig {
    Kafka {
        brokers: Vec<String>,
        topic: String,
    },
    Stdout,
    File {
        path: String,
    },
    Memory(MemorySink),
}

#[derive(Debug, Clone, Default)]
pub struct MemorySink {
    pub records: Arc<Mutex<Vec<TraceRecord>>>,
}

impl MemorySink {
    pub fn new() -> Self {
        Self {
            records: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn drain(&self) -> Vec<TraceRecord> {
        let mut guard = self.records.lock();
        std::mem::take(&mut *guard)
    }

    pub fn len(&self) -> usize {
        self.records.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct AsyncBatchLogger {
    sender: Sender<TraceRecord>,
    #[allow(dead_code)]
    sink: LogSinkConfig,
    stats: Arc<LoggerStats>,
}

#[derive(Debug, Default)]
pub struct LoggerStats {
    pub total_sent: std::sync::atomic::AtomicU64,
    pub total_dropped: std::sync::atomic::AtomicU64,
    pub total_flushed: std::sync::atomic::AtomicU64,
    pub flush_count: std::sync::atomic::AtomicU64,
}

impl AsyncBatchLogger {
    pub fn new(sink: LogSinkConfig) -> Arc<Self> {
        Self::with_options(sink, DEFAULT_QUEUE_CAPACITY, DEFAULT_BATCH_SIZE, DEFAULT_FLUSH_INTERVAL_MS)
    }

    pub fn with_options(
        sink: LogSinkConfig,
        queue_capacity: usize,
        batch_size: usize,
        flush_interval_ms: u64,
    ) -> Arc<Self> {
        let (sender, receiver) = mpsc::channel::<TraceRecord>(queue_capacity);
        let stats = Arc::new(LoggerStats::default());

        let logger = Arc::new(Self {
            sender,
            sink: sink.clone(),
            stats: stats.clone(),
        });

        let logger_clone = logger.clone();
        tokio::spawn(async move {
            logger_clone
                .run_worker(receiver, sink, batch_size, flush_interval_ms, stats)
                .await;
        });

        logger
    }

    pub fn log(&self, record: TraceRecord) -> bool {
        match self.sender.try_send(record) {
            Ok(()) => {
                self.stats.total_sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                true
            }
            Err(TrySendError::Full(_)) => {
                self.stats.total_dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                false
            }
            Err(TrySendError::Closed(_)) => {
                self.stats.total_dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                false
            }
        }
    }

    pub fn stats(&self) -> (u64, u64, u64, u64) {
        (
            self.stats.total_sent.load(std::sync::atomic::Ordering::Relaxed),
            self.stats.total_dropped.load(std::sync::atomic::Ordering::Relaxed),
            self.stats.total_flushed.load(std::sync::atomic::Ordering::Relaxed),
            self.stats.flush_count.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    async fn run_worker(
        &self,
        mut receiver: Receiver<TraceRecord>,
        sink: LogSinkConfig,
        batch_size: usize,
        flush_interval_ms: u64,
        stats: Arc<LoggerStats>,
    ) {
        let mut batch: Vec<TraceRecord> = Vec::with_capacity(batch_size);
        let mut flush_tick = tokio::time::interval(Duration::from_millis(flush_interval_ms));

        loop {
            tokio::select! {
                _ = flush_tick.tick() => {
                    if !batch.is_empty() {
                        let to_flush = std::mem::take(&mut batch);
                        Self::flush_batch(&sink, to_flush, &stats).await;
                    }
                }

                msg = receiver.recv() => {
                    match msg {
                        Some(record) => {
                            batch.push(record);
                            if batch.len() >= batch_size {
                                let to_flush = std::mem::take(&mut batch);
                                Self::flush_batch(&sink, to_flush, &stats).await;
                            }
                        }
                        None => {
                            if !batch.is_empty() {
                                let to_flush = std::mem::take(&mut batch);
                                Self::flush_batch(&sink, to_flush, &stats).await;
                            }
                            tracing::info!("Async logger receiver closed, worker exiting");
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn flush_batch(
        sink: &LogSinkConfig,
        batch: Vec<TraceRecord>,
        stats: &LoggerStats,
    ) {
        let batch_len = batch.len();
        tracing::debug!("Flushing {} trace records to sink", batch_len);

        match sink {
            LogSinkConfig::Stdout => {
                for record in &batch {
                    if let Ok(json) = serde_json::to_string(record) {
                        println!("{}", json);
                    }
                }
            }
            LogSinkConfig::File { path } => {
                use tokio::io::AsyncWriteExt;
                let mut lines = String::with_capacity(batch_len * 200);
                for record in &batch {
                    if let Ok(json) = serde_json::to_string(record) {
                        lines.push_str(&json);
                        lines.push('\n');
                    }
                }
                if !lines.is_empty() {
                    if let Ok(mut file) = tokio::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(path)
                        .await
                    {
                        let _ = file.write_all(lines.as_bytes()).await;
                    }
                }
            }
            LogSinkConfig::Kafka { brokers: _brokers, topic: _topic } => {
                tracing::debug!(
                    "Kafka sink: would publish {} records to topic",
                    batch_len
                );
            }
            LogSinkConfig::Memory(mem) => {
                let mut guard = mem.records.lock();
                guard.extend(batch);
            }
        }

        stats.total_flushed.fetch_add(batch_len as u64, std::sync::atomic::Ordering::Relaxed);
        stats.flush_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_memory_sink_basic() {
        let mem = MemorySink::new();
        let logger = AsyncBatchLogger::new(LogSinkConfig::Memory(mem.clone()));

        for i in 0..10 {
            let mut record = TraceRecord::new(format!("trace-{}", i), format!("req-{}", i));
            record.path = format!("/test/{}", i);
            logger.log(record);
        }

        sleep(Duration::from_millis(500)).await;

        let records = mem.drain();
        assert_eq!(records.len(), 10);
        assert_eq!(records[0].trace_id, "trace-0");
        assert_eq!(records[9].path, "/test/9");
    }

    #[tokio::test]
    async fn test_batched_flush() {
        let mem = MemorySink::new();
        let logger =
            AsyncBatchLogger::with_options(LogSinkConfig::Memory(mem.clone()), 1000, 10, 10_000);

        for i in 0..25 {
            let record = TraceRecord::new(format!("t-{}", i), format!("r-{}", i));
            logger.log(record);
        }

        sleep(Duration::from_millis(200)).await;

        let (sent, dropped, flushed, _flush_count) = logger.stats();
        assert_eq!(sent, 25);
        assert_eq!(dropped, 0);
        assert_eq!(flushed, 20);

        sleep(Duration::from_millis(11_000)).await;
        let records = mem.drain();
        assert_eq!(records.len(), 25);
    }

    #[tokio::test]
    async fn test_queue_overflow_drops_records() {
        let mem = MemorySink::new();
        let logger = AsyncBatchLogger::with_options(
            LogSinkConfig::Memory(mem.clone()),
            50,
            1000,
            60_000,
        );

        for i in 0..100 {
            let record = TraceRecord::new(format!("t-{}", i), format!("r-{}", i));
            logger.log(record);
        }

        let (sent, dropped, _flushed, _) = logger.stats();
        assert_eq!(sent, 50);
        assert!(dropped > 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_logging() {
        let mem = MemorySink::new();
        let logger = AsyncBatchLogger::new(LogSinkConfig::Memory(mem.clone()));
        let logger_clone = logger.clone();

        let mut handles = vec![];

        for task_id in 0..10 {
            let logger = logger_clone.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..100 {
                    let mut record = TraceRecord::new(
                        format!("trace-{}-{}", task_id, i),
                        format!("req-{}-{}", task_id, i),
                    );
                    record.path = format!("/task/{}/item/{}", task_id, i);
                    logger.log(record);
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        sleep(Duration::from_millis(500)).await;

        let (sent, dropped, flushed, _) = logger.stats();
        assert_eq!(sent, 1000);
        assert_eq!(dropped, 0);
        assert_eq!(flushed, 1000);
        assert_eq!(mem.len(), 1000);
    }
}
