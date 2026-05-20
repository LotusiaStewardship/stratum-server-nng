use anyhow::Result;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};
use tracing::info;
use rusqlite::Connection;
use std::sync::Arc;
use parking_lot::Mutex;

/// Graceful shutdown coordinator for managing server lifecycle.
///
/// Broadcasts shutdown signal to all registered tasks and waits for them to complete.
pub struct ShutdownCoordinator {
    shutdown_tx: broadcast::Sender<()>,
    shutdown_timeout_secs: u64,
    flush_timeout_secs: u64,
    db_conn: Option<Arc<Mutex<Connection>>>,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl ShutdownCoordinator {
    /// Get a clone of the broadcast sender for external use.
    pub fn broadcast_channel(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
    }

    /// Create a new shutdown coordinator.
    pub fn new() -> Self {
        let (shutdown_tx, _) = broadcast::channel(1024);
        Self {
            shutdown_tx,
            shutdown_timeout_secs: 30,
            flush_timeout_secs: 5,
            db_conn: None,
            tasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Create a new shutdown coordinator with database connection for WAL checkpoint.
    pub fn with_db_conn(
        db_conn: Arc<Mutex<Connection>>,
    ) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1024);
        Self {
            shutdown_tx,
            shutdown_timeout_secs: 30,
            flush_timeout_secs: 5,
            db_conn: Some(db_conn),
            tasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get a shutdown signal handle for a task to listen on.
    pub fn signal(&self) -> ShutdownSignal {
        ShutdownSignal {
            rx: self.shutdown_tx.subscribe(),
        }
    }

    /// Initiate graceful shutdown by broadcasting signal to all tasks.
    pub fn initiate_shutdown(&self) {
        info!("initiating graceful shutdown");
        let _ = self.shutdown_tx.send(());
    }

    /// Initiate emergency shutdown - immediate exit without flush.
    ///
    /// Intentionally does NOT broadcast the shutdown signal — tasks receiving
    /// it might start cleanup that will be interrupted by the immediate
    /// `exit(1)` in main.rs, potentially leaving partial state.
    pub fn initiate_emergency_shutdown(&self) {
        info!("initiating emergency shutdown (no flush, no cleanup)");
    }

    /// Register a task handle to be awaited during shutdown.
    pub fn register_task<T: Send + 'static>(&self, handle: JoinHandle<T>) {
        // We use a type-erased approach - store as JoinHandle<()> by mapping the result
        let handle_erased = tokio::spawn(async move {
            let _ = handle.await;
        });
        self.tasks.lock().push(handle_erased);
    }

    /// Wait for all tasks to complete shutdown (max timeout).
    /// Performs WAL checkpoint on database connection if available.
    pub async fn wait_for_completion(&self) -> Result<()> {
        info!(
            timeout_secs = self.shutdown_timeout_secs,
            "waiting for tasks to complete shutdown"
        );

        // Wait for all registered tasks to complete (with timeout)
        let tasks = {
            let mut tasks_guard = self.tasks.lock();
            std::mem::take(&mut *tasks_guard)
        };

        if !tasks.is_empty() {
            info!(task_count = tasks.len(), "awaiting tasks to complete");
            
            // Wait for all tasks with overall timeout
            let wait_all = async {
                for task in tasks {
                    let _ = task.await;
                }
            };
            
            timeout(
                Duration::from_secs(self.shutdown_timeout_secs),
                wait_all,
            )
            .await
            .map_err(|_| anyhow::anyhow!("shutdown timeout exceeded"))?;
            
            info!("all tasks completed");
        }

        // Perform WAL checkpoint if database connection is available
        if let Some(conn) = &self.db_conn {
            info!("performing WAL checkpoint");
            let conn_guard = conn.lock();
            conn_guard
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .unwrap_or_else(|e| {
                    info!(error = %e, "WAL checkpoint failed (non-fatal)");
                });
            info!("WAL checkpoint complete");
        }

        info!("shutdown complete");
        Ok(())
    }

    /// Get the flush timeout (how long to wait for in-flight operations).
    pub fn flush_timeout(&self) -> Duration {
        Duration::from_secs(self.flush_timeout_secs)
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle for listening to shutdown signals.
pub struct ShutdownSignal {
    rx: broadcast::Receiver<()>,
}

impl ShutdownSignal {
    /// Wait for shutdown signal. Returns when shutdown is initiated.
    pub async fn recv(&mut self) {
        let _ = self.rx.recv().await;
    }

    /// Check if shutdown has been initiated (non-blocking).
    pub fn is_shutdown(&self) -> bool {
        // Try to peek at the channel
        self.rx.resubscribe().try_recv().is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_shutdown_signal() {
        let coordinator = ShutdownCoordinator::new();
        let mut signal = coordinator.signal();

        // Initiate shutdown
        coordinator.initiate_shutdown();

        // Wait for signal to be received (should complete immediately)
        tokio::time::timeout(Duration::from_millis(100), signal.recv())
            .await
            .expect("shutdown signal should be received");
    }

    #[tokio::test]
    async fn test_shutdown_recv() {
        let coordinator = ShutdownCoordinator::new();
        let mut signal = coordinator.signal();

        // Spawn task that waits for shutdown
        let task = tokio::spawn(async move {
            signal.recv().await;
            "shutdown_received"
        });

        // Give task time to start waiting
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Initiate shutdown
        coordinator.initiate_shutdown();

        // Task should complete
        let result = timeout(Duration::from_secs(1), task)
            .await
            .expect("task should complete")
            .expect("task should not panic");

        assert_eq!(result, "shutdown_received");
    }

    #[tokio::test]
    async fn test_wait_for_completion() {
        let coordinator = ShutdownCoordinator::new();

        coordinator.initiate_shutdown();
        let result = coordinator.wait_for_completion().await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_wait_for_completion_awaits_tasks() {
        use tokio::sync::Mutex;

        let coordinator = Arc::new(ShutdownCoordinator::new());
        let completed = Arc::new(Mutex::new(false));
        let completed_clone = completed.clone();
        let coordinator_clone = coordinator.clone();

        // Register a task that takes 500ms to complete
        let task = tokio::spawn(async move {
            let mut signal = coordinator_clone.signal();
            signal.recv().await; // Wait for shutdown signal
            tokio::time::sleep(Duration::from_millis(500)).await; // Simulate cleanup
            *completed_clone.lock().await = true;
        });

        // Give task time to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Initiate shutdown and wait
        coordinator.initiate_shutdown();
        coordinator.register_task(task);
        let result = coordinator.wait_for_completion().await;

        assert!(result.is_ok());
        assert!(*completed.lock().await, "task should have completed before wait_for_completion returned");
    }
}
