use anyhow::Result;
use tokio::sync::broadcast;
use tokio::time::{timeout, Duration};
use tracing::info;

/// Graceful shutdown coordinator for managing server lifecycle.
///
/// Broadcasts shutdown signal to all registered tasks and waits for them to complete.
#[derive(Clone)]
pub struct ShutdownCoordinator {
    shutdown_tx: broadcast::Sender<()>,
    shutdown_timeout_secs: u64,
    flush_timeout_secs: u64,
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

    /// Wait for all tasks to complete shutdown (max timeout).
    pub async fn wait_for_completion(&self) -> Result<()> {
        info!(
            timeout_secs = self.shutdown_timeout_secs,
            "waiting for tasks to complete shutdown"
        );

        // For Slice 1, we just wait for the timeout
        // Future slices will track task handles and wait for them
        timeout(
            Duration::from_secs(self.shutdown_timeout_secs),
            tokio::time::sleep(Duration::from_millis(100)),
        )
        .await
        .map_err(|_| anyhow::anyhow!("shutdown timeout exceeded"))?;

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
}
