//! Stopping a running server.

use crate::error::AlreadyStoppedError;
use std::sync::Arc;
use tokio_util::{
    sync::{CancellationToken, DropGuard},
    task::TaskTracker,
};

/// A handle to a running server, to stop it and wait until it stopped.
///
/// The server also stops once every clone of its handle is dropped.
#[derive(Debug, Clone)]
pub struct ServerHandle {
    // Cancelled to stop the server.
    stop: CancellationToken,
    // The server's accept loop and connections.
    tasks: TaskTracker,
    // Stops the server when the last clone is dropped.
    _stop_on_drop: Arc<DropGuard>,
}

impl ServerHandle {
    /// Creates the handle of a server that has not started yet.
    ///
    /// A server with its own accept loop, such as one on another transport, waits on
    /// [`Self::stop_token`] and spawns its tasks on [`Self::tasks`], which [`Self::stopped`] waits
    /// for.
    pub fn new() -> Self {
        let stop = CancellationToken::new();
        let stop_on_drop = Arc::new(stop.clone().drop_guard());
        Self { stop, tasks: TaskTracker::new(), _stop_on_drop: stop_on_drop }
    }

    /// Returns the token that is cancelled when the server is asked to stop.
    pub fn stop_token(&self) -> CancellationToken {
        self.stop.clone()
    }

    /// Returns the tracker of the server's tasks, which [`Self::stopped`] waits for.
    pub const fn tasks(&self) -> &TaskTracker {
        &self.tasks
    }

    /// Stops accepting connections and lets the open ones finish their requests.
    ///
    /// # Errors
    ///
    /// Returns [`AlreadyStoppedError`] if the server was already stopped.
    pub fn stop(&self) -> Result<(), AlreadyStoppedError> {
        if self.stop.is_cancelled() {
            return Err(AlreadyStoppedError);
        }
        self.stop.cancel();
        Ok(())
    }

    /// Waits until the server stopped and its connections closed.
    pub async fn stopped(self) {
        self.stop.cancelled().await;
        self.tasks.close();
        self.tasks.wait().await;
    }

    /// Returns `true` once the server was asked to stop.
    pub fn is_stopped(&self) -> bool {
        self.stop.is_cancelled()
    }
}

impl Default for ServerHandle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn stopped_waits_for_tasks_of_an_own_accept_loop() {
        let handle = ServerHandle::new();
        let (finish, finished) = oneshot::channel::<()>();
        let stop = handle.stop_token();
        handle.tasks().spawn(async move {
            stop.cancelled().await;
            finished.await.unwrap();
        });
        handle.stop().unwrap();
        // The task saw the stop but hasn't finished, so the server hasn't stopped yet.
        let waiting = tokio::time::timeout(Duration::from_millis(50), handle.clone().stopped());
        assert!(waiting.await.is_err());
        finish.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), handle.stopped()).await.unwrap();
    }
}
