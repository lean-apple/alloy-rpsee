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
    // A handle stopping through `stop`, whose server runs on `tasks`.
    pub(crate) fn new(stop: CancellationToken, tasks: TaskTracker) -> Self {
        let stop_on_drop = Arc::new(stop.clone().drop_guard());
        Self { stop, tasks, _stop_on_drop: stop_on_drop }
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
