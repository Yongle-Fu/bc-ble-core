//! Tokio runtime helpers — auto-adapt to inside/outside async context.

use std::{future::Future, sync::Arc};
use tokio::runtime::{Handle, Runtime};

lazy_static::lazy_static! {
  pub(crate) static ref GLOBAL_RUNTIME: Arc<Runtime> = Arc::new(Runtime::new().unwrap());
}

pub fn get_runtime() -> Arc<Runtime> {
    GLOBAL_RUNTIME.clone()
}

/// Synchronously block on an async Future (auto-adapts to inside/outside tokio runtime).
pub fn block_on_any<F: Future>(fut: F) -> F::Output {
    if Handle::try_current().is_ok() {
        return tokio::task::block_in_place(|| Handle::current().block_on(fut));
    }
    let rt = get_runtime();
    rt.block_on(fut)
}

/// Spawn an async Future (fire-and-forget, auto-adapts to inside/outside tokio runtime).
pub fn spawn_any<F>(fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    if Handle::try_current().is_ok() {
        tokio::spawn(fut);
    } else {
        let rt = get_runtime();
        rt.spawn(fut);
    }
}

/// Spawn an async Future and return a JoinHandle for awaiting the result.
pub fn spawn_any_with_handle<F>(fut: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    if Handle::try_current().is_ok() {
        tokio::spawn(fut)
    } else {
        let rt = get_runtime();
        rt.spawn(fut)
    }
}

/// Spawn a CPU-heavy or blocking task on the blocking thread pool (awaits result).
pub async fn spawn_blocking_any<F, R>(func: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    if Handle::try_current().is_ok() {
        tokio::task::spawn_blocking(func)
            .await
            .expect("spawn_blocking failed")
    } else {
        let rt = get_runtime();
        rt.spawn_blocking(func)
            .await
            .expect("spawn_blocking failed")
    }
}

/// Spawn a blocking task (fire-and-forget, does not await).
pub fn spawn_blocking_detached<F>(func: F)
where
    F: FnOnce() + Send + 'static,
{
    if Handle::try_current().is_ok() {
        std::mem::drop(tokio::task::spawn_blocking(func));
    } else {
        let rt = get_runtime();
        std::mem::drop(rt.spawn_blocking(func));
    }
}
