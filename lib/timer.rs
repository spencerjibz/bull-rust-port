use crate::*;

use async_atomic::Atomic;
use futures::future::{BoxFuture, Future, FutureExt};
use std::cell::RefCell;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tokio::{
    task::{self, JoinHandle},
    time::{sleep, Instant},
};
pub type EmptyCb = dyn Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static;
use tokio_util::sync::CancellationToken;
#[derive(Clone)]
pub struct Timer {
    interval: Duration,
    callback: Arc<EmptyCb>,
    cancel: CancellationToken,
    status: Arc<AtomicBool>,
}

impl Timer {
    pub fn new<C, F>(delay_secs: u64, cb: C) -> Self
    where
        C: Fn() -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        let interval = Duration::from_secs(delay_secs);
        #[allow(clippy::redundant_closure)]
        let parsed_cb = move || cb().boxed();
        Self {
            interval,
            callback: Arc::new(parsed_cb),
            cancel: Default::default(),
            status: Arc::default(),
        }
    }

    pub fn run(&self) -> JoinHandle<()> {
        let mut interval = tokio::time::interval(self.interval);
        let callback = Arc::clone(&self.callback);
        let token = self.cancel.clone();
        let mut task = task::spawn(async move {
            while !token.is_cancelled() {
                interval.tick().await;

                callback().await;
            }
        });
        self.status
            .store(true, std::sync::atomic::Ordering::Relaxed);
        task
    }

    pub fn stop(&self) {
        self.cancel.cancel();
        self.status
            .swap(false, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn is_running(&self) -> bool {
        self.status.load(std::sync::atomic::Ordering::Relaxed)
    }
}

// write a test for this
#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicI32;

    use super::*;
    #[tokio_shared_rt::test(shared = false)]
    async fn runs_and_stops() {
        let mut timer = Timer::new(1, || async { println!("hello") });
        timer.run();
        dbg!(timer.is_running());

        tokio::time::sleep(Duration::from_secs(3)).await;
        timer.stop();

        assert!(!timer.is_running());
    }
}
