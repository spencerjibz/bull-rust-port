use crate::*;
use async_event_emitter::AsyncCB;

use futures::future::{BoxFuture, Future, FutureExt};
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tokio::{
    task::{self, JoinHandle},
    time::{sleep, Instant},
};
pub type EmptyCb = dyn Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static;

#[derive(Clone)]
pub struct Timer {
    interval: Duration,
    callback: Arc<EmptyCb>,
    task: Option<Arc<JoinHandle<()>>>,
    _ok: bool,
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
        let mut timer = Self {
            interval,
            callback: Arc::new(parsed_cb),
            task: None,
            _ok: true,
        };

        timer.run();

        timer
    }

    pub fn run(&mut self) {
        let mut interval = tokio::time::interval(self.interval);
        let callback = Arc::clone(&self.callback);
        let mut _ok = self._ok;
        let mut task = task::spawn(async move {
            loop {
                interval.tick().await;

                callback().await;
            }
        });
        self.task = Some(Arc::new(task));
    }

    pub fn stop(&mut self) {
        match &self.task {
            Some(task) => {
                self._ok = false;
                task.abort();
                self.task = None;
            }
            None => {} //  self._task.abort();
        }
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
        assert!(timer._ok);

        tokio::time::sleep(Duration::from_secs(3)).await;
        timer.stop();

        assert!(!timer._ok);
    }
}
