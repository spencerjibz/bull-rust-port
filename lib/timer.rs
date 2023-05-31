use crate::emitter::AsyncCB;
use crate::*;
use bincode;
use futures::future::{BoxFuture, Future, FutureExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::{
    task::{self, JoinHandle},
    time::{sleep, Instant},
};
pub type EmptyCb = dyn Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static;

pub struct Timer {
    interval: Duration,
    callback: Arc<EmptyCb>,
    task: Option<JoinHandle<()>>,
    _ok: bool,
}

impl Timer {
    pub fn new<F>(delay: u64, cb: F) -> Self
    where
        F: Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        let interval = Duration::from_secs(delay);
        #[allow(clippy::redundant_closure)]
        let parsed_cb = move || cb();
        Self {
            interval,
            callback: Arc::new(parsed_cb),
            task: None,
            _ok: true,
        }
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
        self.task = Some(task);
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
    use super::*;
    #[tokio::test]
    async fn runs_and_stops() {
        let mut x = 0;
        let mut timer = Timer::new(1, || async move { println!("hello") }.boxed());

        timer.run();

        tokio::time::sleep(Duration::from_secs(2)).await;
        timer.stop();

        assert!(!timer._ok);
    }
}
