use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, Instant};
pub type Callback<T, R> = dyn FnMut(T) -> (dyn Future<Output = R>) + Sync + Send + 'static;
use crate::*;
pub struct Timer<F> {
    interval: Duration,
    callback: F,
    _ok: bool,
}

impl<F> Timer<F> {
    pub fn new<A, R, Fut>(delay: u64, cb: F) -> Self
    where
        F: Fn(A) -> Fut + Send + Sync + 'static,
        A: Send + Sync + 'static,

        Fut: Future<Output = R> + 'static,
        R: Serialize + Deserialize<'static> + Send + Sync + 'static,
    {
        let interval = Duration::from_secs(delay);
        Self {
            interval,
            callback: cb,
            _ok: true,
        }
    }
    pub async fn run<A, Fut, R>(&mut self, args: A) -> Option<R>
    where
        F: FnMut(A) -> Fut,
        A: Send + Sync + 'static + Clone,
        Fut: Future<Output = R> + 'static,
        R: Serialize + Deserialize<'static> + Send + Sync + 'static,
    {
        //
        if self._ok {
            sleep(self.interval).await;
            let now = Instant::now();
            let res = (self.callback)(args.clone());
            let v = res.await;

            return Some(v);
        }
        None
    }

    pub fn stop(&mut self) {
        self._ok = false;
        let _ = *self;
        //  self._task.abort();
    }
}

// write a test for this
#[cfg(test)]
mod tests  {
    use super::*;
    #[tokio::test]
    async fn return_result() {
        let mut timer = Timer::new(1, |x| async move { x + 1 });
        let mut x = 0;
        let res = timer.run(x).await;
        //assert_eq!(x, 1);
        assert_eq!(res, Some(1));
    }

    #[tokio::test]
    async fn should_stop() {
        let mut timer = Timer::new(1, |x| async move { x + 1 });
        let mut x = 0;
        timer.stop();
        let res = timer.run(x).await;
        assert_eq!(res, None);
    }
}
