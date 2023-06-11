use crate::emitter::AsyncEventEmitter;
use crate::timer::Timer;
use crate::*;
use futures::future::{ok, BoxFuture, Future, FutureExt};
use std::collections::HashSet;
pub type WorkerCallback<'a, D, R> = dyn Fn(D) -> (dyn Future<Output = R> + Send + Sync) + Send + Sync + 'a;
use std::cell::RefCell;
use  futures::lock::Mutex;
use std::sync::Arc;

struct Worker<'a, D, R> {
    pub name: &'a str,
    pub connection: Pool,
    pub options: WorkerOptions,
    pub emitter: AsyncEventEmitter,
    pub timer: Option<Timer>,
    pub closing: bool,
    pub closed: bool,
    running: bool,
    scripts: Mutex<script::Scripts<'a>>,
    pub jobs: HashSet<Job<'a, D, R>>,
    pub processing: HashSet<Job<'a, D, R>>,
    pub prefix: String,
    force_closing: bool,
    processor: Arc<WorkerCallback<'a, D, R>>,
}

// impl     Worker<'a, D, R>
impl<'a, D: Send + Sync, R:Send + Sync> Worker<'a, D, R> {
    pub async fn new<F>(name: &'a str, processor: F, opts: WorkerOptions) -> Worker<'a, D, R>
    where
        F: Fn(D) -> (dyn Future<Output = R> + Send + Sync) + 'static + Send + Sync,
    {
        let emitter = AsyncEventEmitter::new();
        let con_string = to_static_str(opts.clone().connection);
        let redis_opts = RedisOpts::from_conn_str(con_string);
        let prefix = opts.clone().prefix;
        let connection = RedisConnection::init(redis_opts).await.unwrap();
        let scripts = script::Scripts::new(to_static_str(prefix), name, connection.conn);

        let mut  worker = Self {
            name,
            processing: HashSet::new(),
            jobs: HashSet::new(),
            connection: connection.pool,
            options: opts.clone(),
            emitter,
            prefix: opts.clone().prefix,
            timer: None,
            force_closing: false,
            processor: Arc::new(processor),
            running: false,
            closed: false,
            closing: false,
            scripts: Mutex::new(scripts),
        };

        if opts.autorun {
            worker.run().await;
        }
        worker
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        if self.running {
            return Err(anyhow::anyhow!("Worker is already running"));
        }
         
        Ok(())
    }

    async fn run_stalled_jobs(&mut self) -> anyhow::Result<()> {
        let mut scripts = self.scripts.lock().await;
        let mut connection = self.connection.get().await?;

        let mut result = scripts
            .move_stalled_jobs_to_wait(
                self.options.max_stalled_count,
                self.options.stalled_interval,
            )
            .await?;
        if let [Some(failed), Some(stalled)] = [result.get(0), result.get(1)] {
            for job_id in failed {
                self.emitter.emit("failed", job_id.to_string()).await;
            }
            for job_id in stalled {
                self.emitter.emit("stalled", job_id.to_string()).await;
            }

            return Ok(());
        }
        let e = anyhow::anyhow!("Error checking stalled jobs");
        self.emitter.emit("error", e.to_string()).await;
        Err(e)
    }
}
