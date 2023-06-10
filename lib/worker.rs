use crate::emitter::AsyncEventEmitter;
use crate::timer::Timer;
use crate::*;
use futures::future::{BoxFuture, Future, FutureExt, ok};
use std::collections::HashSet;
pub type WorkerCallback<'a, D, R> = dyn Fn(D) -> (dyn Future<Output = R> + Send + Sync);
use std::sync::Arc;
use std::cell::RefCell;
struct Worker<'a, D, R> {
    pub name: &'a str,
    pub connection: Pool,
    pub options: WorkerOptions,
    pub emitter: AsyncEventEmitter,
    pub timer: Option<Timer>,
    pub closing: bool,
    pub closed: bool,
    running: bool,
     scripts: RefCell<script::Scripts<'a>>,
    pub jobs: HashSet<Job<'a, D, R>>,
    pub processing: HashSet<Job<'a, D, R>>,
    pub prefix: String,
    force_closing: bool,
    processor: Arc<WorkerCallback<'a, D, R>>,
}

// impl     Worker<'a, D, R>
impl<'a, D, R> Worker<'a, D, R> {
    pub async fn new<F>(name: &'a str, processor: F, opts: WorkerOptions) -> Worker<'a, D, R>
    where
        F: Fn(D) -> (dyn Future<Output = R> + Send + Sync) + 'static,
    {
        let emitter = AsyncEventEmitter::new();
        let con_string = to_static_str(opts.clone().connection);
        let redis_opts = RedisOpts::from_conn_str(con_string);
        let prefix = opts.clone().prefix;
        let connection = RedisConnection::init(redis_opts).await.unwrap();
        let scripts = script::Scripts::new( to_static_str(prefix), name, connection.conn);
         
         let worker = Self {
            name,
            processing: HashSet::new(),
            jobs: HashSet::new(),
            connection: connection.pool,
            options: opts.clone(),
            emitter,
            prefix:opts.clone().prefix,
            timer: None,
            force_closing: false,
            processor: Arc::new(processor),
            running: false,
            closed: false,
            closing: false,
            scripts: RefCell::new(scripts),
        };

          if opts.autorun {
            worker.run().await;
          }
        worker
    }

     async fn run (&self)  -> anyhow::Result<()> {
         if self.running {
             return Err(anyhow::anyhow!("Worker is already running"))
         }

         
         Ok(())
     }

      async fn run_stalled_jobs(&self) -> anyhow::Result<()> {
         
          Ok(())
      }
}
