use crate::emitter::AsyncEventEmitter;
use crate::timer::Timer;
use crate::*;
use anyhow::Ok;
use futures::future::{BoxFuture, Future, FutureExt};
use redis::{FromRedisValue, ToRedisArgs};
use std::collections::HashMap;
use std::collections::HashSet;
use std::time;
pub type WorkerCallback<'a, D, R> =    
dyn Fn(D) -> BoxFuture<'a, anyhow::Result<R>> + Send + Sync + 'static;
use anyhow::{anyhow, Context, Result};
use futures::lock::Mutex;
use std::cell::RefCell;
use std::sync::Arc;
use tokio::task::{self, JoinHandle};

#[derive(Clone)]
struct JobSetPair<'a, D, R>(Job<'a, D, R>, &'a str);

impl<'a, D, R> PartialEq for JobSetPair<'a, D, R> {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}

impl<'a, D, R> Eq for JobSetPair<'a, D, R> {}

impl<'a, D, R> std::hash::Hash for JobSetPair<'a, D, R> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.id.hash(state);
        self.1.hash(state);
    }
}

type ProcessingHandle<R> = Arc<JoinHandle<anyhow::Result<R>>>;
#[derive(Clone)]
struct Worker<'a, D, R> {
    pub name: &'a str,
    pub connection: Pool,
    pub options: WorkerOptions,
    emitter: AsyncEventEmitter,
    pub timer: Option<Timer>,
    pub stalled_check_timer: Option<Timer>,
    pub closing: bool,
    pub closed: bool,
    running: bool,
    scripts: Arc<Mutex<Scripts<'a>>>,
    pub jobs: HashSet<JobSetPair<'a, D, R>>,
    pub processing: Vec<ProcessingHandle<R>>,
    pub prefix: String,
    force_closing: bool,
    pub processor: Arc<WorkerCallback<'static, D, R>>,
    pub queue: &'a Queue<'a>,
}

// impl     Worker<'a, D, R>
impl<
        'a,
        D: Deserialize<'a> + Send + Sync + Clone + 'static + Serialize,
        R: Send
            + Sync
            + Clone
            + ToRedisArgs
            + FromRedisValue
            + Serialize
            + std::fmt::Debug
            + 'static
            + Deserialize<'a>,
    > Worker<'a, D, R>
{
    pub async fn new<F>(
        name: &'a str,
        queue: &'a Queue<'a>,
        processor: F,
        opts: WorkerOptions,
    ) -> Worker<'a, D, R>
    where
        F: Fn(D) -> BoxFuture<'static, anyhow::Result<R>> + Send + Sync + 'static,
    {
        let emitter = AsyncEventEmitter::new();
        let con_string = to_static_str(opts.clone().connection);
        let redis_opts = RedisOpts::from_conn_str(con_string);
        let prefix = opts.clone().prefix;
        let connection = RedisConnection::init(redis_opts).await.unwrap();
        let scripts = script::Scripts::new(to_static_str(prefix), name, connection.pool.clone());

        Self {
            name,
            processing: Vec::new(),
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
            scripts: Arc::new(Mutex::new(scripts)),
            stalled_check_timer: None,
            queue,
        }
    }

    async fn run(&'static mut self) -> anyhow::Result<()> {
        if self.running {
            return Err(anyhow::anyhow!("Worker is already running"));
        }

        let copy = Arc::new(Mutex::new(self.clone()));
        let timer = Timer::new(self.options.lock_duration as u64 / 2, move || {
            let mut worker = copy.clone();
            async move {
                worker.lock().await.extend_locks().await;
            }
            .boxed()
        });

        let cp = Arc::new(Mutex::new(self.clone()));
        let stalled_check_timer = Timer::new(self.options.stalled_interval as u64, move || {
            let mut worker = cp.clone();
            async move {
                worker.lock().await.run_stalled_jobs().await;
            }
            .boxed()
        });

        self.running = true;

        self.timer = Some(timer);
        self.stalled_check_timer = Some(stalled_check_timer);

        let token = uuid::Uuid::new_v4().to_string();
        let stat_token = to_static_str(token);

        while !self.closed {
            if self.jobs.is_empty()
                && self.processing.len() < self.options.concurrency
                && !self.closing
            {
                //self.emitter.emit("drained", String::from("")).await;
                let awaiting_job = self.get_next_job(stat_token).await?;
                if let Some(waited) = awaiting_job {
                    let processor = self.processor.clone();

                    let task = task::spawn(async move {
                        let data = waited.data.clone();
                        processor(data).await
                    });

                    self.processing.push(Arc::new(task));
                }
            }

            if !self.jobs.is_empty() {
                let jobs_to_process = self.jobs.clone().into_iter().map(|e| {
                    let data = e.0.data;
                    let processor = self.processor.clone();

                    let task = task::spawn(async move { processor(data).await });
                    Arc::new(task)
                });

                self.processing.extend(jobs_to_process);
            }
            let (finished, pending) = self.get_completed().await?;
            self.processing = pending;
            if finished.is_empty() && self.processing.is_empty() && self.closing {
                break;
            }
        }
        self.running = false;
        self.timer.as_mut().unwrap().stop();
        self.stalled_check_timer.as_mut().unwrap().stop();

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
        self.emitter
            .emit("error", (e.to_string(), String::from("all")))
            .await;
        Err(e)
    }

    pub async fn extend_locks(&mut self) -> anyhow::Result<()> {
        let jobs = self.jobs.clone();
        let mut scripts = self.scripts.lock().await;
        let mut connection = self.connection.get().await?;
        for JobSetPair(job, token) in jobs {
            scripts
                .extend_lock(&job.id, token, self.options.lock_duration)
                .await?;
        }

        Ok(())
    }
    pub async fn process_job(
        &mut self,
        mut job: Job<'a, D, R>,
        token: &'a str,
    ) -> anyhow::Result<Option<MoveToAciveResult>> {
        self.jobs.insert(JobSetPair(job.clone(), token));

        let callback = self.processor.clone();
        let data = job.data.clone();

        let returned = callback(data).await; //.context("Error processing job ")?;

        match returned {
            (res) => {
                if res.is_ok() {
                    let result = res.ok().unwrap();

                    if !self.force_closing {
                        let mut scripts = self.scripts.lock().await;
                        let mut connection = self.connection.get().await?;
                        let opts = self.options.clone();
                        let remove_on_complete = job.opts.remove_on_complete.bool;
                        let fetch = !self.closing;
                        let end = scripts
                            .move_to_completed(
                                &mut job,
                                result.clone(),
                                remove_on_complete,
                                token,
                                &self.options,
                                fetch,
                            )
                            .await
                            .context("Error completing job")?;
                        let done = result.clone();
                        let name = job.name;
                        let id = job.id.clone();

                        self.emitter.emit("completed", (name, id, done)).await;
                        self.jobs.remove(&JobSetPair(job.clone(), token));

                        return Ok(end);
                    }
                    Ok(None)
                } else {
                    let e = res.err().unwrap();
                    self.emitter.emit("error", e.to_string()).await;
                    self.jobs.remove(&JobSetPair(job.clone(), token));

                    Err(anyhow!("Error processing job"))
                }
            }
            Err(e) => {
                self.emitter
                    .emit("error", (e.to_string(), job.name, job.id.clone()))
                    .await;

                if !self.force_closing {
                    println!("Error processing job: {}", e);
                    job.move_to_failed(e.to_string(), token, false).await?;
                    let name = job.name;
                    let id = job.id.clone();
                    self.emitter.emit("failed", (name, id, e.to_string())).await;
                }
                self.jobs.remove(&JobSetPair(job.clone(), token));
                Err(anyhow!("Error processing job"))
            }
        }
    }

    pub async fn get_next_job(&mut self, token: &'a str) -> anyhow::Result<Option<Job<'a, D, R>>> {
        let mut scripts = self.scripts.lock().await;
        let options = self.options.clone();
        let (mut job, mut job_id, mut limit_until, mut delay_until) =
            scripts.move_to_active(token, options.clone()).await?;

        // if there are no jobs in the waiting list, we keep waiting with BBPOPLPUSH;

        if job.is_none() {
            let current_time = generate_timestamp()?;
            let delay = if let Some(num) = delay_until {
                num
            } else {
                5000
            };

            let timeout = std::cmp::min(delay, 5000) / 1000;

            let mut connection = self.connection.get().await?;
            let mut scripts = self.scripts.lock().await;
            let wait_key = scripts.keys.get("wait").unwrap();
            let active_key = scripts.keys.get("active").unwrap();
            job_id = connection
                .brpoplpush(wait_key, active_key, timeout as usize)
                .await?;

            if let Some(id) = job_id {
                (job, job_id, limit_until, delay_until) =
                    scripts.move_to_active(token, options).await?;
            }
        }

        if let (Some(j), Some(id)) = (job, job_id) {
            let json_string = serde_json::to_string(&j).unwrap();
            let static_id = to_static_str(id);

            let result = Job::<D, R>::from_json(self.queue, json_string, static_id).await?;
            return Ok(Some(result));
        }

        Ok(None)
    }

    pub async fn get_completed(
        &self,
    ) -> anyhow::Result<(Vec<Result<R>>, Vec<ProcessingHandle<R>>)> {
        let (finished, pending): (Vec<_>, Vec<_>) = self
            .processing
            .clone()
            .into_iter()
            .partition(|e| e.is_finished());

        let mut completed = Vec::new();
        for job in finished {
            let result = Arc::into_inner(job);
            if let Some(handle) = result {
                let res = handle.await?;
                completed.push(res);
            }
        }

        Ok((completed, pending))
    }

    pub fn cancel_processing(&mut self) {
        for job in self.processing.clone() {
            if !job.is_finished() {
                job.abort();
            }
        }
    }

    pub fn close(&mut self, force: bool) {
        if force {
            self.force_closing = true;
            self.cancel_processing();
        }
        self.closing = true;
        self.connection.close();
    }
}
