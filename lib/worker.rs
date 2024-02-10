use crate::timer::Timer;
use crate::*;
use anyhow::Ok;
use async_event_emitter::AsyncEventEmitter;

use futures::future::{BoxFuture, Future, FutureExt};

use futures::StreamExt;
use redis::Cmd;
use redis::{FromRedisValue, ToRedisArgs};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::format;
use std::time;
use std::{any, cmp};
use uuid::Uuid;

use anyhow::{anyhow, Context, Result};
use futures::lock::Mutex;
use futures::stream::FuturesUnordered;
use std::cell::RefCell;
use std::sync::Arc;
use tokio::task::{self, JoinHandle};
#[derive(Clone)]
pub struct JobSetPair<D, R>(pub Job<D, R>, pub &'static str);

impl<D, R> PartialEq for JobSetPair<D, R> {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}

impl<D, R> Eq for JobSetPair<D, R> {}

impl<D, R> std::hash::Hash for JobSetPair<D, R> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.id.hash(state);
        self.1.hash(state);
    }
}

/// types
type WorkerCallback<'a, D, R> =
    dyn Fn(JobSetPair<D, R>) -> BoxFuture<'a, anyhow::Result<R>> + Send + Sync + 'static;

type ProcessingHandle<R> = JoinHandle<anyhow::Result<R>>;

pub struct Worker<D, R> {
    pub id: String,
    pub name: String,
    pub connection: Arc<Pool>,
    pub options: Arc<WorkerOptions>,
    emitter: Arc<Mutex<AsyncEventEmitter>>,
    pub timer: Option<Timer>,
    pub stalled_check_timer: Option<Timer>,
    pub closing: bool,
    pub closed: bool,
    running: bool,
    scripts: Arc<Mutex<Scripts>>,
    pub jobs: Arc<Mutex<HashSet<JobSetPair<D, R>>>>,
    pub processing: FuturesUnordered<ProcessingHandle<Option<Job<D, R>>>>,
    pub prefix: String,
    force_closing: bool,
    pub processor: Arc<WorkerCallback<'static, D, R>>,
    pub queue: Arc<Queue>,
    block_until: Arc<Mutex<u64>>,
    waiting: Arc<Mutex<Option<String>>>,
    drained: Arc<Mutex<bool>>,
    pub tasks_completed: Arc<Mutex<u64>>,
}
impl<
        D: Deserialize<'static> + Send + Sync + Clone + 'static + Serialize + std::fmt::Debug,
        R: Send
            + Sync
            + Clone
            + ToRedisArgs
            + FromRedisValue
            + Serialize
            + std::fmt::Debug
            + 'static
            + Deserialize<'static>,
    > Worker<D, R>
{
    pub async fn init<F, C>(
        name: &str,
        queue: &Queue,
        processor: C,
        mut opts: WorkerOptions,
        _emitter: Option<&AsyncEventEmitter>,
    ) -> Result<Worker<D, R>>
    where
        C: Fn(JobSetPair<D, R>) -> F + Send + Sync + 'static,
        F: Future<Output = anyhow::Result<R>> + Send + Sync + 'static,
    {
        let emitter = if let Some(value) = _emitter {
            value.clone()
        } else {
            AsyncEventEmitter::default()
        };
        let prefix = queue.prefix.to_owned();
        let queue_name = &queue.name;
        let connection = queue.manager.clone();
        let scripts = script::Scripts::new(prefix, queue_name.to_owned(), connection.pool.clone());
        let callback = move |data: JobSetPair<D, R>| processor(data).boxed();
        let queue_copy = queue.clone();

        let mut worker = Self {
            id: Uuid::new_v4().to_string(),
            name: name.to_owned(),
            processing: FuturesUnordered::new(),
            jobs: Arc::new(Mutex::default()),
            connection: Arc::new(connection.pool.clone()),
            options: Arc::new(opts.clone()),
            emitter: Arc::new(Mutex::new(emitter.clone())),
            prefix: opts.clone().prefix,
            timer: None,
            force_closing: false,
            processor: Arc::new(callback),
            running: false,
            closed: false,
            closing: false,
            scripts: Arc::new(Mutex::new(scripts)),
            stalled_check_timer: None,
            queue: Arc::new(queue_copy),
            block_until: Arc::new(Mutex::default()),
            waiting: Arc::new(Mutex::default()),
            drained: Arc::new(Mutex::new(false)),
            tasks_completed: Arc::new(Mutex::default()),
        };
        // running worker could fail
        if (worker.options.autorun) {
            worker.run().await?;
        }
        Ok(worker)
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        if self.running {
            return Err(anyhow::anyhow!("Worker is already running"));
        }
        let queue_name = self.queue.name.clone();
        let prefix = self.queue.prefix.to_owned();
        let options = self.options.clone();
        let jobs = self.jobs.clone();
        let copy_pool = self.queue.manager.pool.clone();
        let timer = Timer::new((self.options.lock_duration as u64 / 2000), move || {
            let jobs = jobs.clone();
            let options = options.clone();

            let pool = copy_pool.clone();
            let queue_name = queue_name.clone();
            let prefix = prefix.clone();
            async move {
                extend_locks(jobs, options, pool, queue_name, prefix).await;
            }
        });

        let emitter = self.emitter.clone();
        let queue_name = self.queue.name.clone();
        let prefix = self.queue.prefix.to_string();
        let options = self.options.clone();

        let copy_pool = self.queue.manager.pool.clone();
        let mut stalled_check_timer = Timer::new(self.options.stalled_interval as u64, move || {
            let emitter = emitter.clone();
            let options = options.clone();
            let pool = copy_pool.clone();
            let queue_name = queue_name.clone();
            let prefix = prefix.clone();
            async move {
                run_stalled_jobs(queue_name, prefix, pool, emitter, options).await;
                println!("move stalled_");
            }
        });
        self.running = true;
        self.timer = Some(timer);
        self.stalled_check_timer = Some(stalled_check_timer);
        let mut token_prefix = 0;

        while !self.closed {
            while self.waiting.lock().await.is_none()
                && self.processing.len() < self.options.concurrency
                && !self.closing
            {
                token_prefix += 1;
                let stat_token = to_static_str(format!("{}:{}", self.id, token_prefix));
                //self.emitter.emit("drained", String::from("")).await;
                let waiting = self.waiting.clone();
                let drained = self.drained.clone();
                let block_until = self.block_until.clone();
                let queue = self.queue.clone();
                let scripts = self.scripts.clone();
                let opts = self.options.clone();
                let awaiting_job = get_next_job(
                    waiting,
                    drained,
                    block_until,
                    queue,
                    scripts,
                    opts,
                    stat_token,
                );

                let task = tokio::spawn(awaiting_job);
                self.processing.push(task);
            }

            let mut tasks =
                get_completed(&mut self.processing, self.tasks_completed.clone()).await?;

            while let Some(job) = tasks.pop() {
                let token = job.token;
                let args: PackedProcessArgs<D, R> = (
                    self.jobs.clone(),
                    self.emitter.clone(),
                    self.processor.clone(),
                    self.options.clone(),
                    self.queue.clone(),
                    self.force_closing,
                    self.closing,
                );
                let next_job = tokio::spawn(process_job(args, job.clone(), token));

                self.processing.push(next_job)

                // progress event task;
            }

            //self.processing.extend(jobs_to_process);
            let count = &self.tasks_completed;
            println!("jobs completed: {:#?}", *count);

            if self.processing.is_empty() {
                break;
            }
        }
        self.running = false;
        self.timer.as_mut().unwrap().stop();
        self.stalled_check_timer.as_mut().unwrap().stop();
        Ok(())
    }

    pub fn cancel_processing(&mut self) {
        for job in self.processing.iter() {
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
        dbg!(self.closing, self.force_closing);
    }
    pub async fn on<F, T, C>(&mut self, event: &str, callback: C) -> String
    where
        for<'de> T: Deserialize<'de> + std::fmt::Debug,
        C: Fn(T) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        let mut emitter = self.emitter.lock().await;
        emitter.on(event, callback)
    }
    pub async fn once<F, T, C>(&mut self, event: &str, callback: C) -> String
    where
        for<'de> T: Deserialize<'de> + std::fmt::Debug,
        C: Fn(T) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        let mut emitter = self.emitter.lock().await;
        emitter.once(event, callback)
    }
}

// ---------------------------------------------- UTILITY FUNCTIONS FOR WORKER ---------------------------------------------------------------
// These functions are to be passed to Tokio::task, so the need their own static parameters
async fn run_stalled_jobs(
    queue_name: String,
    prefix: String,
    pool: Pool,
    emit: Arc<Mutex<AsyncEventEmitter>>,
    options: Arc<WorkerOptions>,
) -> anyhow::Result<()> {
    let con_string = to_static_str(options.connection.clone());
    let mut emitter = emit.lock().await;
    let mut scripts = Scripts::new(prefix, queue_name, pool);

    let mut result = scripts
        .move_stalled_jobs_to_wait(options.max_stalled_count, options.stalled_interval)
        .await?;
    if let [Some(failed), Some(stalled)] = [result.first(), result.get(1)] {
        for job_id in failed {
            emitter.emit("failed", job_id.to_string()).await?;
        }
        for job_id in stalled {
            emitter.emit("stalled", job_id.to_string()).await?;
        }

        return Ok(());
    }
    let e = anyhow::anyhow!("Error checking stalled jobs");
    emitter
        .emit("error", (e.to_string(), String::from("all")))
        .await;
    Err(e)
}
async fn extend_locks<
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
>(
    job_map: Arc<Mutex<HashSet<JobSetPair<D, R>>>>,
    options: Arc<WorkerOptions>,
    pool: Pool,
    queue_name: String,
    prefix: String,
) -> anyhow::Result<()> {
    let mut jobs = job_map.lock().await;
    let mut scripts = Scripts::new(prefix, queue_name, pool);
    for JobSetPair(job, token) in jobs.iter() {
        scripts
            .extend_lock(&job.id, token, options.lock_duration)
            .await?;
    }

    Ok(())
}

pub fn map_from_string(input_string: &str) -> HashMap<String, String> {
    let mut map = HashMap::with_capacity(input_string.len() / 2);
    let slice: Vec<&str> = input_string.split(',').collect();
    let mut chunks = slice.chunks(2);
    while let Some([key, value]) = chunks.next() {
        map.insert(key.to_string(), value.to_string());
    }

    map
}

pub fn map_from_vec(value: &[String]) -> HashMap<String, String> {
    let mut map = HashMap::with_capacity(value.len() / 2);

    let mut chunks = value.chunks(2);
    while let Some([key, value]) = chunks.next() {
        map.insert(key.to_string(), value.to_string());
    }

    map
}

pub async fn wait_for_job(
    scripts: Arc<Mutex<script::Scripts>>,
    block_until: Arc<Mutex<u64>>,
) -> anyhow::Result<Option<String>> {
    use std::time::Duration;
    let scripts = scripts.lock().await;
    let mut con = scripts.connection.get().await?;
    let block_until = block_until.lock().await;

    let now = generate_timestamp()?;
    let timeout = if let Some(block_until) = Some(block_until) {
        cmp::min(block_until.saturating_div(now) as u64, 5000)
    } else {
        5000
    };
    let timeout = Duration::from_millis(timeout);

    let redis_version = scripts.redis_version.clone();

    let timeout = if *redis_version > *"6.0.0" {
        (timeout.as_secs() + if timeout.subsec_millis() > 0 { 1 } else { 0 }) as f64
    } else {
        timeout.as_secs_f64()
    };
    let keys = scripts.get_keys(&["wait", "active"]);

    let srckey = &keys[0];
    let dstkey = &keys[1];

    let job_id: Option<String> = Cmd::brpoplpush(srckey, dstkey, timeout as usize)
        .query_async(&mut con)
        .await?;
    Ok(job_id)
}

type PackedArgs = (
    Arc<Mutex<bool>>,    // drained
    Arc<Mutex<u64>>,     // block_util
    Arc<Queue>,          // queue
    Option<Vec<String>>, // job_data
    Option<String>,      // job_id
    i64,                 // limit_until
    Option<i64>,
); // delay_util
async fn next_job_from_job_data<
    'a,
    D: Deserialize<'a> + Send + Sync + Clone + 'static + Serialize + std::fmt::Debug,
    R: Send
        + Sync
        + Clone
        + ToRedisArgs
        + FromRedisValue
        + Serialize
        + std::fmt::Debug
        + 'static
        + Deserialize<'a>,
>(
    packed_args: PackedArgs,
    token: Option<String>,
) -> Result<Option<Job<D, R>>> {
    let (drained, block_until, queue, job_data, job_id, limit_until, delay_until): PackedArgs =
        packed_args;
    let mut drained = drained.lock().await;
    let mut block_until = block_until.lock().await;
    if job_data.is_none() && !*drained {
        *drained = true;
        *block_until = 0;
    }

    if let Some(delay) = delay_until {
        *block_until = cmp::max(delay, 0) as u64;
    }
    if let (Some(mut data), Some(id), Some(token_str)) = (job_data, job_id, token) {
        *drained = false;
        let map = map_from_vec(&data);
        let json = serde_json::to_string(&map)?;

        // let static_id = to_static_str(id);
        let mut job: Job<D, R> = Job::from_id(&queue, &id).await?.unwrap();

        job.token = to_static_str(token_str);

        return Ok(Some(job));
    }
    Ok(None)
}

async fn move_to_active<
    'a,
    D: Deserialize<'a> + Send + Sync + Clone + 'static + Serialize + std::fmt::Debug,
    R: Send
        + Sync
        + Clone
        + ToRedisArgs
        + FromRedisValue
        + Serialize
        + std::fmt::Debug
        + 'static
        + Deserialize<'a>,
>(
    drained: Arc<Mutex<bool>>,
    block_until: Arc<Mutex<u64>>,
    queue: Arc<Queue>,
    scripts: Arc<Mutex<Scripts>>,
    options: Arc<WorkerOptions>,
    token: &str,
    job_id: Option<String>,
) -> Result<Option<Job<D, R>>> {
    let script_copy = scripts.clone();
    let mut script = script_copy.lock().await;
    let mut connection = script.connection.get().await?;
    if let Some(id) = job_id.clone() {
        if id.starts_with("0:") {
            let time: u64 = id.split(':').next().unwrap().parse()?;
            *block_until.lock().await = time;
        }
    }
    let result = script
        .move_to_active(token, &options, job_id.clone())
        .await?;

    let (job_data, id, limit_until, delay_until) = result;
    let opt_token = Some(token.to_owned());

    next_job_from_job_data(
        (
            drained,
            block_until,
            queue,
            job_data,
            job_id,
            limit_until,
            delay_until,
        ),
        opt_token,
    )
    .await
}

pub async fn get_next_job<
    'a,
    D: Deserialize<'a> + Send + Sync + Clone + 'static + Serialize + std::fmt::Debug,
    R: Send
        + Sync
        + Clone
        + ToRedisArgs
        + FromRedisValue
        + Serialize
        + std::fmt::Debug
        + 'static
        + Deserialize<'a>,
>(
    waiting: Arc<Mutex<Option<String>>>,
    drained: Arc<Mutex<bool>>,
    block_until: Arc<Mutex<u64>>,
    queue: Arc<Queue>,
    scripts: Arc<Mutex<Scripts>>,
    options: Arc<WorkerOptions>,
    token: &str,
) -> anyhow::Result<Option<Job<D, R>>> {
    let options = options.clone();
    let mut waiting = waiting.lock().await;

    if waiting.is_none() {
        let result = wait_for_job(scripts.clone(), block_until.clone()).await?;
        let copy = result.clone();
        *waiting = copy;
        let moved =
            move_to_active(drained, block_until, queue, scripts, options, token, result).await?;
        *waiting = None;
        return Ok(moved);
    }
    move_to_active(drained, block_until, queue, scripts, options, token, None).await
}

pub async fn get_completed<
    'a,
    D: Deserialize<'a> + Send + Sync + Clone + 'static + Serialize + std::fmt::Debug,
    R: Send
        + Sync
        + Clone
        + ToRedisArgs
        + FromRedisValue
        + Serialize
        + std::fmt::Debug
        + 'static
        + Deserialize<'a>,
>(
    jobs: &mut FuturesUnordered<ProcessingHandle<Option<Job<D, R>>>>,
    task_count: Arc<Mutex<u64>>,
) -> anyhow::Result<Vec<Job<D, R>>> {
    let mut completed = Vec::new();
    if let Some(handle) = jobs.next().await {
        let result = handle.unwrap()?;
        if let Some(job) = result {
            if !job.is_completed() {
                completed.push(job.clone());
            } else {
                *task_count.lock().await += 1;
            }
        }
    }

    Ok(completed)
}

type PackedProcessArgs<D, R> = (
    Arc<Mutex<HashSet<JobSetPair<D, R>>>>, // jobs
    Arc<Mutex<AsyncEventEmitter>>,         //emitter
    Arc<WorkerCallback<'static, D, R>>,    // processor
    Arc<WorkerOptions>,                    // workerOptions
    Arc<Queue>,                            // queue
    bool,                                  //force_closing,
    bool,                                  // closing,
);

/// Process each  job in a separate task
pub async fn process_job<
    'a,
    D: Deserialize<'a> + Send + Sync + Clone + 'static + Serialize + std::fmt::Debug,
    R: Send
        + Sync
        + Clone
        + ToRedisArgs
        + FromRedisValue
        + Serialize
        + std::fmt::Debug
        + 'static
        + Deserialize<'a>,
>(
    args: PackedProcessArgs<D, R>,

    mut job: Job<D, R>,
    token: &'static str,
) -> anyhow::Result<Option<Job<D, R>>> {
    let (jobs, emitter, processor, options, queue, force_closing, closing) = args;

    let mut jobs = jobs.lock().await;
    let mut emitter = emitter.lock().await;
    let callback = processor.clone();
    let data = JobSetPair(job.clone(), token);

    let returned = callback(data).await; //.context("Error processing job ")?;

    match returned {
        (res) => {
            if res.is_ok() {
                let result = res.ok().unwrap();

                if !force_closing {
                    let queue_name = queue.name.clone();
                    let prefix = queue.prefix;
                    let pool = queue.manager.pool.clone();
                    let mut scripts = Scripts::new(prefix.to_string(), queue_name, pool);

                    let remove_on_complete = job.opts.remove_on_complete.bool;

                    let fetch = !closing;
                    let end = scripts
                        .move_to_completed(
                            &mut job,
                            serde_json::to_string(&result)?,
                            remove_on_complete,
                            token,
                            &options,
                            fetch,
                        )
                        .await
                        .context("Error completing job")?;
                    let done = result.clone();
                    let name = job.name;
                    let id = job.id.clone();

                    emitter.emit("completed", (name, id, done)).await?;
                    jobs.remove(&JobSetPair(job.clone(), token));
                    let finished_job = end.0.clone();
                    if let Some(mut data) = finished_job {
                        let static_id = to_static_str(job.id.clone());
                        let job: Job<D, R> =
                            Job::from_raw_job(&mut data, &queue, static_id).await?;
                        return Ok(Some(job));
                    }
                }
                Ok(None)
            } else {
                let e = res.err().unwrap();
                emitter.emit("error", e.to_string()).await;
                jobs.remove(&JobSetPair(job.clone(), token));

                Err(anyhow!("Error processing job"))
            }
        }
        Err(e) => {
            emitter
                .emit("error", (e.to_string(), job.name, job.id.clone()))
                .await;

            if !force_closing {
                println!("Error processing job: {}", e);
                job.move_to_failed(e.to_string(), token, false).await?;
                let name = job.name;
                let id = job.id.clone();
                emitter.emit("failed", (name, id, e.to_string())).await;
            }
            jobs.remove(&JobSetPair(job.clone(), token));
            Err(anyhow!("Error processing job"))
        }
    }
}

//the main loop;

/*
async fn mainLoop () {

    let mut token_prefix = 0;

      while !self.closed {
            while self.waiting.lock().await.is_none()
                && self.processing.len() < self.options.concurrency
                && !self.closing
            {
                token_prefix += 1;
                let stat_token = to_static_str(format!("{}:{}", self.id, token_prefix));
                //self.emitter.emit("drained", String::from("")).await;
                let waiting = self.waiting.clone();
                let drained = self.drained.clone();
                let block_until = self.block_until.clone();
                let queue = self.queue.clone();
                let scripts = self.scripts.clone();
                let opts = self.options.clone();
                let awaiting_job = get_next_job(
                    waiting,
                    drained,
                    block_until,
                    queue,
                    scripts,
                    opts,
                    stat_token,
                );

                let task = tokio::spawn(awaiting_job);
                self.processing.push(task);
            }

            let mut tasks =
                get_completed(&mut self.processing, self.tasks_completed.clone()).await?;

            while let Some(job) = tasks.pop() {
                let token = job.token;
                let args: PackedProcessArgs<D, R> = (
                    self.jobs.clone(),
                    self.emitter.clone(),
                    self.processor.clone(),
                    self.options.clone(),
                    self.queue.clone(),
                    self.force_closing,
                    self.closing,
                );
                let next_job = tokio::spawn(process_job(args, job.clone(), token));

                self.processing.push(next_job)

                // progress event task;
            }

            //self.processing.extend(jobs_to_process);
            let count = &self.tasks_completed;
            println!("jobs completed: {:#?}", *count);

            if self.processing.is_empty() {
                break;
            }
        }

}

*/
