use async_atomic::Atomic;
use emitter::TypedEmitter;
use enums::BullError;
use enums::WorkerError;
use futures::future::{BoxFuture, Future, FutureExt};
use futures::lock::Mutex;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::StreamExt;
use futures::TryFutureExt;
use redis::AsyncCommands;
use redis::Cmd;
use redis::{FromRedisValue, ToRedisArgs};
use std::cmp::{self, min};
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::fmt::Debug;
use std::future::IntoFuture;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::sync::RwLock;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::backtrace_utils::*;
use crate::timer::Timer;
use crate::*;

#[derive(Clone, Debug)]
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

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum Events {
    Closed,
    Completed,
    Drained,
    Error,
    Failed,
    Stalled,
}

type AsyncEventEmitter<D, R> = TypedEmitter<Events, CallBackParams<D, R>, ()>;
#[derive(Clone)]
pub enum CallBackParams<D, R> {
    /// (job, id reason)
    Completed(Job<D, R>, R),
    // (job.id, reason)
    Failed(Option<Job<D, R>>, String),
    Error(Option<Job<D, R>>, String),
    Stalled(String),
}
/// types
pub(crate) type WorkerCallback<'a, D, R> =
    dyn Fn(JobSetPair<D, R>) -> BoxFuture<'a, Result<R, BullError>> + Send + Sync + 'static;

type ProcessingHandles<R> = Mutex<FuturesOrdered<JoinHandle<R>>>;

pub struct Worker<D, R> {
    pub id: String,
    pub name: String,
    pub connection: Arc<Pool>,
    pub options: Arc<WorkerOptions>,
    emitter: Arc<AsyncEventEmitter<D, R>>,
    extend_lock_timer: Arc<Timer>,
    pub stalled_check_timer: Arc<Timer>,
    pub closing: &'static Atomic<bool>,
    pub closed: &'static Atomic<bool>,
    running: &'static Atomic<bool>,
    scripts: Arc<Scripts>,
    pub jobs: Arc<RwLock<HashSet<JobSetPair<D, R>>>>,
    pub processing: Arc<ProcessingHandles<Option<Job<D, R>>>>,
    pub prefix: String,
    force_closing: &'static Atomic<bool>,
    pub processor: Arc<WorkerCallback<'static, D, R>>,
    pub queue: Arc<Queue>,
    block_until: &'static Atomic<u64>,
    waiting: Arc<Mutex<Option<i32>>>,
    drained: &'static Atomic<bool>,
    pub main_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub tasks_completed: &'static Atomic<u64>,
}
impl<D, R> Worker<D, R>
where
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
{
    pub async fn build<F, C>(
        name: &str,
        queue: &Queue,
        processor: C,
        mut opts: WorkerOptions,
    ) -> Result<Arc<Worker<D, R>>, BullError>
    where
        C: Fn(JobSetPair<D, R>) -> F + Send + Sync + 'static,
        F: Future<Output = Result<R, Box<dyn Error + Send + Sync>>> + Send + Sync + 'static,
    {
        let prefix = queue.prefix.to_owned();
        let queue_name = &queue.name;
        let connection = queue.manager.clone();
        let scripts = script::Scripts::new(prefix, queue_name.to_owned(), connection.pool.clone());
        let callback = move |data: JobSetPair<D, R>| {
            let fut = async_backtrace::frame!(processor(data));
            fut.map_err(|err| err.into()).boxed()
        };
        let queue_copy = queue.clone();
        static FORCE_CLOSING: Atomic<bool> = Atomic::new(false);
        static CLOSING: Atomic<bool> = Atomic::new(false);
        static CLOSED: Atomic<bool> = Atomic::new(false);
        static DRAINED: Atomic<bool> = Atomic::new(false);
        static TASKS_COMPLETED: Atomic<u64> = Atomic::new(0);
        static BLOCK_UNTIL: Atomic<u64> = Atomic::new(0);
        static RUNNING: Atomic<bool> = Atomic::new(false);
        let emitter: Arc<AsyncEventEmitter<D, R>> = Arc::new(AsyncEventEmitter::new());
        let options = Arc::new(opts.clone());
        let jobs = Arc::new(RwLock::new(HashSet::default()));
        let emitter_clone = emitter.clone();
        let queue_name = queue.name.clone();
        let prefix = queue.prefix.to_string();
        let options_clone = options.clone();
        let installed_interval = options.stalled_interval;

        let copy_pool = queue.manager.pool.clone();
        let mut stalled_check_timer = Timer::new(installed_interval as u64 / 1000, move || {
            let emitter = emitter_clone.clone();
            let options = options_clone.clone();
            let pool = copy_pool.clone();
            let queue_name = queue_name.clone();
            let prefix = prefix.clone();
            async move {
                run_stalled_jobs(queue_name, prefix, pool, emitter, options).await;
                println!("move stalled_");
            }
        });
        let jobs_clone = jobs.clone();
        let copy_pool = queue.manager.pool.clone();
        let options_clone = options.clone();
        let queue_name = queue.name.clone();
        let lock_duration = options.lock_duration;
        let queue_name = queue.name.clone();
        let prefix = queue.prefix.to_string();

        let extend_lock_timer = Timer::new((lock_duration as u64 / 2000), move || {
            let jobs = jobs_clone.clone();
            let options = options_clone.clone();

            let pool = copy_pool.clone();
            let queue_name = queue_name.clone();
            let prefix = prefix.clone();
            async move {
                extend_locks(jobs, options, pool, queue_name, prefix).await;
                println!("extend_lock");
            }
        });

        let mut worker = Arc::new(Self {
            id: Uuid::new_v4().to_string(),
            name: name.to_owned(),
            processing: Arc::new(Mutex::default()),
            jobs,

            connection: Arc::new(connection.pool.clone()),
            options,
            emitter,
            prefix: opts.clone().prefix,
            extend_lock_timer: Arc::new(extend_lock_timer),
            force_closing: FORCE_CLOSING.as_ref(),
            processor: Arc::new(callback),
            running: &RUNNING,
            closed: &CLOSED,
            closing: &CLOSING,
            scripts: Arc::new(scripts),
            stalled_check_timer: Arc::new(stalled_check_timer),
            queue: Arc::new(queue_copy),
            block_until: &BLOCK_UNTIL,
            waiting: Arc::new(Mutex::default()),
            drained: &DRAINED,
            tasks_completed: &TASKS_COMPLETED,
            main_task: Arc::default(),
        });

        let worker_clone = worker.clone();
        // running worker could fail
        if worker.options.autorun {
            worker_clone.run().await?;
        }
        Ok(worker)
    }
    pub async fn run(&self) -> Result<(), BullError> {
        if self.running.as_ref().load() {
            return Err(WorkerError::WorkerAlreadyRunningWithId(self.id.clone()).into());
        }
        let packed_args = (
            self.id.clone(),
            self.closed,
            self.waiting.clone(),
            self.options.clone(),
            self.closing,
            self.force_closing,
            self.drained,
            self.block_until,
            self.queue.clone(),
            self.tasks_completed,
            self.emitter.clone(),
            self.jobs.clone(),
            self.processor.clone(),
        );

        let (stalled_timer, extend_timer) = self.start_timers().await;

        let main_loop_task = tokio::spawn(async_backtrace::frame!(main_loop(
            packed_args,
            self.processing.clone()
        )));

        let main_task = tokio::spawn(async move {
            tokio::select! {
                    res=  stalled_timer => {
                   },
                    res = extend_timer =>  {
                   }
                  res =  main_loop_task => {
                 }
            };
        });
        let current_task = self.main_task.clone();
        let mut current_task = current_task.lock().await;
        *current_task = Some(main_task);
        self.running.store(true);

        Ok(())
    }

    pub async fn close(&self, force: bool) {
        if force {
            self.force_closing.as_ref().swap(true);
        }
        self.cancel_timers();
        self.closing.as_ref().swap(true);
        self.closed.swap(true);
    }
    async fn start_timers(&self) -> (JoinHandle<()>, JoinHandle<()>) {
        let stalled = self.stalled_check_timer.run();
        let extend_timers = self.extend_lock_timer.run();
        (stalled, extend_timers)
    }

    fn cancel_timers(&self) {
        self.stalled_check_timer.stop();
        self.extend_lock_timer.stop();
    }
    pub async fn on<F, C>(&self, event: Events, callback: C) -> String
    where
        C: Fn(CallBackParams<D, R>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        let emitter = self.emitter.clone();

        emitter.on(event, callback)
    }
    pub async fn once<F, C>(&self, event: Events, callback: C) -> String
    where
        C: Fn(CallBackParams<D, R>) -> F + Send + Sync + 'static,
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        let emitter = self.emitter.clone();
        emitter.once(event, callback)
    }
}

// ---------------------------------------------- UTILITY FUNCTIONS FOR WORKER ---------------------------------------------------------------
// These functions are to be passed to Tokio::task, so the need their own static parameters
async fn run_stalled_jobs<D, R>(
    queue_name: String,
    prefix: String,
    pool: Pool,
    emitter: Arc<AsyncEventEmitter<D, R>>,
    options: Arc<WorkerOptions>,
) -> Result<(), BullError>
where
    D: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
{
    let con_string = to_static_str(options.connection.clone());

    let mut scripts = Scripts::new(prefix, queue_name, pool);

    let mut result = scripts
        .move_stalled_jobs_to_wait(options.max_stalled_count, options.stalled_interval)
        .await?;
    if let Some((failed, stalled)) = Some(result) {
        for job_id in failed {
            emitter
                .emit(
                    Events::Failed,
                    CallBackParams::Failed(None, job_id.to_string()),
                )
                .await;
        }
        for job_id in stalled {
            emitter
                .emit(Events::Stalled, CallBackParams::Stalled(job_id.to_string()))
                .await
        }

        return Ok(());
    }
    let e: BullError = WorkerError::FailedToCheckStalledJobs.into();
    emitter
        .emit(Events::Error, CallBackParams::Error(None, e.to_string()))
        .await;

    Err(e)
}
async fn extend_locks<
    'a,
    D: Deserialize<'a> + Send + Sync + Clone + 'static + Serialize + Debug,
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
    jobs: Arc<RwLock<HashSet<JobSetPair<D, R>>>>,
    options: Arc<WorkerOptions>,
    pool: Pool,
    queue_name: String,
    prefix: String,
) -> Result<(), BullError> {
    let mut scripts = Scripts::new(prefix, queue_name, pool);
    for JobSetPair(job, token) in jobs.read().await.iter() {
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
    queue: Arc<Queue>,
    block_until: &'static Atomic<u64>,
) -> Result<i32, BullError> {
    use std::time::Duration;
    let mut con = queue.manager.pool.get().await?;
    let pool = queue.manager.pool.clone();
    let queue_name = queue.name.to_owned();
    let prefix = queue.prefix.to_owned();
    let mut scripts = Scripts::new(prefix, queue_name, pool);

    let current_time = generate_timestamp()?;

    let redis_version = scripts.redis_version.clone();
    let timeout_duration = block_until.load().saturating_sub(current_time);
    let timeout = min(timeout_duration, 5000) as f64 / 1000.0;

    let timeout = if timeout <= 0.0 { 0.00001 } else { timeout };

    let timeout = if *redis_version > *"6.0.0" {
        (timeout.ceil() as u64) as f64
    } else {
        timeout
    };
    let marker_key = scripts.to_key("marker");
    dbg!(timeout);

    if let Some((key, member, score)) = con
        .bzpopmin::<String, Option<(String, String, i32)>>(marker_key, timeout)
        .await?
    {
        if member.is_empty() {
            return Ok(score);
        }
    }
    Ok(0)
}
type PackedArgs = (
    &'static Atomic<bool>, // drained
    &'static Atomic<u64>,  // block_util
    Arc<Queue>,            // queue
    Option<Vec<String>>,   // job_data
    Option<String>,        // job_id
    i64,                   // limit_until
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
) -> Result<Option<Job<D, R>>, BullError> {
    let (drained, block_until, queue, job_data, job_id, limit_until, delay_until): PackedArgs =
        packed_args;

    if job_data.is_none() && !drained.load() {
        drained.swap(true);
        block_until.store(0);
    }

    if let Some(delay) = delay_until {
        block_until.store(cmp::max(delay, 0) as u64);
    }
    if let (Some(mut data), Some(id), Some(token_str)) = (job_data, job_id, token) {
        drained.store(false);

        let map = map_from_vec(&data);
        let mut raw_job = JobJsonRaw::from_map(map)?;

        // let static_id = to_static_str(id);
        let mut job: Job<D, R> = Job::from_raw_job(&mut raw_job, &queue, &id).await?;

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
    drained: &'static Atomic<bool>,
    block_until: &'static Atomic<u64>,
    queue: Arc<Queue>,
    options: Arc<WorkerOptions>,
    token: &str,
) -> Result<Option<Job<D, R>>, BullError> {
    let queue_name = queue.name.clone();
    let prefix = queue.prefix.to_owned();
    let pool = queue.manager.pool.clone();
    let mut script = Scripts::new(prefix, queue_name, pool);
    let mut connection = script.connection.get().await?;

    let result = script.move_to_active(token, &options).await?;

    let (job_data, id, limit_until, delay_until) = result;
    let opt_token = Some(token.to_owned());

    next_job_from_job_data(
        (
            drained,
            block_until,
            queue,
            job_data,
            id,
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
    waiting: Arc<Mutex<Option<i32>>>,
    drained: &'static Atomic<bool>,
    block_until: &'static Atomic<u64>,
    queue: Arc<Queue>,
    options: Arc<WorkerOptions>,
    token: &str,
) -> Option<Job<D, R>> {
    let options = options.clone();

    if waiting.lock().await.is_none() && drained.load() {
        let result = wait_for_job(queue.clone(), block_until).await.ok()?;
        dbg!(result);

        *waiting.lock().await = Some(result);
        let moved = move_to_active(drained, block_until, queue, options, token)
            .await
            .ok()?;
        dbg!(&moved);
        return moved;
    }
    move_to_active(drained, block_until, queue, options, token)
        .await
        .ok()?
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
    processing: Arc<ProcessingHandles<Option<Job<D, R>>>>,
) -> Vec<Job<D, R>> {
    let mut completed = Vec::new();
    if let Some(handle) = processing.lock().await.next().await {
        match handle {
            std::result::Result::Ok(result) => {
                if let Some(job) = result {
                    completed.push(job.clone());
                }
            }
            Err(err) => {
                if err.is_panic() {
                    // Handle cancelled task
                }
            }
        }
    }

    completed
}
type PackedProcessArgs<D, R> = (
    Arc<RwLock<HashSet<JobSetPair<D, R>>>>, // jobs
    Arc<AsyncEventEmitter<D, R>>,           //emitter
    Arc<WorkerCallback<'static, D, R>>,     // processor
    Arc<WorkerOptions>,                     // workerOptions
    Arc<Queue>,                             // queue
    &'static Atomic<bool>,                  //force_closing,
    &'static Atomic<bool>,                  // closing,
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
) -> Option<Job<D, R>> {
    let (jobs, emitter, processor, options, queue, force_closing, closing) = args;

    let callback = processor.clone();
    let data = JobSetPair(job.clone(), token);
    jobs.write().await.insert(data.clone());

    let returned = BacktraceCatcher::catch(callback(data)).await;
    let mut emitter = emitter.clone();

    match returned {
        Ok(result) => {
            if !force_closing.load() {
                let queue_name = queue.name.clone();
                let prefix = queue.prefix;
                let pool = queue.manager.pool.clone();
                let mut scripts = Scripts::new(prefix.to_string(), queue_name, pool);

                let remove_on_complete = job.opts.remove_on_complete.clone().unwrap_or_default();

                let fetch = !closing.load();
                let end = scripts
                    .move_to_completed(
                        &mut job,
                        serde_json::to_string(&result).ok()?,
                        remove_on_complete,
                        token,
                        &options,
                        fetch,
                    )
                    .await
                    .ok()?;
                let done = result.clone();
                let name = job.name;
                let id = job.id.clone();

                let finished_job = end;
                println!("{:?} {}", &finished_job, &name);

                match finished_job {
                    MoveToFinishedResults::MoveToNext(data) => {
                        let move_to_active: MoveToAciveResult = data.into();
                        if let Ok((data, _id, _ts, _)) = convert_errors(move_to_active) {
                            let static_id = to_static_str(job.id.clone());
                            if let Some(mut data) = data {
                                return Job::from_raw_job(&mut data, &queue, static_id).await.ok();
                            }
                        }
                    }
                    MoveToFinishedResults::Completed => {
                        emitter
                            .emit(
                                Events::Completed,
                                CallBackParams::Completed(job.clone(), done),
                            )
                            .await;

                        jobs.write().await.remove(&JobSetPair(job.clone(), token));

                        return None;
                    }
                    MoveToFinishedResults::Error(code) => {
                        let err = finished_errors(code, &job.id, "finished", "active");
                        jobs.write().await.remove(&JobSetPair(job.clone(), token));

                        dbg!(err);
                    }
                };
            }
            None
        }
        Err(err) => {
            let failed_reason = String::new();
            let e = match err {
                CaughtError::Panic(str) => str,
                CaughtError::Error(error, backtrace) => format!("{:#?}", backtrace),
            };
            emitter
                .emit(
                    Events::Error,
                    CallBackParams::Error(Some(job.clone()), e.clone()),
                )
                .await;

            if !force_closing.load() {
                job.move_to_failed(e.clone(), token, false).await;

                emitter
                    .emit(Events::Failed, CallBackParams::Failed(Some(job.clone()), e))
                    .await;
            }
            jobs.write().await.remove(&JobSetPair(job.clone(), token));
            None
        }
    }
}

// packed Args;

type PackedMainLoopArg<D, R> = (
    String,                                 // id
    &'static Atomic<bool>,                  // closed
    Arc<Mutex<Option<i32>>>,                // waiting;
    Arc<WorkerOptions>,                     // options
    &'static Atomic<bool>,                  // closing (self.closing),
    &'static Atomic<bool>,                  // self.force_closing
    &'static Atomic<bool>,                  // drained
    &'static Atomic<u64>,                   // block_until
    Arc<Queue>,                             // queue
    &'static Atomic<u64>,                   // tasks_completed,
    Arc<AsyncEventEmitter<D, R>>,           // emitter
    Arc<RwLock<HashSet<JobSetPair<D, R>>>>, // jobs
    Arc<WorkerCallback<'static, D, R>>,     // processor
);
async fn main_loop<D, R>(
    packed_args: PackedMainLoopArg<D, R>,
    mut processing: Arc<ProcessingHandles<Option<Job<D, R>>>>,
) where
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
{
    let (
        id,
        mut closed,
        waiting,
        options,
        closing,
        force_closing,
        drained,
        block_until,
        queue,
        tasks_completed,
        emitter,
        jobs,
        processor,
    ) = packed_args;

    let mut token_prefix = 0;

    while !closed.load() {
        while waiting.lock().await.is_none()
            && processing.lock().await.len() < options.concurrency
            && !closing.load()
        {
            token_prefix += 1;
            let stat_token = to_static_str(format!("{}:{}", id, token_prefix));
            //self.emitter.emit("drained", String::from("")).await;
            let waiting_clone = waiting.clone();

            let queue = queue.clone();

            let opts = options.clone();

            let task = tokio::spawn(async_backtrace::frame!(get_next_job(
                waiting_clone,
                drained,
                block_until,
                queue,
                opts,
                stat_token,
            )));

            *waiting.lock().await = None;
            processing.lock().await.push_back(task);
        }

        let mut tasks = get_completed(processing.clone()).await;

        while let Some(job) = tasks.pop() {
            if !job.is_completed() {
                let token = to_static_str(job.token.to_owned());
                let args: PackedProcessArgs<D, R> = (
                    jobs.clone(),
                    emitter.clone(),
                    processor.clone(),
                    options.clone(),
                    queue.clone(),
                    force_closing,
                    closing,
                );

                let next_job = tokio::spawn(async_backtrace::frame!(process_job(
                    args,
                    job.clone(),
                    token
                )));

                processing.lock().await.push_back(next_job)
            } else {
                // progress event task;
                tasks_completed.fetch_add(1);
                dbg!(tasks_completed.load());
            }
        }

        //self.processing.extend(jobs_to_process);
        let count = tasks_completed.load();
        if count > 0 {
            println!("jobs completed: {:#?}", count);
        }
    }
}
