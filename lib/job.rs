use super::*;

use core::num;

use enums::BullError;
use futures::{self, lock::Mutex};
use redis::Commands;
use redis::{FromRedisValue, RedisResult, ToRedisArgs};
use serde::de::{value, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeStruct, Serializer};
use serde_json::Value;
use std::any::Any;
use std::fmt::format;
use std::sync::Arc;
use std::time;
use std::{borrow::Borrow, clone, collections::HashMap};
#[derive(Clone)]
pub struct Job<D, R> {
    pub name: &'static str,
    pub queue: Arc<Queue>,
    pub timestamp: i64,
    pub attempts_made: i64,
    pub attempts: i64,
    pub delay: u64,
    pub id: String,               // jsonString
    pub progress: Option<String>, // 0 to 100
    pub opts: JobOptions,
    pub data: D,
    pub return_value: Option<R>,
    scripts: Arc<script::Scripts>,
    pub repeat_job_key: Option<&'static str>,
    pub failed_reason: Option<String>,
    pub stack_trace: Vec<String>,
    pub remove_on_complete: Option<RemoveOnCompletionOrFailure>,
    pub remove_on_fail: Option<RemoveOnCompletionOrFailure>,
    pub processed_on: Option<u64>,
    pub finished_on: Option<u64>,
    pub discarded: bool,
    pub parent_key: Option<String>,
    pub with_children_key: Option<String>,
    pub parent: Option<Parent>,
    pub token: &'static str,
    pub remove_deps_on_failure: bool,
    pub priority: i64,
}

impl<D: Serialize, R: Serialize> Serialize for Job<D, R> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Job", 15)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("queue", &self.queue.name)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.serialize_field("attempts_made", &self.attempts_made)?;
        state.serialize_field("attempts", &self.attempts)?;
        state.serialize_field("delay", &self.delay)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("progress", &self.progress)?;
        state.serialize_field("opts", &self.opts)?;
        state.serialize_field("data", &self.data)?;
        state.serialize_field("return_value", &self.return_value)?;
        state.serialize_field("repeat_job_key", &self.repeat_job_key)?;
        state.serialize_field("failed_reason", &self.failed_reason)?;
        state.serialize_field("stack_trace", &self.stack_trace)?;
        state.serialize_field("remove_on_complete", &self.remove_on_complete)?;
        state.serialize_field("remove_on_fail", &self.remove_on_fail)?;
        state.serialize_field("processed_on", &self.processed_on)?;
        state.serialize_field("finished_on", &self.finished_on)?;
        state.serialize_field("discarded", &self.discarded)?;
        state.serialize_field("parent_key", &self.parent_key)?;
        state.serialize_field("with_children_key", &self.with_children_key)?;
        state.serialize_field("parent", &self.parent)?;
        state.end()
    }
}

impl<
        D: Deserialize<'static> + Serialize + Clone + std::fmt::Debug + Send + Sync,
        R: Deserialize<'static> + Serialize + Any + Send + Sync + Clone,
    > PartialEq for Job<D, R>
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl<
        'a,
        D: Deserialize<'a> + Serialize + Clone + Send + Sync,
        R: Deserialize<'a>
            + Serialize
            + Send
            + Sync
            + Clone
            + 'static
            + std::fmt::Debug,
    > Job<D, R>
{
    pub fn is_completed(&self) -> bool {
        self.return_value.is_some() && self.finished_on > Some(0)
    }
    pub async fn get_state(&self) -> Result<String, BullError> {
        self.scripts.get_state(&self.id).await
    }
    pub async fn remove(&self, remove_children: bool) -> Result<(), BullError> {
        self.scripts.remove(self.id.clone(), remove_children).await
    }

    pub async fn update_progress<T: Serialize>(
        &mut self,
        progress: T,
    ) -> Result<Option<i8>, BullError> {
        self.progress = Some(serde_json::to_string(&progress)?);
        self.scripts.update_progress(&self.id, progress).await
    }

    pub async fn update_data(&mut self, data: D) -> Result<(), BullError> {
        let result = self.scripts.update_data(&self.id, data.clone()).await?;
        self.data = data;

        Ok(())
    }

    pub async fn new(
        name: &str,
        queue: &Queue,
        data: D,

        opts: JobOptions,
        job_id: Option<String>,
    ) -> Result<Job<D, R>, BullError> {
        let prefix = &queue.prefix;
        let queue_name = &queue.name;
        let dup_conn = queue.manager.pool.clone();
        let conn_str = queue.manager.to_conn_string();
        let mut opts_copy = opts.clone();

        let parent = opts.parent;

        let parent_key = parent.clone().map(|v| format!("{}:{}", &v.queue, &v.id));
        let id = if let Some(id) = job_id {
            id
        } else {
            opts.job_id.unwrap_or_default()
        };
        let que = queue.clone();

        Ok(Self {
            opts: opts_copy,
            queue: Arc::new(que),
            name: to_static_str(name.to_string()),
            id,
            progress: None,
            timestamp: opts.timestamp.unwrap(),
            delay: opts.delay as u64,
            attempts_made: 0,
            attempts: opts.attempts,
            data,
            return_value: None,
            remove_on_complete: opts.remove_on_complete,
            processed_on: None,
            finished_on: None,
            repeat_job_key: None,
            failed_reason: None,
            stack_trace: vec![],
            remove_on_fail: opts.remove_on_fail,
            with_children_key: None,
            scripts: Arc::new(Scripts::new(
                prefix.to_string(),
                queue_name.to_owned(),
                dup_conn,
            )),
            parent_key,
            discarded: false,
            parent,
            token: "",
            remove_deps_on_failure: false,
            priority: 0,
        })
    }
    pub async fn from_map(
        mut queue: &Queue,
        map: HashMap<String, String>,
        job_id: &str,
    ) -> Result<Self, BullError> {
        let mut obj = JobJsonRaw::from_map(map)?;
        Self::from_raw_job(&mut obj, queue, job_id).await
    }
    pub async fn from_json(
        mut queue: &Queue,
        raw_string: String,
        job_id: &str,
    ) -> Result<Job<D, R>, BullError> {
        let mut json = JobJsonRaw::fromStr(raw_string)?;
        Self::from_raw_job(&mut json, queue, job_id).await
    }
    pub fn opts_from_json(&mut self, raw_opts: String) -> Result<JobOptions, BullError> {
        let opts = serde_json::from_str::<JobOptions>(&raw_opts)?;
        Ok(opts)
    }
    pub async fn from_raw_job(
        json: &mut JobJsonRaw,
        queue: &Queue,
        job_id: &str,
    ) -> Result<Self, BullError> {
        let name = json.name;
        let mut opts = serde_json::from_str::<JobOptions>(json.opts).unwrap_or_default();

        let d = to_static_str(json.data.to_owned());
        let data = serde_json::from_str::<D>(d)?;

        let mut job = Self::new(name, queue, data, opts, None).await?;
        job.id = job_id.to_string();
        job.progress = json.progress.map(|v| v.to_owned());
        job.attempts_made = json.attempts_made.unwrap_or_default();
        job.timestamp = json.timestamp.unwrap_or_default() as i64;
        job.delay = json.delay.unwrap_or_default();
        if let Some(returned) = json.return_value {
            let data = to_static_str(returned.to_string());
            let return_value = serde_json::from_str::<R>(data)?;
            job.return_value = Some(return_value);
        }
        job.finished_on = json.finished_on;
        job.processed_on = json.processed_on;
        job.failed_reason = json.failed_reason.map(|v| v.to_owned());
        job.priority = json.priority.unwrap_or_default();

        job.repeat_job_key = json.rjk;

        job.stack_trace = json.stack_trace.clone();

        job.parent = None;
        job.parent_key = None;

        if let Some(value) = &json.parent {
            let parent: Parent = serde_json::from_str(value)?;

            let parent_key = format!("{}:{}", parent.queue, parent.id);

            job.parent = Some(parent);
            job.parent_key = Some(parent_key);
        }

        Ok(job)
    }
    pub async fn is_waiting(&self) -> Result<bool, BullError> {
        let result = self.is_in_in_list("wait").await? || self.is_in_in_list("paused").await?;

        Ok(result)
    }

    pub async fn promote(&mut self) -> Result<(), BullError> {
        let result = self.scripts.promote(&self.id).await?;
        self.delay = 0;
        Ok(())
    }
    pub async fn is_delayed(&self) -> Result<bool, BullError> {
        let mut conn = self.queue.manager.pool.get().await?;
        let score: Option<isize> = conn
            .zscore(self.scripts.to_key("delayed"), &self.id)
            .await?;
        Ok(score.is_some())
    }
    pub async fn is_in_zset(&self, set: &str) -> Result<bool, BullError> {
        let mut conn = self.queue.manager.pool.get().await?;
        let key = self.scripts.to_key(set);
        let score: Option<isize> = conn.zscore(&key, &self.id).await?;
        Ok(score.is_some())
    }

    pub async fn is_in_in_list(&self, list_name: &str) -> Result<bool, BullError> {
        let key = self.scripts.to_key(list_name);
        let result = self.scripts.is_job_in_list(key.as_str(), &self.id).await?;
        Ok(result)
    }

    pub async fn from_id(queue: &Queue, job_id: &str) -> Result<Option<Job<D, R>>, BullError> {
        // use redis to get the job;
        let key = format!("{}:{}:{}", &queue.prefix, &queue.name, job_id);
        let mut conn = queue.manager.pool.clone().get().await?;

        let raw_data: HashMap<String, String> =
            redis::Cmd::hgetall(key).query_async(&mut conn).await?;

        if raw_data.is_empty() {
            return Ok(None);
        }

        let raw_string = serde_json::to_string(&raw_data)?;
        let id = to_static_str(job_id.to_string());
        let job = Self::from_json(queue, raw_string, id).await?;
        Ok(Some(job))
    }
    pub async fn move_to_failed(
        &mut self,
        err: String,
        token: &str,
        fetch_next: bool,
    ) -> Result<(), BullError> {
        self.failed_reason = Some(err.clone());
        let mut move_to_failed = false;

        let mut finished_on = 0;
        let mut command = "moveToFailed";
        let mut conn = self.queue.manager.pool.clone().get().await?;
        self.save_to_stacktrace(&mut conn, err.clone()).await?;
        let backoff_s = BackOff::new();

        if self.attempts_made < self.opts.attempts && !self.discarded {
            let backoff = BackOff::normalize(self.opts.backoff.clone());
            let custom_strategy = if let Some(q) = self.queue.opts.settings.backoff_strategy.clone()
            {
                Arc::into_inner(q)
            } else {
                None
            };

            let delay = backoff_s.calculate(backoff, self.attempts_made, custom_strategy);
            if let Some(num) = delay {
                if num == -1 {
                    move_to_failed = true;
                }
                let timestamp = generate_timestamp()?;
                let (keys, args) =
                    self.scripts
                        .move_to_delayed_args(&self.id, timestamp as i64 + num, token)?;
                self.scripts
                    .commands
                    .get("moveToDelayed")
                    .unwrap()
                    .key(keys)
                    .arg(args)
                    .invoke_async::<_, >(&mut conn)
                    .await?;
                command = "delayed";
            } else {
                let (keys, args) = self
                    .scripts
                    .retry_jobs_args(&self.id, self.opts.lifo, token)?;
                self.scripts
                    .commands
                    .get("retryJobs")
                    .unwrap()
                    .key(keys)
                    .arg(args)
                    .invoke_async::<_, >(&mut conn)
                    .await?;
                command = "retryJob";
            }
        }
        move_to_failed = true;

        if move_to_failed {
            let worker_opts = WorkerOptions::default();

            let err_message = err.clone();
            let mut job = self.clone();
            let remove_onfail = self.opts.remove_on_fail.clone().unwrap_or_default();
            let result = job
                .scripts
                .move_to_failed(
                    self,
                    err_message,
                    remove_onfail,
                    token,
                    &worker_opts,
                    fetch_next,
                )
                .await?;

            finished_on = generate_timestamp()?;
        }
        if finished_on > 0 {
            self.finished_on = Some(finished_on);
        }

        Ok(())
    }

    pub async fn add_log(
        &self,
        job_id: &str,
        log_row: &str,
        keep_logs: Option<isize>,
    ) -> Result<isize, BullError> {
        let scripts = self.scripts.clone();
        let key = scripts.to_key(format!("{job_id}:logs").as_str());
        let mut connection = scripts.connection.get().await?;
        let mut pipeline = redis::pipe();
        pipeline.rpush(&key, log_row);

        if let Some(log_count) = keep_logs {
            pipeline.ltrim(&key, -log_count, -1);
        }

        let result: Vec<isize> = pipeline.query_async(&mut connection).await?;
        if let Some(count) = keep_logs {
            return Ok(std::cmp::min(count, result[0]));
        }
        Ok(result[0])
    }

    pub async fn save_to_stacktrace(
        &mut self,
        conn: &mut Connection,
        err_stack: String,
    ) -> Result<(), BullError> {
        if !err_stack.is_empty() {
            self.stack_trace.push(err_stack.clone());
            if let Some(limit) = self.opts.stacktrace_limit {
                if self.stack_trace.len() > limit {
                    self.stack_trace = self.stack_trace[0..limit].to_vec();
                }
            }
            let stack_trace = serde_json::to_string(&self.stack_trace)?;
            let (keys, args) =
                self.scripts
                    .save_stacktrace_args(&self.id, &stack_trace, &err_stack);

            self.scripts
                .commands
                .get("saveStacktrace")
                .unwrap()
                .key(keys)
                .arg(args)
                .invoke_async(conn)
                .await?;
        }
        Ok(())
    }

    pub fn to_string(&self) -> Result<String, BullError> {
        let string = serde_json::to_string(&self)?;
        Ok(string)
    }
    pub fn save_to_file(&self, path: &str) -> Result<(), BullError> {
        let file = std::fs::File::create(path)?;
        serde_json::to_writer_pretty(file, self)?;

        Ok(())
    }
}

// Custom implementation of Debug for Job, don't care about the logging the queue and the scripts
impl<D: std::fmt::Debug, R: std::fmt::Debug> std::fmt::Debug for Job<D, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("name", &self.name)
            .field("queue", &self.queue)
            .field("timestamp", &self.timestamp)
            .field("attempts_made", &self.attempts_made)
            .field("attempts", &self.attempts)
            .field("delay", &self.delay)
            .field("id", &self.id)
            .field("progress", &self.progress)
            .field("opts", &self.opts)
            .field("data", &self.data)
            .field("scripts", &self.scripts)
            .field("repeat_job_key", &self.repeat_job_key)
            .field("failed_reason", &self.failed_reason)
            .field("stack_trace", &self.stack_trace)
            .field("remove_on_complete", &self.remove_on_complete)
            .field("remove_on_fail", &self.remove_on_fail)
            .field("processed_on", &self.processed_on)
            .field("finished_on", &self.finished_on)
            .field("discarded", &self.discarded)
            .field("return_value", &self.return_value)
            .field("parent", &self.parent)
            .field("parent_key", &self.parent_key)
            .finish()
    }
}
pub fn to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Data {
    #[serde(rename = "socketId")]
    pub socket_id: String,
    pub cid: String,
    #[serde(rename = "fileID")]
    pub file_id: String,
    pub sizes: Vec<Size>,
    #[serde(rename = "userID")]
    pub user_id: String,
    #[serde(rename = "trackingID")]
    pub tracking_id: String,
}
use derive_redis_json::RedisJsonValue;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]

pub struct ReturnedData {
    pub status: String,
    pub count: f32,
    #[serde(rename = "socketId")]
    pub socket_id: String,
    pub cid: String,
    #[serde(rename = "fileID")]
    pub file_id: String,
    pub sizes: Vec<Size>,
    #[serde(rename = "userID")]
    pub user_id: String,
    #[serde(rename = "trackingID")]
    pub tracking_id: String,
}
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Size {
    pub width: i64,
    pub height: i64,
}
