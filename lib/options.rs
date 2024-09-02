use std::{borrow::Borrow, collections::HashMap, fmt::Display};
//implement fmt::Debug for QueueSettings
use std::fmt;
use std::sync::Arc;

use crate::worker::map_from_vec;
use crate::StoredFn;
use crate::{to_static_str, Queue};
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use deadpool_redis::Connection;
pub use derive_redis_json::RedisJsonValue;
use redis_derive::FromRedisValue;
pub use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize, Serialize, RedisJsonValue, Clone, Copy)]
pub struct KeepJobs {
    pub age: Option<i64>,   // Maximum age in seconds for jobs to kept;
    pub count: Option<i64>, // Maximum Number of jobs to keep
}
#[derive(Debug, Serialize, Deserialize, RedisJsonValue, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JobOptions {
    pub priority: i64,
    pub job_id: Option<String>,
    pub timestamp: Option<i64>,
    /// timestamp when  the job was created
    pub delay: i64,
    /// number of milliseconds to wait until this job can be processed
    pub attempts: i64,
    /// total number of attempts to try the job until it completes.
    pub remove_on_complete: Option<RemoveOnCompletionOrFailure>,
    pub remove_on_fail: Option<RemoveOnCompletionOrFailure>,
    #[serde(rename = "fpof")]
    pub fail_parent_on_failure: bool,
    /// if true, moves parent to failed
    pub stacktrace_limit: Option<usize>,
    pub backoff: Option<BackOffJobOptions>, //
    pub lifo: bool,
    /// if true, adds the job to the right of the queue instead of the left
    pub parent: Option<Parent>,
    #[serde(rename = "rdof")]
    pub remove_deps_on_failure: bool,

    // reapeat options
    pub repeat: Option<RepeatOpts>,
}

#[derive(Debug, Serialize, Deserialize, RedisJsonValue, Clone)]
#[serde(untagged)]
pub enum BackOffJobOptions {
    Number(i64),
    Opts(BackOffOptions),
}

#[derive(Debug, Default, Deserialize, Serialize, RedisJsonValue, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Parent {
    pub id: String,
    pub queue: String,
    pub remove_deps_on_failure: bool,
    pub fail_parent_on_failure: bool,
}

#[derive(Debug, Default, Deserialize, Serialize, RedisJsonValue, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RepeatOpts {
    pub pattern: String,
    pub limit: i64,
    pub every: i64,
    pub immmediately: bool,
    pub count: i64,
    pub prev_millis: i64,
    pub offset: i64,
    pub job_id: String,
    pub current_date: NaiveDateTime,
    pub start_date: NaiveDateTime,
    pub end_date: NaiveDateTime,
}

#[derive(Debug, Deserialize, Serialize, RedisJsonValue, Clone)]
#[serde(untagged)]
pub enum RemoveOnCompletionOrFailure {
    Bool(bool), // if true, remove the job when it completes
    Int(i64),   //  number is passed, its specifies the maximum amount of jobs to keeps
    Opts(KeepJobs),
    None,
}

impl Default for RemoveOnCompletionOrFailure {
    fn default() -> Self {
        Self::Bool(false)
    }
}

impl Default for JobOptions {
    fn default() -> Self {
        use std::time::SystemTime;
        let id: u16 = rand::random();
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs_f32();
        let remove_opts = RemoveOnCompletionOrFailure::default();

        Self {
            priority: 0,
            timestamp: Some((timestamp * 1000.0).round() as i64),
            job_id: None,
            delay: 0,
            attempts: 0,
            remove_on_complete: Some(remove_opts.clone()),
            remove_on_fail: Some(remove_opts),
            fail_parent_on_failure: false,
            stacktrace_limit: None,
            backoff: None,
            lifo: false,
            parent: None,
            remove_deps_on_failure: false,
            repeat: None,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, RedisJsonValue, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MoveToFinishOpts {
    pub keep_jobs: KeepJobs,
    pub token: String,
    pub attempts: i64,
    pub attempts_made: i64,
    pub max_metrics_size: Option<String>,
    #[serde(rename = "fpof")]
    pub fail_parent_on_failure: bool,
    #[serde(rename = "rdof")]
    pub remove_deps_on_failure: bool,
    pub limiter: Limiter,
    pub lock_duration: i64,
}
#[derive(Debug, Default, Serialize, Deserialize, RedisJsonValue, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JobMoveOpts {
    pub token: String,
    pub lock_duration: i64,
    pub(crate) limiter: Option<Limiter>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct RetryJobOptions {
    pub state: String,
    pub count: i64,
    pub timestamp: i64,
}
#[derive(Debug, Serialize, Deserialize, RedisJsonValue, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WorkerOptions {
    pub autorun: bool, //  condition to start processer at instance creation, default true
    pub concurrency: usize, // number of parallel jobs per worker, default: No of cpus/2
    pub max_stalled_count: i64, // n of jobs to be recovered from stalled state, default:1
    pub stalled_interval: i64, // milliseconds between stallness checks, default 30000
    pub lock_duration: i64, // Duration of lock for job in milliseconds, default: 30000
    pub prefix: String, // prefix for all queue, keys
    pub connection: String, // redis connection string
    pub limiter: Limiter, //
    pub metrics: Option<MetricOptions>, // metrics options
    #[serde(rename = "removeOnComplete")]
    pub remove_on_completion: RemoveOnCompletionOrFailure,
    #[serde(rename = "removeOnFail")]
    pub remove_on_fail: RemoveOnCompletionOrFailure,
}

#[derive(Debug, Serialize, Deserialize, RedisJsonValue, Clone)]
pub struct Limiter {
    pub max: i64,
    pub duration: i64,
}

impl Default for Limiter {
    fn default() -> Self {
        Self {
            max: 1000,
            duration: 10000,
        }
    }
}
#[derive(Debug, Default, Serialize, Deserialize, RedisJsonValue, Clone)]
pub struct MetricOptions {
    pub max_data_points: i64,
}

#[derive(Default, Clone)]
pub struct QueueSettings {
    pub backoff_strategy: Option<Arc<StoredFn>>,
}

impl fmt::Debug for QueueSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueSettings").finish()
    }
}

impl Default for WorkerOptions {
    fn default() -> Self {
        let cpu_count = num_cpus::get();
        Self {
            autorun: false,
            concurrency: cpu_count,
            max_stalled_count: 1,
            stalled_interval: 3000,
            lock_duration: 3000,
            prefix: "".to_string(),
            connection: "redis://localhost:6379".to_ascii_lowercase(),
            limiter: Limiter::default(),
            metrics: None,
            remove_on_completion: RemoveOnCompletionOrFailure::default(),
            remove_on_fail: RemoveOnCompletionOrFailure::default(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct QueueOptions {
    pub prefix: Option<&'static str>,
    pub settings: QueueSettings,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
// make all fields public
#[serde(rename_all = "snake_case")]
pub struct JobJsonRaw {
    #[serde(borrow)]
    pub id: &'static str,
    #[serde(borrow)]
    pub name: &'static str,
    #[serde(borrow)]
    pub data: &'static str,
    pub delay: Option<u64>,
    #[serde(borrow)]
    pub opts: &'static str,
    #[serde(borrow)]
    pub progress: Option<&'static str>,
    pub attempts_made: Option<i64>,
    pub timestamp: Option<u64>,
    #[serde(borrow)]
    pub failed_reason: Option<&'static str>,
    pub stack_trace: Vec<String>,
    #[serde(borrow)]
    pub return_value: Option<&'static str>,
    #[serde(borrow)]
    pub parent: Option<&'static str>,
    #[serde(borrow)]
    pub rjk: Option<&'static str>,
    pub finished_on: Option<u64>,
    pub processed_on: Option<u64>,
    pub priority: Option<i64>,
}

impl JobJsonRaw {
    pub fn from_map(map: HashMap<String, String>) -> anyhow::Result<JobJsonRaw> {
        let mut job = JobJsonRaw::default();
        for (k, v) in map {
            let v = to_static_str(v);
            match k.as_str() {
                "id" => job.id = v,
                "name" => job.name = v,
                "data" => job.data = v,
                "delay" => job.delay = v.parse().map(Some).unwrap_or_default(),
                "opts" => job.opts = v,
                "progress" => job.progress = if !v.is_empty() { Some(v) } else { None },
                "attempts_made" | "attemptsMade" => {
                    job.attempts_made = v.parse().map(Some).unwrap_or_default()
                }
                "timestamp" => job.timestamp = v.parse().map(Some).unwrap_or_default(),
                "failed_reason" | "failedReason" => {
                    job.failed_reason = if !v.is_empty() { Some(v) } else { None }
                }
                "stack_trace" | "stacktrace" => job.stack_trace = serde_json::from_str(v)?,
                "returnvalue" | "return_value" | "returnValue" | "returnedvalue"
                | "returnedValue" => job.return_value = if !v.is_empty() { Some(v) } else { None },
                "parent" => job.parent = Some(v),
                "rjk" => job.rjk = Some(v),
                "finished_on" | "finishedOn" => {
                    job.finished_on = v.parse().map(Some).unwrap_or_default()
                }
                "processed_on" | "processedOn" => {
                    job.processed_on = v.parse().map(Some).unwrap_or_default()
                }
                "priority" => job.priority = v.parse().map(Some).unwrap_or_default(),
                _ => (),
            }
        }

        Ok(job)
    }

    pub async fn from_id(job_id: String, queue: &Queue) -> anyhow::Result<JobJsonRaw> {
        let key = format!("{}:{}:{}", &queue.prefix, &queue.name, job_id);
        let mut conn = queue.manager.pool.clone().get().await?;
        let raw_data: Vec<String> = redis::Cmd::hgetall(key).query_async(&mut conn).await?;

        let map = map_from_vec(&raw_data);

        if raw_data.is_empty() {
            return Err(anyhow::Error::msg("job not failed"));
        }
        JobJsonRaw::from_map(map)
    }

    pub fn from_value_map(map: HashMap<String, serde_json::Value>) -> anyhow::Result<JobJsonRaw> {
        let mut job = JobJsonRaw::default();
        for (k, v) in map {
            match k.as_str() {
                "id" => job.id = to_static_str(v.as_str().unwrap_or("").to_string()),
                "name" => job.name = to_static_str(v.as_str().unwrap_or("").to_string()),
                "data" => job.data = to_static_str(v.as_str().unwrap_or("").to_string()),
                "delay" => {
                    job.delay = to_static_str(v.as_str().unwrap_or("").to_string())
                        .parse()
                        .map(Some)
                        .unwrap_or_default()
                }
                "opts" => job.opts = to_static_str(v.as_str().unwrap_or("").to_string()),
                "progress" => {
                    job.progress = Some(to_static_str(v.as_str().unwrap_or("").to_string()))
                }
                "attempts_made" | "attemptsMade" => {
                    job.attempts_made = to_static_str(v.as_str().unwrap_or("").to_string())
                        .parse()
                        .map(Some)
                        .unwrap_or_default()
                }
                "timestamp" => {
                    job.timestamp = to_static_str(v.as_str().unwrap_or("").to_string())
                        .parse()
                        .map(Some)
                        .unwrap_or_default()
                }
                "failed_reason" | "failedReason" => {
                    job.failed_reason = Some(to_static_str(v.as_str().unwrap_or("").to_string()))
                }
                "stack_trace" | "stacktrace" => job.stack_trace = serde_json::from_value(v)?,
                "returnvalue" | "return_value" | "returnValue" | "returnedvalue"
                | "returnedValue" => {
                    job.return_value = Some(to_static_str(v.as_str().unwrap_or("").to_string()))
                }
                "parent" => {
                    job.parent = if v.is_null() {
                        None
                    } else {
                        Some(to_static_str(v.as_str().unwrap_or("").to_string()))
                    }
                }
                "rjk" => {
                    job.rjk = if v.is_null() {
                        None
                    } else {
                        Some(to_static_str(v.as_str().unwrap_or("").to_string()))
                    }
                }
                "finished_on" | "finishedOn" => {
                    job.finished_on = if v.is_null() {
                        None
                    } else {
                        to_static_str(v.as_str().unwrap_or("").to_string())
                            .parse()
                            .map(Some)
                            .unwrap_or_default()
                    }
                }
                "processed_on" | "processedOn" => {
                    job.processed_on = if v.is_null() {
                        None
                    } else {
                        to_static_str(v.as_str().unwrap_or("").to_string())
                            .parse()
                            .map(Some)
                            .unwrap_or_default()
                    }
                }
                "priority" => {
                    job.priority = if v.is_null() {
                        None
                    } else {
                        to_static_str(v.as_str().unwrap_or("").to_string())
                            .parse()
                            .map(Some)
                            .unwrap_or_default()
                    }
                }
                _ => (),
            }
        }

        Ok(job)
    }
    #[allow(non_snake_case)]
    pub fn fromStr(s: String) -> anyhow::Result<JobJsonRaw> {
        // passed the map;
        let static_str = to_static_str(s);
        let map: HashMap<String, serde_json::Value> = serde_json::from_str(static_str)?;

        let json = JobJsonRaw::from_value_map(map)?;
        Ok(json)
    }
    pub fn save_to_file(&self, path: &str) -> anyhow::Result<()> {
        let file = std::fs::File::create(path)?;
        serde_json::to_writer_pretty(file, self)?;

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BackOffOptions {
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub delay: Option<i64>,
}
