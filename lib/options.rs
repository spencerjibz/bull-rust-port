// Specify which jobs to keep after finishing. If both age and count are
//  specified, then the jobs kept will be the ones that satisfies both
// properties.
use crate::redis_connection::RedisOpts;
use crate::to_static_str;
use crate::BoxedFn;
pub use derive_redis_json::RedisJsonValue;
use rand::prelude::*;
use redis::{FromRedisValue, RedisError, RedisResult, ToRedisArgs, Value};
pub use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{borrow::Borrow, collections::HashMap, default, fmt::Display};
#[derive(Debug, Default, Deserialize, Serialize, RedisJsonValue, Clone, Copy)]
pub struct KeepJobs {
    pub age: Option<i64>,   // Maximum age in seconds for jobs to kept;
    pub count: Option<i64>, // Maximum Number of jobs to keep
}
#[derive(Debug, Serialize, Deserialize, RedisJsonValue, Clone)]
pub struct JobOptions {
     pub priority: i64,
    pub job_id: Option<String>,
    pub timestamp: Option<i64>, // timestamp when  the job was created
    pub delay: i64,             // number of milliseconds to wait until this job can be processed
    pub attempts: i64,          // total number of attempts to try the job until it completes.
    pub remove_on_complete: RemoveOnCompletionOrFailure,
    pub remove_on_fail: RemoveOnCompletionOrFailure,
    pub fail_parent_on_failure: bool, // if true, moves parent to failed
    pub stacktrace_limit: Option<usize>,
    pub backoff: (i64, Option<BackOffOptions>), //
    pub lifo: bool, // if true, adds the job to the right of the queue instead of the left
    pub parent: Option<Parent>,
}

#[derive(Debug, Default, Deserialize, Serialize, RedisJsonValue, Clone)]
pub struct Parent {
    pub id: String,
    pub queue: String,
}

#[derive(Debug, Default, Deserialize, Serialize, RedisJsonValue, Clone)]
pub struct RepeatOpts {
    pub pattern: String,
    pub limit: i64,
    pub every: i64,
    pub immmediately: bool,
    pub count: i64,
    pub prev_millis: i64,
    pub offset: i64,
    pub job_id: String,
}

#[derive(Debug, Default, Deserialize, Serialize, RedisJsonValue, Clone)]
pub struct RemoveOnCompletionOrFailure {
    pub bool: bool, // if true, remove the job when it completes
    pub int: i64,   //  number is passed, its specifies the maximum amount of jobs to keeps
    pub keep: Option<KeepJobs>,
}

impl Default for JobOptions {
    fn default() -> Self {
        use std::time::SystemTime;
        let id: u16 = rand::random();
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs_f32();
        //dbg!("{} {}", id, timestamp);
        Self {
            priority: 0,
            timestamp: Some((timestamp * 1000.0).round() as i64),
            job_id: Some(id.to_string()),
            delay: 0,
            attempts: 0,
            remove_on_complete: RemoveOnCompletionOrFailure::default(),
            remove_on_fail: RemoveOnCompletionOrFailure::default(),
            fail_parent_on_failure: false,
            stacktrace_limit: None,
            backoff: (0, None),
            lifo: false,
            parent: None,
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct RetryJobOptions {
    pub state: String,
    pub count: i64,
    pub timestamp: i64,
}
#[derive(Debug, Serialize, Deserialize, RedisJsonValue, Clone)]
pub struct WorkerOptions {
    pub autorun: bool, //  condition to start processer at instance creation, default true
    pub concurrency: usize, // number of parallel jobs per worker, default: 1
    pub max_stalled_count: i64, // n of jobs to be recovered from stalled state, default:1
    pub stalled_interval: i64, // milliseconds between stallness checks, default 30000
    pub lock_duration: i64, // Duration of lock for job in milliseconds, default: 30000
    pub prefix: String, // prefix for all queue, keys
    pub connection: String, // redis connection string
    pub limiter: Limiter, //
    pub metrics: Option<MetricOptions>, // metrics options
    pub remove_on_completion: RemoveOnCompletionOrFailure,
    pub remove_on_fail: RemoveOnCompletionOrFailure,
}

#[derive(Debug, Default, Serialize, Deserialize, RedisJsonValue, Clone)]
pub struct Limiter {
    max: i64,
    duration: i64,
}
#[derive(Debug, Default, Serialize, Deserialize, RedisJsonValue, Clone)]
pub struct MetricOptions {
    pub max_data_points: String,
}

#[derive(Default, Clone)]
pub struct QueueSettings {
    pub backoff_strategy: Option<Arc<BoxedFn>>,
}

//implement fmt::Debug for QueueSettings
use std::fmt;
impl fmt::Debug for QueueSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueSettings").finish()
    }
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            autorun: true,
            concurrency: 1,
            max_stalled_count: 1,
            stalled_interval: 30000,
            lock_duration: 30000,
            prefix: "".to_string(),
            connection: "redis://localhost:6379".to_ascii_lowercase(),
            limiter: Limiter::default(),
            metrics: None,
            remove_on_completion: RemoveOnCompletionOrFailure::default(),
            remove_on_fail: RemoveOnCompletionOrFailure::default(),
        }
    }
}

#[derive(Debug, Default)]
pub struct QueueOptions<'d> {
    pub prefix: Option<&'d str>,
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
    #[serde(borrow)]
    pub delay: &'static str,
    #[serde(borrow)]
    pub opts: &'static str,
    #[serde(borrow)]
    pub progress: &'static str,
    #[serde(borrow)]
    pub attempts_made: &'static str,
    #[serde(borrow)]
    pub timestamp: &'static str,
    #[serde(borrow)]
    pub failed_reason: &'static str,

    pub stack_trace: Vec<String>,
    #[serde(borrow)]
    pub return_value: &'static str,
    #[serde(borrow)]
    pub parent: Option<&'static str>,
    #[serde(borrow)]
    pub rjk: Option<&'static str>,
    #[serde(borrow)]
    pub finished_on: Option<&'static str>,
    #[serde(borrow)]
    pub processed_on: Option<&'static str>,
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
                "delay" => job.delay = v,
                "opts" => job.opts = v,
                "progress" => job.progress = v,
                "attempts_made" | "attemptsMade" => job.attempts_made = v,
                "timestamp" => job.timestamp = v,
                "failed_reason" | "failedReason" => job.failed_reason = v,
                "stack_trace" | "stacktrace" => job.stack_trace = serde_json::from_str(v)?,
                "returnvalue" => job.return_value = v,
                "parent" => job.parent = Some(v),
                "rjk" => job.rjk = Some(v),
                "finished_on" | "finishedOn" => job.finished_on = Some(v),
                "processed_on" | "processedOn" => job.processed_on = Some(v),
                _ => (),
            }
        }

        Ok(job)
    }

    pub fn from_value_map(map: HashMap<String, serde_json::Value>) -> anyhow::Result<JobJsonRaw> {
        let mut job = JobJsonRaw::default();
        for (k, v) in map {
            match k.as_str() {
                "id" => job.id = to_static_str(v.as_str().unwrap_or("").to_string()),
                "name" => job.name = to_static_str(v.as_str().unwrap_or("").to_string()),
                "data" => job.data = to_static_str(v.as_str().unwrap_or("").to_string()),
                "delay" => job.delay = to_static_str(v.as_str().unwrap_or("").to_string()),
                "opts" => job.opts = to_static_str(v.as_str().unwrap_or("").to_string()),
                "progress" => job.progress = to_static_str(v.as_str().unwrap_or("").to_string()),
                "attempts_made" | "attemptsMade" => {
                    job.attempts_made = to_static_str(v.as_str().unwrap_or("").to_string())
                }
                "timestamp" => job.timestamp = to_static_str(v.as_str().unwrap_or("").to_string()),
                "failed_reason" | "failedReason" => {
                    job.failed_reason = to_static_str(v.as_str().unwrap_or("").to_string())
                }
                "stack_trace" | "stacktrace" => job.stack_trace = serde_json::from_value(v)?,
                "returnvalue" | "return_value" | "returnValue" => {
                    job.return_value = to_static_str(v.as_str().unwrap_or("").to_string())
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
                        Some(to_static_str(v.as_str().unwrap_or("").to_string()))
                    }
                }
                "processed_on" | "processedOn" => {
                    job.processed_on = if v.is_null() {
                        None
                    } else {
                        Some(to_static_str(v.as_str().unwrap_or("").to_string()))
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
        let mut file = std::fs::File::create(path)?;
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
