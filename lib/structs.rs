// Specify which jobs to keep after finishing. If both age and count are
//  specified, then the jobs kept will be the ones that satisfies both
// properties.
pub use derive_redis_json::RedisJsonValue;
use rand::prelude::*;
pub use serde::{Deserialize, Serialize};
use std::{collections::HashMap, default, fmt::Display};

#[derive(Debug, Default, Deserialize, Serialize, RedisJsonValue, Clone, Copy)]
pub struct KeepJobs {
    pub age: Option<i64>,   // Maximum age in seconds for jobs to kept;
    pub count: Option<i64>, // Maximum Number of jobs to keep
}
#[derive(Debug, Serialize, Deserialize, RedisJsonValue, Clone)]
pub struct JobOptions {
    pub job_id: String,
    pub timestamp: i64, // timestamp when  the job was created
    pub delay: i64,     // number of milliseconds to wait until this job can be processed
    pub attempts: i64,  // total number of attempts to try the job until it completes.
    pub remove_on_completion: RemoveOnCompletionOrFailure,
    pub remove_on_fail: RemoveOnCompletionOrFailure,
    pub fail_parent_on_failure: bool, // if true, moves parent to failed
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
            timestamp: (timestamp * 1000.0).round() as i64,
            job_id: id.to_string(),
            delay: 0,
            attempts: 0,
            remove_on_completion: RemoveOnCompletionOrFailure::default(),
            remove_on_fail: RemoveOnCompletionOrFailure::default(),
            fail_parent_on_failure: false,
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct RetryJobOptions {
    pub state: String,
    pub count: i64,
    pub timestamp: i64,
}
#[derive(Debug, Serialize, Deserialize, RedisJsonValue)]
pub struct WorkerOptions {
    pub autorun: bool, //  condition to start processer at instance creation, default true
    pub concurrency: i64, // number of parallel jobs per worker, default: 1
    pub max_stalled_count: i64, // n of jobs to be recovered from stalled state, default:1
    pub stalled_interval: i64, // milliseconds between stallness checks, default 30000
    pub lock_duration: i64, // Duration of lock for job in milliseconds, default: 30000
    pub prefix: String, // prefix for all queue, keys
    pub connection: HashMap<String, String>,
    pub limiter: Limiter,               //
    pub metrics: Option<MetricOptions>, // metrics options
    pub remove_on_completion: RemoveOnCompletionOrFailure,
    pub remove_on_fail: RemoveOnCompletionOrFailure,
}

#[derive(Debug, Default, Serialize, Deserialize, RedisJsonValue)]
pub struct Limiter {
    max: i64,
    duration: i64,
}
#[derive(Debug, Default, Serialize, Deserialize, RedisJsonValue)]
pub struct MetricOptions {
    pub max_data_points: String,
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
            connection: HashMap::default(),
            limiter: Limiter::default(),
            metrics: None,
            remove_on_completion: RemoveOnCompletionOrFailure::default(),
            remove_on_fail: RemoveOnCompletionOrFailure::default(),
        }
    }
}

pub struct QueueOptions<'d> {
    pub prefix: Option<&'d str>,
}

#[derive(Debug, Clone,Deserialize, Serialize,RedisJsonValue,)]
// make all fields public
pub struct JobJsonRaw {
    pub id: String,
    pub name: String,
    pub data: String,
    pub delay: String,
    pub opts: String,
    pub progress: String,
    pub attempts_made: String,
    pub timestamp: String,
    pub failed_reason: String,
    pub stack_trace: Vec<String>,
    pub return_value: String,
    pub parent: Option<String>,
    pub rjk: Option<String>,
    pub finished_on: Option<String>,
    pub processed_on: Option<String>,
}
