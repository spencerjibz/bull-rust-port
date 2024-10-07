#![allow(clippy::too_many_arguments)]
#![allow(clippy::field_reassign_with_default)]

use std::any::Any;
use std::collections::HashMap;
use std::num::NonZero;

use derive_redis_json::RedisJsonValue;

use maplit::hashmap;
pub use redis::Script;
use redis::{FromRedisValue, Value};
use redis_derive::FromRedisValue;
use rmp::encode;
use serde::{Deserialize, Serialize};

use crate::enums::BullError;
//use crate::move_to_finished::{self, move_job_to_finished};
use crate::enums;
use crate::enums::JobError;
use crate::redis_connection::*;
use crate::worker::map_from_vec;
use crate::Parent;
use crate::RemoveOnCompletionOrFailure;
use crate::{job::Job, redis_connection::*};
use crate::{JobJsonRaw, JobMoveOpts, KeepJobs, MoveToFinishOpts, WorkerOptions};

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
struct Arguments {
    key_prefix: String,
    custom_id: Option<String>,
    name: String,
    timestamp: u64,
    parent_key: Option<String>,
    wait_children_key: Option<String>,
    parent_dependencies_key: Option<String>,
    parent: Option<Parent>,
    repeat_job_key: Option<String>,
    debounce_key: Option<String>,
}

type ScriptCommands = HashMap<&'static str, Script>;
#[derive(Clone)]
pub struct Scripts {
    pub prefix: String,
    pub queue_name: String,
    pub keys: HashMap<String, String>,
    pub commands: ScriptCommands,
    pub connection: Pool,
    pub redis_version: String,
}

// debug implementation for Scripts
impl std::fmt::Debug for Scripts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scripts")
            .field("prefix", &self.prefix)
            .field("queue_name", &self.queue_name)
            .field("keys", &self.keys)
            .field("commands", &self.commands.keys())
            .field("redis_version", &self.redis_version)
            .finish()
    }
}

impl Scripts {
    pub fn new(prefix: String, queue_name: String, conn: Pool) -> Self {
        let mut keys = HashMap::with_capacity(14);
        let names = [
            "",
            "active",
            "wait",
            "waiting-children",
            "paused",
            "completed",
            "failed",
            "delayed",
            "stalled",
            "limiter",
            "prioritized",
            "id",
            "stalled-check",
            "meta",
            "pc",
            "events",
        ];
        for name in names {
            if name.is_empty() {
                keys.insert(name.to_owned(), format!("{prefix}:{queue_name}"));
                continue;
            }
            keys.insert(name.to_owned(), format!("{prefix}:{queue_name}:{name}"));
        }
        let comands = hashmap! {
            "addJob" => Script::new(&get_script("addJob-9.lua")),
            "getCounts" =>  Script::new(&get_script("getCounts-1.lua")),
            "obliterate" =>  Script::new(&get_script("obliterate-2.lua")),
            "pause" => Script::new(&get_script("pause-4.lua")),
            "moveToActive" =>  Script::new(&get_script("moveToActive-11.lua")),
            "moveToFinished" =>  Script::new(&get_script("moveToFinished-14.lua")),
            "moveStalledJobsToWait"=>  Script::new(&get_script("moveStalledJobsToWait-9.lua")),
            "retryJobs" => Script::new(&get_script("retryJobs-6.lua")),
            "updateProgress"=>  Script::new(&get_script("updateProgress-2.lua")),
            "remove" => Script::new(&get_script("removeJob-1.lua")),
            "getState" => Script::new(&get_script("getState-7.lua")),
            "getStateV2" => Script::new(&get_script("getStateV2-7.lua")),
            "updateData" => Script::new(&get_script("updateData-1.lua")),
            "getRanges" => Script::new(&get_script("getRanges-1.lua")),
            "promote" => Script::new(&get_script("promote-8.lua")),
            "saveStacktrace" => Script::new(&get_script("saveStacktrace-1.lua")),
            "extendLock" => Script::new(&get_script("extendLock-2.lua")),
            "addStandardJob" => Script::new(&get_script("addStandardJob-8.lua")),
            "addDelayedJob" =>  Script::new(&get_script("addDelayedJob-6.lua")),
            "addParentJob" =>  Script::new(&get_script("addParentJob-4.lua")),
            "addPrioritizedJob" =>  Script::new(&get_script("addPrioritizedJob-8.lua")),
            "changePriority" => Script::new(&get_script("changePriority-7.lua")),

        };
        Self {
            queue_name,
            prefix,
            keys,
            commands: comands,
            connection: conn,
            redis_version: String::new(),
        }
    }
    pub fn to_key(&self, name: &str) -> String {
        format!("{}:{}:{}", self.prefix, self.queue_name, name)
    }
    pub fn get_keys(&self, keys: &[&str]) -> Vec<String> {
        keys.iter().map(|&k| self.to_key(k)).collect()
    }
    pub fn save_stacktrace_args(
        &self,
        job_id: &str,
        stacktrace: &str,
        failed_reason: &str,
    ) -> (Vec<String>, Vec<String>) {
        let keys = vec![self.to_key(job_id)];
        let args = vec![stacktrace.to_string(), failed_reason.to_string()];

        (keys, args)
    }
    pub fn retry_jobs_args(
        &self,
        job_id: &str,
        lifo: bool,
        token: &str,
    ) -> Result<(Vec<String>, Vec<String>), BullError> {
        let mut keys = self.get_keys(&["active", "wait", "paused"]);
        keys.push(self.to_key(job_id));
        keys.push(self.to_key("meta"));
        keys.push(self.to_key("events"));
        keys.push(self.to_key("delayed"));
        keys.push(self.to_key("priority"));

        let push_cmd = if lifo { "R" } else { "L" };

        let args = vec![
            self.keys.get("").unwrap().to_owned(),
            generate_timestamp()?.to_string(),
            push_cmd.to_string(),
            job_id.to_string(),
            token.to_string(),
        ];
        Ok((keys, args))
    }
    pub async fn add_job<
        's,
        D: Serialize + Clone + Deserialize<'s>,
        R: Serialize + Clone + Deserialize<'s>,
    >(
        &self,
        job: &Job<D, R>,
    ) -> Result<Option<i64>, BullError> {
        if job.delay > 0 {
            return self.add_delayed_job(job).await;
        } else if job.priority > 0 {
            return self.add_prioritized_job(job).await;
        }
        self.add_standard_job(job).await
    }

    fn add_job_args<
        's,
        D: Serialize + Clone + Deserialize<'s>,
        R: Serialize + Clone + Deserialize<'s>,
    >(
        &self,
        job: &Job<D, R>,
        with_children_key: Option<String>,
    ) -> Result<((Vec<u8>, String, Vec<u8>)), BullError> {
        let name = job.name;
        let parent = job.parent.clone();
        let parent_key = job.parent_key.clone();

        let json_data = serde_json::to_string(&job.data.clone())?;

        let parent_dep_key = parent_key.as_ref().map(|v| format!("{v}:dependencies"));
        let e = job.queue.prefix.to_owned();
        let prefix = self.to_key("");

        let mut args = Arguments::default();
        args.custom_id = Some(job.id.clone());
        args.key_prefix = prefix.to_string();
        args.name = job.name.to_owned();
        args.timestamp = job.timestamp as u64;
        args.parent_key = job.parent_key.clone();
        args.wait_children_key = with_children_key;

        args.parent_key = job.parent_key.clone();
        args.parent = job.parent.clone();
        args.repeat_job_key = job.repeat_job_key.map(|key| key.to_owned());
        let packed_args = rmp_serde::encode::to_vec(&args)?;
        let packed_opts = rmp_serde::encode::to_vec_named(&job.opts)?;

        Ok((packed_args, json_data, packed_opts))
    }
    async fn add_standard_job<
        's,
        D: Serialize + Clone + Deserialize<'s>,
        R: Serialize + Clone + Deserialize<'s>,
    >(
        &self,
        job: &Job<D, R>,
    ) -> Result<Option<i64>, BullError> {
        let keys = self.get_keys(&[
            "wait",
            "paused",
            "meta",
            "id",
            "completed",
            "active",
            "events",
            "marker",
        ]);
        let (packed_args, json_data, packed_opts) = self.add_job_args(job, None)?;
        let mut conn = self.connection.get().await?;
        let mut script_runner = self
            .commands
            .get("addStandardJob")
            .unwrap()
            .prepare_invoke();
        let result = script_runner
            .key(keys)
            .arg(packed_args)
            .arg(json_data)
            .arg(packed_opts)
            .invoke_async(&mut conn)
            .await?;

        Ok(result)
    }

    async fn add_delayed_job<
        's,
        D: Serialize + Clone + Deserialize<'s>,
        R: Serialize + Clone + Deserialize<'s>,
    >(
        &self,
        job: &Job<D, R>,
    ) -> Result<Option<i64>, BullError> {
        let keys = self.get_keys(&["marker", "meta", "id", "delayed", "completed", "events"]);
        let (packed_args, json_data, packed_opts) = self.add_job_args(job, None)?;
        let mut conn = self.connection.get().await?;
        let mut script_runner = self.commands.get("addDelayedJob").unwrap().prepare_invoke();
        let result = script_runner
            .key(keys)
            .arg(packed_args)
            .arg(json_data)
            .arg(packed_opts)
            .invoke_async(&mut conn)
            .await?;
        Ok(result)
    }

    async fn add_prioritized_job<
        's,
        D: Serialize + Clone + Deserialize<'s>,
        R: Serialize + Clone + Deserialize<'s>,
    >(
        &self,
        job: &Job<D, R>,
    ) -> Result<Option<i64>, BullError> {
        let keys = self.get_keys(&[
            "marker",
            "meta",
            "id",
            "prioritized",
            "completed",
            "active",
            "events",
            "pc",
        ]);
        let (packed_args, json_data, packed_opts) = self.add_job_args(job, None)?;
        let mut conn = self.connection.get().await?;
        let mut script_runner = self
            .commands
            .get("addPrioritizedJob")
            .unwrap()
            .prepare_invoke();
        let result = script_runner
            .key(keys)
            .arg(packed_args)
            .arg(json_data)
            .arg(packed_opts)
            .invoke_async(&mut conn)
            .await?;
        Ok(result)
    }

    async fn add_parent_job<
        's,
        D: Serialize + Clone + Deserialize<'s>,
        R: Serialize + Clone + Deserialize<'s>,
    >(
        &self,
        job: &Job<D, R>,
        with_children_key: Option<String>,
    ) -> Result<Option<i64>, BullError> {
        let keys = self.get_keys(&[
            "marker",
            "meta",
            "id",
            "prioritized",
            "completed",
            "active",
            "events",
            "pc",
        ]);
        let (packed_args, json_data, packed_opts) = self.add_job_args(job, with_children_key)?;
        let mut conn = self.connection.get().await?;
        let mut script_runner = self.commands.get("addParentJob").unwrap().prepare_invoke();
        let result = script_runner
            .key(keys)
            .arg(packed_args)
            .arg(json_data)
            .arg(packed_opts)
            .invoke_async(&mut conn)
            .await?;
        Ok(result)
    }

    pub async fn pause(&self, pause: bool) -> Result<(), BullError> {
        let src = if pause { "wait" } else { "paused" };
        let dst = if pause { "paused" } else { "wait" };
        let keys = self.get_keys(&[src, dst, "meta", "events"]);
        let f_ags = if pause {
            "paused".as_bytes()
        } else {
            "resumed".as_bytes()
        };
        let mut conn = self.connection.get().await?;
        self.commands
            .get("pause")
            .unwrap()
            .key(keys)
            .arg(f_ags)
            .invoke_async(&mut conn)
            .await?;
        Ok(())
    }
    pub async fn retry_jobs(
        &self,
        state: String,
        count: i64,
        timestamp: i64,
    ) -> Result<i8, BullError> {
        let current = if !state.is_empty() {
            state
        } else {
            String::from("failed")
        };
        let count = if count > 0 { count } else { 1000 };
        let timestamp = if timestamp > 0 {
            timestamp
        } else {
            (timestamp * 1000)
        };
        let keys = self.get_keys(&["", "events", &current, "wait", "paused", "meta"]);
        let mut conn = self.connection.get().await?;
        let result = self
            .commands
            .get("retryJobs")
            .unwrap()
            .key(keys)
            .arg(count)
            .arg(timestamp)
            .arg(current)
            .invoke_async(&mut conn)
            .await?;
        Ok(result)
    }
    pub async fn obliterate(&self, count: i64, force: bool) -> Result<i64, BullError> {
        use crate::enums::QueueError;
        let count = if count > 0 { count } else { 1000 };
        let mut connection = self.connection.get().await?;
        let keys = self.get_keys(&["meta", ""]);
        let result = self
            .commands
            .get("obliterate")
            .unwrap()
            .key(keys)
            .arg(count)
            .invoke_async(&mut connection)
            .await?;

        if result == -1 {
            return Err(QueueError::FailedToObliterate.into());
        } else if result == -2 {
            return Err(QueueError::CantObliterateWhileJobsActive.into());
        }
        Ok(result)
    }

    pub async fn move_to_active(
        &self,
        token: &str,
        opts: &WorkerOptions,
    ) -> Result<MoveToAciveResult, BullError> {
        let id: u16 = rand::random();
        let timestamp = generate_timestamp()?;
        let lock_duration = opts.lock_duration;
        let limiter = opts.limiter.clone();
        let e = &self.prefix.clone();
        let prefix = self.to_key("");

        // let limiter = opts.
        let keys = self.get_keys(&[
            "wait",
            "active",
            "prioritized",
            "events",
            "stalled",
            "limiter",
            "delayed",
            "paused",
            "meta",
            "pc",
            "marker",
        ]);

        let move_opts = JobMoveOpts {
            token: token.to_owned(),
            lock_duration,
            limiter: Some(limiter),
        };

        let mut connection = self.connection.get().await?;
        let packed_opts = rmp_serde::encode::to_vec_named(&move_opts)?;

        let result: MoveToAciveResults = self
            .commands
            .get("moveToActive")
            .unwrap()
            .key(keys)
            .arg(prefix)
            .arg(timestamp)
            .arg(packed_opts)
            .invoke_async(&mut connection)
            .await?;

        let final_result = result.into();
        dbg!(&final_result);
        Ok(final_result)
    }

    #[allow(clippy::too_many_arguments)]
    async fn move_to_finished<D: Serialize + Clone, R: Any + Send + Sync + Clone + 'static>(
        &self,
        job: &mut Job<D, R>,
        val: String,
        prop_val: &str,
        should_remove: RemoveOnCompletionOrFailure,
        target: &str,
        token: &str,
        opts: &WorkerOptions,
        fetch_next: bool,
    ) -> Result<MoveToFinishedResults, BullError> {
        let timestamp = generate_timestamp()?;
        let metrics_key = self.to_key(&format!("metrics:{target}"));
        let mut keys = self.get_keys(&[
            "wait",
            "active",
            "prioritized",
            "events",
            "stalled",
            "limiter",
            "delayed",
            "paused",
            "meta",
            "pc",
            target,
        ]);

        keys.push(self.to_key(job.id.as_str()));
        keys.push(metrics_key);
        keys.push(self.to_key("marker"));

        let should_remove_key = if target == "completed" {
            opts.remove_on_completion.clone()
        } else {
            opts.remove_on_fail.clone()
        };
        let keep_jobs = self.get_keep_jobs(should_remove_key);

        let max_metrics_size = opts.metrics.clone().map(|m| m.max_data_points.to_string());

        let move_to_finished_opts = MoveToFinishOpts {
            token: token.to_string(),
            keep_jobs,
            attempts_made: job.attempts_made,
            attempts: job.attempts,
            lock_duration: opts.lock_duration,
            max_metrics_size,
            limiter: opts.limiter.clone(),
            fail_parent_on_failure: job.opts.fail_parent_on_failure,
            remove_deps_on_failure: job.opts.remove_deps_on_failure,
        };
        let fetch = if fetch_next { "1" } else { "" };
        let d = &self.to_key("");
        let mut sec_last = self.to_key("");

        let mut connection = self.connection.get().await?;

        let packed_opts = rmp_serde::encode::to_vec_named(&move_to_finished_opts)?;
        let result: MoveToFinishedResults = self
            .commands
            .get("moveToFinished")
            .unwrap()
            .key(keys)
            .arg(job.id.as_str())
            .arg(timestamp)
            .arg(prop_val)
            .arg(&val)
            .arg(target)
            .arg(fetch)
            .arg(sec_last)
            .arg(packed_opts)
            .invoke_async(&mut connection)
            .await?;
        dbg!(&result);
        if let MoveToFinishedResults::Error(code) = result {
            finished_errors(code, &job.id, "finished", "active");
        }

        Ok(result)
    }

    //create a function that generates timestamps;

    fn get_keep_jobs(&self, should_remove: RemoveOnCompletionOrFailure) -> KeepJobs {
        match should_remove {
            RemoveOnCompletionOrFailure::Bool(bool) => {
                if bool {
                    KeepJobs {
                        age: None,
                        count: Some(0),
                    }
                } else {
                    KeepJobs {
                        age: None,
                        count: Some(-1),
                    }
                }
            }
            RemoveOnCompletionOrFailure::Int(num) => KeepJobs {
                count: Some(num),
                age: None,
            },
            RemoveOnCompletionOrFailure::Opts(keep) => keep,
            RemoveOnCompletionOrFailure::None => KeepJobs {
                count: Some(-1),
                age: None,
            },
        }
    }

    fn array_to_object(&self, arr: &[String]) -> HashMap<String, String> {
        let mut result = HashMap::new();
        for (i, v) in arr.iter().enumerate() {
            if i % 2 == 0 {
                result.insert(v.to_string(), arr[i + 1].to_string());
            }
        }
        result
    }
    fn raw_to_next_job_data(&self, raw: &[Vec<String>]) -> NextJobData {
        let len = raw.len();
        if !raw.is_empty() {
            // check if the  first element is present in the array;
            if len > 1 {
                return Some((
                    Some(self.array_to_object(&raw[0])),
                    Some(self.array_to_object(&raw[1])),
                ));
            }
            return Some((None, Some(self.array_to_object(&raw[0]))));
        }
        None
    }

    pub async fn extend_lock(
        &self,
        job_id: &str,
        token: &str,
        duration: i64,
    ) -> Result<u64, BullError> {
        let stalled = self.to_key("stalled");
        let keys = vec![self.to_key(job_id) + ":lock", stalled];
        let mut conn = self.connection.get().await?;
        let extend_lock_result: u64 = self
            .commands
            .get("extendLock")
            .unwrap()
            .key(keys)
            .arg(token)
            .arg(duration.to_string())
            .arg(job_id)
            .invoke_async(&mut conn)
            .await?;
        dbg!(extend_lock_result);
        Ok(extend_lock_result)
    }
    pub async fn move_stalled_jobs_to_wait(
        &self,
        max_stalled_count: i64,
        stalled_interval: i64,
    ) -> Result<(Vec<String>, Vec<String>), BullError> {
        let prefix = self.to_key("");
        let timestamp = generate_timestamp()?;
        let keys = self.get_keys(&[
            "stalled",
            "wait",
            "active",
            "failed",
            "stalled-check",
            "meta",
            "paused",
            "marker",
            "events",
        ]);
        let mut conn = self.connection.get().await?;
        let result = self
            .commands
            .get("moveStalledJobsToWait")
            .unwrap()
            .key(keys)
            .arg(max_stalled_count)
            .arg(prefix)
            .arg(timestamp)
            .arg(stalled_interval)
            .invoke_async(&mut conn)
            .await?;

        Ok(result)
    }
    pub async fn update_progress<P: Serialize>(
        &self,
        job_id: &str,
        progress: P,
    ) -> Result<Option<i8>, BullError> {
        let keys = [self.to_key(job_id), self.to_key("events")];

        let progress = serde_json::to_string(&progress)?;
        let mut conn = self.connection.get().await?;
        let result: Option<i8> = self
            .commands
            .get("updateProgress")
            .unwrap()
            .key(&keys)
            .arg(job_id)
            .arg(progress)
            .invoke_async(&mut conn)
            .await?;

        match result {
            Some(val) => {
                if val >= 0 {
                    return Ok(Some(val));
                }
                Err(finished_errors(val, job_id, "updateProgress", "active"))
            }
            None => Ok(None),
        }
    }

    pub async fn promote(&self, job_id: &str) -> Result<Option<i8>, BullError> {
        let mut keys = self.get_keys(&[
            "delayed",
            "wait",
            "paused",
            "meta",
            "prioritixed",
            "pc",
            "events",
            "marker",
        ]);

        keys.push(self.to_key(job_id));
        keys.push(self.to_key("events"));
        keys.push(self.to_key("paused"));
        keys.push(self.to_key("meta"));
        let prefix = self.to_key("");

        let mut conn = self.connection.get().await?;
        let result: Option<i8> = self
            .commands
            .get("promote")
            .unwrap()
            .key(&keys)
            .arg(prefix)
            .arg(job_id)
            //.arg(progress)
            .invoke_async(&mut conn)
            .await?;

        match result {
            Some(val) => {
                if val >= 0 {
                    return Ok(Some(val));
                }
                Err(finished_errors(val, job_id, "promote", "delayed"))
            }
            None => Ok(None),
        }
    }
    pub async fn move_to_completed<
        's,
        D: Serialize + Clone,
        R: FromRedisValue + Send + Sync + Clone + 'static,
    >(
        &self,
        job: &mut Job<D, R>,
        val: String,
        remove_on_complete: RemoveOnCompletionOrFailure,
        token: &str,
        opts: &WorkerOptions,
        fetch_next: bool,
    ) -> Result<MoveToFinishedResults, BullError> {
        self.move_to_finished(
            job,
            val,
            "returnedvalue",
            remove_on_complete,
            "completed",
            token,
            opts,
            fetch_next,
        )
        .await
    }
    pub async fn move_to_failed<D: Serialize + Clone, R: Any + Send + Sync + 'static + Clone>(
        &self,
        job: &mut Job<D, R>,
        failed_reason: String,
        remove_on_failure: RemoveOnCompletionOrFailure,
        token: &str,
        opts: &WorkerOptions,
        fetch_next: bool,
    ) -> Result<MoveToFinishedResults, BullError> {
        self.move_to_finished(
            job,
            failed_reason,
            "failedReason",
            remove_on_failure,
            "failed",
            token,
            opts,
            fetch_next,
        )
        .await
    }

    pub async fn get_counts(
        &self,
        mut types: impl Iterator<Item = &str>,
    ) -> Result<Vec<i64>, BullError> {
        let keys = self.get_keys(&[""]);
        let mut conn = self.connection.get().await?;
        let transformed_types: Vec<_> = types
            .map(|t| if t == "waiting" { "wait" } else { t })
            .collect();
        let result: Vec<_> = self
            .commands
            .get("getCounts")
            .unwrap()
            .key(keys)
            .arg(transformed_types)
            .invoke_async(&mut conn)
            .await?;
        Ok(result)
    }
    pub async fn get_ranges(
        &self,
        types: &[&str],
        start: isize,
        end: isize,
        asc: bool,
    ) -> Result<Vec<Vec<String>>, BullError> {
        use maplit::hashmap;
        let mut commands = vec![];
        let switcher = hashmap! {"completed" => "zrange",
           "delayed" => "zrange",
           "failed" => "zrange",
           "priority" => "zrange",
           "repeat" => "zrange",
           "waiting-children" => "zrange",
           "active" => "lrange",
           "paused" => "lrange",
           "wait" => "lrange"
        };
        let mut transformed_types = vec![];
        types.iter().for_each(|ty| {
            let transformed_type = if ty == &"waiting" { "wait" } else { ty };
            transformed_types.push(transformed_type);
            if let Some(value) = switcher.get(transformed_type) {
                commands.push(*value);
            }
        });
        let asc_bool = if asc { "1" } else { "0" };

        let prefix = self.get_keys(&[""]);
        let mut conn = self.connection.get().await?;
        let mut responses: Vec<Vec<String>> = self
            .commands
            .get("getRanges")
            .unwrap()
            .key(prefix)
            .arg(start)
            .arg(end)
            .arg(asc_bool)
            .arg(transformed_types)
            .invoke_async(&mut conn)
            .await?;

        responses.iter_mut().enumerate().for_each(|(idx, list)| {
            if let Some(value) = commands.get(idx) {
                if asc && value == &"lrange" {
                    list.reverse();
                }
            }
        });

        Ok(responses)
    }
    pub fn move_to_delayed_args(
        &self,
        job_id: &str,
        timestamp: i64,
        token: &str,
    ) -> Result<(Vec<String>, Vec<String>), BullError> {
        use std::cmp::max;
        let mut max_timestamp = max(0, timestamp);
        let int = job_id.parse::<i64>()?;
        if timestamp > 0 {
            max_timestamp = max_timestamp * 0x1000 + (int & 0xfff);
        }
        let mut keys = self.get_keys(&["wait", "active", "priority", "delayed"]);
        keys.push(self.to_key(job_id));
        keys.push(self.to_key("events"));
        keys.push(self.to_key("paused"));
        keys.push(self.to_key("meta"));

        let current_timestamp = generate_timestamp()?;
        let first_key = self.keys.get("").unwrap().to_owned();
        let args = vec![
            first_key,
            current_timestamp.to_string(),
            max_timestamp.to_string(),
            job_id.to_string(),
            token.to_string(),
        ];
        Ok((keys, args))
    }
    pub async fn update_data<D: Serialize + Clone>(
        &self,
        job_id: &str,
        data: D,
    ) -> Result<i64, BullError> {
        let keys = [self.to_key(job_id)];
        let json_data = serde_json::to_string(&data)?;
        let mut con = self.connection.get().await?;
        let result = self
            .commands
            .get("updateData")
            .unwrap()
            .key(&keys)
            .arg(json_data)
            .invoke_async(&mut con)
            .await?;

        Ok(result)
    }

    pub async fn get_state(&self, job_id: &str) -> Result<String, BullError> {
        let keys = self.get_keys(&[
            "completed",
            "failed",
            "delayed",
            "active",
            "wait",
            "paused",
            "waiting-children",
            "prioritized",
        ]);
        let mut conn = self.connection.get().await?;
        let version = get_server_version(&mut conn).await?;

        // use semver to compare the version of the redis server
        // if the version is greater than 6.2.0 then use the getStateV2 script
        let script = if *version > *"6.0.6" {
            "getStateV2"
        } else {
            "getState"
        };

        let args = vec![job_id.to_string(), self.to_key(job_id)];
        let mut conn = self.connection.get().await.unwrap();
        let result: String = self
            .commands
            .get(script)
            .unwrap()
            .key(keys)
            .arg(args)
            .invoke_async(&mut conn)
            .await?;
        Ok(result)
    }

    pub async fn is_job_in_list(&self, list_key: &str, job_id: &str) -> Result<bool, BullError> {
        let mut result: Vec<String> = vec![];
        let mut conn = self.connection.get().await?;
        let version = self.redis_version.clone();

        // use semver to compare the version of the redis server
        // if the version is greater than 6.2.0 then use the getStateV2 script
        if *version > *"6.0.6" {
            let keys = [list_key];
            result = self
                .commands
                .get("getState")
                .unwrap()
                .key(&keys)
                .arg(job_id)
                .invoke_async(&mut conn)
                .await?;
        } else {
            let id: NonZero<usize> = job_id.parse()?;
            result = redis::Cmd::lpop(list_key, Some(id))
                .query_async(&mut conn)
                .await?;
        };

        Ok(result.contains(&job_id.to_owned()))
    }

    pub async fn remove(&self, job_id: String, remove_children: bool) -> Result<(), BullError> {
        let prefix = self.to_key("");

        //mutate the first value in the keys array

        let mut conn = self.connection.get().await?;
        self.commands
            .get("remove")
            .unwrap()
            .key(vec![prefix])
            .arg(job_id)
            .invoke_async(&mut conn)
            .await?;

        Ok(())
    }
}

pub fn print_type_of<T>(_: &T) -> String {
    std::any::type_name::<T>().to_string()
}

type Map = HashMap<String, String>;
pub type NextJobData = Option<(Option<Map>, Option<Map>)>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MoveToAciveResults {
    RateLimitedOrProcessMore((i32, i32, i32)), // (0,0,time) -> process_more,
    NextDelayedJob((i32, i32, i32, u64)),
    NextJobData((Vec<String>, String, u64)), // (data, jobId, expireTime)
}

impl FromRedisValue for MoveToAciveResults {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        use std::result::Result::Ok;
        match *v {
            // If the Redis value is a bulk string or a list, try to match the expected structure
            Value::Array(ref items) if items.len() == 3 => {
                if let (Value::Int(a), Value::Int(b), Value::Int(c)) =
                    (&items[0], &items[1], &items[2])
                {
                    return Ok(MoveToAciveResults::RateLimitedOrProcessMore((
                        *a as i32, *b as i32, *c as i32,
                    )));
                } else if let (Value::Int(a), Value::Int(b), Value::Int(c), Value::Int(d)) = (
                    &items[0],
                    &items[1],
                    &items[2],
                    &items.get(3).unwrap_or(&Value::Nil),
                ) {
                    return Ok(MoveToAciveResults::NextDelayedJob((
                        *a as i32, *b as i32, *c as i32, *d as u64,
                    )));
                } else if let (
                    Value::Array(data_strings),
                    Value::BulkString(job_id),
                    Value::Int(expire_time),
                ) = (&items[0], &items[1], &items[2])
                {
                    let string_data: Vec<String> = data_strings
                        .iter()
                        .map(|v| redis::from_redis_value(v).expect("failed to parse message"))
                        .collect();
                    return Ok(MoveToAciveResults::NextJobData((
                        string_data,
                        String::from_utf8(job_id.clone()).unwrap(),
                        *expire_time as u64,
                    )));
                }
            }
            _ => {}
        }

        Err(redis::RedisError::from((
            redis::ErrorKind::TypeError,
            "Invalid type for MoveToActiveResults",
        )))
    }
}

/// MoveToAciveResult(next_job_data,job_id, expireTime, delay)
pub type MoveToAciveResult = (Option<Vec<String>>, Option<String>, i64, Option<i64>);

impl From<MoveToAciveResults> for MoveToAciveResult {
    fn from(value: MoveToAciveResults) -> Self {
        match value {
            MoveToAciveResults::RateLimitedOrProcessMore((_, _, pttl)) => {
                (None, None, 0, Some(pttl as i64))
            }
            MoveToAciveResults::NextDelayedJob((_, _, _, next_ts)) => {
                (None, None, 0, Some(next_ts as i64))
            }
            MoveToAciveResults::NextJobData((data, job_id, expire)) => {
                (Some(data), Some(job_id), expire as i64, None)
            }
        }
    }
}
/// MoveToFinishedResult (finished_job_data,job_id, expireTime, delay
pub type MoveToFinishedResult = (Option<crate::JobJsonRaw>, Option<String>, i64, Option<i64>);

// test
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MoveToFinishedResults {
    MoveToNext(MoveToAciveResults),
    Completed,
    Error(i8),
}

impl FromRedisValue for MoveToFinishedResults {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        use std::result::Result::Ok;
        match *v {
            Value::Int(0) => {
                // If the integer is 0, it indicates "Completed"
                Ok(MoveToFinishedResults::Completed)
            }
            Value::Int(err_code) => {
                // If it's a non-zero integer, it indicates an error code
                Ok(MoveToFinishedResults::Error(err_code as i8))
            }
            _ => {
                // Try to interpret it as a `MoveToActiveResults`
                match MoveToAciveResults::from_redis_value(v) {
                    Ok(result) => Ok(MoveToFinishedResults::MoveToNext(result)),
                    Err(err) => Err(err),
                }
            }
        }
    }
}

pub fn get_script(script_name: &'static str) -> String {
    use std::fs;
    fs::read_to_string(format!("commands/{script_name}"))
        .expect("Something went wrong reading the file")
}

#[cfg(test)]
#[test]
fn test_get_script() {
    let script = get_script("addJob-8.lua");
    assert!(!script.is_empty());
}

pub fn generate_timestamp() -> Result<u64, BullError> {
    use std::time::SystemTime;
    let result = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_millis() as u64;

    Ok(result)
}

// write a function that return the version of the redis server
pub async fn get_server_version(conn: &mut Connection) -> Result<String, BullError> {
    let result: String = redis::cmd("INFO").arg("server").query_async(conn).await?;
    for line in result.lines() {
        if line.starts_with("redis_version") {
            let version = line.split(':').nth(1).unwrap();
            return Ok(version.to_string());
        }
    }
    Ok("".to_string())
}

pub fn finished_errors(num: i8, job_id: &str, command: &str, _state: &str) -> BullError {
    let cod_err = JobError::try_from(num).unwrap();
    use JobError::*;

    cod_err.into()
}

// Convert MoveToAcive to MoveToFinished;

pub fn convert_errors(
    active_to_active: MoveToAciveResult,
) -> Result<MoveToFinishedResult, BullError> {
    match active_to_active {
        (list, job_id, limit_until, delay) => {
            let mut job: Option<JobJsonRaw> = None;
            // convert the length to a a raw job;
            if let Some(vec_list) = list {
                let map = map_from_vec(&vec_list);

                let json = JobJsonRaw::from_map(map)?;
                job = Some(json);
            }

            Ok((job, job_id, limit_until, delay))
        }
        _ => Err(BullError::ConversionError {
            from: "move_to_active_results",
            to: "move_to_finished_result",
        }),
    }
}
