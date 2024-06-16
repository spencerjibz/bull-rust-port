#![allow(clippy::too_many_arguments)]
use crate::add_job::add_job_to_queue;
use crate::add_job3::add_job_to_queue_3;
use crate::move_stalled::move_stalled_jobs;
use crate::move_to_active::move_job_to_active;
use crate::move_to_finished::{move_job_to_finished, MoveToFinishedArgs};
use crate::worker::{map_from_string, map_from_vec};
//use crate::move_to_finished::{self, move_job_to_finished};
use crate::{
    script_functions, job, JobJsonRaw, JobMoveOpts, JobOptions, KeepJobs, Limiter, MoveToFinishOpts,
    WorkerOptions,
};
use crate::{job::Job, redis_connection::*};
use anyhow::Error;
use anyhow::Ok;
use anyhow::Result;
use futures::{future::ok, stream::TryFilterMap};
use maplit::hashmap;
pub use redis::Script;
use redis::{FromRedisValue, RedisResult, ToRedisArgs, Value};
use serde::{Deserialize, Serialize};

use std::{collections::HashMap, time};
type ScriptCommands = HashMap<&'static str, Script>;
use crate::enums::ErrorCode::{self, *};
use crate::redis_connection::*;
use std::any::{self, Any};
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
            .field("commands", &self.commands)
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
            "priority",
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
            "moveToActive" =>  Script::new(&get_script("moveToActive-10.lua")),
            "moveToFinished" =>  Script::new(&get_script("moveToFinished-12.lua")),
            "moveStalledJobsToWait"=>  Script::new(&get_script("moveStalledJobsToWait-8.lua")),
            "retryJobs" => Script::new(&get_script("retryJobs-6.lua")),
            "updateProgress"=>  Script::new(&get_script("updateProgress-2.lua")),
            "remove" => Script::new(&get_script("removeJob-1.lua")),
            "getState" => Script::new(&get_script("getState-7.lua")),
            "getStateV2" => Script::new(&get_script("getStateV2-7.lua")),

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
        if name.is_empty() {
            return format!("{}:{}", self.prefix, self.queue_name);
        }
        format!("{}:{}:{}", self.prefix, self.queue_name, name)
    }
    pub fn get_keys(&self, keys: &[&str]) -> Vec<String> {
        keys.iter()
            .map(|&k| String::from(self.keys.get(k).unwrap_or(&String::new())))
            .collect()
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
    ) -> anyhow::Result<(Vec<String>, Vec<String>)> {
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
    pub async fn add_job<'s, D: Serialize + Clone + Deserialize<'s>, R: FromRedisValue>(
        &mut self,
        job: &Job<D, R>,
    ) -> anyhow::Result<Option<i64>> {
        let e = job.queue.prefix.to_owned();
        let prefix = self.keys.get("").unwrap_or(&e);
        let name = job.name;
        let parent = job.parent.clone();
        let parent_key = job.parent_key.clone();

        let json_data = serde_json::to_string(&job.data.clone())?;

        let parent_dep_key = parent_key.as_ref().map(|v| format!("{v}:dependencies"));

        let keys = self.get_keys(&[
            "wait",
            "paused",
            "meta",
            "id",
            "delayed",
            "priority",
            "completed",
            "events",
            "pc",
        ]);

        let mut conn = self.connection.get().await?;
        // get the redis_version here;
        let version = get_server_version(&mut conn).await?;
        self.redis_version = version;

        let mut packed_args: HashMap<&str, String> = HashMap::new();

        packed_args.insert("prefix", prefix.to_owned());
        packed_args.insert("job_id", job.id.clone());

        packed_args.insert("name", job.name.to_owned());
        packed_args.insert("timestamp", job.timestamp.to_string());
        // write Optional parent key
        if let Some(key) = job.parent_key.as_deref() {
            packed_args.insert("parent_key", key.to_owned());
        }
        // write with  parent_deps;
        if let Some(pk) = &parent_key {
            let pk = format!("{}:dependencies", pk);
            packed_args.insert("parent_dep_key", pk.to_owned());
        }
        if let Some(val) = parent {
            let parent_data = serde_json::to_string(&val)?;
            packed_args.insert("parent_data", parent_data);
        }

        let result = add_job_to_queue(&keys, packed_args, json_data, &job.opts, &mut conn).await?;
        Ok(result)
    }
    pub async fn pause<R: FromRedisValue>(&mut self, pause: bool) -> anyhow::Result<R> {
        let src = if pause { "wait" } else { "paused" };
        let dst = if pause { "paused" } else { "wait" };
        let keys = self.get_keys(&[src, dst, "meta", "events"]);
        let f_ags = if pause {
            "paused".as_bytes()
        } else {
            "resumed".as_bytes()
        };
        let mut conn = self.connection.get().await?;
        let result = self
            .commands
            .get("pause")
            .unwrap()
            .key(keys)
            .arg(f_ags)
            .invoke_async(&mut conn)
            .await?;
        Ok(result)
    }
    pub async fn retry_jobs<R: FromRedisValue>(
        &mut self,
        state: String,
        count: i64,
        timestamp: i64,
    ) -> anyhow::Result<R> {
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
        let keys = self.get_keys(&["", &current, "wait", "paused", "meta"]);
        let mut conn = self.connection.get().await?;
        let result = self
            .commands
            .get("retry_jobs")
            .unwrap()
            .key(keys)
            .arg(count)
            .arg(timestamp)
            .arg(current)
            .invoke_async(&mut conn)
            .await?;
        Ok(result)
    }
    pub async fn obliterate(&mut self, count: i64, force: bool) -> anyhow::Result<i64> {
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
            panic!("Cannot obliterate non-paused queue")
        } else if result == -2 {
            panic!("cannot obliterate queue with active jobs")
        }
        Ok(result)
    }

    pub async fn move_to_active(
        &mut self,
        token: &str,
        opts: &WorkerOptions,
        job_id: Option<String>,
    ) -> anyhow::Result<MoveToAciveResult> {
        use std::time::SystemTime;
        let id: u16 = rand::random();
        let timestamp = generate_timestamp()?;
        let lock_duration = opts.lock_duration;
        let limiter = opts.limiter.clone();
        let e = &self.prefix.clone();
        let prefix = self.to_key("");

        // let limiter = opts.
        let keys = self.get_keys(&[
            "wait", "active", "priority", "events", "stalled", "limiter", "delayed", "paused",
            "meta", "pc",
        ]);

        let move_opts = JobMoveOpts {
            token: token.to_owned(),
            lock_duration,
            limiter: Some(limiter),
        };

        let mut con = &mut self.connection.get().await?;
        let args = (prefix.to_owned(), timestamp as i64, job_id, move_opts);

        let result = move_job_to_active(&keys, args, con).await?;
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    async fn move_to_finished<
        D: Serialize + Clone,
        R: FromRedisValue + Any + Send + Sync + Clone + 'static,
    >(
        &mut self,
        job: &mut Job<D, R>,
        val: String,
        prop_val: &str,
        should_remove: bool,
        target: &str,
        token: &str,
        opts: &WorkerOptions,
        fetch_next: bool,
    ) -> anyhow::Result<MoveToFinishedResult> {
        let timestamp = generate_timestamp()?;
        let metrics_key = self.to_key(&format!("metrics:{target}"));
        let mut keys = self.get_keys(&[
            "wait", "active", "priority", "events", "stalled", "limiter", "delayed", "paused",
            "meta", "pc", target,
        ]);
        keys.push(self.to_key(job.id.as_str()));
        keys.push(self.to_key("meta"));
        keys.push(metrics_key);

        let keep_jobs = self.get_keep_jobs(should_remove);

        let max_metrics_size = match &opts.metrics {
            Some(v) => v.max_data_points,
            None => 0,
        };
        /*
                let p_opts = serde_json::to_string(&hashmap! {
                    "token" => token.to_string(),
                    "keepJobs" => serde_json::to_string(&keep_jobs).unwrap(),
                    "limiter" => serde_json::to_string(&opts.limiter).unwrap(),
                    "lockDuration" => opts.lock_duration.to_string(),
                    "attempts" => job.attempts.to_string(),
                    "attemptsMade" => job.attempts_made.to_string(),
                    "maxMetricsSize" => max_metrics_size.to_string(),
                    "fpof" => job.opts.fail_parent_on_failure.to_string(), //fail parent on failure
                })?;
        */
        let move_to_finished_opts = MoveToFinishOpts {
            token: token.to_string(),
            keep_jobs: KeepJobs::default(),
            attempts_made: job.attempts_made,
            attempts: job.attempts,
            lock_duration: opts.lock_duration,
            max_metrics_size,
            limiter: opts.limiter.clone(),
            fail_parent_on_failure: job.opts.fail_parent_on_failure,
            remove_deps_on_failure: job.opts.remove_deps_on_failure,
        };
        let fetch = if fetch_next { "fetch" } else { "" };
        let d = &self.to_key("");
        let mut sec_last = self.to_key("");

        let mut connection = &mut self.connection.get().await?;

        let args = (
            job.id.clone(),
            timestamp as i64,
            prop_val.to_owned(),
            val.clone(),
            target.to_string(),
            Some("".to_owned()),
            fetch_next,
            sec_last.to_owned(),
            move_to_finished_opts,
        );
        /*           encode::write_bin(&mut packed_opts, p_opts.as_bytes())?;
             let result: Option<R> = self
                 .commands
                 .get("moveToFinished")
                 .unwrap()
                 .key(keys)
                 .arg(job.id.as_str())
                 .arg(timestamp)
                 .arg(prop_val)
                 .arg(&val)
                 .arg(target)
                 .arg("")
                 .arg(fetch)
                 .arg(sec_last)
                 .arg(packed_opts)
                 .invoke_async(connection)
                 .await?;
        */

        move_job_to_finished(&keys, args, connection).await

        /*
        use std::any::{Any, TypeId};
        if let Some(res) = result {
            if TypeId::of::<i8>() == res.type_id() {
                let n = Box::new(res.clone()) as Box<dyn Any>;
                let n = n.downcast::<i8>().unwrap();
                if *n < 0 {
                    self.finished_errors(*n, &job.id, "finished", "active");
                }
            }
            // I do not like this as it is using a sideeffect
            job.finished_on = timestamp as i64;
            let m: HashMap<String, String> = HashMap::new();
            let expected = (Some(m), Some("".to_string()), 0_u64, Some(1_u64));

            let slice_of_string = print_type_of(&expected);

            if print_type_of(&res) == slice_of_string {
                let n = Box::new(res) as Box<dyn Any>;
                let n = n.downcast::<MoveToAciveResult>().unwrap();
                let v = *n;

                let returned_result = v;

                return Ok(Some(returned_result));
            }
        }
        */
    }

    //create a function that generates timestamps;

    fn get_keep_jobs(&self, should_remove: bool) -> KeepJobs {
        if should_remove {
            return KeepJobs {
                age: None,
                count: Some(0),
            };
        }
        KeepJobs {
            age: None,
            count: Some(-1),
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
    fn raw_to_next_job_data(&self, mut raw: &Vec<Vec<String>>) -> NextJobData {
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
        &mut self,
        job_id: &str,
        token: &str,
        duration: i64,
    ) -> anyhow::Result<u64> {
        let stalled = self.keys.get("stalled").unwrap();
        let keys = vec![self.to_key(job_id) + ":lock", stalled.to_string()];
        let conn = &mut self.connection.get().await?;
        let result: u64 = self
            .commands
            .get("extendLock")
            .unwrap()
            .key(keys)
            .arg(token)
            .arg(duration.to_string())
            .arg(job_id)
            .invoke_async(conn)
            .await?;
        Ok(result)
    }
    pub async fn move_stalled_jobs_to_wait(
        &mut self,
        max_stalled_count: i64,
        stalled_interval: i64,
    ) -> anyhow::Result<(Vec<String>, Vec<String>)> {
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
            "events",
        ]);
        let conn = &mut self.connection.get().await?;
        let result = self
            .commands
            .get("moveStalledJobsToWait")
            .unwrap()
            .key(keys)
            .arg(max_stalled_count)
            .arg(prefix)
            .arg(timestamp)
            .arg(stalled_interval)
            .invoke_async(conn)
            .await?;
        /*

           let result = check_stalled_jobs(
            &keys,
            max_stalled_count as usize,
            &prefix,
            timestamp,
            stalled_interval,
            con,
        )
        .await?;

            */
        // println!(" stalled {result:?}");
        Ok(result)
    }
    pub async fn update_progress<P: Serialize>(
        &mut self,
        job_id: &str,
        progress: P,
    ) -> anyhow::Result<Option<i8>> {
        let keys = [
            self.to_key(job_id),
            self.keys.get("events").unwrap().to_string(),
        ];

        let progress = serde_json::to_string(&progress)?;
        let conn = &mut self.connection.get().await?;
        let result: Option<i8> = self
            .commands
            .get("updateProgress")
            .unwrap()
            .key(&keys)
            .arg(job_id)
            .arg(progress)
            .invoke_async(conn)
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
    pub async fn move_to_completed<
        's,
        D: Serialize + Clone,
        R: FromRedisValue + Send + Sync + Clone + 'static,
    >(
        &mut self,
        job: &mut Job<D, R>,
        val: String,
        remove_on_complete: bool,
        token: &str,
        opts: &WorkerOptions,
        fetch_next: bool,
    ) -> anyhow::Result<MoveToFinishedResult> {
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
    pub async fn move_to_failed<
        D: Serialize + Clone,
        R: FromRedisValue + Any + Send + Sync + 'static + Clone,
    >(
        &mut self,
        job: &mut Job<D, R>,
        failed_reason: String,
        remove_on_failure: bool,
        token: &str,
        opts: &WorkerOptions,
        fetch_next: bool,
    ) -> anyhow::Result<MoveToFinishedResult> {
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

    pub async fn get_counts(&mut self, mut types: impl Iterator<Item = &str>) -> Result<Vec<i64>> {
        let keys = self.get_keys(&[""]);
        let conn = &mut self.connection.get().await?;
        let transformed_types: Vec<_> = types
            .map(|t| if t == "waiting" { "wait" } else { t })
            .collect();
        let result: Vec<_> = self
            .commands
            .get("getCounts")
            .unwrap()
            .key(keys)
            .arg(transformed_types)
            .invoke_async(conn)
            .await?;
        Ok(result)
    }

    pub fn movee_to_delayed_args(
        &self,
        job_id: &str,
        timestamp: i64,
        token: &str,
    ) -> anyhow::Result<(Vec<String>, Vec<String>)> {
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

    pub fn move_to_failed_args() -> anyhow::Result<(Vec<String>, Vec<String>)> {
        unimplemented!()
    }
    pub async fn get_state(&self, job_id: &str) -> anyhow::Result<String> {
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

    pub async fn remove(&self, job_id: String, remove_children: bool) -> anyhow::Result<()> {
        let mut keys = self.get_keys(&[""]);
        let args = vec![
            job_id,
            if remove_children {
                "1".to_owned()
            } else {
                "0".to_owned()
            },
        ];

        //mutate the first value in the keys array

        for (i, v) in keys.iter_mut().enumerate() {
            if i == 0 {
                v.push(':');
            }
        }
        let mut conn = self.connection.get().await?;
        self.commands
            .get("remove")
            .unwrap()
            .key(keys)
            .arg(args)
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

/// MoveToAciveResult(next_job_data,job_id, expireTime, delay)
pub type MoveToAciveResult = (Option<Vec<String>>, Option<String>, i64, Option<i64>);

/// MoveToFinishedResult (finished_job_data,job_id, expireTime, delay
pub type MoveToFinishedResult = (Option<crate::JobJsonRaw>, Option<String>, i64, Option<i64>);
// test

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

pub fn generate_timestamp() -> anyhow::Result<u64> {
    use std::time::SystemTime;
    let result = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_secs()
        * 1000;
    Ok(result)
}

// write a function that return the version of the redis server
pub async fn get_server_version(conn: &mut Connection) -> anyhow::Result<String> {
    let result: String = redis::cmd("INFO").arg("server").query_async(conn).await?;
    for line in result.lines() {
        if line.starts_with("redis_version") {
            let version = line.split(':').nth(1).unwrap();
            return Ok(version.to_string());
        }
    }
    Ok("".to_string())
}

pub fn finished_errors(num: i8, job_id: &str, command: &str, state: &str) -> anyhow::Error {
    let code = ErrorCode::try_from(num).unwrap();
    match code {
        code_job_not_exist => {
            anyhow::Error::msg(format!("Missing Key for job ${job_id}. {command}"))
        }
        JobLockNotExist => anyhow::Error::msg(format!("missing lock for job {job_id}. {command}")),
        JobNotInState => {
            anyhow::Error::msg(format!("Job {job_id} is not in state {state}. {command}"))
        }
        job_pending_dependencies_not_found => anyhow::Error::msg(format!(
            "Job {job_id} pending dependencies not found. {command}"
        )),
        parent_job_not_exists => {
            anyhow::Error::msg(format!("Parent job {job_id} not found. {command}"))
        }
        JobLockMismatch => anyhow::Error::msg(format!("Job {job_id} lock mismatch. {command}")),
        _ => anyhow::Error::msg(format!("Unknown code {num} error for  {job_id}. {command}")),
    }
}

// Convert MoveToAcive to MoveToFinished;

pub fn convert_errors(active_to_active: MoveToAciveResult) -> Result<MoveToFinishedResult> {
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
        _ => Err(anyhow::Error::msg("failed to convert")),
    }
}
