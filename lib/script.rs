#![allow(clippy::too_many_arguments)]
use crate::add_job::add_job_to_queue;
use crate::{functions, job, JobOptions, KeepJobs, WorkerOptions};
use crate::{job::Job, redis_connection::*};
use anyhow::Error;
use anyhow::Ok;
use anyhow::Result;
use futures::{future::ok, stream::TryFilterMap};
use maplit::hashmap;
pub use redis::Script;
use redis::{FromRedisValue, RedisResult, ToRedisArgs, Value};
use rmp::encode;
use serde::{Deserialize, Serialize};

use std::{collections::HashMap, time};
pub type ScriptCommands = HashMap<&'static str, Script>;
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

            "extendLock" =>  Script::new(&get_script("extendLock-2.lua")),
            "getCounts" =>  Script::new(&get_script("getCounts-1.lua")),
            "obliterate" =>  Script::new(&get_script("obliterate-2.lua")),
            "pause" => Script::new(&get_script("pause-4.lua")),
            "moveToActive" =>  Script::new(&get_script("moveToActive-9.lua")),
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
        }
    }
    fn to_key(&self, name: &str) -> String {
        format!("{}:{}:{}", self.prefix, self.queue_name, name)
    }
    fn get_keys(&self, keys: &[&str]) -> Vec<String> {
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
    pub async fn add_job<'s, D: Serialize + Clone, R: FromRedisValue>(
        &mut self,
        job: &Job<D, R>,
    ) -> anyhow::Result<i64> {
        let e = String::from("");
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
        ]);

        let mut conn = self.connection.get().await?;
        let result = add_job_to_queue(
            &keys,
            (
                prefix.to_owned(),
                job.id.to_owned(),
                name.to_owned(),
                job.timestamp,
                parent_key,
                None,
                parent_dep_key,
                parent,
                None,
            ),
            json_data,
            &job.opts,
            &mut conn,
        )
        .await?;

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
    ) -> anyhow::Result<MoveToAciveResult> {
        use std::time::SystemTime;
        let id: u16 = rand::random();
        let timestamp = generate_timestamp()?;
        let lock_duration = opts.lock_duration.to_string();
        let limiter = serde_json::to_string(&opts.limiter)?;

        // let limiter = opts.
        let keys = self.get_keys(&[
            "wait", "active", "priority", "events", "stalled", "limiter", "delayed", "paused",
            "meta",
        ]);
        let mut packed_opts = Vec::new();
        let p_opts = serde_json::to_string(&hashmap! {
           "token" => token,
           "lockDuration" => &lock_duration,
           "limiter" => &limiter ,
        })?;
        let d = &self.to_key("");
        let first_arg = self.keys.get("").unwrap_or(d);
        encode::write_bin(&mut packed_opts, p_opts.as_bytes())?;
        let mut conn = &mut self.connection.get().await?;
        let result: MoveToAciveResult = self
            .commands
            .get("moveToActive")
            .unwrap()
            .key(keys)
            .arg(first_arg)
            .arg(timestamp)
            .arg(id)
            .arg(packed_opts)
            .invoke_async(conn)
            .await?;

        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    async fn move_to_finished<
        D: Serialize + Clone,
        R: FromRedisValue + Any + Send + Sync + Clone + 'static,
        V: Any + ToRedisArgs,
    >(
        &mut self,
        job: &mut Job<D, R>,
        val: V,
        prop_val: &str,
        should_remove: bool,
        target: &str,
        token: &str,
        opts: &WorkerOptions,
        fetch_next: bool,
    ) -> anyhow::Result<Option<MoveToAciveResult>> {
        let timestamp = generate_timestamp()?;
        let metrics_key = self.to_key(&format!("metrics:{target}"));
        let mut keys = self.get_keys(&[
            "wait", "active", "priority", "events", "stalled", "limiter", "delayed", "paused",
            target,
        ]);
        keys.push(self.to_key(job.id.as_str()));
        keys.push(self.to_key("meta"));
        keys.push(metrics_key);

        let keep_jobs = self.get_keep_jobs(should_remove);

        let mut packed_opts = Vec::new();
        let max_metrics_size = match &opts.metrics {
            Some(v) => v.max_data_points.to_string(),
            None => String::from(""),
        };

        let p_opts = serde_json::to_string(&hashmap! {
            "token" => token.to_string(),
            "keepJobs" => serde_json::to_string(&keep_jobs).unwrap(),
            "limiter" => serde_json::to_string(&opts.limiter).unwrap(),
            "lockDuration" => opts.lock_duration.to_string(),
            "attempts" => job.attempts.to_string(),
            "attemptsMade" => job.attempts_made.to_string(),
            "maxMetricsSize" => max_metrics_size,
            "fpof" => job.opts.fail_parent_on_failure.to_string(), //fail parent on failure
        })?;
        let fetch = if fetch_next { "fetch" } else { "" };
        let d = &self.to_key("");
        let sec_last = self.keys.get("").unwrap_or(d);
        let mut connection = &mut self.connection.get().await?;
        encode::write_bin(&mut packed_opts, p_opts.as_bytes())?;
        let result: Option<R> = self
            .commands
            .get("moveToFinished")
            .unwrap()
            .key(keys)
            .arg(job.id.as_str())
            .arg(timestamp)
            .arg(prop_val)
            .arg(val)
            .arg(target)
            .arg("")
            .arg(fetch)
            .arg(sec_last)
            .arg(packed_opts)
            .invoke_async(connection)
            .await?;
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
        Ok(None)
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

    fn finished_errors(&self, num: i8, job_id: &str, command: &str, state: &str) -> anyhow::Error {
        let code = ErrorCode::try_from(num).unwrap();
        match code {
            code_job_not_exist => {
                anyhow::Error::msg(format!("Missing Key for job ${job_id}. {command}"))
            }
            JobLockNotExist => {
                anyhow::Error::msg(format!("missing lock for job {job_id}. {command}"))
            }
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
    ) -> anyhow::Result<Vec<Vec<String>>> {
        let stalled = self.keys.get("").unwrap();
        let timestamp = generate_timestamp()?;
        let keys = self.get_keys(&[
            "stalled",
            "wait",
            "active",
            "failed",
            "stalled-check",
            "meta",
            "events",
        ]);
        let conn = &mut self.connection.get().await?;

        let result = self
            .commands
            .get("moveStalledJobToWait")
            .unwrap()
            .key(keys)
            .arg(max_stalled_count)
            .arg(stalled)
            .arg(timestamp)
            .arg(stalled_interval)
            .invoke_async(conn)
            .await?;
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
                Err(self.finished_errors(val, job_id, "updateProgress", "active"))
            }
            None => Ok(None),
        }
    }
    pub async fn move_to_completed<
        's,
        D: Serialize + Clone,
        R: FromRedisValue + Send + Sync + Clone + 'static,
        V: Any + ToRedisArgs,
    >(
        &mut self,
        job: &mut Job<D, R>,
        val: V,
        remove_on_complete: bool,
        token: &str,
        opts: &WorkerOptions,
        fetch_next: bool,
    ) -> anyhow::Result<Option<MoveToAciveResult>> {
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
    ) -> anyhow::Result<Option<MoveToAciveResult>> {
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

        let version = self.get_server_version().await?;
        // use semver to compare the version of the redis server
        // if the version is greater than 6.2.0 then use the getStateV2 script
        let script = if *version > *"6.0.6" {
            "getStateV2"
        } else {
            "getState"
        };
        println!("version: {}", script);
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
    // write a function that return the version of the redis server
    pub async fn get_server_version(&self) -> anyhow::Result<String> {
        let mut conn = self.connection.get().await?;
        let result: String = redis::cmd("INFO")
            .arg("server")
            .query_async(&mut conn)
            .await?;

        for line in result.lines() {
            if line.starts_with("redis_version") {
                let version = line.split(':').nth(1).unwrap();
                return Ok(version.to_string());
            }
        }
        Ok("".to_string())
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
                v.push(':')
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
pub type MoveToAciveResult = (Option<Map>, Option<String>, u64, Option<u64>);

// test

pub fn get_script(script_name: &'static str) -> String {
    use std::fs;

    fs::read_to_string(format!("commands/{script_name}"))
        .expect("Something went wrong reading the file")
}

#[cfg(test)]
#[test]
fn test_get_script() {
    // read out the file addJob-8.lua  and return a string with contents
    // of the

    let script = get_script("addJob-8.lua");
    // println!("{:?}", script);
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
