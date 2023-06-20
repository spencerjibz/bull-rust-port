#![allow(clippy::too_many_arguments)]
use crate::{job, JobOptions, KeepJobs, WorkerOptions};
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
pub type ScriptCommands<'c> = HashMap<&'c str, Script>;
use crate::enums::ErrorCode::{self, *};
use crate::redis_connection::*;
use std::any::{self, Any};
#[derive(Clone)]
pub struct Scripts<'s> {
    pub prefix: &'s str,
    pub queue_name: &'s str,
    pub keys: HashMap<&'s str, String>,
    pub commands: ScriptCommands<'s>,
    pub connection: Pool,
}

// debug implementation for Scripts
impl<'s> std::fmt::Debug for Scripts<'s> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scripts")
            .field("prefix", &self.prefix)
            .field("queue_name", &self.queue_name)
            .field("keys", &self.keys)
            .field("commands", &self.commands)
            .finish()
    }
}

impl<'s> Scripts<'s> {
    pub fn new(prefix: &'s str, queue_name: &'s str, conn: Pool) -> Self {
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
            keys.insert(name, format!("{prefix}:{queue_name}:{name}"));
        }
        let comands = hashmap! {
             "addJob" =>  Script::new(&get_script("addJob-8.lua")),
            "extendLock" =>  Script::new(&get_script("extendLock-2.lua")),
            "getCounts" =>  Script::new(&get_script("getCounts-1.lua")),
            "obliterate" =>  Script::new(&get_script("obliterate-2.lua")),
            "pause" => Script::new(&get_script("pause-4.lua")),
            "moveToActive" =>  Script::new(&get_script("moveToActive-9.lua")),
            "moveToFinished" =>  Script::new(&get_script("moveToFinished-12.lua")),
            "moveStalledJobsToWait"=>  Script::new(&get_script("moveStalledJobsToWait-8.lua")),
            "retryJobs" => Script::new(&get_script("retryJobs-6.lua")),
            "updateProgress"=>  Script::new(&get_script("updateProgress-2.lua")),

        };
        Self {
            queue_name,
            prefix,
            keys,
            commands: comands,
            connection: conn,
        }
    }
    fn to_key(&self, name: &'s str) -> String {
        format!("{}:{}:{}", self.prefix, self.queue_name, name)
    }
    fn get_keys(&self, keys: &[&'s str]) -> Vec<String> {
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
    pub async fn add_job<D: Serialize + Clone, R: FromRedisValue>(
        &mut self,
        job: &Job<'s, D, R>,
    ) -> anyhow::Result<R> {
        let e = String::from("");
        let prefix = self.keys.get("").unwrap_or(&e);
        let mut packed_args = Vec::new();
        encode::write_bin(&mut packed_args, prefix.as_bytes())?;

        encode::write_bin(
            &mut packed_args,
            format!("{}{}{}", &job.id, &job.name, &job.timestamp).as_bytes(),
        )?;
        // write the id,

        let json_data = serde_json::to_string(&job.data.clone())?;
        let mut packed_opts = Vec::new();
        let opts = format!("{:?}", job.opts);
        let j_opts = opts.as_bytes();
        encode::write_bin(&mut packed_opts, j_opts)?;
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
        let mut packed_json = Vec::new();
        encode::write_bin(&mut packed_json, json_data.as_bytes())?;
        let mut conn = self.connection.get().await?;
        let result = self
            .commands
            .get("addJob")
            .unwrap()
            .key(keys)
            .arg(packed_args)
            .arg(json_data)
            .arg(packed_opts)
            .invoke_async(&mut conn)
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
            panic!("cannot obliterate queu with active jobs")
        }
        Ok(result)
    }

    async fn move_to_active_queue<R: FromRedisValue>(
        &mut self,
        token: &str,
        opts: WorkerOptions,
    ) -> anyhow::Result<R> {
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
        let result: R = self
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
        job: &mut Job<'s, D, R>,
        val: V,
        prop_val: &str,
        should_remove: bool,
        target: &str,
        token: &str,
        opts: &WorkerOptions,
        fetch_next: bool,
    ) -> anyhow::Result<(NextJobData)> {
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

            let slice_of_string = print_type_of(&vec![vec![""]]);

            if print_type_of(&res) == slice_of_string {
                let n = Box::new(res) as Box<dyn Any>;
                let n = n.downcast::<Vec<Vec<&str>>>().unwrap();
                let v = *n;

                let returned_result = self.raw_to_next_job_data(&v);

                return Ok(returned_result);
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

    fn array_to_object(&self, arr: &[&str]) -> HashMap<String, String> {
        let mut result = HashMap::new();
        for (i, v) in arr.iter().enumerate() {
            if i % 2 == 0 {
                result.insert(v.to_string(), arr[i + 1].to_string());
            }
        }
        result
    }
    fn raw_to_next_job_data(&self, mut raw: &Vec<Vec<&str>>) -> NextJobData {
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
        D: Serialize + Clone,
        R: FromRedisValue + Send + Sync + Clone + 'static,
        V: Any + ToRedisArgs,
    >(
        &mut self,
        job: &mut Job<'s, D, R>,
        val: V,
        remove_on_complete: bool,
        token: &str,
        opts: &WorkerOptions,
        fetch_next: bool,
    ) -> anyhow::Result<(NextJobData)> {
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
        R: FromRedisValue + Any + Send + Sync  + 'static + Clone,
    >(
        &mut self,
        job: &mut Job<'s, D, R>,
        failed_reason: String,
        remove_on_failure: bool,
        token: &str,
        opts: &WorkerOptions,
        fetch_next: bool,
    ) -> anyhow::Result<(NextJobData)> {
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

}

pub fn print_type_of<T>(_: &T) -> String {
    std::any::type_name::<T>().to_string()
}

type Map = HashMap<String, String>;
type NextJobData = Option<(Option<Map>, Option<Map>)>;

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
