use std::{borrow::Borrow, clone, collections::HashMap};

use super::*;
use anyhow::Ok;
use futures::{self, lock::Mutex};
use redis::Commands;
use serde::de::{Deserialize, Deserializer, Error, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeStruct, Serializer};
use serde_json::Value;
use std::sync::Arc;
#[derive(Clone)]
pub struct Job<'a, D, R> {
    pub name: &'a str,
    pub queue: &'a Queue<'a>,
    pub timestamp: i64,
    pub attempts_made: i64,
    pub attempts: i64,
    pub delay: i64,
    pub id: String,       // jsonString
    pub progress: String, // 0 to 100
    pub opts: JobOptions,
    pub data: D,
    pub return_value: Option<R>,
    scripts: Arc<Mutex<script::Scripts<'a>>>,
    pub repeat_job_key: Option<&'a str>,
    pub failed_reason: Option<String>,
    pub stack_trace: Vec<String>,
    pub remove_on_complete: RemoveOnCompletionOrFailure,
    pub remove_on_fail: RemoveOnCompletionOrFailure,
    pub processed_on: i64,
    pub finished_on: i64,
    pub discarded: bool,
}
// implement Serialize for Job
impl<'a, D: Serialize, R: Serialize> Serialize for Job<'a, D, R> {
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
        state.end()
    }
}

// implement Deserialize for Job

//implement PartialEq for Job
impl<
        'a,
        D: Deserialize<'a> + Clone + std::fmt::Debug + Send + Sync,
        R: Deserialize<'a> + Serialize + Send + Sync,
    > PartialEq for Job<'a, D, R>
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<
        'a,
        D: Deserialize<'a> + Clone + std::fmt::Debug + Send + Sync,
        R: Deserialize<'a> + Serialize + Send + Sync,
    > Job<'a, D, R>
{
    async fn update_progress<T: Serialize>(&mut self, progress: T) -> anyhow::Result<Option<i8>> {
        self.progress = serde_json::to_string(&progress).unwrap();
        self.scripts
            .lock()
            .await
            .update_progress(&self.id, progress)
            .await

        // return a script to update the progress;
    }

    pub async fn new(
        name: &'a str,
        queue: &'a Queue<'a>,
        data: D,
        opts: JobOptions,
    ) -> anyhow::Result<Job<'a, D, R>> {
        let prefix = queue.prefix;
        let queue_name = queue.name;
        let dup_conn = queue.manager.pool.clone();
        let conn_str = queue.manager.to_conn_string();

        Ok(Self {
            opts: opts.clone(),
            queue,
            name,
            id: opts.job_id.unwrap(),
            progress: String::default(),
            timestamp: opts.timestamp.unwrap(),
            delay: opts.delay,
            attempts_made: 0,
            attempts: opts.attempts,
            data,
            return_value: None,
            remove_on_complete: opts.remove_on_complete,
            processed_on: 0,
            finished_on: 0,
            repeat_job_key: None,
            failed_reason: None,
            stack_trace: vec![],
            remove_on_fail: opts.remove_on_fail,
            scripts: Arc::new(Mutex::new(Scripts::new(prefix, queue_name, dup_conn))),
            discarded: false,
        })
    }

    pub async fn from_json(
        mut queue: &'a Queue<'a>,
        raw_string: String,
        job_id: &'a str,
    ) -> anyhow::Result<Job<'a, D, R>> {
        // use serde_json to convert to job;

        let json = JobJsonRaw::fromStr(to_static_str(raw_string))?;
        //println!("json: {:?}", json);

        let name = json.name;
        let mut opts = serde_json::from_str::<JobOptions>(json.opts).unwrap_or_default();

        let d = json.data;
        let data = serde_json::from_str::<D>(d)?;
        // println!("data: {:?}", data);

        //let  return_value = serde_json::from_str::<R>(json.return_value)?;

        let mut job = Self::new(name, queue, data, opts).await?;
        job.id = job_id.to_string();
        job.progress = json.progress.to_string();
        job.attempts_made = json.attempts_made.parse::<i64>().unwrap_or_default();
        job.timestamp = json.timestamp.parse::<i64>()?;
        job.delay = json.delay.parse::<i64>()?;
        //
        job.finished_on = json
            .finished_on
            .unwrap_or(&String::from("0"))
            .parse::<i64>()?;
        job.processed_on = json
            .processed_on
            .unwrap_or(&String::from("0"))
            .parse::<i64>()?;
        job.failed_reason = Some(json.failed_reason.to_string());

        job.repeat_job_key = json.rjk;

        job.stack_trace = json
            .stack_trace
            .into_iter()
            .map(|x| x.to_string())
            .collect();

        Ok(job)
    }
    pub fn opts_from_json(&mut self, raw_opts: String) -> anyhow::Result<JobOptions> {
        let opts = serde_json::from_str::<JobOptions>(&raw_opts)?;
        Ok(opts)
    }

    pub async fn from_id(
        queue: &'a mut Queue<'a>,
        job_id: &'a str,
    ) -> anyhow::Result<Job<'a, D, R>> {
        // use redis to get the job;
        let key = format!("{}:{}:{}", &queue.prefix, &queue.name, job_id);

        let raw_data: HashMap<String, String> = queue.client.hgetall(key).await?;
        let raw_string = serde_json::to_string(&raw_data)?;

        let job = Self::from_json(queue, raw_string, job_id).await?;
        Ok(job)
    }
    pub async fn move_to_failed(
        &mut self,
        err: String,
        token: &str,
        fetch_next: bool,
    ) -> anyhow::Result<()> {
        self.failed_reason = Some(err.clone());
        let mut move_to_failed = false;

        let finished_on = 0;
        let command = "moveToFailed";
        let mut conn = self.queue.manager.pool.clone().get().await?;
        self.save_to_stacktrace(&mut conn, err).await?;
        let backoff_s = BackOff::new();

        if self.attempts_made < self.opts.attempts && !self.discarded {
            let backoff = BackOff::normalize(self.opts.backoff.clone());
            let custom_strategy = if let Some(q) = self.queue.opts.settings.backoff_strategy.clone()
            {
                Arc::into_inner(q)
            } else {
                None
            };

            let delay = backoff_s.calculate(backoff, self.attempts_made, custom_strategy)?;
        }

        Ok(())
    }

    pub async fn save_to_stacktrace(
        &mut self,
        conn: &mut Connection,
        err_stack: String,
    ) -> anyhow::Result<()> {
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
                    .lock()
                    .await
                    .save_stacktrace_args(&self.id, &stack_trace, &err_stack);

            self.scripts
                .lock()
                .await
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
}

// implement fmt::Debug for Job;
impl<
        'a,
        D: Deserialize<'a> + Clone + Serialize + std::fmt::Debug,
        R: Deserialize<'a> + Serialize,
    > std::fmt::Debug for Job<'a, D, R>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("name", &self.name)
            //.field("queue", &self.queue)
            .field("timestamp", &self.timestamp)
            .field("attempts_made", &self.attempts_made)
            .field("attempts", &self.attempts)
            .field("delay", &self.delay)
            .field("id", &self.id)
            .field("progress", &self.progress)
            .field("opts", &self.opts)
            .field("data", &self.data)
            // .field("scripts", &self.scripts)
            .field("repeat_job_key", &self.repeat_job_key)
            .field("failed_reason", &self.failed_reason)
            .field("stack_trace", &self.stack_trace)
            .field("remove_on_complete", &self.remove_on_complete)
            .field("remove_on_fail", &self.remove_on_fail)
            .field("processed_on", &self.processed_on)
            .field("finished_on", &self.finished_on)
            .field("discarded", &self.discarded)
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

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Size {
    pub width: i64,
    pub height: i64,
}
#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Ok;
    use dotenv_codegen::dotenv;
    use std::collections::HashMap;
    use std::env;
    use std::fs::File;
    use std::time::{Instant, SystemTime};
    const PASS: &str = dotenv!("REDIS_PASSWORD");

    #[test]
    fn creating_a_new_job() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut config = HashMap::new();
            config.insert("password", PASS);

            let redis_opts = RedisOpts::Config(config);

            let mut queue = Queue::<'_>::new("test", redis_opts, QueueOptions::default())
                .await
                .unwrap();
            let job = Job::<'_, String, String>::new(
                "test",
                &queue,
                "test".to_string(),
                JobOptions::default(),
            )
            .await
            .unwrap();
            //println!("{:#?}", job);
            assert_eq!(job.name, "test");
        });
    }
    #[test]
    fn create_job_from_string() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut config = HashMap::new();
            config.insert("password", PASS);

            let redis_opts = RedisOpts::Config(config);

            let mut queue = Queue::<'_>::new("test", redis_opts, QueueOptions::default())
                .await
                .unwrap();

            let result: HashMap<String, String> =
                queue.client.hgetall("bull:pinningQueue:201").await.unwrap();
            //let json = JobJsonRaw::from_map(result.clone()).unwrap();
            //json.save_to_file("test.json").unwrap();

            // println!("{:#?}", worker.clone());
            let contents = serde_json::to_string(&result).unwrap_or("{}".to_string());

            let job = Job::<Data, String>::from_json(&queue, contents, "254")
                .await
                .unwrap();
            //println!(" job : {job:#?}",);
            assert_eq!(job.name, "QmTkNd9nQHasSbQwmcsRkBeiFsMgqhgDNnWqYRgwZLmCgP");
        });
    }
}
