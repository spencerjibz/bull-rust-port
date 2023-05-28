use std::{borrow::Borrow, clone};

use super::*;
use redis::Commands;
use serde_json::Value;

pub struct Job<'a, D, R> {
    pub name: &'a str,
    pub queue: &'a Queue<'a>,
    pub timestamp: i64,
    pub attempts_made: i64,
    pub attempts: i64,
    pub delay: i64,
    pub id: String,
    pub progress: i8, // 0 to 100
    pub opts: JobOptions,
    pub data: D,
    pub return_value: Option<R>,
    scripts: script::Stripts<'a>,
    pub repeat_job_key: Option<String>,
    pub failed_reason: Option<String>,
    pub stack_trace: Vec<String>,
    pub remove_on_complete: RemoveOnCompletionOrFailure,
    pub remove_on_fail: RemoveOnCompletionOrFailure,
    pub processed_on: i64,
    pub finished_on: i64,
}

impl<'a, D: Deserialize<'a> + Serialize, R: Deserialize<'a> + Serialize> Job<'a, D, R> {
    fn update_progress(&mut self, progress: i8) {
        self.progress = progress;
        // return a script to update the progress;
    }

    pub async fn new(
        name: &'a str,
        queue: &'a Queue<'a>,
        data: D,
        opts: JobOptions,
    ) -> anyhow::Result<Job<'a, D, R>> {
        let new_conn = queue.conn.conn_options.clone();
        let prefix = queue.prefix;
        let queue_name = queue.name;
        let dup_conn = RedisConnection::init(new_conn).await?;

        Ok(Self {
            opts: opts.clone(),
            queue,
            name,
            id: opts.job_id,
            progress: 0,
            timestamp: opts.timestamp,
            delay: opts.delay,
            attempts_made: 0,
            attempts: opts.attempts,
            data,
            return_value: None,
            remove_on_complete: opts.remove_on_completion,
            processed_on: 0,
            finished_on: 0,
            repeat_job_key: None,
            failed_reason: None,
            stack_trace: vec![],
            remove_on_fail: opts.remove_on_fail,
            scripts: script::Stripts::<'a>::new(prefix, queue_name, dup_conn),
        })
    }

    async fn from_json(
        mut queue: &'a Queue<'a>,
        json: &'a JobJsonRaw,
        job_id: String,
    ) -> anyhow::Result<Job<'a, D, R>> {
        // use serde_json to convert to job;
        let data = serde_json::from_str(&json.data)?;
        let opts = serde_json::from_str::<JobOptions>(&json.opts)?;
        let mut job = Job::<'a, D, R>::new(&json.data, queue, data, opts).await?;
        job.id = job_id.to_string();
        job.progress = json.progress.parse::<i8>()?;
        job.attempts_made = json.attempts_made.parse::<i64>()?;
        job.timestamp = json.timestamp.parse::<i64>()?;
        job.delay = json.delay.parse::<i64>()?;
        job.return_value = Some(serde_json::from_str::<R>(&json.return_value)?);
        job.finished_on = json
            .finished_on
            .clone()
            .unwrap_or(String::from("0"))
            .parse::<i64>()?;
        job.processed_on = json
            .processed_on
            .clone()
            .unwrap_or(String::from("0"))
            .parse::<i64>()?;
        job.failed_reason = Some(json.failed_reason.clone());
        job.repeat_job_key = json.rjk.clone();
        job.stack_trace = json.stack_trace.clone();

        Ok(job)
    }
    pub fn opts_from_json(&mut self, raw_opts: String) -> anyhow::Result<JobOptions> {
        let opts = serde_json::from_str::<JobOptions>(&raw_opts)?;
        Ok(opts)
    }
}

pub async fn from_id<'a, D: Deserialize<'a> + Serialize, R: Deserialize<'a> + Serialize>(
    queue: &'a mut Queue<'a>,
    job_id: String,
) -> anyhow::Result<Job<'a, D, R>> {
    // use redis to get the job;
    let key = format!("{}:{}:{}", &queue.prefix, &queue.name, job_id);

    let raw_data: JobJsonRaw = queue.client.hgetall(key)?;

    //let job=  self.from_json(queue,  raw_data.clone(), job_id).await?;

    todo!()
}
#[cfg(test)]
#[test]
fn test_job() {
    use dotenv_codegen::dotenv;
    use std::collections::HashMap;
    use std::env;
    use std::time::{Instant, SystemTime};
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let pass = dotenv!("REDIS_PASSWORD");
        let mut config = HashMap::new();
        config.insert("password", pass);
        let redis_opts = RedisOpts::Config(config);

        let mut queue = Queue::<'_>::new("test", redis_opts, QueueOptions { prefix: None })
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

        assert_eq!(job.name, "test");
    });
}
