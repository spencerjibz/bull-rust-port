use crate::job::Job;
use crate::options::{JobOptions, QueueOptions, RetryJobOptions};
use crate::redis_connection::{Client, RedisConnection, RedisOpts};
use crate::script;
use anyhow::Ok;
use deadpool_redis::{Connection, Pool, Runtime};
use futures::future::ok;
use redis::{AsyncCommands, FromRedisValue, Script};
use std::collections::HashMap;
use std::future::Future;
pub type ListenerCallback<T> = dyn FnMut(T) -> (dyn Future<Output = ()> + Send + Sync + 'static);
use crate::RedisConnectionTrait;
use futures::lock::Mutex;
use redis::streams::StreamMaxlen;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;

pub struct Queue<'c> {
    pub prefix: &'c str,
    pub name: &'c str,
    pub client: Connection,
    pub opts: QueueOptions<'c>,
    pub scripts: Mutex<script::Scripts<'c>>,
    pub manager: RedisConnection<'c>,
}

impl<'c> Queue<'c> {
    pub async fn new(
        name: &'c str,
        redis_opts: RedisOpts<'c>,
        queue_opts: QueueOptions<'c>,
    ) -> anyhow::Result<Queue<'c>> {
        let prefix = queue_opts.prefix.unwrap_or("bull");

        let new_connection = RedisConnection::init(redis_opts.clone()).await?;
        let last_connection = RedisConnection::init(redis_opts.clone()).await?;
        let connection = RedisConnection::init(redis_opts.clone()).await?;
        let conn_str = redis_opts.to_conn_string();
        let scripts = script::Scripts::new(prefix, name, connection.pool);

        Ok(Self {
            prefix,
            name,
            opts: queue_opts,
            scripts: Mutex::new(scripts),
            client: new_connection.conn,
            manager: last_connection,
        })
    }
    pub async fn add<
        D: Deserialize<'c> + Serialize + Clone + Send + Sync + 'static + std::fmt::Debug,
        R: Deserialize<'c>
            + Serialize
            + FromRedisValue
            + Send
            + Sync
            + 'static
            + Clone
            + std::fmt::Debug,
    >(
        &'c self,
        name: &'static str,
        data: D,
        opts: JobOptions,
    ) -> anyhow::Result<Job<D, R>> {
        let mut job = Job::<D, R>::new(name, self, data, opts).await?;
        let mut scripts = self.scripts.lock().await;
        let job_id = scripts.add_job(&job).await?;
        job.id = serde_json::to_string(&job_id)?;

        Ok(job)
    }

    pub async fn pause<R: Deserialize<'c> + Serialize + FromRedisValue + Send + Sync + 'static>(
        &'c self,
    ) -> anyhow::Result<R> {
        let result = self.scripts.lock().await.pause(true).await?;

        Ok(result)
    }
    pub async fn resume<R: FromRedisValue>(&'c self) -> anyhow::Result<R> {
        let result = self.scripts.lock().await.pause(false).await?;

        Ok(result)
    }

    pub async fn is_paused<RV: FromRedisValue + Sync + Send>(&self) -> anyhow::Result<RV> {
        let b = format!("bull:{}:meta", self.name);
        let key = self.opts.prefix.unwrap_or(&b);
        let mut conn = self.manager.pool.get().await?;

        let paused_key_exists = redis::Cmd::hexists(key, "paused")
            .query_async(&mut conn)
            .await?;
        Ok(paused_key_exists)
    }

    async fn obliterate(&'static self, force: bool) -> anyhow::Result<()> {
        self.pause().await?;
        loop {
            let cursor = self.scripts.lock().await.obliterate(1000, force).await?;
            if cursor == 0 {
                break;
            }
        }
        Ok(())
    }

    pub async fn retry_jobs<'s>(&'s self, opts: RetryJobOptions) -> anyhow::Result<()> {
        loop {
            let cursor = self
                .scripts
                .lock()
                .await
                .retry_jobs::<i64>(opts.state.clone(), opts.count, opts.timestamp)
                .await?;
            if cursor == 0 {
                break;
            }
        }
        Ok(())
    }

    pub async fn trim_events<RV: FromRedisValue + Send + Sync>(
        &self,
        max_length: usize,
    ) -> anyhow::Result<RV> {
        let b = format!("bull:{}:events", self.name);
        let key = self.opts.prefix.unwrap_or(&b);
        let mut conn = self.manager.pool.get().await?;
        let result = redis::Cmd::xtrim(key, StreamMaxlen::Approx(max_length))
            .query_async(&mut conn)
            .await?;
        Ok(result)
    }

    pub async fn get_job_counts(&self, types: &[&'c str]) -> anyhow::Result<HashMap<String, i64>> {
        let mut counts = HashMap::new();

        let mut current_types = self.sanitize_job_types(types);
        let cloned_types = current_types.clone();
        let resources = self
            .scripts
            .lock()
            .await
            .get_counts(cloned_types.into_iter())
            .await?;

        for (index, value) in resources.into_iter().enumerate() {
            counts.insert(current_types[index].to_string(), value);
        }

        Ok(counts)
    }

    fn sanitize_job_types(&self, types: &[&'c str]) -> Vec<&str> {
        if !types.is_empty() {
            let mut v = types.to_vec();

            if v.contains(&"waiting") {
                v.push("paused");
            }

            v.dedup();

            return v;
        }

        vec![
            "active",
            "completed",
            "delayed",
            "failed",
            "paused",
            "waiting",
            "waiting-children",
        ]
    }

    pub async fn remove_job(&self, job_id: String, remove_children: bool) -> anyhow::Result<()> {
        let mut scripts = self.scripts.lock().await;
        scripts.remove(job_id, remove_children).await?;
        Ok(())
    }

    pub async fn get_job_state(&self, job_id: &str) -> anyhow::Result<String> {
        let mut scripts = self.scripts.lock().await;
        let state = scripts.get_state(job_id).await?;
        Ok(state)
    }

    pub async fn close(&self) {
        self.manager.close().await;
    }
}

use std::fmt;
impl fmt::Debug for Queue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Queue")
            .field("prefix", &self.prefix)
            .field("name", &self.name)
            .field("opts", &self.opts)
            //.field("manager", &self.manager) // sensitive data
            .finish()
    }
}
