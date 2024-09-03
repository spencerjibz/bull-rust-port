use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::ops::RangeInclusive;
use std::sync::Arc;

use anyhow::Ok;
use deadpool_redis::Connection;
use futures::lock::Mutex;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use redis::streams::StreamMaxlen;
use redis::{AsyncCommands, FromRedisValue};
use serde::{Deserialize, Serialize};

use crate::enums::{PttlError, QueueError};
use crate::job::{self, Job};
use crate::options::{JobOptions, QueueOptions, RetryJobOptions};
use crate::redis_connection::{RedisConnection, RedisOpts};
use crate::{script, JobJsonRaw};
use crate::{to_static_str, RedisConnectionTrait};

pub type ListenerCallback<T> = dyn FnMut(T) -> (dyn Future<Output = ()> + Send + Sync + 'static);

pub struct Logs {
    pub logs: Vec<String>,
    pub count: i64,
}
#[derive(Clone)]
pub struct Queue {
    pub prefix: &'static str,
    pub name: String,
    pub client: Arc<Mutex<Connection>>,
    pub opts: Arc<QueueOptions>,
    pub scripts: Arc<Mutex<script::Scripts>>,
    pub manager: Arc<RedisConnection>,
}

impl Queue {
    pub async fn new(
        name: &str,
        redis_opts: RedisOpts,
        queue_opts: QueueOptions,
    ) -> anyhow::Result<Queue> {
        let prefix = queue_opts.prefix.unwrap_or("bull");

        let new_connection = RedisConnection::init(redis_opts.clone()).await?;
        let last_connection = RedisConnection::init(redis_opts.clone()).await?;
        let connection = RedisConnection::init(redis_opts.clone()).await?;
        let conn_str = redis_opts.to_conn_string();
        let scripts = script::Scripts::new(prefix.to_owned(), name.to_owned(), connection.pool);

        Ok(Self {
            prefix,
            name: name.to_owned(),
            opts: Arc::new(queue_opts),
            scripts: Arc::new(Mutex::new(scripts)),
            client: Arc::new(Mutex::new(new_connection.conn)),
            manager: Arc::new(last_connection),
        })
    }
    pub async fn add<
        'a,
        D: Deserialize<'a> + Serialize + Clone + Send + Sync + std::fmt::Debug,
        R: Deserialize<'a>
            + Serialize
            + FromRedisValue
            + Send
            + Sync
            + 'static
            + Clone
            + std::fmt::Debug,
    >(
        &self,
        name: &'a str,
        data: D,
        opts: JobOptions,
        job_id: Option<String>,
    ) -> anyhow::Result<Job<D, R>> {
        let copy = self.clone();
        let mut job = Job::<D, R>::new(name, self, data, opts, job_id).await?;
        let job_id = self.scripts.lock().await.add_job(&job).await?;
        job.id = serde_json::to_string(&job_id)?;

        Ok(job)
    }

    pub async fn pause(&self) -> anyhow::Result<()> {
        self.scripts.lock().await.pause(true).await
    }
    pub async fn resume(&self) -> anyhow::Result<()> {
        self.scripts.lock().await.pause(false).await
    }

    pub async fn is_paused(&self) -> anyhow::Result<bool> {
        let prefix = self.opts.prefix.unwrap_or("bull");
        let key = format!("{}:{}:meta", prefix, self.name);
        let mut conn = self.manager.pool.get().await?;

        let paused_key_exists = redis::Cmd::hexists(key, "paused")
            .query_async(&mut conn)
            .await?;
        Ok(paused_key_exists)
    }

    pub async fn obliterate(&self, force: bool) -> anyhow::Result<()> {
        self.pause().await?;
        loop {
            let cursor = self.scripts.lock().await.obliterate(1000, force).await?;

            if cursor == 0 {
                break;
            }
        }
        Ok(())
    }

    pub async fn retry_jobs(&self, opts: RetryJobOptions) -> anyhow::Result<()> {
        loop {
            let cursor = self
                .scripts
                .lock()
                .await
                .retry_jobs(opts.state.clone(), opts.count, opts.timestamp)
                .await?;
            if cursor == 0 {
                break;
            }
        }
        Ok(())
    }

    pub async fn get_job_logs(
        &self,
        job_id: &str,
        range: Option<RangeInclusive<isize>>,
        asc: bool,
    ) -> anyhow::Result<Logs> {
        let scripts = self.scripts.lock().await;
        let key = scripts.to_key(format!("{job_id}:logs").as_str());
        let mut con = self.manager.pool.get().await?;
        let mut pipeline = redis::pipe();
        let mut start = 0;
        let mut end = -1;
        if let Some(range) = range {
            start = *range.start();
            end = *range.end()
        };
        match asc {
            true => &mut pipeline.lrange(&key, start, end),
            _ => &mut pipeline.lrange(&key, -(end + 1), -(start + 1)),
        };
        pipeline.llen(key);
        let (mut logs, count): (Vec<String>, i64) = pipeline.query_async(&mut con).await?;
        if !asc {
            logs.reverse()
        }
        Ok(Logs { logs, count })
    }
    pub async fn get_rate_limit_ttl(&self) -> Result<i64, QueueError> {
        use std::result::Result::Ok;
        let limiter_key = self.scripts.lock().await.to_key("limiter");
        let mut con = self.manager.pool.get().await?;
        let result: i64 = redis::Cmd::pttl(&limiter_key).query_async(&mut con).await?;

        match result {
            -1 => Err(QueueError::RedisPttLError(PttlError::NoExpirationWithKey(
                limiter_key,
            ))),
            -2 => Err(QueueError::RedisPttLError(PttlError::KeyNotFound)),
            _ => Ok(result),
        }
    }
    pub async fn trim_events(&self, max_length: usize) -> anyhow::Result<i8> {
        let b = format!("bull:{}:events", self.name);
        let key = self.opts.prefix.unwrap_or(&b);
        let mut conn = self.manager.pool.get().await?;
        let result = redis::Cmd::xtrim(key, StreamMaxlen::Approx(max_length))
            .query_async(&mut conn)
            .await?;
        Ok(result)
    }

    pub async fn get_job_counts(
        &self,
        types: &[&'static str],
    ) -> anyhow::Result<HashMap<String, i64>> {
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
    pub async fn get_jobs(
        &self,
        types: &[&'static str],
        range: Option<RangeInclusive<isize>>,
        asc: bool,
    ) -> anyhow::Result<(Vec<JobJsonRaw>)> {
        let mut start = 0;
        let mut end = -1;
        if let Some(range) = range {
            start = *range.start();
            end = *range.end()
        };
        let current_type = self.sanitize_job_types(types);

        let job_ids = self
            .scripts
            .lock()
            .await
            .get_ranges(types, start, end, asc)
            .await?;

        let mut result_set = vec![];
        let mut futures: FuturesUnordered<_> = job_ids
            .into_iter()
            .flatten()
            .map(|id| async {
                let mut job = JobJsonRaw::from_id(id.clone(), self).await?;
                job.id = to_static_str(id);
                Ok(job)
            })
            .collect();

        while let Some(std::result::Result::Ok(job)) = futures.next().await {
            result_set.push(job);
        }
        result_set.sort_by(|a, b| a.id.cmp(b.id));
        Ok(result_set)
    }
    fn sanitize_job_types(&self, types: &[&'static str]) -> Vec<&'static str> {
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
        self.scripts
            .lock()
            .await
            .remove(job_id, remove_children)
            .await?;
        Ok(())
    }

    pub async fn get_job_state(&self, job_id: &str) -> anyhow::Result<String> {
        let state = self.scripts.lock().await.get_state(job_id).await?;
        Ok(state)
    }

    pub async fn close(&self) {
        self.manager.close().await;
    }
}

impl fmt::Debug for Queue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Queue")
            .field("prefix", &self.prefix)
            .field("name", &self.name)
            .field("opts", &self.opts)
            //.field("manager", &self.manager) // sensitive data
            .finish()
    }
}
