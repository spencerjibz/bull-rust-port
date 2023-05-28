use crate::job::Job;
use crate::redis_connection::{Client, RedisConnection, RedisOpts};
use crate::script;
use crate::structs::{JobOptions, QueueOptions, RetryJobOptions};
use anyhow::Ok;
use futures::future::ok;
use redis::{Commands, Connection, FromRedisValue, Script};
use std::collections::HashMap;
use std::future::Future;
pub type ListenerCallback<T> = dyn FnMut(T) -> (dyn Future<Output = ()> + Send + Sync + 'static);
use redis::streams::StreamMaxlen;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;

pub struct Queue<'c> {
    pub prefix: &'c str,
    pub name: &'c str,
    pub conn: RedisConnection<'c>,
    pub opts: QueueOptions<'c>,
    pub client: Client,
    pub scripts: RefCell<script::Stripts<'c>>,
}

impl<'c> Queue<'c> {
    pub async fn new(
        name: &'c str,
        redis_opts: RedisOpts<'c>,
        queue_opts: QueueOptions<'c>,
    ) -> anyhow::Result<Queue<'c>> {
        let prefix = queue_opts.prefix.unwrap_or("bull");

        let new_connection = RedisConnection::init(redis_opts.clone()).await?;
        let connection = RedisConnection::init(redis_opts).await?;
        let scripts = script::Stripts::new(prefix, name, connection);

        Ok(Self {
            prefix,
            name,
            client: new_connection.client.clone(),
            opts: queue_opts,
            scripts: RefCell::new(scripts),
            conn: new_connection,
        })
    }
    pub async fn add<
        D: Deserialize<'c> + Serialize + Clone,
        R: Deserialize<'c> + Serialize + FromRedisValue,
    >(
        &'c self,
        name: &'c str,
        data: D,
        opts: JobOptions,
    ) -> anyhow::Result<Job<D, R>> {
        let mut job = Job::<'c, D, R>::new(name, self, data, opts).await?;
        let job_id = self.scripts.borrow_mut().add_job(&job).await?;
        job.id = serde_json::to_string(&job_id)?;

        Ok(job)
    }

    pub async fn pause<R: Deserialize<'c> + Serialize + FromRedisValue>(
        &'c self,
    ) -> anyhow::Result<R> {
        let result = self.scripts.borrow_mut().pause(true).await?;

        Ok(result)
    }
    pub async fn resume<R: FromRedisValue>(&'c self) -> anyhow::Result<R> {
        let result = self.scripts.borrow_mut().pause(false).await?;

        Ok(result)
    }

    pub fn is_paused<RV: FromRedisValue>(&mut self) -> anyhow::Result<RV> {
        let b = format!("bull:{}:meta", self.name);
        let key = self.opts.prefix.unwrap_or(&b);

        let paused_key_exists = self.client.hexists(key, "paused")?;
        Ok(paused_key_exists)
    }

    async fn obliterate(&'static self, force: bool) -> anyhow::Result<()> {
        self.pause().await?;
        loop {
            let cursor = self.scripts.borrow_mut().obliterate(1000, force).await?;
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
                .borrow_mut()
                .retry_jobs::<i64>(opts.state.clone(), opts.count, opts.timestamp)
                .await?;
            if cursor == 0 {
                break;
            }
        }
        Ok(())
    }

    pub fn trim_events<RV: FromRedisValue>(&mut self, max_length: usize) -> anyhow::Result<RV> {
        let b = format!("bull:{}:events", self.name);
        let key = self.opts.prefix.unwrap_or(&b);
        let result = self.client.xtrim(key, StreamMaxlen::Approx(max_length))?;
        Ok(result)
    }
}
