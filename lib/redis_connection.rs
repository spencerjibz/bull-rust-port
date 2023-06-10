pub use async_trait::async_trait;
pub use deadpool_redis::{
    redis::{AsyncCommands, Client, RedisResult},
    Connection, Pool, Runtime,
};
use futures::prelude::*;

use super::*;

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, default};

pub struct RedisConnection<'b> {
    pub conn: Connection,
    pub conn_options: RedisOpts<'b>,
    pub pool: Pool,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum RedisOpts<'a> {
    Url(&'a str),
    Config(HashMap<&'a str, &'a str>),
}

impl<'b> RedisConnection<'b> {
    pub async fn init(redis_options: RedisOpts<'b>) -> anyhow::Result<RedisConnection> {
        use RedisOpts::*;
        match &redis_options {
            Url(s) => {
                let mut cfg = deadpool_redis::Config::from_url(*s);
                let client = cfg.create_pool(Some(Runtime::Tokio1))?;
                let conn = client.get().await?;
                Ok(Self {
                    conn,
                    conn_options: redis_options.clone(),
                    pool: client,
                })
            }
            Config(map) => {
                let host = *map.get(&"host").unwrap_or(&"127.0.0.1");

                let port = *map.get(&"port").unwrap_or(&"6379");
                let db = *map.get(&"db").unwrap_or(&"");
                let password = *map.get(&"password").unwrap_or(&"");
                let username = *map.get(&"username").unwrap_or(&"default");

                let url = format!("redis://{username}:{password}@{host}:{port}");
                //println!("{}",url);
                let mut cfg = deadpool_redis::Config::from_url(url);
                let client = cfg.create_pool(Some(Runtime::Tokio1))?;
                let conn = client.get().await?;

                Ok(Self {
                    conn,
                    pool: client,
                    conn_options: redis_options.clone(),
                })
            }
        }
    }
}

#[async_trait]
pub trait RedisConnectionTrait {
    async fn disconnect(&self) -> anyhow::Result<()>;
    async fn close(&self) -> anyhow::Result<()>;
}

#[async_trait]
impl RedisConnectionTrait for RedisConnection<'_> {
    async fn disconnect(&self) -> anyhow::Result<()> {
        let mut conn = self.pool.get().await?;
        let _ = redis::cmd("CLIENT")
            .arg("KILL")
            .arg("TYPE")
            .arg("normal")
            .query_async::<_, ()>(&mut conn)
            .await?;

        Ok(())
    }
    async fn close(&self) -> anyhow::Result<()> {
        let mut conn = self.pool.get().await?;
        let _ = redis::cmd("SHUTDOWN")
            .query_async::<_, ()>(&mut conn)
            .await?;
        Ok(())
    }
}

//impl Default for RedisOpts<'_>
impl default::Default for RedisOpts<'_> {
    fn default() -> Self {
        RedisOpts::Url("redis://localhost:6379")
    }
}

impl RedisOpts<'_> {
    pub fn new() -> Self {
        RedisOpts::default()
    }
    pub fn from_conn_str(s: &'static str) -> Self {
        RedisOpts::Url(s)
    }
    pub fn to_conn_string(&self) -> String {
        match self {
            RedisOpts::Url(s) => s.to_string(),
            RedisOpts::Config(map) => {
                let host = *map.get(&"host").unwrap_or(&"127.0.0.1");

                let port = *map.get(&"port").unwrap_or(&"6379");
                let db = *map.get(&"db").unwrap_or(&"");
                let password = *map.get(&"password").unwrap_or(&"");
                let username = *map.get(&"username").unwrap_or(&"default");

                format!("redis://{username}:{password}@{host}:{port}")
            }
        }
    }
}
