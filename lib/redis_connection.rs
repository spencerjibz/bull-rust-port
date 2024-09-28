pub use async_trait::async_trait;
pub use deadpool_redis::{
    redis::{AsyncCommands, Client, RedisResult},
    Connection, Pool, Runtime,
};
use enums::BullError;
use futures::prelude::*;

use super::*;

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, default, hash::Hash};

pub struct RedisConnection {
    pub conn: Connection,
    pub conn_options: RedisOpts,
    pub pool: Pool,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum RedisOpts {
    Url(&'static str),
    Config(HashMap<&'static str, &'static str>),
}

impl RedisConnection {
    pub async fn init(redis_options: RedisOpts) -> Result<RedisConnection, BullError> {
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
    pub fn to_conn_string(&self) -> String {
        use RedisOpts::*;
        let opts = &self.conn_options;
        opts.to_conn_string()
    }
}

#[async_trait]
pub trait RedisConnectionTrait {
    async fn disconnect(&self) -> Result<(), BullError>;
    async fn close(&self) -> Result<(), BullError>;
}

#[async_trait]
impl RedisConnectionTrait for RedisConnection {
    async fn disconnect(&self) -> Result<(), BullError> {
        let mut conn = self.pool.get().await?;
        let _ = redis::cmd("CLIENT")
            .arg("KILL")
            .arg("TYPE")
            .arg("normal")
            .query_async::<_, ()>(&mut conn)
            .await?;

        Ok(())
    }
    async fn close(&self) -> Result<(), BullError> {
        let mut conn = self.pool.get().await?;
        let _ = redis::cmd("SHUTDOWN")
            .query_async::<_, ()>(&mut conn)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl RedisConnectionTrait for Pool {
    async fn disconnect(&self) -> Result<(), BullError> {
        let mut conn = self.get().await?;
        let _ = redis::cmd("CLIENT")
            .arg("KILL")
            .arg("TYPE")
            .arg("normal")
            .query_async::<_, ()>(&mut conn)
            .await?;

        Ok(())
    }
    async fn close(&self) -> Result<(), BullError> {
        let mut conn = self.get().await?;
        let _ = redis::cmd("SHUTDOWN")
            .query_async::<_, ()>(&mut conn)
            .await?;
        Ok(())
    }
}
//impl Default for RedisOpts<'_>
impl default::Default for RedisOpts {
    fn default() -> Self {
        RedisOpts::Url("redis://localhost:6379")
    }
}

impl RedisOpts {
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

    pub fn from_string_map<T: ToString + Eq + Hash>(map: HashMap<T, T>) -> RedisOpts {
        let mut hash: HashMap<&'static str, &'static str> = HashMap::new();

        for (key) in map.keys() {
            let key_static = to_static_str(key.to_string());
            if let Some(val) = map.get(key) {
                let result = to_static_str(val.to_string());
                hash.insert(key_static, result);
            }
        }
        RedisOpts::Config(hash)
    }
}

pub fn fetch_redis_pass() -> String {
    use dotenv;
    if let Err(err) = dotenv::dotenv() {
        // dothing; continue
    }
    std::env::var("REDIS_PASSWORD").unwrap_or_default()
}
