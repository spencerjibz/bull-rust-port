use futures::prelude::*;
pub use redis::{aio::Connection, AsyncCommands, Client, RedisError, RedisResult};

use super::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, default};

pub struct RedisConnection<'b> {
    pub conn: Connection,
    pub client: Client,
    pub conn_options: RedisOpts<'b>,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum RedisOpts<'a> {
    Url(&'a str),
    Config(HashMap<&'a str, &'a str>),
}

impl<'b> RedisConnection<'b> {
    pub async fn init(redis_options: RedisOpts<'b>) -> RedisResult<RedisConnection> {
        use RedisOpts::*;
        match &redis_options {
            Url(s) => {
                let client = Client::open(*s)?;
                let conn = client.get_async_connection().await?;
                Ok(Self {
                    conn,
                    client,
                    conn_options: redis_options.clone(),
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
                let client = Client::open(url)?;
                let conn = client.get_async_connection().await?;

                Ok(Self {
                    conn,
                    client,
                    conn_options: redis_options.clone(),
                })
            }
        }
    }
}
