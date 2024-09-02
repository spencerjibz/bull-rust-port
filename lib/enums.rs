use derive_redis_json::RedisJsonValue;
use redis::FromRedisValue;
use serde::{Deserialize, Serialize};

use thiserror::Error;
#[repr(i8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Deserialize, Serialize, RedisJsonValue, Error)]
pub enum ErrorCode {
    #[error("The job does not exist")]
    JobNotExist = -1,
    #[error("The job lock does not exist")]
    JobLockNotExist = -2,
    #[error("The job is not in the expected state")]
    JobNotInState = -3,
    #[error("The job has pending dependencies")]
    JobPendingDependencies = -4,
    #[error("The parent job does not exist")]
    ParentJobNotExist = -5,
    #[error("The job lock does not match")]
    JobLockMismatch = -6,
}

pub enum MetricsTime {
    ONEMINUTE = 1,
    FIVEMINUTES = 5,
    FIFTEENMINUTES = 15,
    THIRTYMINUTES = 30,
    ONEHOUR = 60,
    ONEWEEK = 60 * 24 * 7,
    TWOWEEKS = 60 * 24 * 7 * 2,
    ONEMONTH = 60 * 24 * 7 * 2 * 4,
}

use std::convert::TryFrom;

impl TryFrom<i8> for ErrorCode {
    type Error = ();

    fn try_from(v: i8) -> anyhow::Result<Self, Self::Error> {
        match v {
            -1 => Ok(ErrorCode::JobNotExist),
            -2 => Ok(ErrorCode::JobLockNotExist),
            -3 => Ok(ErrorCode::JobNotInState),
            -4 => Ok(ErrorCode::JobPendingDependencies),
            -5 => Ok(ErrorCode::ParentJobNotExist),
            -6 => Ok(ErrorCode::JobLockMismatch),
            _ => Err(()),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize, RedisJsonValue, Error)]
pub enum PttlError {
    #[error("No expiration associated with Key({0})")]
    NoExpirationWithKey(String),
    #[error("Key not Found")]
    KeyNotFound,
}

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("RedisError: {0}")]
    RedisError(#[from] redis::RedisError),
    #[error("PoolError: {0}")]
    DealPoolError(#[from] deadpool_redis::PoolError),
    #[error("RedisPttlError: {0}")]
    RedisPttLError(PttlError),
}
