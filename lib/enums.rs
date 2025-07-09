use async_backtrace::backtrace;
use derive_redis_json::RedisJsonValue;
use redis::FromRedisValue;
use redis_json::error;
use serde::{Deserialize, Serialize};
use std::backtrace::Backtrace;

use thiserror::Error;
#[repr(i8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Deserialize, Serialize, Error)]
pub enum JobError {
    #[error("The job does not exist")]
    JobNotFound = -1,
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

use derive_more::Display;
#[derive(Debug, Display, Error)]
pub enum QueueError {
    FailedToObliterate,
    CantObliterateWhileJobsActive,
}

#[derive(Debug, Display, Error)]
pub enum WorkerError {
    WorkerAlreadyRunningWithId(String),
    FailedToCheckStalledJobs,
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

use std::{convert::TryFrom, io};

impl TryFrom<i8> for JobError {
    type Error = ();

    fn try_from(v: i8) -> Result<Self, Self::Error> {
        match v {
            -1 => Ok(JobError::JobNotFound),
            -2 => Ok(JobError::JobLockNotExist),
            -3 => Ok(JobError::JobNotInState),
            -4 => Ok(JobError::JobPendingDependencies),
            -5 => Ok(JobError::ParentJobNotExist),
            -6 => Ok(JobError::JobLockMismatch),
            _ => Err(()),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize, Error)]
pub enum PttlError {
    #[error("No expiration associated with Key({0})")]
    NoExpirationWithKey(String),
    #[error("Key not Found")]
    KeyNotFound,
}

#[derive(Debug, Error)]
pub enum BullError {
    #[error("RedisError: {0}")]
    RedisError(#[from] redis::RedisError),
    #[error("DeadPoolError: {0}")]
    DealPoolError(#[from] deadpool_redis::PoolError),
    #[error("DeadPoolError: {0}")]
    DealPoolConfig(#[from] deadpool_redis::CreatePoolError),
    #[error("RedisPttlError: {0}")]
    RedisPttLError(PttlError),
    #[error("J: {0}")]
    RedisJobError(#[from] JobError),
    #[error("SerdeJsonError: {0}")]
    SerdeJsonError(#[from] serde_json::Error), // Handle serde_json errors
    #[error("SerdeDeserializeError: {0}")]
    SerdeDeserializeError(#[from] serde::de::value::Error),
    // Standard library errors
    #[error("IOError: {0}")]
    IoError(#[from] io::Error),
    #[error("FmtError: {0}")]
    FmtError(#[from] std::fmt::Error),
    #[error("ParseIntError: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    // Dynamic Error type for handling any other errors
    #[error("GeneralError: {0}")]
    GeneralError(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error("MessagePackEncodeError: {0}")]
    MessagePackErr(#[from] rmp_serde::encode::Error),
    #[error("SystemTimeError: {0}")]
    SystemTimeError(#[from] std::time::SystemTimeError),
    #[error("QueueError: {0}")]
    QueueError(#[from] QueueError),
    #[error("Failed to Convert: from {from} to {to}")]
    ConversionError {
        from: &'static str,
        to: &'static str,
    },
    #[error("Emitter: {0}")]
    EmitterError(String),

    #[error("WorkerError: {0:#?}")]
    WorkerError(#[from] WorkerError),
}
