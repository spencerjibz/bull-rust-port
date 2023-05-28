use derive_redis_json::RedisJsonValue;
use redis::FromRedisValue;
use serde::{Deserialize, Serialize};
#[repr(i8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Deserialize, Serialize, RedisJsonValue)]
pub enum ErrorCode {
    JobNotExist = -1,
    JobLockNotExist = -2,
    JobNotInState = -3,
    JobPendingDependencies = -4,
    ParentJobNotExist = -5,
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
