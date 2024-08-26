use core::time;
use std::{collections::HashMap, fmt::format};

use crate::*;
use crate::{
    add_job3::get_next_delayed_timestamp, script_functions::add_job3::add_job_with_priority,
};
use anyhow::{Ok, Result};
use deadpool_redis::Connection;
use redis::{Cmd, Value};

use self::add_job3::get_target_queue_list;
type PackedArgs = (
    String,         // prefix
    i64,            // timestamp
    Option<String>, // optional job ID
    JobMoveOpts,    //  - {token, lockDuration,limiter}
);
/* Direct translation of functions in Commands/MoveToAcive-9.lua

*/
async fn move_job_from_await_to_acive(
    keys: &[String],
    key_prefix: &str,
    job_id: &str,
    processed_on: i64,
    opts: &JobMoveOpts,
    con: &mut Connection,
) -> Result<MoveToAciveResult> {
    let max_job_count = opts.limiter.as_ref().map(|limiter| limiter.max);
    let mut expire_time = 0_i64;
    if let Some(max_jobs) = max_job_count {
        if let Some(rate_limiter_key) = keys.get(6) {
            expire_time = Cmd::pttl(rate_limiter_key).query_async(con).await?;
            if expire_time <= 0 {
                Cmd::del(rate_limiter_key).query_async(con).await?;
            }
            let mut job_counter: i64 = redis::cmd("INCR")
                .arg(rate_limiter_key)
                .query_async(con)
                .await?;
            if job_counter == 1 {
                let limiter_duration = opts.limiter.as_ref().map(|l| l.duration).unwrap();
                Cmd::pexpire(rate_limiter_key, limiter_duration as usize)
                    .query_async(con)
                    .await?;
            }
            // check if we passed the rate limit, we need to remove the job and return expireTime
            if job_counter > max_jobs {
                // remove from active queue and add back to wait list;
                if let (Some(first), Some(second)) = (keys.first(), keys.get(1)) {
                    Cmd::lrem(second, 1, job_id).query_async(con).await?;
                    Cmd::rpush(first, job_id).query_async(con).await?;
                    // return here
                    let result: MoveToAciveResult =
                        (None, Some(0.to_string()), 0, Some(expire_time));
                    return Ok(result);
                }
            }
        }
    };

    let job_key = format!("{key_prefix}{job_id}");

    let local_key = format!("{job_key}:lock");

    if opts.token != "0" {
        Cmd::pset_ex(local_key, &opts.token, opts.lock_duration as usize)
            .query_async(con)
            .await?;
    }

    let third_key = keys.get(2).unwrap();
    let forth_key = keys.get(3).unwrap();

    Cmd::zrem(third_key, job_id).query_async(con).await?;

    let items = [("event", "active"), ("jobId", job_id), ("prev", "waiting")];
    Cmd::xadd(forth_key, "*", &items).query_async(con).await?;
    Cmd::hset(&job_key, "processedOn", processed_on)
        .query_async(con)
        .await?;
    Cmd::hincr(&job_key, "attemptsMade", 1)
        .query_async(con)
        .await?;

    let next_job: Vec<String> = Cmd::hgetall(job_key).query_async(con).await?;

    let result: MoveToAciveResult = (
        Some(next_job),
        Some(job_id.to_owned()),
        0,
        Some(expire_time),
    );

    Ok(result)
}

pub async fn get_rate_limit_ttl(
    opts: &JobMoveOpts,
    limiter_key: &str,
    con: &mut Connection,
) -> Result<i64> {
    let max_job_count = opts.limiter.as_ref().map(|limiter| limiter.max);

    if let Some(max_jobs) = max_job_count {
        let job_counter: Option<i64> = Cmd::get(limiter_key).query_async(con).await?;
        if let Some(counter) = job_counter {
            if counter >= max_jobs {
                let result: i64 = Cmd::pttl(limiter_key).query_async(con).await?;
                return Ok(result);
            }
        }
    }
    Ok(0)
}
#[allow(clippy::too_many_arguments)]
pub async fn promote_delayed_jobs(
    delayed_key: &str,
    wait_key: &str,
    target_key: &str,
    priority_key: &str,
    event_stream_key: &str,
    prefix: &str,
    timestamp: i64,
    paused: bool,
    priority_counter_key: &str,
    con: &mut Connection,
) -> Result<()> {
    let limit = 1000;
    let jobs: Vec<String> =
        Cmd::zrangebyscore_limit(delayed_key, 0, (timestamp + 1) * 0x1000, 0, limit)
            .query_async(con)
            .await?;
    dbg!(&jobs, delayed_key);

    if !jobs.is_empty() {
        Cmd::zrem(delayed_key, &jobs).query_async(con).await?;
        for job_id in jobs.iter() {
            let job_key = format!("{prefix}:{job_id}");
            dbg!(&job_key);
            let priority: i64 = Cmd::hget(job_key, "priority").query_async(con).await?;

            if priority == 0 {
                Cmd::lpush(target_key, job_id).query_async(con).await?;
            } else {
                add_job_with_priority(
                    con,
                    wait_key,
                    priority_key,
                    priority,
                    paused,
                    job_id,
                    priority_counter_key,
                )
                .await?;
            }
            // emit waiting event;
            let items = [("event", "waiting"), ("jobId", job_id), ("prev", "delayed")];
            Cmd::xadd(event_stream_key, "*", &items)
                .query_async(con)
                .await?;

            Cmd::hset(format!("{prefix}:{job_id}"), "delay", 0)
                .query_async(con)
                .await?;
        }
    }

    Ok(())
}

pub async fn move_job_to_active(
    keys: &[String],
    args: PackedArgs,
    con: &mut Connection,
) -> Result<MoveToAciveResult> {
    //---------------------------KEYS--------------------------------------------------
    let wait_key = keys.first().unwrap();
    let active_key = keys.get(1).unwrap();
    let priority_key = keys.get(2).unwrap();
    let event_stream_key = keys.get(3).unwrap();
    let stalled_key = keys.get(4).unwrap();
    // --- Rate limiting ---
    let rate_limiter_key = keys.get(5).unwrap();
    let delayed_key = keys.get(6).unwrap();
    // ---- Promoting delayed jobs
    let paused_key = keys.get(7).unwrap();
    let meta_key = keys.get(8).unwrap();
    let priority_counter_key = keys.get(9).unwrap();
    // - --------------------------------------------------------------------

    // ------------------ARGUMENTS--------------------------------------------------------
    let (prefix, timestamp, mut job_id, opts) = args;
    // --------------------------------------------------------------------------------------

    let (target, paused) = get_target_queue_list(meta_key, wait_key, paused_key, con).await?;
    let mut next_job_id: Option<String> = None;

    // check if there are delayed jobs that we can move to wait

    promote_delayed_jobs(
        delayed_key,
        wait_key,
        &target,
        priority_key,
        event_stream_key,
        &prefix,
        timestamp,
        paused,
        priority_counter_key,
        con,
    )
    .await?;
    let max_jobs = opts.limiter.as_ref().map(|limiter| limiter.max);
    let expire_time = get_rate_limit_ttl(&opts, rate_limiter_key, con).await?;

    if job_id.as_ref().is_some() && !job_id.as_ref().unwrap().is_empty() {
        let id = job_id.as_ref().unwrap();
        // clean stalled
        next_job_id = Some(id.clone());
        Cmd::srem(stalled_key, id).query_async(con).await?;
    }
    let job_id_ref = job_id.as_ref();
    if job_id_ref.is_none() || (job_id.is_some() && job_id_ref.unwrap().starts_with("0:")) {
        // if jobId is special ID with 0:delay, then there is no job to process;
        if let Some(val) = job_id_ref {
            Cmd::lrem(active_key, 1, val).query_async(con).await?
        }
        // check if wa're rate-limited first
        if expire_time > 0 {
            let result = (None, None, expire_time, None);
            return Ok(result);
        }

        // paused queue;
        if paused {
            let paused_result = (None, None, 0, None);
            return Ok(paused_result);
        }
        // no job, try non-blocking move from wait to active
        next_job_id = Cmd::rpoplpush(wait_key, active_key)
            .query_async(con)
            .await?;
        // since its possible that between a call to BROPLPUSH and moveToActive
        // Another script puts a new maker in wait, we need to check again
        if job_id_ref.is_some() && job_id_ref.unwrap().starts_with("0:") {
            let id = job_id_ref.unwrap();
            Cmd::lrem(active_key, 1, id).query_async(con).await?;
            next_job_id = Cmd::rpoplpush(wait_key, active_key)
                .query_async(con)
                .await?;
        }
    }
    let mut copy_next_id = next_job_id.clone();
    if let Some(id) = copy_next_id {
        return prepare_job_for_processing(
            keys,
            &prefix,
            &target,
            &id,
            timestamp,
            max_jobs,
            expire_time,
            &opts,
            false,
            con,
        )
        .await;
    }

    copy_next_id =
        move_job_from_priority_to_active(priority_key, active_key, priority_counter_key, con)
            .await?;

    if let Some(id) = copy_next_id {
        return prepare_job_for_processing(
            keys,
            &prefix,
            &target,
            &id,
            timestamp,
            max_jobs,
            expire_time,
            &opts,
            false,
            con,
        )
        .await;
    }

    // return the timestamp for the next delayed job if any.
    let next_timestamp = get_next_delayed_timestamp(con, &keys[6]).await?;

    Ok((None, None, 0, next_timestamp))
}

#[allow(clippy::too_many_arguments)]
pub async fn prepare_job_for_processing(
    keys: &[String],
    key_prefix: &str,
    target_key: &str,
    job_id: &str,
    processed_on: i64,
    max_jobs: Option<i64>,
    expire_time: i64,
    opts: &JobMoveOpts,
    move_to_finish: bool,
    con: &mut Connection,
) -> Result<MoveToAciveResult> {
    let job_key = format!("{}:{}", key_prefix, job_id);

    if let Some(max_jobs) = max_jobs {
        let rate_limiter_key = &keys[5];
        if expire_time > 0 {
            Cmd::lrem(&keys[1], 1, job_id).query_async(con).await?;
            let priority: i64 = Cmd::hget(job_key.as_str(), "priority")
                .query_async(con)
                .await?;
            if priority == 0 {
                Cmd::rpush(target_key, job_id).query_async(con).await?;
            } else {
                push_back_job_with_priority(&keys[2], priority, job_id, con).await?;
            }
            return Ok((None, None, expire_time, None));
        }
        let job_counter: i64 = Cmd::incr(rate_limiter_key, 1).query_async(con).await?;
        if job_counter == 1 {
            if let Some(limiter) = &opts.limiter {
                let limiter_duration = limiter.duration;
                let integer_duration = limiter_duration.abs();
                Cmd::pexpire(rate_limiter_key, integer_duration as usize)
                    .query_async(con)
                    .await?;
            }
        }
    }

    let lock_key = format!("{}:lock", &job_key);
    if opts.token != "0" {
        Cmd::set_ex(lock_key, &opts.token, opts.lock_duration as usize)
            .query_async(con)
            .await?;
    }

    Cmd::xadd(
        &keys[3],
        "*",
        &[("event", "active"), ("jobId", job_id), ("prev", "waiting")],
    )
    .query_async(con)
    .await?;
    Cmd::hset(job_key.as_str(), "processedOn", processed_on)
        .query_async(con)
        .await?;
    // only increment when moving the job to finished;
    if move_to_finish {
        Cmd::hincr(job_key.as_str(), "attemptsMade", 1)
            .query_async(con)
            .await?;
    }

    let job_data: Vec<String> = Cmd::hgetall(job_key).query_async(con).await?;

    dbg!(&job_data, move_to_finish);
    Ok((Some(job_data), Some(job_id.to_owned()), 0, None))
}

// Helper function to push back job with priority (not provided in the original Lua code)
async fn push_back_job_with_priority(
    priority_queue_key: &str,
    priority: i64,
    job_id: &str,
    con: &mut Connection,
) -> Result<()> {
    let score = priority * 0x100000000;
    Cmd::zadd(priority_queue_key, job_id, score)
        .query_async(con)
        .await?;

    Ok(())
}

pub async fn move_job_from_priority_to_active(
    priority_key: &str,
    active_key: &str,
    priority_counter_key: &str,
    con: &mut Connection,
) -> Result<Option<String>> {
    let prioritized_jobs: Vec<String> = Cmd::zpopmin(priority_key, 1).query_async(con).await?;

    if !prioritized_jobs.is_empty() {
        Cmd::lpush(active_key, &prioritized_jobs[0])
            .query_async(con)
            .await?;
        Ok(Some(prioritized_jobs[0].clone()))
    } else {
        Cmd::del(priority_counter_key).query_async(con).await?;
        Ok(None)
    }
}
