use crate::script_functions::*;
use anyhow::{Ok, Result};
use deadpool_redis::Connection;
use redis::{Cmd, Commands};
use serde_json::Value;

use self::{
    add_job::get_target_queue_list,
    move_to_finished::{
        batches, remove_job, remove_jobs_by_max_age, remove_jobs_by_max_count, trim_events,
    },
};

// Function to get target queue list.

// Function to check stalled jobs.
pub async fn check_stalled_jobs(
    keys: &[String],
    // keys
    max_stalled_job_count: usize,
    queue_key_prefix: &str,
    timestamp: u64,
    max_check_time: i64,
    con: &mut Connection,
) -> Result<(Vec<String>, Vec<String>)> {
    // ----------------------------- KEYS ---------------------------------

    let stalled_key = keys.first().unwrap();
    let wait_key = keys.get(1).unwrap();
    let active_key = keys.get(2).unwrap();
    let failed_key = keys.get(3).unwrap();
    let stalled_check_key = keys.get(4).unwrap();
    let meta_key = keys.get(5).unwrap();
    let paused_key = keys.get(6).unwrap();
    let event_stream_key = keys.get(7).unwrap();
    let stalled_key_exists: isize = Cmd::exists(stalled_check_key).query_async(con).await?;
    if dbg!(stalled_key_exists) == 1 {
        return Ok((vec![], vec![]));
    }
    Cmd::set_ex(stalled_check_key, timestamp, max_check_time as usize)
        .query_async(con)
        .await?;
    trim_events(meta_key, event_stream_key, con).await?;
    let (failed, stalled) = move_stalled_jobs(
        con,
        stalled_key,
        wait_key,
        active_key,
        failed_key,
        meta_key,
        paused_key,
        event_stream_key,
        max_stalled_job_count,
        queue_key_prefix,
        timestamp,
    )
    .await?;
    Ok((failed, stalled))
}

// Function to move stalled jobs.
#[allow(clippy::too_many_arguments)]
pub async fn move_stalled_jobs(
    con: &mut Connection,
    stalled_key: &str,
    wait_key: &str,
    active_key: &str,
    failed_key: &str,
    meta_key: &str,
    paused_key: &str,
    event_stream_key: &str,
    max_stalled_job_count: usize,
    queue_key_prefix: &str,
    timestamp: u64,
) -> Result<(Vec<String>, Vec<String>)> {
    let stalling: Vec<String> = Cmd::smembers(stalled_key).query_async(con).await?;
    let mut stalled = Vec::new();
    let mut failed = Vec::new();
    if !stalling.is_empty() {
        Cmd::del(stalled_key).query_async(con).await?;
        let max_stalled_job_count = max_stalled_job_count as u64;
        for job_id in stalling {
            if job_id.starts_with("0:") {
                Cmd::lrem(active_key, 1, &job_id).query_async(con).await?;
            } else {
                let job_key = format!("{}:{}", queue_key_prefix, job_id);
                let job_key_exists: bool = Cmd::exists(&format!("{}:lock", job_key))
                    .query_async(con)
                    .await?;
                if job_key_exists {
                    let removed: isize = Cmd::lrem(active_key, 1, &job_id).query_async(con).await?;
                    if removed > 0 {
                        let stalled_count: u64 = Cmd::hincr(&job_key, "stalledCounter", 1)
                            .query_async(con)
                            .await?;
                        if stalled_count > max_stalled_job_count {
                            let raw_opts: Option<String> =
                                Cmd::hget(&job_key, "opts").query_async(con).await?;
                            if let Some(raw_opts) = raw_opts {
                                if let core::result::Result::Ok(opts) =
                                    serde_json::from_str::<Value>(&raw_opts)
                                {
                                    let remove_on_fail_type = opts["removeOnFail"].as_str();
                                    Cmd::zadd(failed_key, timestamp, &job_id)
                                        .query_async(con)
                                        .await?;
                                    let failed_reason = "job stalled more than allowable limit";
                                    Cmd::hset_multiple(
                                        &job_key,
                                        &[
                                            ("failedReason", failed_reason),
                                            ("finishedOn", &timestamp.to_string()),
                                        ],
                                    )
                                    .query_async(con)
                                    .await?;
                                    Cmd::xadd(
                                        event_stream_key,
                                        "*",
                                        &[
                                            ("event", "failed"),
                                            ("jobId", &job_id),
                                            ("prev", "active"),
                                            ("failedReason", failed_reason),
                                        ],
                                    )
                                    .query_async(con)
                                    .await?;
                                    match remove_on_fail_type {
                                        Some("number") => {
                                            if let Some(max) = opts["removeOnFail"].as_u64() {
                                                remove_jobs_by_max_count(
                                                    max as isize,
                                                    failed_key,
                                                    queue_key_prefix,
                                                    con,
                                                )
                                                .await?;
                                            }
                                        }
                                        Some("boolean") => {
                                            if let Some(remove) = opts["removeOnFail"].as_bool() {
                                                if remove {
                                                    remove_job(
                                                        &job_id,
                                                        false,
                                                        queue_key_prefix,
                                                        con,
                                                    )
                                                    .await?;
                                                    Cmd::zrem(failed_key, &job_id)
                                                        .query_async(con)
                                                        .await?;
                                                }
                                            }
                                        }
                                        Some(_) => {
                                            if let Some(max_age) =
                                                opts["removeOnFail"]["age"].as_u64()
                                            {
                                                remove_jobs_by_max_age(
                                                    timestamp as i64,
                                                    max_age as i64,
                                                    failed_key,
                                                    queue_key_prefix,
                                                    con,
                                                )
                                                .await?;
                                            }
                                            if let Some(max_count) =
                                                opts["removeOnFail"]["count"].as_u64()
                                            {
                                                remove_jobs_by_max_count(
                                                    max_count as isize,
                                                    failed_key,
                                                    queue_key_prefix,
                                                    con,
                                                )
                                                .await?;
                                            }
                                        }
                                        None => {}
                                    }
                                    failed.push(job_id);
                                }
                            }
                        } else {
                            let (target, _) =
                                get_target_queue_list(meta_key, wait_key, paused_key, con).await?;
                            Cmd::rpush(&target, &job_id).query_async(con).await?;
                            Cmd::xadd(
                                event_stream_key,
                                "*",
                                &[("event", "waiting"), ("jobId", &job_id), ("prev", "active")],
                            )
                            .query_async(con)
                            .await?;
                            Cmd::xadd(
                                event_stream_key,
                                "*",
                                &[("event", "stalled"), ("jobId", &job_id)],
                            )
                            .query_async(con)
                            .await?;
                            stalled.push(job_id);
                        }
                    }
                }
            }
        }
    }

    let active: Vec<String> = Cmd::lrange(active_key, 0, -1).query_async(con).await?;

    for (from, to) in batches(active.len(), 7000) {
        dbg!(from, to);
        let batch: Vec<&str> = active.iter().map(|x| x.as_str()).collect();
        Cmd::sadd(stalled_key, &batch).query_async(con).await?;
    }

    Ok((failed, stalled))
}
