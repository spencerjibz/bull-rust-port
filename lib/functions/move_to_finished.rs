use crate::convert_errors;
use crate::functions::add_job3::*;
use crate::functions::move_to_active::*;
use crate::MoveToAciveResult;
use crate::MoveToFinishedResult;
use crate::Parent;
use crate::{job, JobMoveOpts};
use anyhow::{Ok, Result};
use async_recursion::async_recursion;
use core::time;
use deadpool_redis::Connection;
use futures::future::ok;
use redis::Cmd;
use std::fmt::format;
use std::{collections::HashMap, path::Ancestors};

///
/// Function to loop in batches.
/// Just a bit of warning, some commands as ZREM
///could receive a maximum of 7000 parameters per call.

fn batches(n: usize, batch_size: usize) -> impl Iterator<Item = (usize, usize)> {
    (0..).scan(0, move |state, _| {
        let from = *state * batch_size + 1;
        *state += 1;
        if from <= n {
            let to = std::cmp::min(from + batch_size - 1, n);
            Some((from, to))
        } else {
            None
        }
    })
}

async fn collect_metrics(
    meta_key: &str,
    data_points_list: &str,
    max_data_points: isize,
    timestamp: i64,
    con: &mut Connection,
) -> Result<()> {
    // Increment current count
    let mut count: i64 = Cmd::hincr(meta_key, "count", 1).query_async(con).await?;
    count -= 1;
    // Compute how many data points we need to add to the list, N.
    let prev_ts: Option<i64> = Cmd::hget(meta_key, "prevTS").query_async(con).await?;
    if prev_ts.is_none() {
        // If prevTS is nil, set it to the current timestamp
        Cmd::hset(meta_key, "prevTS", timestamp)
            .query_async(con)
            .await?;
        Cmd::hset(meta_key, "prevCount", 0).query_async(con).await?;
        return Ok(());
    }
    let prev_ts = prev_ts.unwrap();
    let n = (timestamp - prev_ts) / 60000;
    if n > 0 {
        let other: i64 = Cmd::hget(meta_key, "prevCount").query_async(con).await?;
        let delta = count - other;
        // If N > 1, add N-1 zeros to the list
        if n > 1 {
            let mut points = vec![delta];
            points.extend(vec![0; (n - 1) as usize]);
            for points_chunk in points.chunks(7000) {
                Cmd::lpush(data_points_list, points_chunk)
                    .query_async(con)
                    .await?;
            }
        } else {
            // LPUSH delta to the list
            Cmd::lpush(data_points_list, delta).query_async(con).await?;
        }
        // LTRIM to keep list to its max size
        Cmd::ltrim(data_points_list, 0, max_data_points - 1)
            .query_async(con)
            .await?;
        // update prev count with current count
        Cmd::hset(meta_key, "prevCount", count)
            .query_async(con)
            .await?;
        Cmd::hset(meta_key, "prevTS", timestamp)
            .query_async(con)
            .await?;
    }
    Ok(())
}

async fn move_parent_to_wait(
    parent_prefix: &str,
    parent_id: &str,
    emit_event: bool,
    con: &mut Connection,
) -> Result<()> {
    let parent_target = format!(
        "{}{}:{}{}:{}{}",
        parent_prefix, "meta", parent_prefix, "wait", parent_prefix, "paused"
    );
    Cmd::rpush(&parent_target, parent_id)
        .query_async(con)
        .await?;

    if emit_event {
        let parent_event_stream = format!("{}{}", parent_prefix, "events");

        Cmd::xadd(
            &parent_event_stream,
            "*",
            &[
                ("event", "waiting"),
                ("jobId", parent_id),
                ("prev", "waiting-children"),
            ],
        )
        .query_async(con)
        .await?;
    }
    Ok(())
}

async fn remove_job(job_id: &str, hard: bool, base_key: &str, con: &mut Connection) -> Result<()> {
    let job_key = format!("{}{}", base_key, job_id);
    remove_parent_dependency_key(&job_key, hard, None, base_key, con);
    Cmd::del(&[
        job_key.clone(),
        format!("{}:logs", job_key),
        format!("{}:dependencies", job_key),
        format!("{}:processed", job_key),
    ])
    .query_async(con)
    .await?;

    Ok(())
}

///   Functions to remove jobs by max count.
async fn remove_jobs_by_max_age(
    timestamp: i64,
    max_age: i64,
    target_set: &str,
    prefix: &str,
    con: &mut Connection,
) -> Result<()> {
    let start = timestamp - max_age * 1000;
    let job_ids: Vec<String> = Cmd::zrevrangebyscore(target_set, start, "-inf")
        .query_async(con)
        .await?;
    for job_id in job_ids.iter() {
        remove_job(job_id, false, prefix, con).await?;
    }
    Cmd::zrembyscore(target_set, "-inf", start)
        .query_async(con)
        .await?;
    Ok(())
}

async fn remove_jobs_by_max_count(
    max_count: isize,
    target_set: &str,
    prefix: &str,
    con: &mut Connection,
) -> Result<()> {
    let start = max_count;
    let job_ids: Vec<String> = Cmd::zrevrange(target_set, start, -1)
        .query_async(con)
        .await?;
    for job_id in job_ids.iter() {
        remove_job(job_id, false, prefix, con).await?;
    }
    Cmd::zremrangebyrank(target_set, 0, -(max_count + 1))
        .query_async(con)
        .await?;
    Ok(())
}

use uuid::timestamp;

use crate::{KeepJobs, MoveToFinishOpts, WorkerOptions};
#[async_recursion]
async fn remove_parent_dependency_key(
    job_key: &str,
    hard: bool,
    parent_key: Option<String>,
    base_key: &str,
    con: &mut Connection,
) -> Result<()> {
    if let Some(pk) = parent_key {
        let parent_dependencies_key = format!("{}:dependencies", pk);
        let result: isize = Cmd::srem(&parent_dependencies_key, job_key)
            .query_async(con)
            .await?;
        if result > 0 {
            let pending_dependencies: isize = Cmd::scard(&parent_dependencies_key)
                .query_async(con)
                .await?;
            if pending_dependencies == 0 {
                let parent_id = get_job_id_from_key(&pk).unwrap_or_default();
                let parent_prefix = get_job_key_prefix(&pk, parent_id);
                let num_removed_elements: isize =
                    Cmd::zrem(format!("{}waiting-children", parent_prefix), parent_id)
                        .query_async(con)
                        .await?;
                if num_removed_elements == 1 {
                    if hard {
                        if parent_prefix == base_key {
                            remove_parent_dependency_key(&pk, hard, None, base_key, con).await?;
                            Cmd::del(&[
                                &pk,
                                &format!("{}:logs", pk),
                                &format!("{}:dependencies", pk),
                                &format!("{}:processed", pk),
                            ])
                            .query_async(con)
                            .await?;
                        } else {
                            move_parent_to_wait(&parent_prefix, parent_id, false, con).await?;
                        }
                    } else {
                        move_parent_to_wait(&parent_prefix, parent_id, false, con).await?;
                    }
                }
            }
        }
    } else {
        let missed_parent_key: Option<String> =
            Cmd::hget(job_key, "parentKey").query_async(con).await?;
        if let Some(mpk) = missed_parent_key {
            let mpk_exists: bool = Cmd::exists(&mpk).query_async(con).await?;
            if !mpk.is_empty() && mpk_exists {
                let parent_dependencies_key = format!("{}:dependencies", mpk);
                let result: isize = Cmd::srem(&parent_dependencies_key, job_key)
                    .query_async(con)
                    .await?;
                if result > 0 {
                    let pending_dependencies: isize = Cmd::scard(&parent_dependencies_key)
                        .query_async(con)
                        .await?;
                    if pending_dependencies == 0 {
                        let parent_id = get_job_id_from_key(&mpk).unwrap_or_default();
                        let parent_prefix = get_job_key_prefix(&mpk, parent_id);
                        let num_removed_elements: isize =
                            Cmd::zrem(format!("{}waiting-children", parent_prefix), parent_id)
                                .query_async(con)
                                .await?;
                        if num_removed_elements == 1 {
                            if hard {
                                if parent_prefix == base_key {
                                    remove_parent_dependency_key(&mpk, hard, None, base_key, con)
                                        .await?;
                                    Cmd::del(&[
                                        &mpk,
                                        &format!("{}:logs", &mpk),
                                        &format!("{}:dependencies", &mpk),
                                        &format!("{}:processed", &mpk),
                                    ])
                                    .query_async(con)
                                    .await?;
                                } else {
                                    move_parent_to_wait(&parent_prefix, parent_id, false, con)
                                        .await?;
                                }
                            } else {
                                move_parent_to_wait(&parent_prefix, parent_id, true, con).await?;
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Extracts the job ID from a given job key.
fn get_job_id_from_key(job_key: &str) -> Option<&str> {
    job_key.split(':').last()
}

/// Extracts the job key prefix by removing the job ID from the given job key.
fn get_job_key_prefix(job_key: &str, job_id: &str) -> String {
    job_key[0..job_key.len() - job_id.len()].to_string()
}

pub async fn trim_events(
    meta_key: &str,
    event_stream_key: &str,
    con: &mut Connection,
) -> Result<()> {
    use redis::streams::StreamMaxlen;
    let max_events: Option<usize> = Cmd::hget(meta_key, "opts.maxLenEvents")
        .query_async(con)
        .await
        .unwrap_or(None);
    let max_events = max_events.unwrap_or(10000);
    Cmd::xtrim(event_stream_key, StreamMaxlen::Approx(max_events))
        .query_async(con)
        .await?;

    Ok(())
}

#[async_recursion]
async fn move_parent_from_waiting_children_to_failed(
    parent_queue_key: &str,
    parent_key: &str,
    parent_id: &str,
    job_id_key: &str,
    timestamp: i64,
    con: &mut Connection,
) -> Result<()> {
    let waiting_children_key = format!("{}:waiting-children", parent_queue_key);
    let failed_key = format!("{}:failed", parent_queue_key);
    let events_key = format!("{}:events", parent_queue_key);
    let waiting_chd_count: i64 = Cmd::zrem(&waiting_children_key, parent_id)
        .query_async(con)
        .await?;
    if waiting_chd_count == 1 {
        Cmd::zadd(&failed_key, parent_id, timestamp)
            .query_async(con)
            .await?;
        let failed_reason = format!("child {} failed", job_id_key);
        Cmd::hset_multiple(
            parent_key,
            &[
                ("failedReason", failed_reason.as_str()),
                ("finishedOn", &timestamp.to_string()),
            ],
        )
        .query_async(con)
        .await?;
        Cmd::xadd(
            &events_key,
            "*",
            &[
                ("event", "failed"),
                ("jobId", parent_id),
                ("failedReason", failed_reason.as_str()),
                ("prev", "waiting-children"),
            ],
        )
        .query_async(con)
        .await?;
        let raw_parent_data: Option<String> =
            Cmd::hget(parent_key, "parent").query_async(con).await?;
        if let Some(raw_data) = raw_parent_data {
            let parent_data: Parent = serde_json::from_str(&raw_data)?;
            if parent_data.fail_parent_on_failure {
                move_parent_from_waiting_children_to_failed(
                    parent_data.queue.as_str(),
                    &format!("{}:{}", parent_data.queue.as_str(), parent_data.id.as_str()),
                    parent_data.id.as_str(),
                    parent_key,
                    timestamp,
                    con,
                )
                .await?;
            } else if parent_data.remove_deps_on_failure {
                let grand_parent_key =
                    format!("{}:{}", parent_data.queue.as_str(), parent_data.id.as_str());
                let grand_parent_dependencies_set = format!("{}:dependencies", grand_parent_key);
                let grand_parent_deps_counts: i64 =
                    Cmd::srem(&grand_parent_dependencies_set, parent_key)
                        .query_async(con)
                        .await?;
                if grand_parent_deps_counts == 1 {
                    move_parent_to_wait_if_needed(
                        con,
                        &parent_data.queue,
                        &grand_parent_dependencies_set,
                        &grand_parent_key,
                        &parent_data.id,
                        timestamp,
                    )
                    .await?;
                }
            }
        }
    }
    Ok(())
}
#[allow(clippy::too_many_arguments)]
pub async fn update_parent_deps_if_needed(
    parent_key: &str,
    parent_queue_key: &str,
    parent_deps_key: &str,
    parent_id: &str,
    job_id_key: &str,
    returned_value: &str,
    timestamp: i64,
    con: &mut Connection,
) -> Result<()> {
    let processed_set = format!("{parent_key}:processed");
    Cmd::hset(processed_set, job_id_key, returned_value)
        .query_async(con)
        .await?;

    move_parent_to_wait_if_needed(
        con,
        parent_queue_key,
        parent_deps_key,
        parent_key,
        parent_id,
        timestamp,
    )
    .await
}

pub(crate) type MoveToFinishedArgs = (
    String,           // jobId;
    i64,              // timestamp
    String,           //return_value or failedReason (prop_name)
    String,           // return_value or failedReason
    String,           // target (completed/failed)
    Option<String>,   // event_data
    bool,             //fetch_next,
    String,           // prefix
    MoveToFinishOpts, // opts
);

pub async fn move_job_to_finished(
    keys: &[String],
    args: MoveToFinishedArgs,
    con: &mut Connection,
) -> Result<MoveToFinishedResult> {
    let (
        job_id,
        timestamp,
        prop_value,
        return_value_or_failed_reason,
        target,
        event_data,
        fetch_next,
        prefix_key,
        opts,
    ) = args;
    // KEYS

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
    let completed_or_failed_key = keys.get(10).unwrap();
    let job_id_key = keys.get(11).unwrap();
    let metrics_keys = keys.get(12).unwrap();
    // OPTIONS
    let token = opts.token;
    let attempts = opts.attempts;
    let attempts_made = opts.attempts_made;
    let max_metrics_size = opts.max_metrics_size;
    let max_count = opts.keep_jobs.count.unwrap_or(1);
    let max_age = opts.keep_jobs.age.unwrap_or_default();
    let fpof = opts.fail_parent_on_failure;
    let rdof = opts.remove_deps_on_failure;

    // Arguments
    let move_opts = &JobMoveOpts {
        token: token.clone(),
        limiter: Some(opts.limiter.clone()),
        lock_duration: opts.lock_duration,
    };

    let job_key_exists: bool = Cmd::exists(job_id_key).query_async(con).await?;
    if job_key_exists {
        if token != "0" {
            let lock_key = format!("{}:lock", job_id_key);
            let lock_token: Option<String> = Cmd::get(&lock_key).query_async(con).await?;
            if let Some(l_token) = lock_token.as_ref() {
                if l_token == &token {
                    Cmd::del(&lock_key).query_async(con).await?;
                    Cmd::srem(stalled_key, &job_id).query_async(con).await?;
                } else {
                    if lock_token.as_ref().is_some() {
                        return Ok((None, None, -6, None));
                    }
                    return Ok((None, None, -2, None));
                }
            }
        }
        // Make sure it does not have pending dependencies
        let pending_deps_count: i64 = Cmd::scard(&format!("{}:dependencies", job_id_key))
            .query_async(con)
            .await?;
        if pending_deps_count > 0 {
            return Ok((None, None, -4, None));
        }

        let parent_references: Vec<String> = Cmd::hget(job_id_key, &[("parentKey", "parent")])
            .query_async(con)
            .await
            .unwrap_or_default();
        let mut parent_key = parent_references.first().cloned().unwrap_or_default();
        let mut parent_id = String::new();
        let mut parent_queue_key = String::new();

        if let Some(parent_val) = parent_references.get(1) {
            let json_parent: Parent = serde_json::from_str(parent_val)?;
            let id = json_parent.clone().id;
            parent_id = id;
            parent_queue_key = json_parent.queue;
        }

        let num_removed_elements: i64 = Cmd::lrem(active_key, -1, &job_id).query_async(con).await?;
        if num_removed_elements < 1 {
            return Ok((None, None, -3, None));
        }

        //trim_events(meta_key, event_stream_key, con).await?;

        if parent_id.is_empty() && !parent_key.is_empty() {
            parent_id = get_job_id_from_key(&parent_key)
                .unwrap_or_default()
                .to_owned();
            parent_key = get_job_key_prefix(parent_key.as_str(), &format!(":{}", &parent_id));
        }

        if !parent_id.is_empty() {
            if &target == "completed" {
                let dependencies_set = format!("{}:dependencies", &parent_key);
                let deps_count: i64 = Cmd::srem(&parent_key, job_id_key).query_async(con).await?;
                if deps_count == 1 {
                    update_parent_deps_if_needed(
                        &parent_key,
                        &parent_queue_key,
                        &dependencies_set,
                        &parent_id,
                        job_id_key,
                        &return_value_or_failed_reason,
                        timestamp,
                        con,
                    )
                    .await?;
                }
            } else if fpof {
                move_parent_from_waiting_children_to_failed(
                    &parent_queue_key,
                    &parent_key,
                    &parent_id,
                    job_id_key,
                    timestamp,
                    con,
                );
            } else if rdof {
                let deps_set_key = format!("{}:dependencies", parent_key);
                let deps_set_count: i32 = Cmd::srem(&deps_set_key, job_id_key)
                    .query_async(con)
                    .await?;
                if deps_set_count == 1 {
                    move_parent_to_wait_if_needed(
                        con,
                        &parent_queue_key,
                        &deps_set_key,
                        &parent_key,
                        &parent_id,
                        timestamp,
                    )
                    .await?;
                }
            }
        }

        if max_count != 0 {
            let target_set = &completed_or_failed_key;
            Cmd::zadd(target_set, &job_id, timestamp)
                .query_async(con)
                .await?;
            Cmd::hset_multiple(
                job_id_key,
                &[
                    (prop_value.as_str(), &return_value_or_failed_reason),
                    ("finishedOn", &timestamp.to_string()),
                ],
            )
            .query_async(con)
            .await?;
            let prefix = &prefix_key;
            if max_age > 0 {
                remove_jobs_by_max_age(timestamp, max_age, target_set, prefix, con).await?;
            }
            if max_count > 0 {
                remove_jobs_by_max_count(max_count as isize, target_set, prefix, con).await?;
            }
        } else {
            Cmd::del(&[
                job_id_key,
                &format!("{}:logs", job_id_key),
                &format!("{}:processed", job_id_key),
            ])
            .query_async(con)
            .await?;
            if !parent_key.is_empty() {
                remove_parent_dependency_key(job_id_key, false, None, &parent_key, con).await?;
            }
        }

        Cmd::xadd(
            event_stream_key,
            "*",
            &[
                ("event", &target),
                ("jobId", &job_id),
                (&prop_value, &return_value_or_failed_reason),
            ],
        )
        .query_async(con)
        .await?;

        if &target == "failed" && attempts_made >= attempts {
            Cmd::xadd(
                event_stream_key,
                "*",
                &[
                    ("event", "retries-exhausted"),
                    ("jobId", &job_id),
                    ("attemptsMade", &attempts_made.to_string()),
                ],
            )
            .query_async(con)
            .await?;
        }

        if max_metrics_size > 0 {
            collect_metrics(
                metrics_keys,
                &format!("{}:data", &metrics_keys),
                max_metrics_size as isize,
                timestamp,
                con,
            )
            .await?;
        }

        if fetch_next {
            let (target, paused) =
                get_target_queue_list(meta_key, wait_key, paused_key, con).await?;
            promote_delayed_jobs(
                delayed_key,
                wait_key,
                &target,
                priority_key,
                event_stream_key,
                &prefix_key,
                timestamp,
                paused,
                priority_counter_key,
                con,
            )
            .await?;

            let max_jobs = opts.limiter.max;

            let expire_time = get_rate_limit_ttl(move_opts, rate_limiter_key, con).await?;
            if expire_time > 0 {
                return Ok((None, None, expire_time, None));
            }
            if paused {
                return Ok((None, None, 0, None));
            }
            let mut job_id_temp: Option<String> = Cmd::rpoplpush(wait_key, active_key)
                .query_async(con)
                .await?;
            let mut copy_job_id = job_id_temp.clone();
            if let Some(ref id) = &job_id_temp {
                if id.starts_with("0:") {
                    Cmd::lrem(active_key, 1, id).query_async(con).await?;
                    if id == "0:0" {
                        copy_job_id = move_job_from_priority_to_active(
                            priority_key,
                            active_key,
                            priority_counter_key,
                            con,
                        )
                        .await?;
                        let result = prepare_job_for_processing(
                            keys,
                            &prefix_key,
                            &target,
                            id,
                            timestamp,
                            Some(max_jobs),
                            expire_time,
                            move_opts,
                            true,
                            con,
                        )
                        .await?;
                        return convert_errors(result);
                    }
                } else {
                    let response = prepare_job_for_processing(
                        keys,
                        &prefix_key,
                        &target,
                        id,
                        timestamp,
                        Some(max_jobs),
                        expire_time,
                        move_opts,
                        true,
                        con,
                    )
                    .await?;
                    return convert_errors(response);
                }
            } else {
                copy_job_id = move_job_from_priority_to_active(
                    priority_key,
                    active_key,
                    priority_counter_key,
                    con,
                )
                .await?;
                if let Some(ref val) = &copy_job_id {
                    let result = prepare_job_for_processing(
                        keys,
                        &prefix_key,
                        &target,
                        val,
                        timestamp,
                        Some(max_jobs),
                        expire_time,
                        move_opts,
                        true,
                        con,
                    )
                    .await?;

                    return convert_errors(result);
                }
            }
            let next_delay_timestamp = get_next_delayed_timestamp(con, delayed_key).await?;
            if let Some(delay) = next_delay_timestamp {
                return Ok((None, None, 0, Some(delay)));
            }
        }

        let wait_len: i64 = Cmd::llen(wait_key).query_async(con).await?;
        if wait_len == 0 {
            let active_len: i64 = Cmd::llen(active_key).query_async(con).await?;
            if active_len == 0 {
                let prioritied_len: i64 = Cmd::zcard(priority_key).query_async(con).await?;
                if prioritied_len == 0 {
                    Cmd::xadd(event_stream_key, "*", &[("event", "drained")])
                        .query_async(con)
                        .await?;
                }
            }
        }

        return Ok((None, None, 0, None));
    }
    Ok((None, None, -1, None))
}
