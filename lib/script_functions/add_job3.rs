use anyhow::{Ok, Result};
use deadpool_redis::Connection;
use redis::Cmd;
use redis::Commands;
use serde_json::json;
use std::collections::HashMap;
use uuid::timestamp;

use crate::JobOptions;
use crate::Parent;
pub async fn add_job_to_queue_3(
    keys: &[String],
    args: HashMap<&str, String>,
    data: String,
    opts: &JobOptions,
    con: &mut Connection,
) -> Result<i64> {
    let wait_key = keys.first().unwrap();
    let paused_key = keys.get(1).unwrap();
    let meta_key = keys.get(2).unwrap();
    let id_key = keys.get(3).unwrap();
    let delayed_key = keys.get(4).unwrap();
    let priority_key = keys.get(5).unwrap();
    let completed_key = keys.get(6).unwrap();
    let events_stream_key = keys.get(7).unwrap();
    let priorty_counter_key = keys.get(8).unwrap();
    let job_id: String;
    let job_id_key: String;

    let parent_key = args.get("parent_key").cloned();
    let repeat_job_key = args.get("rjk").cloned();
    let mut parent_data: Option<Parent> = None;

    // Validate and move or add dependencies to parent.
    // Includes

    // Validate and move parent to active if needed.
    // Includes

    if let Some(parent_key) = &parent_key {
        let parent_key_exists: bool = Cmd::exists(parent_key).query_async(con).await?;
        if !parent_key_exists {
            return Ok(-5);
        }
        let data = args.get("parent_data");
        if let Some(json) = data {
            let parsed_value: Parent = serde_json::from_str(json)?;
            parent_data = Some(parsed_value)
        }
    }
    let parent_key: Option<String> = args.get("parent_key").cloned();
    let repeat_job_key: Option<String> = args.get("rjk").cloned();
    let job_counter: i64 = Cmd::incr(id_key, 1).query_async(con).await?;
    let max_events: i32 = Cmd::hget(meta_key, "opts.maxLenEvents")
        .query_async(con)
        .await
        .unwrap_or(10000);
    let timestamp: String = args.get("timestamp").cloned().unwrap_or_default();
    let parent_dependencies_key = args.get("parent_dep_key").cloned();
    let timestamp: i64 = args
        .get("timestamp")
        .cloned()
        .unwrap_or_default()
        .parse()
        .unwrap_or_default();
    let mut job_id = args.get("job_id").cloned().unwrap_or_default();
    let prefix = args.get("prefix").cloned().unwrap_or_default();
    let name = args.get("prefix").cloned().unwrap_or_default();

    if job_id.is_empty() {
        job_id = job_counter.to_string();
        job_id_key = format!("{}:{}", prefix, job_id);
    } else {
        job_id_key = format!("{}:{}", prefix, job_id);
        let job_id_key_exits: bool = Cmd::exists(&job_id_key).query_async(con).await?;
        if job_id_key_exits {
            let returned: Option<String> = Cmd::hget(&job_id_key, "returnvalue")
                .query_async(con)
                .await?;
            if let Some(return_value) = returned {
                let zscore_exists: bool =
                    Cmd::zscore(completed_key, &job_id).query_async(con).await?;
                if zscore_exists && parent_data.as_ref().is_some() {
                    let parent_queue_key = &parent_data.as_ref().unwrap().queue;
                    let parent_id = &parent_data.as_ref().unwrap().id;

                    if let (Some(p_key), p_queue_key, Some(parent_deps_key), parent_id) = (
                        parent_key,
                        parent_queue_key,
                        parent_dependencies_key,
                        parent_id,
                    ) {
                        update_parent_deps_if_needed(
                            &p_key,
                            p_queue_key,
                            &parent_deps_key,
                            parent_id,
                            &job_id_key,
                            return_value,
                            timestamp,
                            con,
                        )
                        .await?;
                    }
                } else if let Some(parent_dependencies_key) = parent_dependencies_key {
                    Cmd::sadd(&parent_dependencies_key, &job_id_key)
                        .query_async(con)
                        .await?;
                }
            }
            Cmd::xadd(
                events_stream_key,
                "*",
                &[("event", "duplicated"), ("jobId", &job_id)],
            )
            .query_async(con)
            .await?;
            let numb = job_id.parse().unwrap_or_default();
            return Ok(numb);
        }
    }

    // Store the job.
    let json_opts = serde_json::to_string(&opts)?;
    let delay = opts.delay;
    let priority = opts.priority;
    let mut optional_values: Vec<(&str, &str)> = vec![];
    if let Some(key) = parent_key.as_ref() {
        optional_values.push(("parentKey", key));
        let data = args.get("parent_data").unwrap();

        optional_values.push(("parent", data));
    }
    if let Some(repeat_job_key) = repeat_job_key {
        optional_values.push(("rjk", &repeat_job_key));
    }
    let t_stamp = timestamp.to_string();
    let delay_str = delay.to_string();
    let priority_str = priority.to_string();
    let mut hmset_args = vec![
        ("name", &name),
        ("data", &data),
        ("opts", &json_opts),
        ("timestamp", &t_stamp),
        ("delay", &delay_str),
        ("priority", &priority_str),
    ];
    //hmset_args.extend(optional_values.iter());
    Cmd::hset_multiple(&job_id_key, &hmset_args)
        .query_async(con)
        .await?;

    Cmd::xadd(
        events_stream_key,
        "*",
        &[("event", "added"), ("jobId", &job_id), ("name", &name)],
    )
    .query_async(con)
    .await?;

    // Check if job is delayed
    let delayed_timestamp = if delay > 0 { timestamp + delay } else { 0 };
    use redis::streams::StreamMaxlen;
    let max_len = StreamMaxlen::Approx(max_events as usize);
    // Check if job is a parent, if so add to the parents set
    let wait_children_key = args.get("with_children_key").cloned();
    if let Some(wait_children_key) = wait_children_key {
        Cmd::zadd(&wait_children_key, timestamp, &job_id)
            .query_async(con)
            .await?;

        Cmd::xadd_maxlen(
            events_stream_key,
            max_len,
            "*",
            &[("event", "waiting-children"), ("jobId", &job_id)],
        )
        .query_async(con)
        .await?;
    } else if delayed_timestamp != 0 {
        let score = delayed_timestamp * 0x1000 + (job_counter & 0xfff);
        Cmd::zadd(delayed_key, &job_id, score)
            .query_async(con)
            .await?;
        Cmd::xadd_maxlen(
            events_stream_key,
            max_len,
            "*",
            &[
                ("event", "delayed"),
                ("jobId", &job_id),
                ("delay", &delayed_timestamp.to_string()),
            ],
        )
        .query_async(con)
        .await?;
        // If wait list is empty, and this delayed job is the next one to be processed,
        // then we need to signal the workers by adding a dummy job (jobId 0:delay) to the wait list.
        let (target, _) = get_target_queue_list(meta_key, wait_key, paused_key, con).await?;
        add_delay_marker_if_needed(con, &target, delayed_key).await?;
    } else {
        let (target, paused) = get_target_queue_list(meta_key, wait_key, paused_key, con).await?;
        // Standard or priority add
        if priority == 0 {
            // LIFO or FIFO
            let push_cmd = if opts.lifo { "RPUSH" } else { "LPUSH" };
            Cmd::rpush(&target, &job_id).query_async(con).await?;
        } else {
            add_job_with_priority(
                con,
                wait_key,
                priority_key,
                priority,
                paused,
                &job_id,
                priorty_counter_key,
            )
            .await?;
        }
        // Emit waiting event
        Cmd::xadd_maxlen(
            events_stream_key,
            max_len,
            "*",
            &[("event", "waiting"), ("jobId", &job_id)],
        )
        .query_async(con)
        .await?;
    }

    // Check if this job is a child of another job, if so add it to the parents dependencies
    // TODO: Should not be possible to add a child job to a parent that is not in the "waiting-children" status.
    // fail in this case.
    if let Some(parent_dependencies_key) = parent_dependencies_key {
        Cmd::sadd(&parent_dependencies_key, &job_id_key)
            .query_async(con)
            .await?;
    }
    let numb = job_id.parse().unwrap_or_default();
    Ok(numb)
}

pub async fn add_job_with_priority(
    con: &mut Connection,
    wait_key: &str,
    priority_key: &str,
    priority: i64,
    paused: bool,
    job_id: &str,
    priority_counter_key: &str,
) -> Result<()> {
    let priority_counter: i64 = Cmd::incr(priority_counter_key, 1).query_async(con).await?;
    let score = priority * 0x100000000 + (priority_counter & 0xffffffffffff);
    Cmd::zadd(priority_key, job_id, score)
        .query_async(con)
        .await?;

    if !paused {
        add_priority_marker_if_needed(wait_key, con).await?;
    }

    Ok(())
}

// Function to check for the meta.paused key to decide if we are paused or not
// (since an empty list and !EXISTS are not really the same).
pub async fn get_target_queue_list(
    queue_meta_key: &str,
    wait_key: &str,
    paused_key: &str,
    con: &mut Connection,
) -> Result<(String, bool)> {
    let exists: bool = Cmd::hexists(queue_meta_key, "paused")
        .query_async(con)
        .await?;
    if exists {
        Ok((paused_key.to_string(), true))
    } else {
        Ok((wait_key.to_string(), false))
    }
}

// Function to add job considering priority.
// Includes

// Function priority marker to wait if needed
// in order to wake up our workers and to respect priority
// order as much as possible
pub async fn add_priority_marker_if_needed(wait_key: &str, con: &mut Connection) -> Result<()> {
    let wait_len: i32 = Cmd::llen(wait_key).query_async(con).await?;
    if wait_len == 0 {
        Cmd::lpush(wait_key, "0:0").query_async(con).await?;
    }
    Ok(())
}

// Validate and move or add dependencies to parent.

// Validate and move parent to active if needed.

pub async fn move_parent_to_wait_if_needed(
    con: &mut Connection,
    parent_queue_key: &str,
    parent_dependencies_key: &str,
    parent_key: &str,
    parent_id: &str,
    timestamp: i64,
) -> Result<()> {
    let is_parent_active: Option<i64> = redis::cmd("ZSCORE")
        .arg(format!("{}:waiting-children", parent_queue_key))
        .arg(parent_id)
        .query_async(con)
        .await?;
    let scard: i64 = redis::cmd("SCARD")
        .arg(parent_dependencies_key)
        .query_async(con)
        .await?;
    if scard == 0 && is_parent_active.is_some() {
        redis::cmd("ZREM")
            .arg(format!("{}:waiting-children", parent_queue_key))
            .arg(parent_id)
            .query_async(con)
            .await?;
        let parent_wait_key = format!("{}:wait", parent_queue_key);
        let (parent_target, paused) = get_target_queue_list(
            &format!("{}:meta", parent_queue_key),
            &parent_wait_key,
            &format!("{}:paused", parent_queue_key),
            con,
        )
        .await?;
        let job_attributes: Vec<String> = redis::cmd("HMGET")
            .arg(parent_key)
            .arg("priority")
            .arg("delay")
            .query_async(con)
            .await?;
        let priority: i64 = job_attributes[0].parse()?;
        let delay: i64 = job_attributes[1].parse()?;
        if delay > 0 {
            let delayed_timestamp = timestamp + delay;
            let score = delayed_timestamp * 0x1000;
            let parent_delayed_key = format!("{}:delayed", parent_queue_key);
            redis::cmd("ZADD")
                .arg(&parent_delayed_key)
                .arg(score)
                .arg(parent_id)
                .query_async(con)
                .await?;
            redis::cmd("XADD")
                .arg(format!("{}:events", parent_queue_key))
                .arg("*")
                .arg("event")
                .arg("delayed")
                .arg("jobId")
                .arg(parent_id)
                .arg("delay")
                .arg(delayed_timestamp)
                .query_async(con)
                .await?;
            add_delay_marker_if_needed(con, &parent_target, &parent_delayed_key).await?;
        } else {
            if priority == 0 {
                redis::cmd("RPUSH")
                    .arg(&parent_target)
                    .arg(parent_id)
                    .query_async(con)
                    .await?;
            } else {
                add_job_with_priority(
                    con,
                    &parent_wait_key,
                    &format!("{}:priority", parent_queue_key),
                    priority,
                    paused,
                    parent_id,
                    &format!("{}:pc", parent_queue_key),
                )
                .await?;
            }
            redis::cmd("XADD")
                .arg(format!("{}:events", parent_queue_key))
                .arg("*")
                .arg("event")
                .arg("waiting")
                .arg("jobId")
                .arg(parent_id)
                .arg("prev")
                .arg("waiting-children")
                .query_async(con)
                .await?;
        }
    }
    Ok(())
}

async fn add_delay_marker_if_needed(
    con: &mut Connection,
    target_key: &str,
    delayed_key: &str,
) -> Result<()> {
    let wait_len: i64 = Cmd::llen(target_key).query_async(con).await?;
    dbg!(wait_len);
    if wait_len <= 1 {
        if let Some(next_timestamp) = get_next_delayed_timestamp(con, delayed_key).await? {
            if wait_len == 1 {
                let marker: String = redis::cmd("LINDEX")
                    .arg(target_key)
                    .arg(0)
                    .query_async(con)
                    .await?;
                let old_timestamp: i64 = marker[2..].parse()?;
                if old_timestamp > next_timestamp {
                    redis::cmd("LSET")
                        .arg(target_key)
                        .arg(0)
                        .arg(format!("0:{}", next_timestamp))
                        .query_async(con)
                        .await?;
                }
            } else {
                redis::cmd("LPUSH")
                    .arg(target_key)
                    .arg(format!("0:{}", next_timestamp))
                    .query_async(con)
                    .await?;
            }
        }
    }
    Ok(())
}

pub async fn get_next_delayed_timestamp(
    con: &mut Connection,
    delayed_key: &str,
) -> anyhow::Result<Option<i64>> {
    let mut result: Vec<String> = Cmd::zrange_withscores(delayed_key, 0, 0)
        .query_async(con)
        .await?;
    if !result.is_empty() {
        if let std::result::Result::Ok(mut next_time_stamp) = result.get(1).unwrap().parse::<i64>()
        {
            next_time_stamp /= 0x1000;
            return Ok(Some(next_time_stamp));
        }
    }
    Ok(None)
}
#[allow(clippy::too_many_arguments)]
async fn update_parent_deps_if_needed(
    parent_key: &str,
    parent_queue_key: &str,
    parent_dependencies_key: &str,
    parent_id: &str,
    job_id_key: &str,
    return_value: String,
    timestamp: i64,
    con: &mut Connection,
) -> Result<()> {
    let processed_set = format!("{parent_key}:processed");
    Cmd::hset(processed_set, job_id_key, return_value)
        .query_async(con)
        .await?;

    move_parent_to_wait_if_needed(
        con,
        parent_queue_key,
        parent_dependencies_key,
        parent_key,
        parent_id,
        timestamp,
    )
    .await?;

    Ok(())
}
