use std::{collections::HashMap, vec};

use crate::*;

use deadpool_redis::Connection;
use redis::{Cmd, Value};
use serde::de;
use uuid::timestamp;

/// Function that returns the next delayed job timestamp
pub async fn get_next_delayed_timestamp(
    delayed_key: &str,
    con: &mut Connection,
) -> anyhow::Result<Option<i64>> {
    let mut result: Vec<(String, i64)> = Cmd::zrange_withscores(delayed_key, 0, 0)
        .query_async(con)
        .await?;

    if let Some((_, mut next_time_stamp)) = result.first() {
        next_time_stamp /= 0x1000;
        return Ok(Some(next_time_stamp));
    }
    Ok(None)
}

pub async fn add_delay_marker_if_needed(
    target: String,
    delayed_key: String,
    con: &mut Connection,
) -> anyhow::Result<Option<isize>> {
    let llen_result: i64 = Cmd::llen(&target).query_async(con).await?;
    if llen_result == 0 {
        let next_time_stamp = get_next_delayed_timestamp(&delayed_key, con).await?;
        if let Some(time_stamp) = next_time_stamp {
            let pushed_result_index: isize = Cmd::lpush(target, format!("0:{time_stamp}"))
                .query_async(con)
                .await?;
            return Ok(Some(pushed_result_index));
        }
    }
    Ok(None)
}
/// add a job considering its priority
pub async fn add_job_with_priority(
    priority_key: &str,
    priority: i64,
    target_key: &str,
    job_id: &str,
    con: &mut Connection,
) -> anyhow::Result<Option<i32>> {
    Cmd::zadd(priority_key, job_id, priority)
        .query_async(con)
        .await?;
    let count: isize = Cmd::zcount(priority_key, 0, priority)
        .query_async(con)
        .await?;
    let len: isize = Cmd::llen(target_key).query_async(con).await?;
    let id: Option<String> = Cmd::lindex(target_key, len - (count - 1))
        .query_async(con)
        .await?;
    let result: Option<i32> = match id {
        Some(value) => {
            Cmd::linsert_before(target_key, value, job_id)
                .query_async(con)
                .await?
        }
        _ => Cmd::rpush(target_key, job_id).query_async(con).await?,
    };
    Ok(result)
}
/**
check for the meta.paused key to decide if we are paused or not
 (since an empty list and !EXISTS are not really the same).
*/
pub async fn get_target_queue_list(
    queue_meta_key: &str,
    wait_key: &str,
    paused_key: &str,
    con: &mut Connection,
) -> String {
    let exists: isize = Cmd::hexists(queue_meta_key, "paused")
        .query_async(con)
        .await
        .unwrap();
    if exists != 1 {
        return wait_key.to_owned();
    }
    paused_key.to_owned()
}

async fn trim_events(
    meta_key: &str,
    event_stream_key: &str,
    con: &mut Connection,
) -> anyhow::Result<()> {
    let max_events: Option<isize> = Cmd::hget(meta_key, "opts.maxLenEvents")
        .query_async(con)
        .await
        .ok();
    if let Some(value) = max_events {
        redis::cmd("XTRIM")
            .arg(event_stream_key)
            .arg("MAXLEN")
            .arg("~")
            .arg(value)
            .query_async(con)
            .await?
    } else {
        redis::cmd("XTRIM")
            .arg(event_stream_key)
            .arg("MAXLEN")
            .arg("~")
            .arg(1000)
            .query_async(con)
            .await?
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn update_parent_deps_if_needed(
    parent_key: &str,
    parent_queue_key: &str,
    parent_dep_key: &str,
    parent_id: &str,
    job_id_key: &str,
    return_value: &str,
    timestamp: &str,
    con: &mut Connection,
) -> anyhow::Result<Option<String>> {
    let processed_set = format!("{}:processed", parent_key);

    Cmd::hset(processed_set, job_id_key, return_value)
        .query_async(con)
        .await?;

    let active_parent: Option<i64> =
        Cmd::zscore(format!("{parent_queue_key}:waiting-children"), parent_id)
            .query_async(con)
            .await?;
    let scard: isize = Cmd::scard(parent_dep_key).query_async(con).await?;

    if scard == 0 && active_parent.is_some() {
        Cmd::zrem(format!("{parent_queue_key}:waiting-children"), parent_id)
            .query_async(con)
            .await?;
        let parent_target = get_target_queue_list(
            &format!("{parent_queue_key}:meta"),
            &format!("{parent_queue_key}:wait"),
            &format!("{parent_queue_key}:paused"),
            con,
        )
        .await;

        let job_attributes: Vec<String> = Cmd::hset_multiple(parent_key, &[("priority", "delay")])
            .query_async(con)
            .await?;

        let priority = job_attributes[0].parse::<i64>().unwrap_or_default();
        let delay = job_attributes[1].parse::<i64>().unwrap_or_default();

        if delay > 0 {
            let delayed_timestamp = timestamp.parse::<i64>().unwrap_or_default() + delay;
            let score = delayed_timestamp * 0x1000;
            let parent_delayed_key = format!("{parent_queue_key}:delayed");

            Cmd::zadd(&parent_delayed_key, parent_id, score)
                .query_async(con)
                .await?;
            add_delay_marker_if_needed(parent_target, parent_delayed_key, con).await?;
        } else if priority == 0 {
            Cmd::rpush(parent_target, parent_id)
                .query_async(con)
                .await?;
        } else {
            add_job_with_priority(
                &format!("{parent_queue_key}:priority"),
                priority,
                &parent_target,
                parent_id,
                con,
            )
            .await?;
        }
        let items = [
            ("event", "waiting"),
            ("jobId", parent_id),
            ("prev", "waiting-children"),
            ("queue", parent_queue_key),
        ];
        let result: String = Cmd::xadd(format!("{parent_queue_key}:events"), "*", &items)
            .query_async(con)
            .await?;
        return Ok(Some(result));
    }

    Ok(None)
}

type PackedArg = (
    String,         // prefix
    String,         // custom job id
    String,         // job_name
    i64,            // timestamp
    Option<String>, // parent_key
    Option<String>, // with_children_key
    Option<String>, // parent_dep_key
    Option<Parent>, // parent
    Option<String>, // repeat job key
);
pub async fn add_job_to_queue(
    keys: &[String],
    args: HashMap<&str, String>,
    data: String,
    job_opts: &JobOptions,
    con: &mut Connection,
) -> anyhow::Result<i64> {
    let opts = job_opts;
    let mut parent_data: String = "".to_owned();
    let mut job_id = "".to_owned();
    let mut job_id_key = "".to_owned();

    // use hashmap;
    let e = String::new();
    let custom_job_id = args.get("job_id").cloned().unwrap_or_default();
    let prefix = args.get("prefix").unwrap_or(&e);
    let job_name = args.get("name").unwrap_or(&e);
    let timestamp: i64 = args
        .get("timestamp")
        .cloned()
        .unwrap_or_default()
        .parse()
        .unwrap_or_default();
    let with_children_key = args.get("with_children").cloned();
    let parent_key: Option<String> = args.get("parent_key").cloned();
    let parent_dep_key: Option<String> = args.get("parent_key").cloned();
    let repeat_job_key: Option<String> = args.get("rjk").cloned();
    let mut parent: Option<Parent> = args
        .get("parent_data")
        .map(|e| serde_json::from_str(e).unwrap());

    if let Some(key) = parent_key.as_ref() {
        let key_exists: isize = Cmd::exists(key).query_async(con).await?;
        if key_exists == 0 {
            return Ok(-5);
        }
        parent_data = serde_json::to_string(&parent)?;
    }
    let job_counter: i64 = Cmd::incr(&keys[3], "1").query_async(con).await?;
    trim_events(&keys[2], &keys[7], con).await?;
    if custom_job_id.is_empty() {
        job_id = job_counter.to_string();
        job_id_key = format!("{}:{}", prefix, job_id);
    } else {
        job_id = custom_job_id;
        job_id_key = format!("{}:{}", prefix, job_id);
    }
    let job_id_key_exists: isize = Cmd::exists(&job_id_key).query_async(con).await?;
    if job_id_key_exists == 1 {
        let zscore: String = Cmd::zscore(&keys[6], &job_id).query_async(con).await?;
        if let Some(key) = parent_key.as_ref() {
            if !zscore.is_empty() {
                let return_value: String = Cmd::hget(&job_id_key, "returnvalue")
                    .query_async(con)
                    
                    .await?;
                let parent_obj = parent.as_ref().unwrap();
                let timestamp_str = timestamp.to_string();
                update_parent_deps_if_needed(
                    key,
                    &parent_obj.queue,
                    parent_dep_key.as_ref().unwrap(),
                    &parent_obj.id,
                    &job_id_key,
                    &return_value,
                    &timestamp_str,
                    con,
                )
                .await?;
            }
        } else if let Some(deps_key) = parent_dep_key.as_ref() {
            Cmd::sadd(deps_key, &job_id_key).query_async(con).await?;
        } else if parent_key.is_some() {
            redis::cmd("HMSET")
                .arg(&job_id_key)
                .arg("parentKey")
                .arg(parent_key)
                .arg("parent")
                .arg(parent_data)
                .query_async(con)
                .await?;
        }
        let items = [("event", "duplicated"), ("jobId", &job_id)];
        Cmd::xadd(&keys[7], "*", &items).query_async(con).await?;
        return Ok(job_id.parse::<i64>().unwrap_or_default());
    }
    // store the job
    let json_opts = serde_json::to_string(opts)?;
    let delay = opts.delay;
    let priority = opts.priority;
    let mut optional_values = HashMap::new();

    if let Some(p_key) = parent_key {
        optional_values.insert("parentKey", p_key);
        optional_values.insert("parent", parent_data);
    }
    if let Some(rjk) = repeat_job_key {
        optional_values.insert("repeatJobKey", rjk);
    }
    let mut items = vec![
        ("name", job_name.clone()),
        ("data", data),
        ("opts", json_opts),
        ("timestamp", timestamp.to_string()),
        ("delay", delay.to_string()),
        ("priority", priority.to_string()),
    ];
    for (key, value) in optional_values {
        items.push((key, value));
    }
    Cmd::hset_multiple(&job_id_key, &items)
        .query_async(con)
        .await?;
    let args = [("event", "added"), ("jobId", &job_id), ("name", &job_name)];
    Cmd::xadd(&keys[7], "*", &args).query_async(con).await?;
    // check if the job is delayed
    let delayed_timestamp = if delay > 0 { timestamp + delay } else { 0 };
    // check if job is a  parent, if so add to the parents set
    if let Some(with_children_key) = with_children_key {
        Cmd::zadd(&with_children_key, &job_id, timestamp)
            .query_async(con)
            .await?;
        let items = [("event", "waiting-children"), ("jobId", &job_id)];
        Cmd::xadd(&keys[7], "*", &items).query_async(con).await?;
    } else if delayed_timestamp != 0 {
        let score = delayed_timestamp.wrapping_mul(0x1000) + (job_counter & 0xfff);
        Cmd::zadd(&keys[4], &job_id, score).query_async(con).await?;
        let items = [
            ("event", "delayed"),
            ("jobId", &job_id),
            ("delay", &delayed_timestamp.to_string()),
        ];
        Cmd::xadd(&keys[7], "*", &items).query_async(con).await?;
        let target = get_target_queue_list(&keys[2], &keys[0], &keys[1], con).await;
        add_delay_marker_if_needed(target, keys[4].clone(), con).await?;
    } else {
        let target = get_target_queue_list(&keys[2], &keys[0], &keys[1], con).await;
        // standard or priority add
        if priority == 0 {
            // LIFO or FIFO
            let push_cmd = if opts.lifo {
                Cmd::rpush(&target, &job_id)
            } else {
                Cmd::lpush(&target, &job_id)
            };
            push_cmd.query_async(con).await?;
        } else {
            add_job_with_priority(&keys[5], priority, &target, &job_id, con).await?;
        }
        // emit waiting event
        Cmd::xadd(&keys[7], "*", &[("event", "waiting"), ("jobId", &job_id)])
            .query_async(con)
            .await?;
    }
    /*
        -- Check if this job is a child of another job, if so add it to the parents dependencies
    -- TODO: Should not be possible to add a child job to a parent that is not in the "waiting-children" status.
    -- fail in this case.
         */
    if let Some(parent_dep_key) = parent_dep_key {
        Cmd::sadd(&parent_dep_key, &job_id_key)
            .query_async(con)
            .await?;
    }
    Ok(job_id.parse::<i64>().unwrap_or_default())
}
