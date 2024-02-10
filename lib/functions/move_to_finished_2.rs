use crate::functions::add_job3::*;
use crate::functions::move_to_active::*;
use crate::move_to_finished::*;
use crate::MoveToAciveResult;
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

pub async fn move_job_to_finished_2(
    keys: &[String],
    args: MoveToFinishedArgs,
    con: &mut Connection,
) -> Result<MoveToAciveResult> {
    println!("keys{keys:#?}");
    println!("args:{args:#?}");
    // args
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
    //let attempts_made = opts.atempt_made;
    let max_metrics_size = opts.max_metrics_size;
    let max_count = opts.keep_jobs.count.unwrap_or_default();
    let max_age = opts.keep_jobs.age.unwrap_or_default();
    let fpof = opts.fail_parent_on_failure;
    let rdof = opts.remove_deps_on_failure;

    // Arguments
    let move_opts = &JobMoveOpts {
        token: token.clone(),
        limiter: Some(opts.limiter.clone()),
        lock_duration: opts.lock_duration,
    };
    todo!()
}
