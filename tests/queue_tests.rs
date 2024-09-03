// write test for the queue
#![allow(unused_imports, dead_code)]
#![allow(clippy::needless_return)]
#[cfg(test)]
mod queue {

    use anyhow::Ok;
    use async_lazy::Lazy;
    use bull::*;

    use std::collections::HashMap;

    static QUEUE: Lazy<Queue> = Lazy::const_new(|| {
        Box::pin(async {
            let mut config = HashMap::new();
            let pass = fetch_redis_pass();

            config.insert("password", to_static_str(pass));
            let redis_opts = RedisOpts::Config(config);

            Queue::new("test", redis_opts, QueueOptions::default())
                .await
                .unwrap()
        })
    });

    #[tokio_shared_rt::test(shared)]
    async fn add_job_to_queue() -> anyhow::Result<()> {
        let queue = QUEUE.force().await;

        let data = Data {
            socket_id: "w3ess2".to_ascii_lowercase(),
            cid: "Qufaufsduafsudafusaufusdaf".to_ascii_lowercase(),
            file_id: "".to_owned(),
            sizes: vec![],
            user_id: "123".to_owned(),
            tracking_id: "fadfasfdsaf".to_ascii_lowercase(),
        };
        let job_opts = JobOptions {
            job_id: Some("23".to_owned()),
            ..Default::default()
        };
        let id = job_opts.job_id.clone().unwrap();

        let job: Job<Data, String> = queue.add("test", data, job_opts, None).await?;

        assert_eq!(job.id, id.clone());
        // cleanup
        queue.obliterate(true).await?;
        Ok(())
    }
    #[tokio_shared_rt::test(shared)]
    async fn add_job_to_queue_with_options() -> anyhow::Result<()> {
        let queue = QUEUE.force().await;

        let data = Data {
            socket_id: "w3ess2".to_ascii_lowercase(),
            cid: "Qufaufsduafsudafusaufusdaf".to_ascii_lowercase(),
            file_id: "".to_owned(),
            sizes: vec![],
            user_id: "123".to_owned(),
            tracking_id: "fadfasfdsaf".to_ascii_lowercase(),
        };
        let mut job_opts = JobOptions {
            job_id: Some("23".to_owned()),
            ..Default::default()
        };
        let id = job_opts.job_id.clone().unwrap();

        job_opts.attempts = 3;

        job_opts.delay = 1000;

        let job: Job<Data, String> = queue.add("test", data, job_opts, None).await?;

        assert_eq!(job.id, id.clone());
        assert_eq!(job.opts.attempts, 3);
        assert_eq!(job.opts.delay, 1000);
        // cleanup
        queue.remove_job(id, false).await?;
        queue.obliterate(true).await?;

        Ok(())
    }

    #[tokio_shared_rt::test(shared)]

    async fn remove_job_from_queue() -> anyhow::Result<()> {
        let queue = QUEUE.force().await;

        let data = Data {
            socket_id: "w3ess2".to_ascii_lowercase(),
            cid: "Qufaufsduafsudafusaufusdaf".to_ascii_lowercase(),
            file_id: "".to_owned(),
            sizes: vec![],
            user_id: "123".to_owned(),
            tracking_id: "fadfasfdsaf".to_ascii_lowercase(),
        };
        let job_opts = JobOptions {
            job_id: Some("23".to_owned()),
            ..Default::default()
        };
        let id = job_opts.job_id.clone().unwrap();

        let job: Job<Data, String> = queue.add("test", data, job_opts, None).await?;

        queue.remove_job(id.clone(), false).await?;
        let result: Option<Job<Data, String>> = Job::from_id(queue, &job.id).await?;
        assert_eq!(result, None);
        queue.obliterate(true).await?;
        Ok(())
    }

    #[tokio_shared_rt::test(shared)]
    async fn get_job_state() -> anyhow::Result<()> {
        let queue = QUEUE.force().await;

        let data = Data {
            socket_id: "w3ess2".to_ascii_lowercase(),
            cid: "Qufaufsduafsudafusaufusdaf".to_ascii_lowercase(),
            file_id: "".to_owned(),
            sizes: vec![],
            user_id: "123".to_owned(),
            tracking_id: "fadfasfdsaf".to_ascii_lowercase(),
        };
        let job_opts = JobOptions::default();

        let job: Job<Data, String> = queue.add("test", data, job_opts, None).await?;
        let state = queue.get_job_state(&job.id).await?;

        assert_eq!(state, "waiting".to_string());
        let _ = queue.obliterate(true).await;

        Ok(())
    }

    #[tokio_shared_rt::test(shared)]

    async fn is_paused() -> anyhow::Result<()> {
        let queue = QUEUE.force().await;
        queue.pause().await?;
        let mut paused: bool = queue.is_paused().await?;
        assert!(paused);

        queue.resume().await?;
        paused = queue.is_paused().await?;
        assert!(!paused);
        queue.obliterate(false).await?;
        Ok(())
    }
    #[tokio_shared_rt::test(shared)]
    async fn is_paused_with_custom_prefix() -> anyhow::Result<()> {
        let mut config = HashMap::new();
        let pass = fetch_redis_pass();

        config.insert("password", to_static_str(pass));
        let redis_opts = RedisOpts::Config(config);

        let queue = Queue::new(
            "test",
            redis_opts,
            QueueOptions {
                prefix: Some("custom"),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        queue.pause().await?;
        let mut paused: bool = queue.is_paused().await?;
        assert!(paused);

        queue.resume().await?;
        paused = queue.is_paused().await?;
        assert!(!paused);
        queue.obliterate(false).await?;
        Ok(())
    }

    #[tokio_shared_rt::test(shared)]
    async fn trim_events_manually() -> anyhow::Result<()> {
        let mut config = HashMap::new();
        let pass = fetch_redis_pass();

        config.insert("password", to_static_str(pass));
        let redis_opts = RedisOpts::Config(config);

        let queue = Queue::new("test_copy", redis_opts, QueueOptions::default())
            .await
            .unwrap();
        let mut conn = queue.manager.pool.get().await?;
        let job_opts = JobOptions::default();
        // add multiple jobs
        let _job1: Job<String, String> = queue
            .add("test", "1".to_ascii_lowercase(), job_opts.clone(), None)
            .await?;
        let _job2: Job<String, String> = queue
            .add("test", "2".to_ascii_lowercase(), job_opts.clone(), None)
            .await?;
        let _job3: Job<String, String> = queue
            .add("test", "3".to_ascii_lowercase(), job_opts, None)
            .await?;
        let events_length: isize = redis::cmd("XLEN")
            .arg("bull:test_copy:events")
            .query_async(&mut conn)
            .await?;
        println!("events_length: {}", events_length);
        assert_eq!(events_length, 6);
        //let count = queue.trim_events(4).await?;
        queue.obliterate(true).await?;

        Ok(())
    }

    #[tokio_shared_rt::test(shared)]
    async fn trim_events_manually_with_custom_prefix() -> anyhow::Result<()> {
        let mut config = HashMap::new();
        let pass = fetch_redis_pass();

        config.insert("password", to_static_str(pass));
        let redis_opts = RedisOpts::Config(config);
        let prefix = "custom";
        let queue = Queue::new(
            "test_copy",
            redis_opts,
            QueueOptions {
                prefix: Some(prefix),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let mut conn = queue.manager.pool.get().await?;
        let job_opts = JobOptions::default();
        // add multiple jobs
        let _job1: Job<(), ()> = queue.add("test", (), job_opts.clone(), None).await?;
        let _job2: Job<(), ()> = queue.add("test", (), job_opts.clone(), None).await?;
        let _job3: Job<(), ()> = queue.add("test", (), job_opts, None).await?;
        let events_length: isize = redis::cmd("XLEN")
            .arg(format!("{prefix}:test_copy:events"))
            .query_async(&mut conn)
            .await?;
        println!("events_length: {}", events_length);
        assert_eq!(events_length, 6);
        //let count = queue.trim_events(4).await?;
        queue.obliterate(true).await?;

        Ok(())
    }
    #[tokio_shared_rt::test(shared)]
    async fn test_get_jobs() -> anyhow::Result<()> {
        let mut config = HashMap::new();
        let pass = fetch_redis_pass();

        config.insert("password", to_static_str(pass));
        let redis_opts = RedisOpts::Config(config);

        let queue = Queue::new("test_get_job", redis_opts, QueueOptions::default())
            .await
            .unwrap();

        let job_opts = JobOptions::default();
        let job1: Job<String, String> = queue
            .add("test", "1".to_ascii_lowercase(), job_opts.clone(), None)
            .await?;
        let job2: Job<String, String> = queue
            .add("test2", "2".to_ascii_lowercase(), job_opts.clone(), None)
            .await?;

        let jobs = queue.get_jobs(&["wait"], None, false).await?;
        assert_eq!(job1.id, jobs[0].id);
        assert_eq!(job2.id, jobs[1].id);

        queue.obliterate(false).await?;

        Ok(())
    }
}
