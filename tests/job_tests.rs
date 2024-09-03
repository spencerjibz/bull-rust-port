#![allow(unused_imports)]
#![allow(clippy::needless_return)]
#[cfg(test)]
mod job {
    use anyhow::Ok;
    use async_lazy::Lazy;
    use bull::*;

    use std::collections::HashMap;
    use std::env;
    use std::fs::File;
    use std::time::{Instant, SystemTime};

    static QUEUE: Lazy<Queue> = Lazy::const_new(|| {
        Box::pin(async {
            use core::result::Result::Ok;

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
    async fn creating_a_new_job() -> anyhow::Result<()> {
        let queue = QUEUE.force().await;

        let job = Job::<String, String>::new(
            "test",
            queue,
            "test".to_string(),
            JobOptions::default(),
            None,
        )
        .await?;
        assert_eq!(job.name, "test");

        Ok(())
    }

    #[tokio_shared_rt::test]
    async fn create_job_from_string() -> anyhow::Result<()> {
        let pass = fetch_redis_pass();

        let mut config = HashMap::new();
        config.insert("password", pass.as_str());
        let redis_opts = RedisOpts::from_string_map(config);
        let queue = Queue::new("test", redis_opts, QueueOptions::default()).await?;
        let mut client = queue.client.lock().await;
        let result: HashMap<String, String> =
            client.hgetall("bull:pinningQueue:207").await.unwrap();

        let contents = if !result.is_empty() {
            serde_json::to_string(&result).unwrap_or("{}".to_string())
        } else {
            tokio::fs::read_to_string("test.json").await?
        };

        let json = JobJsonRaw::fromStr(contents.clone())?;
        json.save_to_file("test.json")?;

        // println!("{:#?}", worker.clone());

        let job =
            Job::<Data, ReturnedData>::from_json(QUEUE.force().await, contents, "207").await?;

        assert_eq!(job.name, "QmTkNd9nQHasSbQwmcsRkBeiFsMgqhgDNnWqYRgwZLmCgP");
        queue.obliterate(true).await?;
        Ok(())
    }

    #[tokio_shared_rt::test]
    async fn set_and_get_progress_as_number() -> anyhow::Result<()> {
        let mut config = HashMap::new();
        let pass = fetch_redis_pass();

        config.insert("password", to_static_str(pass));
        let redis_opts = RedisOpts::Config(config);
        let prefix = "custom";
        let queue = Queue::new(
            "test_progress_as_an_number",
            redis_opts,
            QueueOptions {
                prefix: Some(prefix),
                ..Default::default()
            },
        )
        .await?;
        let job_opts = JobOptions::default();
        // add multiple jobs
        let mut job: Job<String, String> = queue
            .add("test", "1".to_ascii_lowercase(), job_opts.clone(), None)
            .await?;
        let _ = job.update_progress(64).await;

        let stored_job = Job::<String, String>::from_id(&queue, &job.id)
            .await?
            .unwrap();

        assert_eq!(stored_job.progress, job.progress);

        queue.obliterate(false).await?;

        Ok(())
    }
    #[tokio_shared_rt::test]
    async fn set_and_get_progress_as_an_object() -> anyhow::Result<()> {
        let mut config = HashMap::new();
        let pass = fetch_redis_pass();

        config.insert("password", to_static_str(pass));
        let redis_opts = RedisOpts::Config(config);
        let prefix = "custom";
        let queue = Queue::new(
            "test_progress_as_an_object",
            redis_opts,
            QueueOptions {
                prefix: Some(prefix),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let job_opts = JobOptions::default();
        #[derive(Serialize)]
        struct TestProgress {
            total: i32,
            completed: i32,
        }
        // add multiple jobs
        let mut job: Job<String, String> = queue
            .add("test", "1".to_ascii_lowercase(), job_opts.clone(), None)
            .await?;
        let _ = job
            .update_progress(TestProgress {
                total: 120,
                completed: 40,
            })
            .await;

        let stored_job = Job::<String, String>::from_id(&queue, &job.id)
            .await?
            .unwrap();

        assert_eq!(stored_job.progress, job.progress);

        queue.obliterate(false).await?;

        Ok(())
    }

    #[tokio_shared_rt::test]
    async fn test_job_state() -> anyhow::Result<()> {
        let mut config = HashMap::new();
        let pass = fetch_redis_pass();

        config.insert("password", to_static_str(pass));
        let redis_opts = RedisOpts::Config(config);
        let prefix = "custom";
        let queue = Queue::new(
            "test_get_job_state",
            redis_opts,
            QueueOptions {
                prefix: Some(prefix),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let job_opts = JobOptions::default();
        let job: Job<String, String> = queue
            .add(
                "test_randw",
                "1".to_ascii_lowercase(),
                job_opts.clone(),
                None,
            )
            .await?;
        let state = job.get_state().await?;
        assert_eq!(state, "waiting".to_owned());
        queue.obliterate(true).await?;
        Ok(())
    }
    #[tokio_shared_rt::test]
    async fn update_job_data() -> anyhow::Result<()> {
        use maplit::hashmap;
        let mut config = HashMap::new();
        let pass = fetch_redis_pass();

        config.insert("password", to_static_str(pass));
        let redis_opts = RedisOpts::Config(config);
        let prefix = "custom";
        let queue = Queue::new(
            "test_update_data",
            redis_opts,
            QueueOptions {
                prefix: Some(prefix),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let job_opts = JobOptions::default();

        // add multiple jobs
        let mut job: Job<HashMap<String, String>, String> = queue
            .add(
                "test",
                hashmap! {
                    "foo".to_owned() => "bar".to_string()
                },
                job_opts.clone(),
                None,
            )
            .await?;
        job.update_data(hashmap! {"baz".to_owned() => "qux".to_string()})
            .await?;

        let stored_job = Job::<HashMap<String, String>, String>::from_id(&queue, &job.id)
            .await?
            .unwrap();
        assert_eq!(stored_job.data, job.data);

        queue.obliterate(false).await?;

        Ok(())
    }

    #[tokio_shared_rt::test]
    #[should_panic]
    async fn panic_when_try_to_update_data_of_a_removed_job() {
        use maplit::hashmap;
        let queue = QUEUE.force().await;

        let job_opts = JobOptions::default();

        // add multiple jobs
        let mut job: Job<HashMap<String, String>, String> = queue
            .add(
                "test",
                hashmap! {
                    "foo".to_owned() => "bar".to_string()
                },
                job_opts.clone(),
                None,
            )
            .await
            .unwrap();
        job.update_data(hashmap! {"baz".to_owned() => "qux".to_string()})
            .await
            .unwrap();

        job.remove(true).await.unwrap();

        let stored_job = Job::<HashMap<String, String>, String>::from_id(queue, &job.id)
            .await
            .unwrap();
        queue.obliterate(false).await.unwrap();
        assert_eq!(stored_job, Some(job));
    }

    #[tokio_shared_rt::test(shared)]
    async fn promote_delayed_jobs() -> anyhow::Result<()> {
        use maplit::hashmap;
        let mut config = HashMap::new();
        let pass = fetch_redis_pass();

        config.insert("password", to_static_str(pass));
        let redis_opts = RedisOpts::Config(config);

        let queue = Queue::new("test_promote_jobs", redis_opts, QueueOptions::default())
            .await
            .unwrap();
        let job_opts = JobOptions {
            delay: 1500,
            ..Default::default()
        };

        // add multiple jobs
        let mut job: Job<HashMap<String, String>, String> = queue
            .add(
                "test",
                hashmap! {
                    "foo".to_owned() => "bar".to_string()
                },
                job_opts.clone(),
                None,
            )
            .await?;
        assert!(job.is_delayed().await?);
        job.promote().await?;

        assert_eq!(job.delay, 0);
        let is_delayed_after_promote = job.is_delayed().await?;
        assert!(!is_delayed_after_promote);
        // job is wating
        let result = job.is_waiting().await?;
        queue.obliterate(true).await?;

        assert!(result);
        queue.remove_job(job.id, true).await?;

        Ok(())
    }
}
