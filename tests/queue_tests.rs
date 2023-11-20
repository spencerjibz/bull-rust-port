// write test for the queue
#![allow(unused_imports, dead_code)]
#[cfg(test)]
mod tests {

    use anyhow::Ok;
    use async_lazy::Lazy;
    use bull::*;
    use dotenv_codegen::dotenv;
    use std::collections::HashMap;

    const PASS: &str = dotenv!("REDIS_PASSWORD");

    static QUEUE: Lazy<Queue<'static>> = Lazy::const_new(|| {
        Box::pin(async {
            let mut config = HashMap::new();
            config.insert("password", PASS);
            let redis_opts = RedisOpts::Config(config);

            Queue::<'_>::new("test", redis_opts, QueueOptions::default())
                .await
                .unwrap()
        })
    });

    #[tokio::test]
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
        let job_opts = JobOptions::default();
        let id = job_opts.job_id.clone().unwrap();

        let job: Job<'_, Data, String> = queue.add("test", data, job_opts).await?;

        assert_eq!(job.id, id.clone());
        // cleanup
        queue.remove_job(id, false).await?;
        Ok(())
    }
    #[tokio::test]
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
        let mut job_opts = JobOptions::default();
        let id = job_opts.job_id.clone().unwrap();

        job_opts.attempts = 3;

        job_opts.delay = 1000;

        let job: Job<'_, Data, String> = queue.add("test", data, job_opts).await?;

        assert_eq!(job.id, id.clone());
        assert_eq!(job.opts.attempts, 3);
        assert_eq!(job.opts.delay, 1000);
        // cleanup
        queue.remove_job(id, false).await?;
        Ok(())
    }

    #[tokio::test]

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
        let job_opts = JobOptions::default();
        let id = job_opts.job_id.clone().unwrap();

        let job: Job<'_, Data, String> = queue.add("test", data, job_opts).await?;

        queue.remove_job(id.clone(), false).await?;
        let result: Option<Job<'_, Data, String>> = Job::from_id(queue, &job.id).await?;

        assert_eq!(result, None);
        Ok(())
    }

    #[tokio::test]
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

        let job: Job<'_, Data, String> = queue.add("test", data, job_opts).await?;
        let state = queue.get_job_state(&job.id).await?;

        assert_eq!(state, "waiting".to_string());
        // cleanup
        queue.remove_job(job.id, false).await?;

        Ok(())
    }

    #[tokio::test]

    async fn is_paused() -> anyhow::Result<()> {
        let queue = QUEUE.force().await;
        queue.pause().await?;
        let mut paused: bool = queue.is_paused().await?;
        assert!(paused);

        queue.resume().await?;
        paused = queue.is_paused().await?;
        assert!(!paused);

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn trim_events_manually() -> anyhow::Result<()> {
        let queue = QUEUE.force().await;
        let mut conn = queue.manager.pool.get().await?;
        let job_opts = JobOptions::default();

        // add multiple jobs

        let _job1: Job<'_, String, String> = queue
            .add("test", "1".to_ascii_lowercase(), job_opts.clone())
            .await?;
        let _job2: Job<'_, String, String> = queue
            .add("test", "2".to_ascii_lowercase(), job_opts.clone())
            .await?;
        let _job3: Job<'_, String, String> = queue
            .add("test", "3".to_ascii_lowercase(), job_opts)
            .await?;

        let events_length: isize = redis::cmd("XLEN")
            .arg("bull:test:events")
            .query_async(&mut conn)
            .await?;

        assert_eq!(events_length, 8);

        queue.trim_events(4).await?;

        Ok(())
    }
}
