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
        let  queue = Queue::new("test", redis_opts, QueueOptions::default()).await?;
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
        Ok(())
    }
}
