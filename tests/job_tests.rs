#![allow(unused_imports)]
#[cfg(test)]
mod tests {
    use anyhow::Ok;
    use async_lazy::Lazy;
    use bull::*;
    use dotenv_codegen::dotenv;
    use std::collections::HashMap;
    use std::env;
    use std::fs::File;
    use std::time::{Instant, SystemTime};
    const PASS: &str = dotenv!("REDIS_PASSWORD");

    static QUEUE: Lazy<Queue<'static>> = Lazy::const_new(|| {
        Box::pin(async {
            let mut config = HashMap::new();
            config.insert("password", PASS);
            let redis_opts = RedisOpts::Config(config);
            Queue::<'static>::new("test", redis_opts, QueueOptions::default())
                .await
                .unwrap()
        })
    });

    #[tokio::test]
    async fn creating_a_new_job() -> anyhow::Result<()> {
        let queue = QUEUE.force().await;

        let job = Job::<'_, String, String>::new(
            "test",
            queue,
            "test".to_string(),
            JobOptions::default(),
        )
        .await?;
        assert_eq!(job.name, "test");

        Ok(())
    }

    #[tokio::test]
    async fn create_job_from_string() -> anyhow::Result<()> {
        let mut config = HashMap::new();
        config.insert("password", PASS);
        let redis_opts = RedisOpts::Config(config);
        let mut queue = Queue::<'_>::new("test", redis_opts, QueueOptions::default()).await?;

        let result: HashMap<String, String> =
            queue.client.hgetall("bull:pinningQueue:207").await.unwrap();

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
