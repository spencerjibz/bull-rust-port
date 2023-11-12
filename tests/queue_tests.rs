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
        println!("{:#?}", queue);

        let _job: Job<'_, String, String> = queue
            .add("test", "12".to_string(), JobOptions::default())
            .await?;

        Ok(())
    }
}
