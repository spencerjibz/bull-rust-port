// write test for the queue
#![allow(unused_imports, dead_code)]
#[cfg(test)]
mod tests {

    use anyhow::Ok;
    use async_lazy::Lazy;
    use bull::*;

    use std::collections::HashMap;

    static QUEUE: Lazy<Queue<'static>> = Lazy::const_new(|| {
        Box::pin(async {
            let pass = std::env::var("REDIS_PASSWORD").unwrap();

            let mut config = HashMap::new();
            config.insert("password", to_static_str(pass));
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

        assert_eq!(job.id, id);

        Ok(())
    }
}
