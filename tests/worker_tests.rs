#![allow(unused_imports, dead_code)]
#![allow(clippy::needless_return)]

#[cfg(test)]
mod worker_ {
    use anyhow::Ok;
    use async_atomic::Atomic;
    use async_lazy::Lazy;
    use bull::*;
    use lazy_static::lazy_static;
    use rand::random;
    use std::collections::HashMap;
    use std::time::Instant;
    use uuid::Uuid;
    lazy_static! {
        static ref QUEUE_NAME: String = {
            let token = Uuid::new_v4().to_string();
            format!("_test_queue{token}")
        };
    }
    static QUEUE: Lazy<Queue> = Lazy::const_new(|| {
        Box::pin(async {
            let mut config = HashMap::new();
            let pass = fetch_redis_pass();
            config.insert("password", to_static_str(pass));
            let redis_opts = RedisOpts::Config(config);
            Queue::new(&QUEUE_NAME, redis_opts, QueueOptions::default())
                .await
                .unwrap()
        })
    });
    #[ignore = "need fixing first"]
    #[tokio::test]
    async fn can_process_jobs() -> anyhow::Result<()> {
        use maplit::hashmap;
        use std::sync::Arc;
        use worker::{JobSetPair, Worker};
        let queue = QUEUE.force().await;
        let data = hashmap! {"foo"=> "bar"};
        let job_opts = JobOptions::default();

        type JobDataType = HashMap<&'static str, &'static str>;
        let worker_opts = WorkerOptions {
            autorun: false,
            concurrency: 1,
            ..Default::default()
        };

        let count: Arc<Atomic<usize>> = Arc::new(Atomic::new(1));
        let _job = queue
            .add::<_, String>("test-job", data.clone(), job_opts.clone(), None)
            .await?;

        let processor =
            |_data: JobSetPair<JobDataType, String>| async move { anyhow::Ok("done".to_owned()) };
        let now = Instant::now();

        let worker = Worker::build(&QUEUE_NAME, queue, processor, worker_opts).await?;
        let copy = count.clone();
        worker
            .on(
                "completed",
                move |(_completed_name, _id, returned_value): (String, String, String)| {
                    dbg!(_completed_name);
                    //assert_eq!(id, old_id);
                    assert_eq!(returned_value, "done");
                    //
                    //assert_eq!(completed_name, old_name);
                    println!("{:?} count : {:?}", now.elapsed(), copy.load());

                    let copy = copy.clone();

                    async move {
                        copy.fetch_add(1);
                    }
                },
            )
            .await;
        worker.run().await?;
        loop {
            if count.load() > 0 {
                worker.close(false).await;
                queue.obliterate(true).await?;

                break;
            }
        }
        Ok(())
    }
}
