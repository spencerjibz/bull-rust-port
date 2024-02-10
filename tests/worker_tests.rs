// write test for the queue
#![allow(unused_imports, dead_code)]
#![allow(clippy::needless_return)]
#[cfg(test)]

mod worker {
    use anyhow::Ok;
    use async_event_emitter::AsyncEventEmitter;
    use async_lazy::Lazy;
    use bull::{
        worker::{JobSetPair, Worker},
        *,
    };
    use futures::lock::Mutex;
    use lazy_static::lazy_static;
    use rand::random;
    use std::{
        collections::{hash_map, HashMap},
        sync::Arc,
        time::Duration,
    };
    use tokio::time::Instant;
    use uuid::Uuid;
    lazy_static! {
        static ref QUEUE_NAME: String = {
           // let token = Uuid::new_v4().to_string();
            "_test_queue".to_string()
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

    #[tokio_shared_rt::test(shared = false)]
    async fn should_process_jobs() -> anyhow::Result<()> {
        console_subscriber::init();
        use maplit::hashmap;

        let queue = QUEUE.force().await;
        let data = hashmap! {"foo"=> "bar"};
        let job_opts = JobOptions::default();
        type JobDataType = HashMap<&'static str, &'static str>;
        let worker_opts = WorkerOptions {
            autorun: false,
            ..Default::default()
        };

        let _job = queue
            .add::<JobDataType, String>("test-job", data.clone(), job_opts.clone(), None)
            .await?;

        let processor = |_data: JobSetPair<JobDataType, String>| async move {
            // println!(" processing job {:#?}", data.0);

            //tokio::time::sleep(Duration::from_secs(2)).await;

            anyhow::Ok("done".to_owned())
        };
        let now = Instant::now();
        let mut worker = Worker::init(&QUEUE_NAME, queue, processor, worker_opts, None).await?;

        tokio::time::sleep(Duration::from_secs(2)).await;

        for _ in 0..5 {
            let random_name = Uuid::new_v4().to_string();
            let rand_id: u16 = random();
            queue
                .add::<JobDataType, String>(
                    to_static_str(random_name),
                    data.clone(),
                    job_opts.clone(),
                    Some(rand_id.to_string()),
                )
                .await?;
        }
        worker
            .on(
                "completed",
                move |(completed_name, id, returned_value): (String, String, String)| {
                    //assert_eq!(id, old_id);
                    assert_eq!(returned_value, "done");
                    //
                    //assert_eq!(completed_name, old_name);
                    println!("{:?}", now.elapsed());

                    //assert_eq!(completed_job.return_value, Some("done".to_string()));
                    //assert_ne!(completed_job.finished_on, 0);
                    async move {}
                },
            )
            .await;

        worker.run().await?;
        worker.close(true);
        queue.close().await;

        Ok(())
    }
}
