// write test for the queue
#![allow(unused_imports, dead_code)]
#![allow(clippy::needless_return)]
#[cfg(test)]

mod worker {
    use anyhow::Ok;
    use async_atomic::Atomic;
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

    #[tokio_shared_rt::test(shared = false)]
    #[ignore = "there is still work in progress"]
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
        let count: Atomic<usize> = Atomic::new(1);
        let _job = queue
            .add::<_, String>("test-job", data.clone(), job_opts.clone(), None)
            .await?;

        let processor = |_data: JobSetPair<JobDataType, String>| async move {
            //  println!(" processing job {:#?}", data.0);

            // tokio::time::sleep(Duration::from_secs(2)).await;

            anyhow::Ok("done".to_owned())
        };
        let now = Instant::now();
        let mut worker = Worker::init(&QUEUE_NAME, queue, processor, worker_opts, None).await?;

        for _ in 0..2{
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
                move |(_completed_name, _id, returned_value): (String, String, String)| {
                    count.fetch_add(1);
                    dbg!(_completed_name);
                    //assert_eq!(id, old_id);
                    assert_eq!(returned_value, "done");
                    //
                    //assert_eq!(completed_name, old_name);
                    println!("{:?} count : {:?}", now.elapsed(), count.load());

                    if count.load() == 7 {
                        std::process::exit(0);
                    }
                    //assert_eq!(completed_job.return_value, Some("done".to_string()));
                    //assert_ne!(completed_job.finished_on, 0);
                    async move {}
                },
            )
            .await;

        // tokio::time::sleep(Duration::from_secs(2)).await;

        worker.run().await?;
        //worker.close(true);
        //queue.close().await;

        Ok(())
    }
}
