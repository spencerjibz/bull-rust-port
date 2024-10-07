use async_atomic::Atomic;
use async_lazy::Lazy;
use bull::*;
use enums::BullError;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::time::Instant;
use uuid::Uuid;
use worker::CallBackParams;
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
#[tokio::main]
async fn main() -> Result<(), BullError> {
    use maplit::hashmap;
    use std::sync::Arc;
    use worker::{JobSetPair, Worker};
    let queue = QUEUE.force().await;
    let data = hashmap! {"foo"=> "bar"};
    let job_opts = JobOptions::default();

    type JobDataType = HashMap<&'static str, &'static str>;
    let worker_opts = WorkerOptions {
        autorun: false,
        concurrency: 12,
        ..Default::default()
    };

    let count: Arc<Atomic<usize>> = Arc::new(Atomic::new(1));
    let _job = queue
        .add::<_, String>("test-job", data.clone(), job_opts.clone(), None)
        .await?;

    let processor = |_data: JobSetPair<JobDataType, String>| async move {
        //println!(" processing job {:#?}", _data.0);
        //panic!("test");
        Ok("done".to_owned())
    };
    let now = Instant::now();

    let worker = Worker::build(&QUEUE_NAME, queue, processor, worker_opts).await?;
    let job_count = 9;
    for _ in 0..job_count {
        let random_name = Uuid::new_v4().to_string();
        queue
            .add::<JobDataType, String>(
                to_static_str(random_name),
                data.clone(),
                job_opts.clone(),
                None,
            )
            .await?;
    }

    let copy = count.clone();
    worker
        .on(worker::Events::Completed, move |params| {
            if let CallBackParams::Completed(job, returned_value) = params {
                // println!("{:#?}", job);

                assert_eq!(returned_value, "done");
            }
            //assert_eq!(id, old_id);

            //
            //assert_eq!(completed_name, old_name);
            println!("{:?} count : {:?}", now.elapsed(), copy.load());
            copy.fetch_add(1);

            async move {}
        })
        .await;

    worker.run().await?;
    // tokio::time::sleep(Duration::from_secs(2)).await;

    loop {
        if count.load() == job_count + 1 {
            dbg!(count.load());
            // dbg!(worker.processing.lock().await.len());

            worker.close(false).await;
            // queue.obliterate(true).await?;

            break;
        }
    }
    Ok::<(), BullError>(())
}
