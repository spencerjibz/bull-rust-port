use bull::*;
use enums::BullError;

use std::collections::HashMap;

use tokio::time::Instant;

#[tokio::main]
async fn main() -> Result<(), BullError> {
    let n = Instant::now();
    let pass = fetch_redis_pass();
    let mut config = HashMap::new();
    config.insert("password", to_static_str(pass));
    let redis_opts = RedisOpts::Config(config);
    let client = RedisConnection::init(redis_opts.clone()).await?;
    let mut con = client.conn;
    let result: HashMap<String, String> = con.hgetall("bull:pinningQueue:20").await?;
    let contents = if !result.is_empty() {
        serde_json::to_string(&result).unwrap_or("{}".to_string())
    } else {
        std::fs::read_to_string("test.json")?
    };
    let job_parsing_time = Instant::now();
    let queue = Queue::new("test", redis_opts, QueueOptions::default()).await?;
    let job = Job::<Data, ReturnedData>::from_json(&queue, contents, "207").await?;
    println!("{:#?}", job_parsing_time.elapsed());
    println!("{:#?}", job);
    use chrono::{DateTime, Utc};
    if let (Some(finished_on), Some(processed_on), date) =
        (job.finished_on, job.processed_on, job.timestamp)
    {
        let finished_on = DateTime::from_timestamp_millis(finished_on as i64).unwrap();
        let processed_on = DateTime::from_timestamp_millis(processed_on as i64).unwrap();
        let date = DateTime::from_timestamp_millis(date);
        println!("finished_on: {:#?}", finished_on);
        println!("processed_on: {:#?}", processed_on);
        let datetime: DateTime<Utc> = date.unwrap();
        // Format the datetime how you want
        let newdate = datetime.format("%Y-%m-%d %H:%M:%S");
        println!("date: {}", newdate);
    }
    println!("{:?}", n.elapsed());
    Ok(())
}
