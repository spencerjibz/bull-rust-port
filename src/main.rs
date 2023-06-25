#![allow(unused, dead_code)]
use anyhow::Ok;
use bull::*;
use dotenv_codegen::dotenv;
use std::collections::HashMap;
use std::env;
use std::time::{Instant, SystemTime};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let n = Instant::now();
    let pass = dotenv!("REDIS_PASSWORD");

    let mut config = HashMap::new();
    config.insert("password", pass);
    let redis_opts = RedisOpts::Config(config);
    let mut client = RedisConnection::init(redis_opts.clone()).await?;

    let keeps_opts = KeepJobs {
        age: None,
        count: Some(-1),
    };

    let mut con = client.conn;

    let mut worker = WorkerOptions::default();
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
    con.set("worker", worker).await?;
    // get the values and log it;
    let value: WorkerOptions = con.get("worker").await?;

    let result: HashMap<String, String> = con.hgetall("bull:pinningQueue:167").await?;

    if result.is_empty() {
        println!("No data found");
    } else {
        println!("Data found");

        let mut queue = Queue::<'_>::new("test", redis_opts, QueueOptions::default()).await?;

        let contents = serde_json::to_string(&result).unwrap_or("{}".to_string());

        let job = Job::<Data, ReturnedData>::from_json(&queue, contents, "167").await?;
        println!("{:#?}", job);

        use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

        let (finished_on, processed_on, date) = (job.finished_on, job.processed_on, job.timestamp);

        let finished_on = NaiveDateTime::from_timestamp_millis(finished_on).unwrap();
        let processed_on = NaiveDateTime::from_timestamp_millis(processed_on).unwrap();
        let date = NaiveDateTime::from_timestamp_millis(date);

        println!("finished_on: {:#?}", finished_on);
        println!("processed_on: {:#?}", processed_on);
        let datetime: DateTime<Utc> = DateTime::from_utc(date.unwrap(), Utc);

        // Format the datetime how you want
        let newdate = datetime.format("%Y-%m-%d %H:%M:%S");
        println!("date: {}", newdate);

        println!("{:?}", n.elapsed());
        
    }
    Ok(())
}

// print the type of the variable
