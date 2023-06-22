#![allow(unused, dead_code)]
use anyhow::Ok;
use bull::*;
use dotenv_codegen::dotenv;
use std::collections::HashMap;
use std::env;
use std::time::{Instant, SystemTime};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // print backtrace if either Ru
    use std::backtrace::Backtrace;
    // or forcibly capture the backtrace regardless of environment variable configuration

    let n = Instant::now();
    let pass = dotenv!("REDIS_PASSWORD");

    let mut config = HashMap::new();
    config.insert("password", pass);
    let redis_opts = RedisOpts::Config(config);
    let mut client = RedisConnection::init(redis_opts.clone()).await?;
    // check the connection
    let keeps_opts = KeepJobs {
        age: None,
        count: Some(-1),
    };

    let mut con = client.conn;
    // set time and
    let mut worker = WorkerOptions::default();
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
    con.set("worker", worker).await?;
    // get the values and log it;
    let value: WorkerOptions = con.get("worker").await?;

    let result: HashMap<String, String> = con.hgetall("bull:pinningQueue:202").await?;

    if result.is_empty() {
        println!("No data found");
    } else {
        println!("Data found");

        //json.save_to_file("test.json")?;
        let mut queue = Queue::<'_>::new("test", redis_opts, QueueOptions::default()).await?;
        // println!("{:#?}", worker.clone());
        // let j = serde_json::to_string(&result).unwrap_or("{}".to_string());
        let contents = serde_json::to_string(&result).unwrap_or("{}".to_string());
       println!("{:#?}", contents);

        let job = Job::<Data, Option<String>>::from_json(&queue, contents, "202").await?;
        println!("{:#?}", job);
        // println!(" {value:#?}");
        println!("{:?}", n.elapsed());
    }
    Ok(())
}

// print the type of the variable
