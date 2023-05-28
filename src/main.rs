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

    let result: HashMap<String, String> = con.hgetall("bull:pinningQueue:172").await?;
     let json = JobJsonRaw::from_map(result)?;
    //json.save_to_file("test.json")?;

    // println!("{:#?}", worker.clone());
   // let j = serde_json::to_string(&result).unwrap_or("{}".to_string());

    println!(" {:#?}", json);
    println!("{:?}", n.elapsed());
    Ok(())
}

// print the type of the variable
