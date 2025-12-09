use std::io::{self, BufRead};
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::json;

/// Kafka producer for testing SearchIndex
#[derive(Parser)]
#[command(name = "producer", about = "Produce test documents to Kafka")]
struct Args {
    /// Kafka topic to produce to
    #[arg(long, env = "KAFKA_TOPIC")]
    topic: String,

    /// Kafka bootstrap servers
    #[arg(
        long,
        env = "KAFKA_BOOTSTRAP_SERVERS",
        default_value = "localhost:30092"
    )]
    bootstrap_servers: String,

    /// Read JSON documents from stdin (one per line)
    #[arg(long)]
    stdin: bool,

    /// Send sample test documents
    #[arg(long)]
    sample: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("Producing to topic: {}", args.topic);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &args.bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()?;

    let mut count = 0;

    if args.stdin {
        // Read JSON documents from stdin
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            let doc: serde_json::Value = serde_json::from_str(&line)?;
            let id = doc["id"].as_str().unwrap_or("unknown");

            producer
                .send(
                    FutureRecord::to(&args.topic).payload(&line).key(id),
                    Duration::from_secs(5),
                )
                .await
                .map_err(|(e, _)| anyhow::anyhow!("Failed to send: {:?}", e))?;

            println!("Sent document: {}", id);
            count += 1;
        }
    } else if args.sample {
        // Send sample test documents
        let docs = vec![
            json!({"id": "1", "title": "Introduction to Rust", "content": "Rust is a systems programming language", "category": "programming"}),
            json!({"id": "2", "title": "Kubernetes Operators", "content": "Building operators with kube-rs", "category": "infrastructure"}),
            json!({"id": "3", "title": "Search Engine Basics", "content": "How search engines index documents", "category": "search"}),
        ];

        for doc in docs {
            let payload = serde_json::to_string(&doc)?;
            let id = doc["id"].as_str().unwrap();

            producer
                .send(
                    FutureRecord::to(&args.topic).payload(&payload).key(id),
                    Duration::from_secs(5),
                )
                .await
                .map_err(|(e, _)| anyhow::anyhow!("Failed to send: {:?}", e))?;

            println!("Sent document: {}", id);
            count += 1;
        }
    } else {
        eprintln!("Error: specify --stdin or --sample");
        std::process::exit(1);
    }

    println!("Done! Sent {} documents", count);
    Ok(())
}
