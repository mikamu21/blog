use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio::time::timeout;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

/// Kafka to Meilisearch consumer bridge
#[derive(Parser)]
#[command(
    name = "consumer",
    about = "Consume documents from Kafka and index to Meilisearch"
)]
struct Args {
    /// Kafka topic to consume from
    #[arg(long, env = "KAFKA_TOPIC")]
    topic: String,

    /// Meilisearch index name
    #[arg(long, env = "MEILISEARCH_INDEX")]
    index: String,

    /// Kafka consumer group ID
    #[arg(long, env = "KAFKA_GROUP_ID", default_value = "search-consumer")]
    group_id: String,

    /// Kafka bootstrap servers
    #[arg(
        long,
        env = "KAFKA_BOOTSTRAP_SERVERS",
        default_value = "localhost:30092"
    )]
    bootstrap_servers: String,

    /// Meilisearch URL
    #[arg(
        long,
        env = "MEILISEARCH_URL",
        default_value = "http://localhost:30700"
    )]
    meilisearch_url: String,

    /// Number of documents to batch before flushing
    #[arg(long, env = "BATCH_SIZE", default_value = "100")]
    batch_size: usize,

    /// Batch timeout in milliseconds
    #[arg(long, env = "BATCH_TIMEOUT_MS", default_value = "1000")]
    batch_timeout_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    tracing::info!(
        "Starting consumer: {} -> {}/{}",
        args.topic,
        args.meilisearch_url,
        args.index
    );

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &args.bootstrap_servers)
        .set("group.id", &args.group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("reconnect.backoff.ms", "50")
        .set("reconnect.backoff.max.ms", "1000")
        .create()?;

    consumer.subscribe(&[&args.topic])?;

    let http_client = reqwest::Client::new();
    let batch_timeout = Duration::from_millis(args.batch_timeout_ms);
    let mut batch: Vec<serde_json::Value> = Vec::with_capacity(args.batch_size);

    loop {
        match timeout(batch_timeout, consumer.recv()).await {
            Ok(Ok(msg)) => {
                if let Some(payload) = msg.payload() {
                    match serde_json::from_slice::<serde_json::Value>(payload) {
                        Ok(doc) => {
                            batch.push(doc);
                            if batch.len() >= args.batch_size {
                                flush_batch(
                                    &http_client,
                                    &args.meilisearch_url,
                                    &args.index,
                                    &mut batch,
                                )
                                .await?;
                                consumer.commit_consumer_state(CommitMode::Async)?;
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Failed to parse message as JSON: {}", e);
                        }
                    }
                }
            }
            Ok(Err(e)) => {
                tracing::error!("Kafka error: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(_) => {
                if !batch.is_empty() {
                    flush_batch(&http_client, &args.meilisearch_url, &args.index, &mut batch)
                        .await?;
                    consumer.commit_consumer_state(CommitMode::Async)?;
                }
            }
        }
    }
}

async fn flush_batch(
    client: &reqwest::Client,
    meilisearch_url: &str,
    index: &str,
    batch: &mut Vec<serde_json::Value>,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let url = format!("{}/indexes/{}/documents", meilisearch_url, index);

    tracing::info!("Flushing {} documents to Meilisearch", batch.len());

    let resp = client.post(&url).json(&batch).send().await?;

    if !resp.status().is_success() {
        let body = resp.text().await?;
        return Err(anyhow::anyhow!("Failed to index documents: {}", body));
    }

    batch.clear();
    Ok(())
}
