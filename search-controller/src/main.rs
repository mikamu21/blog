use anyhow::Result;
use kube::Client;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use search_controller::controller;

const DEFAULT_MEILISEARCH_URL: &str = "http://localhost:30700";
const DEFAULT_KAFKA_BOOTSTRAP: &str = "my-cluster-kafka-bootstrap.kafka:9092";
const DEFAULT_CONSUMER_IMAGE: &str = "search-controller:latest";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    tracing::info!("SearchIndex controller starting...");

    let client = Client::try_default().await?;
    let meilisearch_url =
        std::env::var("MEILISEARCH_URL").unwrap_or_else(|_| DEFAULT_MEILISEARCH_URL.to_string());
    let kafka_bootstrap = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| DEFAULT_KAFKA_BOOTSTRAP.to_string());
    let consumer_image =
        std::env::var("CONSUMER_IMAGE").unwrap_or_else(|_| DEFAULT_CONSUMER_IMAGE.to_string());

    tracing::info!("Using Meilisearch at: {}", meilisearch_url);
    tracing::info!("Using Kafka at: {}", kafka_bootstrap);
    tracing::info!("Using consumer image: {}", consumer_image);

    controller::run(client, &meilisearch_url, &kafka_bootstrap, &consumer_image).await?;

    Ok(())
}
