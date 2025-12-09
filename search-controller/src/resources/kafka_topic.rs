use anyhow::Result;
use kube::Client;

// Re-export from shared library
pub use stratum_kafka::{KafkaTopic, KafkaTopicCondition, KafkaTopicSpec, KafkaTopicStatus};

/// Ensure a KafkaTopic exists, creating it if necessary
pub async fn ensure_kafka_topic(
    client: &Client,
    name: &str,
    partitions: i32,
    replicas: i32,
) -> Result<()> {
    if stratum_kafka::topic_exists(client, name).await? {
        tracing::info!("KafkaTopic {} already exists", name);
        return Ok(());
    }

    stratum_kafka::create_kafka_topic(client, name, partitions, replicas).await?;
    tracing::info!("Created KafkaTopic {}", name);
    Ok(())
}

/// Wait for a KafkaTopic to be fully ready (3-layer readiness check)
pub async fn wait_for_topic_ready(client: &Client, name: &str) -> Result<()> {
    stratum_kafka::wait_for_topic_ready(client, name).await?;
    tracing::info!("KafkaTopic {} is ready (3-layer check passed)", name);
    Ok(())
}

/// Delete a KafkaTopic
pub async fn delete_kafka_topic(client: &Client, name: &str) -> Result<()> {
    stratum_kafka::delete_kafka_topic(client, name).await?;
    Ok(())
}
