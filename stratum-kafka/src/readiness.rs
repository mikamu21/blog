use std::sync::Arc;
use std::time::Duration;

use kube::Client;
use kube::api::Api;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use tokio::time::sleep;

use crate::config::{DEFAULT_KAFKA_NAMESPACE, get_bootstrap_servers};
use crate::error::KafkaError;
use crate::topic::KafkaTopic;

/// Wait for a KafkaTopic to be fully ready using the 3-layer readiness check.
///
/// This function verifies readiness at three levels:
/// 1. **K8s CR Ready**: The Strimzi KafkaTopic CR has `Ready=True` condition
/// 2. **Broker Metadata**: The Kafka broker reports the topic in its metadata
/// 3. **GroupCoordinator**: Consumer group operations work (subscribe + recv)
///
/// This 3-layer check prevents flaky tests caused by race conditions where
/// Kubernetes reports the topic as ready before the broker is actually ready
/// to handle consumer group operations.
pub async fn wait_for_topic_ready(client: &Client, name: &str) -> Result<(), KafkaError> {
    let bootstrap_servers = get_bootstrap_servers();
    wait_for_topic_ready_with_options(client, name, &bootstrap_servers, 30, 30, 30).await
}

/// Wait for a KafkaTopic with custom timeout options
pub async fn wait_for_topic_ready_with_options(
    client: &Client,
    name: &str,
    bootstrap_servers: &str,
    k8s_timeout_secs: u64,
    metadata_timeout_secs: u64,
    coordinator_timeout_secs: u64,
) -> Result<(), KafkaError> {
    // Layer 1: Wait for Kubernetes Resource to be Ready
    wait_for_k8s_ready(client, name, k8s_timeout_secs).await?;

    // Layer 2: Wait for Kafka Broker to report metadata
    wait_for_broker_metadata(name, bootstrap_servers, metadata_timeout_secs).await?;

    // Layer 3: Wait for GroupCoordinator to be ready
    wait_for_group_coordinator(name, bootstrap_servers, coordinator_timeout_secs).await?;

    Ok(())
}

/// Layer 1: Check if the KafkaTopic CR has Ready=True condition
async fn wait_for_k8s_ready(
    client: &Client,
    name: &str,
    timeout_secs: u64,
) -> Result<(), KafkaError> {
    let api: Api<KafkaTopic> = Api::namespaced(client.clone(), DEFAULT_KAFKA_NAMESPACE);

    for _ in 0..timeout_secs {
        if let Ok(topic) = api.get(name).await
            && let Some(status) = &topic.status
            && let Some(conditions) = &status.conditions
            && conditions
                .iter()
                .any(|c| c.type_ == "Ready" && c.status == "True")
        {
            tracing::debug!("Layer 1: KafkaTopic '{}' CR is Ready", name);
            return Ok(());
        }
        sleep(Duration::from_secs(1)).await;
    }

    Err(KafkaError::TopicNotReady(name.to_string(), timeout_secs))
}

/// Layer 2: Verify the Kafka broker reports the topic in its metadata
async fn wait_for_broker_metadata(
    name: &str,
    bootstrap_servers: &str,
    timeout_secs: u64,
) -> Result<(), KafkaError> {
    let metadata_consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", "metadata-checker")
        .create()
        .map_err(|e| KafkaError::ClientCreation(e.to_string()))?;

    let metadata_consumer = Arc::new(metadata_consumer);

    for _ in 0..timeout_secs {
        match tokio::task::spawn_blocking({
            let consumer = metadata_consumer.clone();
            let topic_name = name.to_string();
            move || consumer.fetch_metadata(Some(&topic_name), Duration::from_secs(2))
        })
        .await
        {
            Ok(Ok(metadata)) => {
                if !metadata.topics().is_empty() {
                    tracing::debug!("Layer 2: Broker metadata available for topic '{}'", name);
                    return Ok(());
                }
            }
            Ok(Err(e)) => {
                tracing::debug!("Waiting for topic metadata '{}': {:?}", name, e);
            }
            Err(e) => {
                tracing::debug!("Metadata check task failed for '{}': {:?}", name, e);
            }
        }
        sleep(Duration::from_secs(1)).await;
    }

    Err(KafkaError::MetadataNotReady(name.to_string(), timeout_secs))
}

/// Layer 3: Verify GroupCoordinator is ready by subscribing and attempting to receive
async fn wait_for_group_coordinator(
    name: &str,
    bootstrap_servers: &str,
    timeout_secs: u64,
) -> Result<(), KafkaError> {
    let check_consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", "coordinator-checker")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", "earliest")
        // Aggressive reconnection for CI environments
        .set("reconnect.backoff.ms", "50")
        .set("reconnect.backoff.max.ms", "1000")
        .create()
        .map_err(|e| KafkaError::ClientCreation(e.to_string()))?;

    check_consumer
        .subscribe(&[name])
        .map_err(|e| KafkaError::ReceiveFailed(e.to_string()))?;

    // Attempt to receive to force group coordinator initialization
    // Must succeed (or timeout) at least once - don't silently swallow errors
    for _ in 0..timeout_secs {
        match tokio::time::timeout(Duration::from_secs(2), check_consumer.recv()).await {
            Ok(Ok(_)) => {
                // Got a message - definitely ready
                tracing::debug!("Layer 3: GroupCoordinator ready for topic '{}'", name);
                return Ok(());
            }
            Err(_) => {
                // Timeout - connected but no messages, that's fine
                tracing::debug!(
                    "Layer 3: GroupCoordinator ready for topic '{}' (no messages)",
                    name
                );
                return Ok(());
            }
            Ok(Err(e)) => {
                // Kafka error - coordinator might not be ready yet
                tracing::debug!("Waiting for GroupCoordinator for topic '{}': {:?}", name, e);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    Err(KafkaError::CoordinatorNotReady(
        name.to_string(),
        timeout_secs,
    ))
}
