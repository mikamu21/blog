use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use tokio::time::sleep;

use crate::config::DEFAULT_BOOTSTRAP_SERVERS;
use crate::error::KafkaError;
use crate::message::KafkaMessage;

/// Kafka consumer wrapper with simple async API and robust retry logic
pub struct KafkaConsumer {
    inner: StreamConsumer,
    bootstrap_servers: String,
}

impl KafkaConsumer {
    /// Create a new Kafka consumer with default bootstrap servers
    pub fn new(group_id: &str) -> Result<Self, KafkaError> {
        Self::with_bootstrap_servers(DEFAULT_BOOTSTRAP_SERVERS, group_id)
    }

    /// Create a new Kafka consumer with custom bootstrap servers
    pub fn with_bootstrap_servers(
        bootstrap_servers: &str,
        group_id: &str,
    ) -> Result<Self, KafkaError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            // Aggressive reconnection for CI environments
            .set("reconnect.backoff.ms", "50")
            .set("reconnect.backoff.max.ms", "1000")
            .create()
            .map_err(|e| KafkaError::ClientCreation(e.to_string()))?;

        Ok(Self {
            inner: consumer,
            bootstrap_servers: bootstrap_servers.to_string(),
        })
    }

    /// Subscribe to topics
    pub fn subscribe(&self, topics: &[&str]) -> Result<(), KafkaError> {
        self.inner.subscribe(topics).map_err(|e| {
            KafkaError::ReceiveFailed(format!(
                "Failed to subscribe to topics {:?} (bootstrap: {}): {:?}",
                topics, self.bootstrap_servers, e
            ))
        })
    }

    /// Receive a message with retry logic for transient errors
    ///
    /// Retries up to 30 times (60 seconds total) on transport errors.
    /// Returns successfully on first message or timeout (no messages available).
    pub async fn receive(&self) -> Result<KafkaMessage, KafkaError> {
        for attempt in 1..=30 {
            match tokio::time::timeout(Duration::from_secs(2), self.inner.recv()).await {
                Ok(Ok(msg)) => return Ok(KafkaMessage::from_borrowed(msg)),
                Ok(Err(e)) => {
                    // Kafka error - might be transient, retry
                    tracing::debug!(
                        "Attempt {}/30: Kafka error (bootstrap: {}): {:?}",
                        attempt,
                        self.bootstrap_servers,
                        e
                    );
                    sleep(Duration::from_secs(1)).await;
                }
                Err(_) => {
                    // Timeout - no messages yet, keep waiting
                    continue;
                }
            }
        }

        Err(KafkaError::ReceiveTimeout(60))
    }

    /// Get the bootstrap servers this consumer is connected to
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Get access to the underlying rdkafka consumer for advanced operations
    pub fn inner(&self) -> &StreamConsumer {
        &self.inner
    }
}
