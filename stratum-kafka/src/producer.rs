use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::config::DEFAULT_BOOTSTRAP_SERVERS;
use crate::error::KafkaError;

/// Kafka producer wrapper with simple async API
pub struct KafkaProducer {
    inner: FutureProducer,
    bootstrap_servers: String,
}

impl KafkaProducer {
    /// Create a new Kafka producer with default bootstrap servers
    pub fn new() -> Result<Self, KafkaError> {
        Self::with_bootstrap_servers(DEFAULT_BOOTSTRAP_SERVERS)
    }

    /// Create a new Kafka producer with custom bootstrap servers
    pub fn with_bootstrap_servers(bootstrap_servers: &str) -> Result<Self, KafkaError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(|e| KafkaError::ClientCreation(e.to_string()))?;

        Ok(Self {
            inner: producer,
            bootstrap_servers: bootstrap_servers.to_string(),
        })
    }

    /// Send a message to a topic
    pub async fn send(&self, topic: &str, key: &str, payload: &str) -> Result<(), KafkaError> {
        self.inner
            .send(
                FutureRecord::to(topic).payload(payload).key(key),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| {
                KafkaError::SendFailed(format!(
                    "Failed to send to topic '{}' (bootstrap: {}): {:?}",
                    topic, self.bootstrap_servers, e
                ))
            })?;

        Ok(())
    }

    /// Get the bootstrap servers this producer is connected to
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }
}
