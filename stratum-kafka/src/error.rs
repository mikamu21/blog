use thiserror::Error;

#[derive(Error, Debug)]
pub enum KafkaError {
    #[error("Topic '{0}' not ready after {1}s (K8s CR status)")]
    TopicNotReady(String, u64),

    #[error("Broker metadata not available for topic '{0}' after {1}s")]
    MetadataNotReady(String, u64),

    #[error("GroupCoordinator not ready for topic '{0}' after {1}s")]
    CoordinatorNotReady(String, u64),

    #[error("Failed to create Kafka client: {0}")]
    ClientCreation(String),

    #[error("Failed to send message: {0}")]
    SendFailed(String),

    #[error("Failed to receive message: {0}")]
    ReceiveFailed(String),

    #[error("Timeout waiting for message after {0}s")]
    ReceiveTimeout(u64),

    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    #[error("rdkafka error: {0}")]
    RdKafka(#[from] rdkafka::error::KafkaError),
}
