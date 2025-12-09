/// Default Kafka bootstrap servers for local development
pub const DEFAULT_BOOTSTRAP_SERVERS: &str = "localhost:30092";

/// Get Kafka bootstrap servers from environment or use default.
/// Checks KAFKA_BOOTSTRAP_SERVERS env var first.
pub fn get_bootstrap_servers() -> String {
    std::env::var("KAFKA_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| DEFAULT_BOOTSTRAP_SERVERS.to_string())
}

/// Default Kafka namespace in Kubernetes
pub const DEFAULT_KAFKA_NAMESPACE: &str = "kafka";

/// Default Strimzi cluster name
pub const DEFAULT_CLUSTER_NAME: &str = "my-cluster";

/// Default topic retention in milliseconds (2 hours)
pub const DEFAULT_RETENTION_MS: &str = "7200000";

/// Default segment size in bytes (1GB)
pub const DEFAULT_SEGMENT_BYTES: &str = "1073741824";
