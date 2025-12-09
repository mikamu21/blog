use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "stratum.dev",
    version = "v1",
    kind = "SearchIndex",
    namespaced,
    status = "SearchIndexStatus",
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.conditions[?(@.type=='Ready')].status"}"#,
    printcolumn = r#"{"name":"Topic","type":"string","jsonPath":".status.kafkaTopic"}"#,
    printcolumn = r#"{"name":"Index","type":"string","jsonPath":".status.meilisearchIndex"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
pub struct SearchIndexSpec {
    /// Kafka topic configuration
    #[serde(default)]
    pub kafka: KafkaSpec,

    /// Connector (consumer) configuration
    #[serde(default)]
    pub connector: ConnectorSpec,

    /// Search index configuration
    pub index: IndexSpec,
}

/// Kafka topic configuration
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KafkaSpec {
    /// Number of partitions for the Kafka topic
    #[serde(default = "default_partitions")]
    pub partitions: i32,

    /// Number of replicas for the Kafka topic
    #[serde(default = "default_replicas")]
    pub replicas: i32,

    /// Strimzi cluster name (defaults to "my-cluster")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_name: Option<String>,

    /// Topic retention in milliseconds (defaults to 7200000 = 2 hours)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_ms: Option<i64>,
}

/// Connector (Kafka consumer) configuration
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorSpec {
    /// Number of documents to batch before flushing to Meilisearch
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Timeout in milliseconds before flushing partial batch
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
}

impl Default for ConnectorSpec {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            batch_timeout_ms: default_batch_timeout_ms(),
        }
    }
}

/// Search index configuration
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IndexSpec {
    /// Field definitions for the search index
    pub fields: Vec<FieldSpec>,

    /// Primary key field (defaults to "id")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<String>,
}

/// Field specification for search index
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FieldSpec {
    pub name: String,

    #[serde(default)]
    pub searchable: bool,

    #[serde(default)]
    pub filterable: bool,

    #[serde(default)]
    pub sortable: bool,
}

fn default_partitions() -> i32 {
    1
}

fn default_replicas() -> i32 {
    1
}

fn default_batch_size() -> usize {
    100
}

fn default_batch_timeout_ms() -> u64 {
    1000
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SearchIndexStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,

    /// Name of the created Kafka topic
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kafka_topic: Option<String>,

    /// Name of the created Meilisearch index
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meilisearch_index: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    #[serde(rename = "type")]
    pub type_: String,

    pub status: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
}

impl SearchIndex {
    pub fn is_ready(&self) -> bool {
        self.status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|conditions| {
                conditions
                    .iter()
                    .any(|c| c.type_ == "Ready" && c.status == "True")
            })
            .unwrap_or(false)
    }
}
