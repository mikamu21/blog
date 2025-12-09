use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Api, PostParams};
use kube::{Client, CustomResource};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::config::{
    DEFAULT_CLUSTER_NAME, DEFAULT_KAFKA_NAMESPACE, DEFAULT_RETENTION_MS, DEFAULT_SEGMENT_BYTES,
};
use crate::error::KafkaError;

/// Strimzi KafkaTopic custom resource
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "kafka.strimzi.io",
    version = "v1beta2",
    kind = "KafkaTopic",
    namespaced,
    status = "KafkaTopicStatus"
)]
pub struct KafkaTopicSpec {
    pub partitions: i32,
    pub replicas: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct KafkaTopicStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<KafkaTopicCondition>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct KafkaTopicCondition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: String,
}

/// Create a Strimzi KafkaTopic custom resource
pub async fn create_kafka_topic(
    client: &Client,
    name: &str,
    partitions: i32,
    replicas: i32,
) -> Result<(), KafkaError> {
    create_kafka_topic_with_options(
        client,
        name,
        partitions,
        replicas,
        DEFAULT_CLUSTER_NAME,
        None,
    )
    .await
}

/// Create a Strimzi KafkaTopic with custom options
pub async fn create_kafka_topic_with_options(
    client: &Client,
    name: &str,
    partitions: i32,
    replicas: i32,
    cluster_name: &str,
    config: Option<serde_json::Value>,
) -> Result<(), KafkaError> {
    let api: Api<KafkaTopic> = Api::namespaced(client.clone(), DEFAULT_KAFKA_NAMESPACE);

    let mut labels = BTreeMap::new();
    labels.insert("strimzi.io/cluster".to_string(), cluster_name.to_string());

    let topic_config = config.unwrap_or_else(|| {
        serde_json::json!({
            "retention.ms": DEFAULT_RETENTION_MS,
            "segment.bytes": DEFAULT_SEGMENT_BYTES
        })
    });

    let topic = KafkaTopic {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(DEFAULT_KAFKA_NAMESPACE.to_string()),
            labels: Some(labels),
            ..Default::default()
        },
        spec: KafkaTopicSpec {
            partitions,
            replicas,
            config: Some(topic_config),
        },
        status: None,
    };

    api.create(&PostParams::default(), &topic).await?;
    Ok(())
}

/// Check if a KafkaTopic CR exists
pub async fn topic_exists(client: &Client, name: &str) -> Result<bool, KafkaError> {
    let api: Api<KafkaTopic> = Api::namespaced(client.clone(), DEFAULT_KAFKA_NAMESPACE);
    Ok(api.get_opt(name).await?.is_some())
}

/// Delete a KafkaTopic CR
pub async fn delete_kafka_topic(client: &Client, name: &str) -> Result<(), KafkaError> {
    let api: Api<KafkaTopic> = Api::namespaced(client.clone(), DEFAULT_KAFKA_NAMESPACE);

    if api.get_opt(name).await?.is_some() {
        api.delete(name, &Default::default()).await?;
        tracing::info!("Deleted KafkaTopic {}", name);
    }

    Ok(())
}
