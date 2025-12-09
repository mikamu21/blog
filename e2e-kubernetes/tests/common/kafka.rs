use anyhow::Result;
use kube::CustomResource;
use rdkafka::Message as RdkafkaMessage;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "kafka.strimzi.io",
    version = "v1beta2",
    kind = "KafkaTopic",
    namespaced
)]
#[kube(status = "KafkaTopicStatus")]
pub struct KafkaTopicSpec {
    pub partitions: i32,
    pub replicas: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct KafkaTopicStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Condition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: String,
}

pub struct KafkaProducer {
    inner: FutureProducer,
}

impl KafkaProducer {
    pub async fn new() -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", crate::common::KAFKA_BOOTSTRAP_SERVERS)
            .set("message.timeout.ms", "5000")
            .create()?;

        Ok(Self { inner: producer })
    }

    pub async fn send(&self, topic: &str, key: &str, payload: &str) -> Result<()> {
        self.inner
            .send(
                FutureRecord::to(topic).payload(payload).key(key),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| {
                anyhow::anyhow!(
                    "Failed to send message to topic '{}' (bootstrap: {}): {:?}",
                    topic,
                    crate::common::KAFKA_BOOTSTRAP_SERVERS,
                    e
                )
            })?;

        Ok(())
    }
}

pub struct KafkaConsumer {
    inner: StreamConsumer,
}

impl KafkaConsumer {
    pub async fn new(group_id: &str) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", crate::common::KAFKA_BOOTSTRAP_SERVERS)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .set("reconnect.backoff.ms", "50")
            .set("reconnect.backoff.max.ms", "1000")
            .create()?;

        Ok(Self { inner: consumer })
    }

    pub fn subscribe(&self, topics: &[&str]) -> Result<()> {
        self.inner.subscribe(topics).map_err(|e| {
            anyhow::anyhow!(
                "Failed to subscribe to topics {:?} (bootstrap: {}): {:?}",
                topics,
                crate::common::KAFKA_BOOTSTRAP_SERVERS,
                e
            )
        })?;
        Ok(())
    }

    pub async fn receive(&self) -> Result<KafkaMessage> {
        use tokio::time::sleep;

        for _ in 1..=30 {
            match tokio::time::timeout(Duration::from_secs(2), self.inner.recv()).await {
                Ok(Ok(msg)) => return Ok(KafkaMessage::from_borrowed(msg)),
                Ok(Err(_)) => {
                    sleep(Duration::from_secs(1)).await;
                }
                Err(_) => {
                    continue;
                }
            }
        }

        Err(anyhow::anyhow!(
            "Timeout (60s) waiting for message from bootstrap: {}",
            crate::common::KAFKA_BOOTSTRAP_SERVERS
        ))
    }
}

pub async fn create_kafka_topic(
    client: &kube::Client,
    name: &str,
    partitions: i32,
    replicas: i32,
) -> Result<()> {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::api::{Api, PostParams};

    let api: Api<KafkaTopic> = Api::namespaced(client.clone(), "kafka");

    let topic = KafkaTopic {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("kafka".to_string()),
            labels: Some(
                vec![("strimzi.io/cluster".to_string(), "my-cluster".to_string())]
                    .into_iter()
                    .collect(),
            ),
            ..Default::default()
        },
        spec: KafkaTopicSpec {
            partitions,
            replicas,
            config: Some(serde_json::json!({
                "retention.ms": "7200000",
                "segment.bytes": "1073741824"
            })),
        },
        status: None,
    };

    api.create(&PostParams::default(), &topic).await?;
    Ok(())
}

pub async fn wait_for_topic_ready(client: &kube::Client, name: &str) -> Result<()> {
    use kube::api::Api;
    use tokio::time::sleep;

    let api: Api<KafkaTopic> = Api::namespaced(client.clone(), "kafka");

    let mut ready = false;
    for _ in 0..30 {
        if let Ok(topic) = api.get(name).await
            && let Some(status) = &topic.status
            && let Some(conditions) = &status.conditions
            && conditions
                .iter()
                .any(|c| c.type_ == "Ready" && c.status == "True")
        {
            ready = true;
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }

    if !ready {
        return Err(anyhow::anyhow!(
            "Timeout waiting for KafkaTopic '{}' CR to be Ready",
            name
        ));
    }

    let metadata_consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", crate::common::KAFKA_BOOTSTRAP_SERVERS)
        .set("group.id", "metadata-checker")
        .create()?;

    let metadata_consumer = std::sync::Arc::new(metadata_consumer);

    let mut metadata_ready = false;
    for _ in 0..30 {
        if let Ok(metadata) = tokio::task::spawn_blocking({
            let consumer = metadata_consumer.clone();
            let topic_name = name.to_string();
            move || consumer.fetch_metadata(Some(&topic_name), Duration::from_secs(2))
        })
        .await?
            && !metadata.topics().is_empty()
        {
            metadata_ready = true;
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }

    if !metadata_ready {
        return Err(anyhow::anyhow!(
            "Timeout waiting for Kafka Broker to report metadata for topic '{}'",
            name
        ));
    }

    let check_consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", crate::common::KAFKA_BOOTSTRAP_SERVERS)
        .set("group.id", "coordinator-checker")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", "earliest")
        .set("reconnect.backoff.ms", "50")
        .set("reconnect.backoff.max.ms", "1000")
        .create()?;

    check_consumer.subscribe(&[name])?;

    let mut coordinator_ready = false;
    for _ in 0..30 {
        match tokio::time::timeout(Duration::from_secs(2), check_consumer.recv()).await {
            Ok(Ok(_)) => {
                coordinator_ready = true;
                break;
            }
            Err(_) => {
                coordinator_ready = true;
                break;
            }
            Ok(Err(_)) => {
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    if !coordinator_ready {
        return Err(anyhow::anyhow!(
            "Timeout waiting for GroupCoordinator to be ready for topic '{}'",
            name
        ));
    }

    Ok(())
}

pub struct KafkaMessage {
    key: Option<String>,
    payload: Option<String>,
}

impl KafkaMessage {
    pub(crate) fn from_borrowed(msg: rdkafka::message::BorrowedMessage) -> Self {
        let key = msg
            .key_view::<str>()
            .and_then(|k| k.ok())
            .map(|k| k.to_string());

        let payload = msg
            .payload_view::<str>()
            .and_then(|p| p.ok())
            .map(|p| p.to_string());

        Self { key, payload }
    }

    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    pub fn payload(&self) -> Option<&str> {
        self.payload.as_deref()
    }
}
