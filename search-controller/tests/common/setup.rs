use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Result;
use k8s_openapi::api::core::v1::Namespace;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Client;
use kube::api::{Api, PostParams};
use meilisearch_sdk::client::Client as MeilisearchClient;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use super::{KAFKA_BOOTSTRAP, MEILISEARCH_URL};

pub struct TestSetupBuilder {
    test_name: String,
    meilisearch: bool,
    kafka: bool,
    ttl_seconds: u64,
}

impl TestSetupBuilder {
    pub fn new(test_name: &str) -> Self {
        Self {
            test_name: test_name.to_string(),
            meilisearch: false,
            kafka: false,
            ttl_seconds: 300,
        }
    }

    pub fn with_meilisearch(mut self) -> Self {
        self.meilisearch = true;
        self
    }

    pub fn with_kafka(mut self) -> Self {
        self.kafka = true;
        self
    }

    pub fn with_ttl(mut self, ttl_seconds: u64) -> Self {
        self.ttl_seconds = ttl_seconds;
        self
    }

    pub async fn build(self) -> Result<TestEnvironment> {
        let id = &uuid::Uuid::new_v4().to_string()[..8];
        let namespace = format!("{}-{}", self.test_name, id);

        let kube_client = Client::try_default().await?;

        let ns_api: Api<Namespace> = Api::all(kube_client.clone());
        let mut labels: BTreeMap<String, String> = BTreeMap::new();
        labels.insert(
            "cleanup.kyverno.io/ttl".to_string(),
            format!("{}s", self.ttl_seconds),
        );

        let mut annotations: BTreeMap<String, String> = BTreeMap::new();
        annotations.insert(
            "cleanup.kyverno.io/propagation-policy".to_string(),
            "Foreground".to_string(),
        );

        ns_api
            .create(
                &PostParams::default(),
                &Namespace {
                    metadata: ObjectMeta {
                        name: Some(namespace.clone()),
                        labels: Some(labels),
                        annotations: Some(annotations),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await?;

        let meilisearch_client = if self.meilisearch {
            Some(MeilisearchClient::new(MEILISEARCH_URL, None::<&str>)?)
        } else {
            None
        };

        let kafka_producer = if self.kafka {
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", KAFKA_BOOTSTRAP)
                .set("message.timeout.ms", "5000")
                .create()?;
            Some(KafkaProducer { inner: producer })
        } else {
            None
        };

        Ok(TestEnvironment {
            namespace,
            kube_client,
            meilisearch_client,
            kafka_producer,
        })
    }
}

pub struct TestEnvironment {
    pub namespace: String,
    kube_client: Client,
    meilisearch_client: Option<MeilisearchClient>,
    kafka_producer: Option<KafkaProducer>,
}

impl TestEnvironment {
    pub fn kube_client(&self) -> &Client {
        &self.kube_client
    }

    pub fn meilisearch(&self) -> &MeilisearchClient {
        self.meilisearch_client
            .as_ref()
            .expect("Meilisearch client not configured. Call with_meilisearch() in builder.")
    }

    pub fn kafka(&self) -> &KafkaProducer {
        self.kafka_producer
            .as_ref()
            .expect("Kafka producer not configured. Call with_kafka() in builder.")
    }
}

pub struct KafkaProducer {
    inner: FutureProducer,
}

impl KafkaProducer {
    pub async fn produce_all(&self, topic: &str, docs: &[serde_json::Value]) -> Result<()> {
        for (i, doc) in docs.iter().enumerate() {
            let payload = serde_json::to_string(doc)?;
            self.inner
                .send(
                    FutureRecord::to(topic)
                        .payload(&payload)
                        .key(&(i + 1).to_string()),
                    Duration::from_secs(5),
                )
                .await
                .map_err(|(e, _)| anyhow::anyhow!("Failed to send: {:?}", e))?;
        }
        Ok(())
    }
}
