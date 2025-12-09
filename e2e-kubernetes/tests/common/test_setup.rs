use anyhow::Result;
use k8s_openapi::api::core::v1::Namespace;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Client;
use kube::api::{Api, PostParams};
use uuid;

use super::kafka;

/// Internal specification for a Kafka topic to be created
#[derive(Clone)]
struct TopicSpec {
    name: String,
    partitions: i32,
    replicas: i32,
}

/// Builder for creating test environments with automatic TTL-based cleanup
pub struct TestSetupBuilder {
    test_name: String,
    kafka_topics: Vec<TopicSpec>,
    ttl_seconds: u64,
}

impl TestSetupBuilder {
    /// Create a new test setup builder with a test name prefix
    ///
    /// The namespace will be automatically cleaned up after the TTL expires (default: 300 seconds).
    pub fn new(test_name: &str) -> Self {
        Self {
            test_name: test_name.to_string(),
            kafka_topics: vec![],
            ttl_seconds: 300, // 5 minutes default
        }
    }

    /// Configure TTL for automatic cleanup (default: 300 seconds)
    ///
    /// The namespace will be deleted by Kyverno after this TTL expires,
    /// ensuring cleanup even if the test fails or panics.
    pub fn with_ttl(mut self, ttl_seconds: u64) -> Self {
        self.ttl_seconds = ttl_seconds;
        self
    }

    /// Add a Kafka topic to be created (Strimzi KafkaTopic CR)
    pub fn add_kafka_topic(mut self, name: &str, partitions: i32, replicas: i32) -> Self {
        self.kafka_topics.push(TopicSpec {
            name: name.to_string(),
            partitions,
            replicas,
        });
        self
    }

    /// Build the test environment and create all resources
    ///
    /// Creates a namespace with TTL labels/annotations for automatic cleanup,
    /// then creates all configured Kafka topics.
    pub async fn build(self) -> Result<TestEnvironment> {
        // Generate unique namespace name with UUID
        let id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let namespace_name = format!("{}-{}", self.test_name, id);

        // Create Kubernetes client
        let kube_client = Client::try_default().await?;

        // Create the namespace with TTL labels/annotations for automatic cleanup
        let ns_api: Api<Namespace> = Api::all(kube_client.clone());
        ns_api
            .create(
                &PostParams::default(),
                &Namespace {
                    metadata: ObjectMeta {
                        name: Some(namespace_name.clone()),
                        labels: Some({
                            let mut labels = std::collections::BTreeMap::new();
                            labels.insert(
                                "cleanup.kyverno.io/ttl".to_string(),
                                format!("{}s", self.ttl_seconds),
                            );
                            labels
                        }),
                        annotations: Some({
                            let mut annotations = std::collections::BTreeMap::new();
                            annotations.insert(
                                "cleanup.kyverno.io/propagation-policy".to_string(),
                                "Foreground".to_string(),
                            );
                            annotations
                        }),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await?;

        // Create Kafka topics (Strimzi KafkaTopic CRs in kafka namespace)
        let mut created_topics = vec![];
        for topic_spec in &self.kafka_topics {
            kafka::create_kafka_topic(
                &kube_client,
                &topic_spec.name,
                topic_spec.partitions,
                topic_spec.replicas,
            )
            .await?;
            kafka::wait_for_topic_ready(&kube_client, &topic_spec.name).await?;
            created_topics.push(topic_spec.name.clone());
        }

        Ok(TestEnvironment {
            kube_client,
            namespace_name,
            created_topics,
        })
    }
}

/// Test environment with automatic TTL-based cleanup
///
/// The namespace will be automatically deleted by Kyverno after the TTL expires.
/// No explicit cleanup is required, even if the test fails or panics.
#[allow(dead_code)]
pub struct TestEnvironment {
    pub kube_client: Client,
    pub namespace_name: String,
    pub created_topics: Vec<String>,
}
