use std::collections::BTreeMap;

use anyhow::Result;
use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec};
use k8s_openapi::api::core::v1::{Container, EnvVar, PodSpec, PodTemplateSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use kube::api::{Api, PostParams};
use kube::{Client, ResourceExt};

use crate::SearchIndex;

pub struct ConsumerConfig<'a> {
    pub kafka_bootstrap: &'a str,
    pub meilisearch_url: &'a str,
    pub consumer_image: &'a str,
}

pub async fn ensure_consumer_deployment(
    client: &Client,
    search_index: &SearchIndex,
    namespace: &str,
    topic_name: &str,
    index_name: &str,
    config: &ConsumerConfig<'_>,
) -> Result<()> {
    let name = format!("{}-consumer", search_index.name_any());
    let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);

    if api.get_opt(&name).await?.is_some() {
        tracing::info!("Consumer Deployment {} already exists", name);
        return Ok(());
    }

    let labels: BTreeMap<String, String> = [
        ("app".to_string(), "search-consumer".to_string()),
        (
            "stratum.dev/search-index".to_string(),
            search_index.name_any(),
        ),
    ]
    .into();

    let deployment = Deployment {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: Some(namespace.to_string()),
            owner_references: Some(vec![OwnerReference {
                api_version: "stratum.dev/v1".to_string(),
                kind: "SearchIndex".to_string(),
                name: search_index.name_any(),
                uid: search_index.uid().unwrap_or_default(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "consumer".to_string(),
                        image: Some(config.consumer_image.to_string()),
                        image_pull_policy: Some("IfNotPresent".to_string()),
                        command: Some(vec!["/usr/local/bin/consumer".to_string()]),
                        env: Some(vec![
                            EnvVar {
                                name: "KAFKA_TOPIC".to_string(),
                                value: Some(topic_name.to_string()),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "MEILISEARCH_INDEX".to_string(),
                                value: Some(index_name.to_string()),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "KAFKA_BOOTSTRAP_SERVERS".to_string(),
                                value: Some(config.kafka_bootstrap.to_string()),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "MEILISEARCH_URL".to_string(),
                                value: Some(config.meilisearch_url.to_string()),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "BATCH_SIZE".to_string(),
                                value: Some(search_index.spec.connector.batch_size.to_string()),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "BATCH_TIMEOUT_MS".to_string(),
                                value: Some(
                                    search_index.spec.connector.batch_timeout_ms.to_string(),
                                ),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "KAFKA_GROUP_ID".to_string(),
                                value: Some(format!("{}-consumer", search_index.name_any())),
                                ..Default::default()
                            },
                        ]),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    };

    api.create(&PostParams::default(), &deployment).await?;
    tracing::info!("Created consumer Deployment {}", name);

    Ok(())
}
