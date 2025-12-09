mod common;

use std::time::Duration;

use anyhow::Result;
use k8s_openapi::api::apps::v1::Deployment;
use kube::api::{Api, PostParams};
use search_controller::SearchIndex;
use serde_json::json;

use common::TestSetupBuilder;

#[tokio::test]
async fn test_search_index_e2e() -> Result<()> {
    let env = TestSetupBuilder::new("e2e-test")
        .with_meilisearch()
        .with_kafka()
        .with_ttl(60)
        .build()
        .await?;

    let api: Api<SearchIndex> = Api::namespaced(env.kube_client().clone(), &env.namespace);
    api.create(&PostParams::default(), &search_index(&env.namespace))
        .await?;
    wait_for_ready(&api, "products").await?;

    // Wait for consumer deployment to be ready
    let deploy_api: Api<Deployment> = Api::namespaced(env.kube_client().clone(), &env.namespace);
    wait_for_deployment_ready(&deploy_api, "products-consumer").await?;

    let docs = vec![
        json!({"id": "1", "title": "Rust Programming", "content": "Learn Rust language"}),
        json!({"id": "2", "title": "Kubernetes Guide", "content": "Deploy containers"}),
        json!({"id": "3", "title": "Search Engines", "content": "Full text search"}),
    ];
    let topic_name = format!("{}-products-ingest", env.namespace);
    env.kafka().produce_all(&topic_name, &docs).await?;

    // Poll until documents appear in MeiliSearch (consumer needs time to start and process)
    let mut results = None;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let search_result = env
            .meilisearch()
            .index("products")
            .search()
            .with_query("Rust")
            .execute::<serde_json::Value>()
            .await;

        match search_result {
            Ok(search) if !search.hits.is_empty() => {
                results = Some(search);
                break;
            }
            _ => continue, // Index not found or empty results, keep polling
        }
    }

    let results = results.expect("Documents not indexed within 60 seconds");
    assert_eq!(results.hits.len(), 1);
    assert_eq!(results.hits[0].result["title"], "Rust Programming");

    Ok(())
}

fn search_index(namespace: &str) -> SearchIndex {
    serde_yaml::from_str(&format!(
        r#"
apiVersion: stratum.dev/v1
kind: SearchIndex
metadata:
  name: products
  namespace: {namespace}
spec:
  kafka:
    partitions: 1
    replicas: 1
  index:
    fields:
      - name: title
        searchable: true
      - name: content
        searchable: true
"#
    ))
    .expect("Invalid SearchIndex YAML")
}

async fn wait_for_ready(api: &Api<SearchIndex>, name: &str) -> Result<()> {
    for _ in 0..60 {
        if let Ok(idx) = api.get(name).await
            && idx.is_ready()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(anyhow::anyhow!(
        "Timeout waiting for SearchIndex to be Ready"
    ))
}

async fn wait_for_deployment_ready(api: &Api<Deployment>, name: &str) -> Result<()> {
    for _ in 0..60 {
        if let Ok(deploy) = api.get(name).await
            && let Some(status) = &deploy.status
            && status.ready_replicas.unwrap_or(0) >= 1
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(anyhow::anyhow!(
        "Timeout waiting for Deployment {} to be ready",
        name
    ))
}
