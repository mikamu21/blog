use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use futures::StreamExt;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::Controller;
use kube::runtime::controller::Action;
use kube::runtime::finalizer::{Event, finalizer};
use kube::runtime::watcher::Config;
use kube::{Client, ResourceExt};
use serde_json::json;

use crate::resources::{MeilisearchClient, consumer_deployment, kafka_topic};
use crate::{Condition, SearchIndex, SearchIndexStatus};

const FINALIZER: &str = "searchindex.stratum.dev/cleanup";

pub struct Context {
    pub client: Client,
    pub meilisearch: MeilisearchClient,
    pub kafka_bootstrap: String,
    pub meilisearch_url: String,
    pub consumer_image: String,
}

pub async fn run(
    client: Client,
    meilisearch_url: &str,
    kafka_bootstrap: &str,
    consumer_image: &str,
) -> Result<()> {
    let api: Api<SearchIndex> = Api::all(client.clone());

    let ctx = Arc::new(Context {
        client: client.clone(),
        meilisearch: MeilisearchClient::new(meilisearch_url),
        kafka_bootstrap: kafka_bootstrap.to_string(),
        meilisearch_url: meilisearch_url.to_string(),
        consumer_image: consumer_image.to_string(),
    });

    Controller::new(api, Config::default())
        .shutdown_on_signal()
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => tracing::debug!("reconciled {:?}", o),
                Err(e) => tracing::error!("reconcile error: {:?}", e),
            }
        })
        .await;

    Ok(())
}

async fn reconcile(obj: Arc<SearchIndex>, ctx: Arc<Context>) -> Result<Action, kube::Error> {
    let name = obj.name_any();
    let namespace = obj.namespace().unwrap_or_default();
    let api: Api<SearchIndex> = Api::namespaced(ctx.client.clone(), &namespace);

    finalizer(&api, FINALIZER, obj, |event| async {
        match event {
            Event::Apply(obj) => apply(&obj, &ctx, &namespace, &name).await,
            Event::Cleanup(obj) => cleanup(&obj, &ctx, &name).await,
        }
    })
    .await
    .map_err(|e| kube::Error::Service(e.into()))
}

async fn apply(
    obj: &SearchIndex,
    ctx: &Context,
    namespace: &str,
    name: &str,
) -> Result<Action, kube::Error> {
    tracing::info!("Reconciling SearchIndex {}/{}", namespace, name);

    let topic_name = format!("{}-{}-ingest", namespace, name);
    let index_name = name.to_string();

    match reconcile_inner(obj, ctx, namespace, &topic_name, &index_name).await {
        Ok(()) => {
            update_status(&ctx.client, namespace, name, |status| {
                status.kafka_topic = Some(topic_name);
                status.meilisearch_index = Some(index_name);
                status.observed_generation = obj.metadata.generation;
                set_condition(
                    status,
                    "Ready",
                    "True",
                    "Reconciled",
                    "All resources created successfully",
                );
            })
            .await?;

            Ok(Action::requeue(Duration::from_secs(300)))
        }
        Err(e) => {
            tracing::error!("Reconcile error for {}/{}: {:?}", namespace, name, e);

            update_status(&ctx.client, namespace, name, |status| {
                set_condition(status, "Ready", "False", "ReconcileError", &e.to_string());
            })
            .await?;

            Ok(Action::requeue(Duration::from_secs(30)))
        }
    }
}

async fn cleanup(obj: &SearchIndex, ctx: &Context, name: &str) -> Result<Action, kube::Error> {
    let namespace = obj.namespace().unwrap_or_default();
    tracing::info!("Cleaning up SearchIndex {}/{}", namespace, name);

    let topic_name = format!("{}-{}-ingest", namespace, name);
    let index_name = name.to_string();

    if let Err(e) = kafka_topic::delete_kafka_topic(&ctx.client, &topic_name).await {
        tracing::warn!("Failed to delete KafkaTopic {}: {:?}", topic_name, e);
    }

    if let Err(e) = ctx.meilisearch.delete_index(&index_name).await {
        tracing::warn!("Failed to delete Meilisearch index {}: {:?}", index_name, e);
    }

    tracing::info!("Cleanup complete for SearchIndex {}", name);
    Ok(Action::await_change())
}

async fn reconcile_inner(
    obj: &SearchIndex,
    ctx: &Context,
    namespace: &str,
    topic_name: &str,
    index_name: &str,
) -> Result<()> {
    // Create Kafka topic with spec.kafka.* configuration
    kafka_topic::ensure_kafka_topic(
        &ctx.client,
        topic_name,
        obj.spec.kafka.partitions,
        obj.spec.kafka.replicas,
    )
    .await?;

    kafka_topic::wait_for_topic_ready(&ctx.client, topic_name).await?;

    // Create Meilisearch index with spec.index.* configuration
    ctx.meilisearch.create_index(index_name).await?;
    ctx.meilisearch
        .configure_index(index_name, &obj.spec.index.fields)
        .await?;

    // Create consumer Deployment to bridge Kafka -> Meilisearch
    let config = consumer_deployment::ConsumerConfig {
        kafka_bootstrap: &ctx.kafka_bootstrap,
        meilisearch_url: &ctx.meilisearch_url,
        consumer_image: &ctx.consumer_image,
    };
    consumer_deployment::ensure_consumer_deployment(
        &ctx.client,
        obj,
        namespace,
        topic_name,
        index_name,
        &config,
    )
    .await?;

    Ok(())
}

fn error_policy(_obj: Arc<SearchIndex>, _error: &kube::Error, _ctx: Arc<Context>) -> Action {
    Action::requeue(Duration::from_secs(30))
}

async fn update_status<F>(
    client: &Client,
    namespace: &str,
    name: &str,
    mutator: F,
) -> Result<(), kube::Error>
where
    F: FnOnce(&mut SearchIndexStatus),
{
    let api: Api<SearchIndex> = Api::namespaced(client.clone(), namespace);

    let existing = api.get(name).await?;
    let mut status = existing.status.unwrap_or_default();
    mutator(&mut status);

    let patch = json!({
        "status": status
    });

    api.patch_status(
        name,
        &PatchParams::apply("search-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    Ok(())
}

fn set_condition(
    status: &mut SearchIndexStatus,
    type_: &str,
    status_value: &str,
    reason: &str,
    message: &str,
) {
    let now = Utc::now().to_rfc3339();

    let new_condition = Condition {
        type_: type_.to_string(),
        status: status_value.to_string(),
        reason: Some(reason.to_string()),
        message: Some(message.to_string()),
        last_transition_time: Some(now),
    };

    let conditions = status.conditions.get_or_insert_with(Vec::new);

    if let Some(existing) = conditions.iter_mut().find(|c| c.type_ == type_) {
        if existing.status != status_value {
            *existing = new_condition;
        }
    } else {
        conditions.push(new_condition);
    }
}
