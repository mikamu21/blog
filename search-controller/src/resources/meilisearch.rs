use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::FieldSpec;

pub struct MeilisearchClient {
    client: reqwest::Client,
    base_url: String,
}

#[derive(Debug, Serialize)]
struct CreateIndexRequest {
    uid: String,
    #[serde(rename = "primaryKey")]
    primary_key: Option<String>,
}

#[derive(Debug, Serialize)]
struct IndexSettings {
    #[serde(rename = "searchableAttributes")]
    searchable_attributes: Vec<String>,
    #[serde(rename = "filterableAttributes")]
    filterable_attributes: Vec<String>,
    #[serde(rename = "sortableAttributes")]
    sortable_attributes: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct TaskResponse {
    #[serde(rename = "taskUid")]
    task_uid: u64,
}

#[derive(Debug, Deserialize)]
struct TaskStatus {
    status: String,
}

impl MeilisearchClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    pub async fn health(&self) -> Result<bool> {
        let resp = self
            .client
            .get(format!("{}/health", self.base_url))
            .send()
            .await?;

        Ok(resp.status().is_success())
    }

    pub async fn index_exists(&self, uid: &str) -> Result<bool> {
        let resp = self
            .client
            .get(format!("{}/indexes/{}", self.base_url, uid))
            .send()
            .await?;

        Ok(resp.status().is_success())
    }

    pub async fn create_index(&self, uid: &str) -> Result<()> {
        if self.index_exists(uid).await? {
            tracing::info!("Meilisearch index {} already exists", uid);
            return Ok(());
        }

        let req = CreateIndexRequest {
            uid: uid.to_string(),
            primary_key: Some("id".to_string()),
        };

        let resp = self
            .client
            .post(format!("{}/indexes", self.base_url))
            .json(&req)
            .send()
            .await?;

        if !resp.status().is_success() {
            let body = resp.text().await?;
            return Err(anyhow::anyhow!("Failed to create index: {}", body));
        }

        let task: TaskResponse = resp.json().await?;
        self.wait_for_task(task.task_uid).await?;

        tracing::info!("Created Meilisearch index {}", uid);
        Ok(())
    }

    pub async fn configure_index(&self, uid: &str, fields: &[FieldSpec]) -> Result<()> {
        let searchable: Vec<String> = fields
            .iter()
            .filter(|f| f.searchable)
            .map(|f| f.name.clone())
            .collect();

        let filterable: Vec<String> = fields
            .iter()
            .filter(|f| f.filterable)
            .map(|f| f.name.clone())
            .collect();

        let sortable: Vec<String> = fields
            .iter()
            .filter(|f| f.sortable)
            .map(|f| f.name.clone())
            .collect();

        let settings = IndexSettings {
            searchable_attributes: if searchable.is_empty() {
                vec!["*".to_string()]
            } else {
                searchable
            },
            filterable_attributes: filterable,
            sortable_attributes: sortable,
        };

        let resp = self
            .client
            .patch(format!("{}/indexes/{}/settings", self.base_url, uid))
            .json(&settings)
            .send()
            .await?;

        if !resp.status().is_success() {
            let body = resp.text().await?;
            return Err(anyhow::anyhow!("Failed to configure index: {}", body));
        }

        let task: TaskResponse = resp.json().await?;
        self.wait_for_task(task.task_uid).await?;

        tracing::info!("Configured Meilisearch index {} settings", uid);
        Ok(())
    }

    pub async fn delete_index(&self, uid: &str) -> Result<()> {
        if !self.index_exists(uid).await? {
            return Ok(());
        }

        let resp = self
            .client
            .delete(format!("{}/indexes/{}", self.base_url, uid))
            .send()
            .await?;

        if !resp.status().is_success() {
            let body = resp.text().await?;
            return Err(anyhow::anyhow!("Failed to delete index: {}", body));
        }

        let task: TaskResponse = resp.json().await?;
        self.wait_for_task(task.task_uid).await?;

        tracing::info!("Deleted Meilisearch index {}", uid);
        Ok(())
    }

    async fn wait_for_task(&self, task_uid: u64) -> Result<()> {
        for _ in 0..60 {
            let resp = self
                .client
                .get(format!("{}/tasks/{}", self.base_url, task_uid))
                .send()
                .await?;

            if resp.status().is_success() {
                let status: TaskStatus = resp.json().await?;
                match status.status.as_str() {
                    "succeeded" => return Ok(()),
                    "failed" => {
                        return Err(anyhow::anyhow!("Meilisearch task {} failed", task_uid));
                    }
                    _ => {}
                }
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        Err(anyhow::anyhow!(
            "Timeout waiting for Meilisearch task {}",
            task_uid
        ))
    }
}
