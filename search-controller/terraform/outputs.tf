output "cluster_name" {
  description = "Name of the KIND cluster"
  value       = kind_cluster.default.name
}

output "kubeconfig_path" {
  description = "Path to the kubeconfig file"
  value       = kind_cluster.default.kubeconfig_path
}

output "cluster_endpoint" {
  description = "Kubernetes API server endpoint"
  value       = kind_cluster.default.endpoint
}


# Output the bootstrap server address
output "kafka_bootstrap_server" {
  description = "Kafka bootstrap server address (external NodePort)"
  value       = "localhost:${var.kafka_bootstrap_nodeport}"
  depends_on  = [terraform_data.kafka_cluster]
}

output "kafka_bootstrap_server_internal" {
  description = "Kafka bootstrap server address (internal)"
  value       = "${var.kafka_cluster_name}-kafka-bootstrap.${var.kafka_namespace}.svc.cluster.local:9092"
  depends_on  = [terraform_data.kafka_cluster]
}

output "meilisearch_url" {
  description = "Meilisearch URL (external NodePort)"
  value       = "http://localhost:${var.meilisearch_nodeport}"
  depends_on  = [helm_release.meilisearch]
}

output "meilisearch_url_internal" {
  description = "Meilisearch URL (internal)"
  value       = "http://meilisearch.${var.meilisearch_namespace}.svc.cluster.local:7700"
  depends_on  = [helm_release.meilisearch]
}
