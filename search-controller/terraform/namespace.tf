resource "kubernetes_namespace" "kafka" {
  metadata {
    name = var.kafka_namespace
  }
  depends_on = [kind_cluster.default]
}

resource "kubernetes_namespace" "meilisearch" {
  metadata {
    name = var.meilisearch_namespace
  }
  depends_on = [kind_cluster.default]
}
