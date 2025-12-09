resource "kubernetes_namespace" "kafka" {
  metadata {
    name = var.kafka_namespace
  }
  depends_on = [kind_cluster.default]
}
