resource "helm_release" "strimzi" {
  name       = "strimzi-kafka-operator"
  repository = "https://strimzi.io/charts/"
  chart      = "strimzi-kafka-operator"
  version    = var.strimzi_version
  namespace  = var.kafka_namespace
  wait       = true
  timeout    = 100
  depends_on = [kubernetes_namespace.kafka]
}
