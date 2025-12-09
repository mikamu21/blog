resource "helm_release" "meilisearch" {
  name       = "meilisearch"
  repository = "https://meilisearch.github.io/meilisearch-kubernetes"
  chart      = "meilisearch"
  version    = var.meilisearch_version
  namespace  = var.meilisearch_namespace

  set {
    name  = "environment.MEILI_ENV"
    value = "development"
  }

  set {
    name  = "service.type"
    value = "NodePort"
  }

  set {
    name  = "service.nodePort"
    value = var.meilisearch_nodeport
  }

  depends_on = [kubernetes_namespace.meilisearch]
}
