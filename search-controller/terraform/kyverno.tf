resource "kubernetes_namespace" "kyverno" {
  metadata {
    name = "kyverno"
  }

  depends_on = [kind_cluster.default]
}

resource "helm_release" "kyverno" {
  name       = "kyverno"
  repository = "https://kyverno.github.io/kyverno/"
  chart      = "kyverno"
  version    = var.kyverno_version
  namespace  = kubernetes_namespace.kyverno.metadata[0].name
  wait       = true
  timeout    = 90

  depends_on = [kubernetes_namespace.kyverno]
}

# Apply RBAC for Kyverno cleanup controller to delete namespaces
resource "terraform_data" "kyverno_rbac" {
  depends_on = [helm_release.kyverno]

  provisioner "local-exec" {
    command = "kubectl apply -f ${path.module}/kyverno-rbac.yaml"
  }

  provisioner "local-exec" {
    when    = destroy
    command = "kubectl delete -f ${path.module}/kyverno-rbac.yaml --ignore-not-found=true"
  }

  input = filemd5("${path.module}/kyverno-rbac.yaml")
}
