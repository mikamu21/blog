resource "kind_cluster" "default" {
  name            = var.cluster_name
  wait_for_ready  = true
  kubeconfig_path = pathexpand("~/.kube/config")
  node_image      = "kindest/node:v1.32.8@sha256:abd489f042d2b644e2d033f5c2d900bc707798d075e8186cb65e3f1367a9d5a1"

  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"

    node {
      role = "control-plane"

      # Port mapping for Kafka bootstrap NodePort
      extra_port_mappings {
        container_port = var.kafka_bootstrap_nodeport
        host_port      = var.kafka_bootstrap_nodeport
        protocol       = "TCP"
      }

      # Port mapping for Kafka broker NodePort
      extra_port_mappings {
        container_port = var.kafka_broker_nodeport
        host_port      = var.kafka_broker_nodeport
        protocol       = "TCP"
      }
    }
  }

  # Increase inotify limits for containerd to prevent "too many open files" errors
  # See TROUBLESHOOTING.md for details on why this is needed
  provisioner "local-exec" {
    command = <<-EOT
      docker exec ${var.cluster_name}-control-plane sysctl -w fs.inotify.max_user_instances=8192
      docker exec ${var.cluster_name}-control-plane sysctl -w fs.inotify.max_user_watches=524288
    EOT
  }
}
