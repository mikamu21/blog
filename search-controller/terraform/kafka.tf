resource "terraform_data" "kafka_cluster" {
  depends_on = [helm_release.strimzi]

  # Wait for Strimzi operator to be ready
  provisioner "local-exec" {
    command = <<-EOT
      kubectl wait deployment/strimzi-cluster-operator \
        --for=condition=Available \
        --timeout=100s \
        --namespace=${var.kafka_namespace}
    EOT
  }

  # Apply Kafka cluster
  provisioner "local-exec" {
    command = "kubectl apply -f ${path.module}/kafka.yaml"
  }

  # Wait for Kafka CR Ready condition (this validates everything)
  provisioner "local-exec" {
    command = <<-EOT
      kubectl wait kafka/${var.kafka_cluster_name} \
        --for=condition=Ready \
        --timeout=100s \
        --namespace=${var.kafka_namespace}

      echo "✓ Kafka cluster ready!"
    EOT
  }

  # Clean up on destroys
  provisioner "local-exec" {
    when    = destroy
    command = <<-EOT
      # Delete ALL KafkaTopics across all namespaces
      # This handles topics from any test namespace that might still exist
      kubectl delete kafkatopics --all --all-namespaces --ignore-not-found=true --timeout=60s

      # Then delete the Kafka cluster
      kubectl delete -f ${path.module}/kafka.yaml --ignore-not-found=true
    EOT
  }

  # Trigger re-creation if kafka.yaml changes
  input = filemd5("${path.module}/kafka.yaml")
}
