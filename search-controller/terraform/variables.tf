variable "cluster_name" {
  description = "Name of the KIND cluster"
  type        = string
  default     = "search-operator"
}

variable "kafka_namespace" {
  description = "Kubernetes namespace for Kafka resources"
  type        = string
  default     = "kafka"
}

variable "strimzi_version" {
  description = "Strimzi Kafka operator Helm chart version"
  type        = string
  default     = "0.48.0"
}

variable "kafka_version" {
  description = "Kafka version to deploy"
  type        = string
  default     = "4.1.0"
}

variable "kafka_bootstrap_nodeport" {
  description = "NodePort for Kafka bootstrap service"
  type        = number
  default     = 30092
}

variable "kafka_broker_nodeport" {
  description = "NodePort for Kafka broker 0"
  type        = number
  default     = 30093
}

variable "kafka_cluster_name" {
  description = "Name of the Kafka cluster"
  type        = string
  default     = "my-cluster"
}

variable "kyverno_version" {
  description = "Kyverno Helm chart version"
  type        = string
  default     = "3.6.0"
}

variable "meilisearch_namespace" {
  description = "Kubernetes namespace for Meilisearch"
  type        = string
  default     = "meilisearch"
}

variable "meilisearch_nodeport" {
  description = "NodePort for Meilisearch service"
  type        = number
  default     = 30700
}

variable "meilisearch_version" {
  description = "Meilisearch Helm chart version"
  type        = string
  default     = "0.17.1"
}
