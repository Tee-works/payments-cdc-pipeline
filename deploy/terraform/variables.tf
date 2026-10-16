variable "kubeconfig_path" {
  description = "Path to the kubeconfig for the target cluster."
  type        = string
  default     = "~/.kube/config"
}

variable "kube_context" {
  description = "Kubeconfig context to deploy into."
  type        = string
  default     = null
}

variable "namespace" {
  description = "Namespace for the CDC components."
  type        = string
  default     = "payments-cdc"
}

variable "image_tag" {
  description = "Consumer image tag to deploy."
  type        = string
  default     = "0.1.0"
}

variable "kafka_bootstrap_servers" {
  description = "Kafka bootstrap address reachable from the cluster."
  type        = string
}

variable "postgres_host" {
  description = "Warehouse Postgres host."
  type        = string
}

variable "postgres_username" {
  description = "Warehouse user for the consumer."
  type        = string
  sensitive   = true
}

variable "postgres_password" {
  description = "Warehouse password for the consumer."
  type        = string
  sensitive   = true
}

variable "enable_strimzi_connectors" {
  description = "Create Strimzi KafkaConnector resources for the Debezium source and JDBC sink."
  type        = bool
  default     = false
}

variable "enable_reconcile_cronjob" {
  description = "Run the daily source-vs-warehouse reconciliation as a Kubernetes CronJob."
  type        = bool
  default     = false
}
