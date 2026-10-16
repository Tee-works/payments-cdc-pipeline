# Deploys the CDC consumer (and optionally the Strimzi connectors) into an
# existing Kubernetes cluster. Kafka, Kafka Connect, MySQL and Postgres are
# assumed to exist already; in most teams they have their own modules.

provider "kubernetes" {
  config_path    = pathexpand(var.kubeconfig_path)
  config_context = var.kube_context
}

provider "helm" {
  kubernetes {
    config_path    = pathexpand(var.kubeconfig_path)
    config_context = var.kube_context
  }
}

resource "kubernetes_namespace" "cdc" {
  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/part-of" = "payments-cdc"
    }
  }
}

resource "kubernetes_secret" "db" {
  metadata {
    name      = "payments-cdc-db"
    namespace = kubernetes_namespace.cdc.metadata[0].name
  }

  data = {
    username = var.postgres_username
    password = var.postgres_password
  }
}

resource "helm_release" "payments_cdc" {
  name      = "payments-cdc"
  namespace = kubernetes_namespace.cdc.metadata[0].name
  chart     = "${path.module}/../helm/payments-cdc"

  values = [yamlencode({
    image = { tag = var.image_tag }
    kafka = { bootstrapServers = var.kafka_bootstrap_servers }
    postgres = {
      host           = var.postgres_host
      existingSecret = kubernetes_secret.db.metadata[0].name
    }
    strimzi   = { enabled = var.enable_strimzi_connectors }
    reconcile = { enabled = var.enable_reconcile_cronjob }
  })]
}
