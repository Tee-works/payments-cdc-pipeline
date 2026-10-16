output "namespace" {
  value = kubernetes_namespace.cdc.metadata[0].name
}

output "release" {
  value = helm_release.payments_cdc.name
}
