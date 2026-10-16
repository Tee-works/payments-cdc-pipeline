# Terraform inputs for the local k3d cluster that deploy/local/up.sh creates.
# The warehouse credentials are not here: up.sh passes them from .env as
# TF_VAR_postgres_username and TF_VAR_postgres_password.
kube_context              = "k3d-payments-cdc"
namespace                 = "payments-cdc"
image_tag                 = "0.1.0"
kafka_bootstrap_servers   = "payments-kafka-bootstrap:9092"
postgres_host             = "warehouse-postgres"
enable_strimzi_connectors = true
enable_reconcile_cronjob  = true
