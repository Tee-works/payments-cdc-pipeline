#!/usr/bin/env bash
# Run the whole pipeline on a local Kubernetes cluster (k3d), deployed the way
# it would be in production: Strimzi runs Kafka and Kafka Connect, Terraform
# installs the Helm chart, and the chart creates the Debezium connectors.
#
# Needs Docker, k3d, kubectl, helm and terraform. Costs nothing; delete it all
# with deploy/local/down.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
HERE="$ROOT/deploy/local"
CLUSTER=payments-cdc
NS=payments-cdc
CTX="k3d-$CLUSTER"
STRIMZI_VERSION=0.43.0
K3S_IMAGE=rancher/k3s:v1.31.5-k3s1
APP_IMAGE=ghcr.io/tee-works/payments-cdc-pipeline:0.1.0
CONNECT_IMAGE=payments-connect:local
TF=(terraform -chdir="$ROOT/deploy/terraform")
TFVARS=(-var-file="$HERE/local.tfvars")
k() { kubectl --context "$CTX" -n "$NS" "$@"; }

step() { printf '\n==> %s\n' "$*"; }

# Credentials come from .env (created from .env.example if missing).
"$ROOT/scripts/init_env.sh"
set -a; . "$ROOT/.env"; set +a
export TF_VAR_postgres_username="$PG_USER" TF_VAR_postgres_password="$PG_PASSWORD"

secret() {  # secret <name> <username> <password>, created or updated in place
  k create secret generic "$1" --from-literal=username="$2" --from-literal=password="$3" \
    --dry-run=client -o yaml | k apply -f -
}

step "Cluster"
if ! k3d cluster list "$CLUSTER" > /dev/null 2>&1; then
  # Traefik and metrics-server are not needed and cost memory on a laptop.
  # Kubernetes is pinned: Strimzi 0.4x clients cannot parse the version
  # response of Kubernetes 1.33+ and the operator crash-loops.
  k3d cluster create "$CLUSTER" --agents 0 --wait --image "$K3S_IMAGE" \
    --k3s-arg "--disable=traefik@server:0" \
    --k3s-arg "--disable=metrics-server@server:0"
fi

step "Images (built locally, imported into the cluster; no registry needed)"
docker build -q -t "$APP_IMAGE" "$ROOT"
docker build -q -t "$CONNECT_IMAGE" -f "$HERE/connect.Dockerfile" "$HERE"
k3d image import -c "$CLUSTER" "$APP_IMAGE" "$CONNECT_IMAGE"

step "Terraform: namespace and warehouse secret"
"${TF[@]}" init -input=false > /dev/null
# Kafka and the databases need the namespace and secret before the chart can
# be installed, so create those two resources first.
"${TF[@]}" apply -input=false -auto-approve "${TFVARS[@]}" \
  -target=kubernetes_namespace.cdc -target=kubernetes_secret.db

step "Strimzi operator"
helm upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator \
  --kube-context "$CTX" --namespace strimzi --create-namespace \
  --version "$STRIMZI_VERSION" --set "watchNamespaces={$NS}" --wait

step "MySQL source and Postgres warehouse"
secret payments-mysql-root root "$MYSQL_ROOT_PASSWORD"
secret payments-mysql-debezium debezium "$MYSQL_DEBEZIUM_PASSWORD"
secret payments-mysql-app app "$MYSQL_PASSWORD"
k create configmap payments-mysql-init --from-file="$ROOT/mysql/init" \
  --dry-run=client -o yaml | k apply -f -
k create configmap warehouse-postgres-init --from-file="$ROOT/postgres/init" \
  --dry-run=client -o yaml | k apply -f -
k apply -f "$HERE/databases.yaml"
k rollout status deploy/payments-mysql --timeout=300s
k rollout status deploy/warehouse-postgres --timeout=300s

step "Kafka and Kafka Connect"
k apply -f "$HERE/kafka.yaml"
k wait kafka/payments --for=condition=Ready --timeout=600s
k wait kafkaconnect/payments-connect --for=condition=Ready --timeout=600s

step "Terraform: Helm release (consumer, connectors, reconcile CronJob)"
"${TF[@]}" apply -input=false -auto-approve "${TFVARS[@]}"
k rollout status deploy/payments-cdc-payments-cdc --timeout=300s
k wait kafkaconnector/mysql-source --for=condition=Ready --timeout=300s
k wait kafkaconnector/postgres-sink --for=condition=Ready --timeout=300s
# Ready only means the connector was accepted; a task can still be dead.
for c in mysql-source postgres-sink; do
  for i in $(seq 1 60); do
    tasks="$(k get kafkaconnector "$c" -o jsonpath='{.status.connectorStatus.tasks[*].state}')"
    [ "$tasks" = "RUNNING" ] && { echo "$c: task RUNNING"; break; }
    [ "$i" -eq 60 ] && { echo "$c tasks: ${tasks:-none}" >&2; exit 1; }
    sleep 3
  done
done

step "Running"
k get pods
k get kafkaconnectors
echo
echo "Generate traffic and reconcile: deploy/local/demo.sh"
