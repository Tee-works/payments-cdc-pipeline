#!/usr/bin/env bash
# Push traffic through the pipeline running on k3d, then reconcile and show
# what the consumer caught. Same steps as `make demo`, run as Kubernetes Jobs.
set -euo pipefail

NS=payments-cdc
CTX=k3d-payments-cdc
IMAGE=ghcr.io/tee-works/payments-cdc-pipeline:0.1.0
TRANSACTIONS="${TRANSACTIONS:-500}"
ANOMALIES="${ANOMALIES:-5}"
k() { kubectl --context "$CTX" -n "$NS" "$@"; }

# Run one cdc module as a Job with the app's MySQL and warehouse credentials.
run_job() {
  local name="$1"; shift
  local args; args="$(printf '"%s",' "$@")"
  k delete job "$name" --ignore-not-found > /dev/null
  k apply -f - > /dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $name
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      securityContext: {runAsNonRoot: true, runAsUser: 1000}
      containers:
        - name: $name
          image: $IMAGE
          command: ["python", "-m", ${args%,}]
          envFrom:
            - configMapRef: {name: payments-cdc-payments-cdc}
          env:
            - {name: MYSQL_HOST, value: payments-mysql}
            - {name: MYSQL_USER, valueFrom: {secretKeyRef: {name: payments-mysql-app, key: username}}}
            - {name: MYSQL_PASSWORD, valueFrom: {secretKeyRef: {name: payments-mysql-app, key: password}}}
            - {name: PG_USER, valueFrom: {secretKeyRef: {name: payments-cdc-db, key: username}}}
            - {name: PG_PASSWORD, valueFrom: {secretKeyRef: {name: payments-cdc-db, key: password}}}
EOF
  local done=""
  for _ in $(seq 1 300); do
    done="$(k get job "$name" -o jsonpath='{.status.succeeded}{.status.failed}')"
    [ -n "$done" ] && break
    sleep 2
  done
  k logs "job/$name"
  [ "$(k get job "$name" -o jsonpath='{.status.succeeded}')" = "1" ]
}

psql() { k exec deploy/warehouse-postgres -- sh -c 'psql -U "$POSTGRES_USER" -d warehouse -c "$1"' _ "$1"; }

echo "==> Generating $TRANSACTIONS transactions with $ANOMALIES illegal changes"
run_job simulate cdc.simulate --transactions "$TRANSACTIONS" --anomalies "$ANOMALIES"

echo "==> Reconciling MySQL against the warehouse for today"
run_job reconcile-now cdc.reconcile --today --wait-seconds 180

echo "==> Violations flagged by the consumer"
psql "SELECT rule, COUNT(*) FROM cdc.transition_violations GROUP BY rule ORDER BY rule"

echo "==> Status history rows"
psql "SELECT COUNT(*) AS history_rows, COUNT(DISTINCT transaction_id) AS transactions FROM cdc.transaction_status_history"
