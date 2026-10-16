#!/usr/bin/env bash
# Delete the local cluster and the Terraform state that pointed at it.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
k3d cluster delete payments-cdc
rm -f "$ROOT"/deploy/terraform/terraform.tfstate*
