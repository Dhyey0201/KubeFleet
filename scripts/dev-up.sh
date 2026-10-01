#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
cd "$ROOT"
make doctor images
bash scripts/cluster-up.sh
kind load docker-image --name kubefleet kubefleet-worker:compat
bash scripts/install-development.sh
bash scripts/monitoring.sh
make build
bash scripts/fault-image.sh
