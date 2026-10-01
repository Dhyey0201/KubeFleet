#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
bash "$ROOT/scripts/guard.sh"
cd "$ROOT"
CGO_ENABLED=0 GOOS=linux GOARCH="$(go env GOARCH)" go build -tags=faultinject -trimpath -o bin/manager-linux ./cmd/manager
docker build --provenance=false -f Dockerfile.manager -t kubefleet-manager:faults .
kind load docker-image --name kubefleet kubefleet-manager:faults
