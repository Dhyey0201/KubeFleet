#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
bash "$ROOT/scripts/guard.sh"
mkdir -p "$ROOT/bin" "$ROOT/evidence/compatibility"
CGO_ENABLED=0 GOOS=linux GOARCH="$(go env GOARCH)" go build -trimpath -o "$ROOT/bin/worker-linux" ./cmd/worker
docker build --provenance=false -f "$ROOT/Dockerfile.worker" -t kubefleet-worker:compat "$ROOT"
kind load docker-image --name kubefleet kubefleet-worker:compat
export SPIKE_EVIDENCE="$ROOT/evidence/compatibility"
go test -tags=integration -count=1 -v -timeout=20m ./test/compatibility/spike "$@" 2>&1 | tee "$SPIKE_EVIDENCE/test.log"
