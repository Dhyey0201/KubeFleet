#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
export KUBEBUILDER_ASSETS="$ROOT/.tools/controller-tools/envtest"
export ENVTEST_EVIDENCE="$ROOT/evidence/envtest"
mkdir -p "$ENVTEST_EVIDENCE"
go test -tags=envtest -race -count=1 -v -timeout=5m ./test/envtest "$@" 2>&1 | tee "$ENVTEST_EVIDENCE/test.log"
