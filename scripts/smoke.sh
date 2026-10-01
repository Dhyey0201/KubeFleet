#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
bash "$ROOT/scripts/guard.sh"
export SMOKE_EVIDENCE="$ROOT/evidence/smoke"
mkdir -p "$SMOKE_EVIDENCE"
go test -tags=integration -count=1 -v -timeout=8m ./test/smoke "$@" 2>&1 | tee "$SMOKE_EVIDENCE/test.log"
