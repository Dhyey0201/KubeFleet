#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
bash "$ROOT/scripts/guard.sh"
export E2E_EVIDENCE="$ROOT/evidence/e2e"
mkdir -p "$E2E_EVIDENCE"
go test -tags=integration -count=1 -v -timeout=25m ./test/e2e "$@" 2>&1 | tee "$E2E_EVIDENCE/test.log"
