#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
bash "$ROOT/scripts/guard.sh"
cd "$ROOT"
bash scripts/render-check.sh
for profile in isolated shared priority; do
  kubectl --request-timeout=15s apply --dry-run=server -f "evidence/rendered/$profile.yaml"
done
kubectl --request-timeout=15s apply --dry-run=server -f evidence/rendered/monitoring.yaml
