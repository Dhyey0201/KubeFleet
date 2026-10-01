#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
bash "$ROOT/scripts/guard.sh"
cd "$ROOT"
kubectl create namespace kubefleet-monitoring --dry-run=client -o yaml | kubectl apply -f -
if ! kubectl -n kubefleet-monitoring get secret grafana-auth >/dev/null 2>&1; then
  password_file=$(mktemp)
  chmod 600 "$password_file"
  trap 'rm -f "$password_file"' EXIT
  python3 - "$password_file" <<'PY'
import secrets,sys
open(sys.argv[1],'w').write(secrets.token_urlsafe(32))
PY
  kubectl -n kubefleet-monitoring create secret generic grafana-auth --from-file="password=$password_file"
fi
kubectl apply -f config/monitoring/resources.yaml
for name in prometheus grafana kube-state-metrics; do
  kubectl -n kubefleet-monitoring rollout status "deployment/$name" --timeout=180s
done
