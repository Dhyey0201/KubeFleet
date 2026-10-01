#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
bash "$ROOT/scripts/guard.sh"
cd "$ROOT"
mkdir -p bin evidence/development
CGO_ENABLED=0 GOOS=linux GOARCH="$(go env GOARCH)" go build -trimpath -o bin/manager-linux ./cmd/manager
docker build --provenance=false -f Dockerfile.manager -t kubefleet-manager:dev .
kind load docker-image --name kubefleet kubefleet-manager:dev
kubectl apply --server-side -f config/dependencies/cert-manager-v1.18.2.yaml
for deployment in cert-manager cert-manager-cainjector cert-manager-webhook; do
  kubectl -n cert-manager rollout status "deployment/$deployment" --timeout=180s
done
kubectl apply --server-side -f config/crd/bases
kubectl apply -f config/development/queues.yaml
kubectl apply -f config/development/operator.yaml
docker image inspect kubefleet-manager:dev --format '{{.Id}}' > .tools/manager-image-id.txt
python3 - <<'PYIMAGE'
import json
image_id=open('.tools/manager-image-id.txt').read().strip()
json.dump({'spec':{'template':{'metadata':{'annotations':{'batch.kubefleet.dev/image-id':image_id}}}}},open('.tools/manager-image-patch.json','w'))
PYIMAGE
kubectl -n kubefleet-system patch deployment kubefleet-manager --type=merge --patch-file .tools/manager-image-patch.json
kubectl -n kubefleet-system wait --for=condition=Ready certificate/kubefleet-webhook --timeout=120s
kubectl -n kubefleet-system rollout status deployment/kubefleet-manager --timeout=180s
kubectl apply -f config/webhook/validating.yaml
kubectl wait --for=jsonpath='{.webhooks[0].clientConfig.caBundle}' validatingwebhookconfiguration/kubefleet-batchrun-validation --timeout=60s
docker image inspect kubefleet-manager:dev --format '{{.Id}}' > evidence/development/manager-image-id.txt
kubectl get deployment -n kubefleet-system -o json > evidence/development/deployment.json

kubectl apply -f config/kueue/gate.yaml
kubectl wait --for=condition=Active admissioncheck/kubefleet-prerequisite --timeout=60s
for team in team-a team-b team-c; do
  kubectl wait --for=condition=Active "clusterqueue/$team" --timeout=60s
done

kubectl apply -f config/development/tenants.yaml
readiness_deadline=$((SECONDS+60))
until kubectl --request-timeout=5s create --dry-run=server -f config/development/admission-probe.yaml >/dev/null; do
  (( SECONDS < readiness_deadline )) || { echo 'TLS admission readiness deadline expired' >&2;exit 1; }
  sleep 1
done
