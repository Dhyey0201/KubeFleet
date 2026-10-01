#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
image='kindest/node:v1.33.1@sha256:050072256b9a903bd914c0b2866828150cb229cea0efe5892e2b644d5dd3b34f'
if kind get clusters | rg -qx kubefleet; then
  bash "$ROOT/scripts/guard.sh"
else
  kind create cluster --name kubefleet --config "$ROOT/config/kind/cluster.yaml" --image "$image" --wait 180s
  [[ "$(kubectl config current-context)" == kind-kubefleet ]]
  kubectl label namespace kube-system batch.kubefleet.dev/project=kubefleet
fi
bash "$ROOT/scripts/guard.sh"
kubectl apply --server-side -f "$ROOT/test/compatibility/kueue-v0.13.4.yaml"
kubectl -n kueue-system rollout status deployment/kueue-controller-manager --timeout=180s
kubectl get nodes -o wide
