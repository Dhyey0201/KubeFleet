#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
[[ -f "$KUBECONFIG" ]] || { echo 'Missing dedicated kubeconfig' >&2; exit 1; }
[[ "$(kubectl config current-context)" == kind-kubefleet ]] || { echo 'Wrong context; refusing mutation' >&2; exit 1; }
[[ "$(kubectl --request-timeout=10s get namespace kube-system -o jsonpath='{.metadata.labels.batch\.kubefleet\.dev/project}')" == kubefleet ]] || { echo 'Project cluster identity mismatch' >&2; exit 1; }
