#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
bash "$ROOT/scripts/guard.sh"
kind delete cluster --name kubefleet
