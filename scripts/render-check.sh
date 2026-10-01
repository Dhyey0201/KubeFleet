#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
cd "$ROOT"
mkdir -p evidence/rendered
for profile in isolated shared priority;do
  kubectl kustomize "config/kueue/profiles/$profile" > "evidence/rendered/$profile.yaml"
done
cp config/monitoring/resources.yaml evidence/rendered/monitoring.yaml
shasum -a 256 evidence/rendered/*.yaml > evidence/rendered/checksums.txt
