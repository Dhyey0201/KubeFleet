#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
cd "$ROOT"
before=$(shasum -a 256 api/v1alpha1/zz_generated.deepcopy.go config/crd/bases/*.yaml)
bash scripts/generate.sh
after=$(shasum -a 256 api/v1alpha1/zz_generated.deepcopy.go config/crd/bases/*.yaml)
[[ "$before" == "$after" ]] || { echo 'Generated files changed; regenerate and review them.' >&2; exit 1; }
