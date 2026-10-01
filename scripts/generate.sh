#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
cd "$ROOT"
controller-gen object paths='./api/...'
controller-gen crd paths='./api/...' output:crd:artifacts:config=config/crd/bases
