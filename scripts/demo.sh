#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
bash "$ROOT/scripts/guard.sh"
cd "$ROOT"
make build
bash scripts/smoke.sh
bash scripts/e2e.sh -run 'TestM3Profiles|TestM4PrerequisiteGate|TestM5CLI'
