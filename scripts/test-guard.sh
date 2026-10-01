#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
fixture_dir=$(mktemp -d)
trap 'rm -rf "$fixture_dir"' EXIT
cat > "$fixture_dir/wrong.yaml" <<'YAML'
apiVersion: v1
kind: Config
current-context: unrelated-project
contexts:
- name: unrelated-project
  context: {cluster: unrelated-project, user: nobody}
clusters:
- name: unrelated-project
  cluster: {server: 'http://127.0.0.1:1'}
users:
- name: nobody
  user: {}
YAML
set +e
KUBEFLEET_KUBECONFIG="$fixture_dir/wrong.yaml" bash "$ROOT/scripts/cluster-down.sh" > "$fixture_dir/output" 2>&1
status=$?
set -e
[[ "$status" == 1 ]]
rg -q 'Wrong context; refusing mutation' "$fixture_dir/output"
echo 'Passed: destructive cluster cleanup refused a wrong context before any server/Docker call.'
