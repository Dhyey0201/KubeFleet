#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
failed=0
python3 - "$ROOT" <<'PYDISK' || failed=1
import shutil,sys
free=shutil.disk_usage(sys.argv[1]).free
print('HostFreeDiskBytes='+str(free))
if free<8*1024**3:raise SystemExit('Local build/image/test profile requires >=8 GiB free disk; free space before continuing heavy work.')
PYDISK
for tool in go kind kubectl docker; do
  if ! command -v "$tool"; then failed=1; fi
done
if command -v go >/dev/null; then go version; fi
if command -v kind >/dev/null; then kind version; fi
kubectl version --client -o yaml || failed=1
python3 - <<'PYDOCKER' || failed=1
import subprocess
try:
    result=subprocess.run(['docker','info','--format','Docker={{.ServerVersion}} CPUs={{.NCPU}} MemoryBytes={{.MemTotal}}'],timeout=10)
except subprocess.TimeoutExpired:
    raise SystemExit('Docker health check timed out after 10 seconds.')
raise SystemExit(result.returncode)
PYDOCKER
if [[ -f "$KUBECONFIG" ]]; then
  context=$(kubectl config current-context 2>/dev/null || true)
  [[ -z "$context" || "$context" == kind-kubefleet ]] || { echo "Refusing unexpected context: $context" >&2; exit 1; }
  # kind delete clears its context but retains the dedicated kubeconfig file.
  # An absent project cluster is a valid precondition for fresh development setup.
  clusters=$(python3 - <<'PYKIND'
import subprocess
try:
    result=subprocess.run(['kind','get','clusters'],timeout=10,capture_output=True,text=True)
except subprocess.TimeoutExpired:
    raise SystemExit('kind discovery timed out after 10 seconds.')
print(result.stdout,end='')
raise SystemExit(result.returncode)
PYKIND
  ) || failed=1
  if rg -qx kubefleet <<< "$clusters"; then
    [[ "$context" == kind-kubefleet ]] || { echo 'Project cluster exists but its dedicated context is missing.' >&2;exit 1; }
    kubectl --request-timeout=10s get nodes -o wide || failed=1
  else
    echo 'Project cluster absent; cluster-up will create it.'
  fi
fi
exit "$failed"
