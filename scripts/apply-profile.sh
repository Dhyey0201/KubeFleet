#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
bash "$ROOT/scripts/guard.sh"
profile=${1:-isolated}
size=${2:-smoke}
case "$profile" in isolated|shared|priority) ;; *) echo 'Unknown profile' >&2; exit 1 ;; esac
case "$size" in smoke|full) ;; *) echo 'Unknown size' >&2; exit 1 ;; esac
if [[ "$size" == full ]]; then
  docker info --format '{{json .}}' > "$ROOT/.tools/docker-budget.json"
  python3 - "$ROOT/.tools/docker-budget.json" <<'PY'
import json,sys
j=json.load(open(sys.argv[1]))
if j['NCPU']<8 or j['MemTotal']<16*1024**3:raise SystemExit('Full profile requires >=8 CPUs and >=16 GiB Docker memory; refusing application.')
PY
fi
mkdir -p "$ROOT/evidence/profiles"
input="$ROOT/config/kueue/profiles/$profile/resources.yaml"
output="$ROOT/evidence/profiles/$profile-$size.yaml"
python3 - "$input" "$output" "$size" <<'PY'
import json,re,sys
src,dst,size=sys.argv[1:]
docs=[json.loads(x) for x in re.split(r'^---\s*$',open(src).read(),flags=re.M) if x.strip()]
for d in docs:
    if d['kind']=='ClusterQueue':
        d['spec']['admissionChecks']=['kubefleet-prerequisite']
        d['spec']['namespaceSelector']={'matchLabels':{'kubernetes.io/metadata.name':d['metadata']['name']}}
        if size=='full':
            for group in d['spec']['resourceGroups']:
                for flavor in group['flavors']:
                    for r in flavor['resources']:
                        if r['name']=='cpu':
                            values={'250m':'2','500m':'4','0':'0'}
                        else:values={'512Mi':'4Gi','1Gi':'8Gi','0':'0'}
                        for k in ['nominalQuota','borrowingLimit','lendingLimit']:
                            if k in r:r[k]=values[r[k]]
with open(dst,'w') as f:f.write('\n---\n'.join(json.dumps(d,indent=2) for d in docs)+'\n')
PY
for team in team-a team-b team-c; do
  kubectl wait --for=jsonpath='{.status.reservingWorkloads}'=0 "clusterqueue/$team" --timeout=60s
done
kubectl apply -f "$output"
kubectl patch prerequisitegatepolicy default --type=merge -p '{"spec":{"allowedPriorityClassNames":["low","normal","high"]}}'
for team in team-a team-b team-c; do
  kubectl wait --for=condition=Active "clusterqueue/$team" --timeout=60s
done
shasum -a 256 "$output" > "$output.sha256"
