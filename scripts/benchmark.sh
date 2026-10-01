#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
bash "$ROOT/scripts/guard.sh"
cd "$ROOT"
size=${1:-smoke}
mix=${2:-balanced}
case "$size" in smoke|full) ;; *) echo 'size must be smoke or full' >&2;exit 1;;esac
case "$mix" in balanced|asymmetric|arrival|mixed|priority) ;; *) echo 'unsupported comparison mix' >&2;exit 1;;esac
if [[ "$size" == full ]]; then
  docker info --format '{{json .}}' > .tools/benchmark-budget.json
  python3 - .tools/benchmark-budget.json <<'PY'
import json,sys
j=json.load(open(sys.argv[1]))
if j['NCPU']<8 or j['MemTotal']<16*1024**3:raise SystemExit('Full benchmark blocked: requires >=8 CPUs and >=16 GiB Docker memory.')
PY
fi
mkdir -p bin evidence/experiments
go build -o bin/loadgen ./cmd/loadgen
go build -o bin/collector ./cmd/collector
experiment_dir="evidence/experiments/$(date -u +%Y%m%dT%H%M%SZ)-$size-$mix"
mkdir "$experiment_dir"
count=12
repetitions=1
drain_timeout=20m
collector_duration=30m
if [[ "$size" == full ]]; then count=60;repetitions=5;drain_timeout=50m;collector_duration=1h;fi
bin/loadgen generate --seed 42 --count "$count" --size "$size" --mix "$mix" --out "$experiment_dir/trace.json"
shasum -a 256 "$experiment_dir/trace.json" > "$experiment_dir/trace.sha256"
docker info --format '{{json .}}' | python3 -c 'import json,sys; j=json.load(sys.stdin); json.dump({k:j.get(k) for k in ["ServerVersion","NCPU","MemTotal","Driver","KernelVersion","OperatingSystem","OSType","Architecture"]},sys.stdout,indent=2)' > "$experiment_dir/docker.json"
docker image inspect kubefleet-worker:compat --format '{{.Id}}' > "$experiment_dir/worker-image-id.txt"
cp versions.lock.yaml "$experiment_dir/versions.lock.yaml"
git rev-parse HEAD > "$experiment_dir/commit.txt" 2>/dev/null || echo uncommitted > "$experiment_dir/commit.txt"
collector_pid=''
active_submission=''
cleanup_current() {
  if [[ -n "$collector_pid" ]];then kill -TERM "$collector_pid" 2>/dev/null || true;wait "$collector_pid" || true;collector_pid='';fi
  if [[ -n "$active_submission" && -f "$active_submission" ]];then bin/loadgen cleanup --submissions "$active_submission" --timeout 60s;active_submission='';fi
}
trap cleanup_current EXIT
# Untimed warmup is separate from the immutable comparison and its result ledger.
bash scripts/apply-profile.sh shared "$size"
bin/loadgen generate --seed 41 --count 3 --size smoke --mix balanced --out "$experiment_dir/warmup-trace.json"
active_submission="$experiment_dir/warmup-submissions.json"
bin/loadgen replay --trace "$experiment_dir/warmup-trace.json" --out "$active_submission" --timeout 5m
bin/loadgen wait --submissions "$active_submission" --timeout 5m
cleanup_current
for repetition in $(seq 1 "$repetitions");do
  profiles=(isolated shared priority)
  if (( repetition % 2 == 0 ));then profiles=(priority shared isolated);fi
  for profile in "${profiles[@]}";do
    bash scripts/apply-profile.sh "$profile" "$size"
    prefix="$experiment_dir/$profile-$repetition"
    cp "evidence/profiles/$profile-$size.yaml" "$prefix-profile.yaml"
    shasum -a 256 "$prefix-profile.yaml" > "$prefix-profile.sha256"
    kubectl get clusterqueues -o json > "$prefix-queue-status.json"
    bin/collector --experiment "$(basename "$experiment_dir")" --profile "$profile" --repetition "$repetition" --out "$prefix-observations.jsonl" --duration "$collector_duration" > "$prefix-collector.log" 2>&1 &
    collector_pid=$!
    cpu_start=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    # Wait for every list/resourceVersion/watch handoff before the first submit.
    ready=0
    for i in $(seq 1 100);do
      if [[ -s "$prefix-observations.jsonl" ]] && python3 - "$prefix-observations.jsonl" <<'PYREADY'
import json,sys
ready=set()
for line in open(sys.argv[1]):
    try: row=json.loads(line)
    except json.JSONDecodeError: continue
    if row['eventType']=='WATCH_READY':ready.add((row['namespace'],row['kind']))
raise SystemExit(0 if len(ready)==15 else 1)
PYREADY
      then ready=1;break;fi
      kill -0 "$collector_pid"
      sleep 0.1
    done
    [[ "$ready" == 1 ]] || { echo 'Collector watches did not become ready' >&2;exit 1; }
    active_submission="$prefix-submissions.json"
    bin/loadgen replay --trace "$experiment_dir/trace.json" --out "$active_submission" --timeout 20m
    bin/loadgen wait --submissions "$active_submission" --timeout "$drain_timeout"
    sleep 1
    kill -TERM "$collector_pid"
    wait "$collector_pid"
    collector_pid=''
    cpu_end=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    cpu_args=()
    if python3 scripts/export-cpu.py --start "$cpu_start" --end "$cpu_end" --out "$prefix-cpu.json" > "$prefix-cpu-export.log" 2>&1; then
      cpu_args=(--cpu "$prefix-cpu.json")
    fi
    bin/loadgen report "${cpu_args[@]}" --observations "$prefix-observations.jsonl" --submissions "$active_submission" --out "$prefix-summary.json"
    python3 scripts/experiment-manifest.py --summary "$prefix-summary.json" --profile "$profile" --repetition "$repetition" --out "$prefix-manifest.json"
    cat "$prefix-summary.json"
    cleanup_current
  done
done
bash scripts/apply-profile.sh isolated smoke
printf 'Recorded experiment: %s\n' "$experiment_dir"
