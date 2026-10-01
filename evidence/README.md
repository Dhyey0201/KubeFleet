# Evidence

Real compatibility, API/webhook, and lifecycle smoke tests have passed. Raw API
snapshots and command output remain local under this directory; they are excluded
from Git by default. See `docs/progress.md` and `docs/requirements-matrix.md` for
scope, test commands, failures fixed, and unverified requirements. These are local
smoke/integration observations, not full benchmarks or production measurements.

- `compatibility/`: eight real Kueue compatibility scenarios, final test log,
  timestamped Job/Workload/ClusterQueue observations, and earlier failed attempts.
- `envtest/`: real API server and TLS webhook tests with the race detector;
  controller concurrency and status-only watches; earlier failed attempt logs.
- `smoke/`: real worker success, queued/running cancellation, and JobLost;
  terminal run and child resource observations.
- `development-install.log`: dependency and manager rollouts, Certificate Ready,
  and webhook CA injection.
- `unit-vet-race.log`: unit/race tests and vet for current source.
- `bootstrap.log`: official artifact checksum checks.
- `worker-capacity.json`: reported kind node allocatable capacity. All nodes share
  the Docker VM; no independent physical capacity claim follows from the totals.

New local evidence includes `e2e/` lifecycle/fault/profile/gate/CLI/security/monitoring
suites and failures, `experiments/` immutable trace/profile hashes, submission UIDs,
independent JSONL and quality summaries, and full benchmark refusal. Later host disk
exhaustion made Docker unhealthy; current live cluster readiness is not claimed.
