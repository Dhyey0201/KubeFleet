# Testing and evidence

Use bash and `source scripts/env.sh`; the project isolates tools/cache/kubeconfig.
`make test race vet lint build check-generated` covers pure logic, validation,
materialization, client errors, strict parsing, watch reconnection, accounting and
bounded metric labels. `make envtest` starts the real pinned API server/etcd and
TLS webhook, verifying structural defaults, immutable updates, dry-run, collision,
AlreadyExists recovery, conflict preservation and status-only controller watches.
It does not supply scheduler/kubelet/GC execution.

`make smoke` proves real worker success, cancellation and JobLost. `make e2e`
requires native Kueue, manager/gate, fault image, three teams, monitoring and the
CLI binary. It verifies failure/backoff/deadlines, GC, crash boundaries, standby
failover, quotas, preemption/reclaim, prerequisite lifecycle, RBAC, webhook outage,
CLI exits/signals and scheduling/startup diagnostics. Test hooks require the
`faultinject` build tag and exact administrator environment target; release builds
compile no-op hooks. Failures collect selected CRs/Jobs/Pods/Workloads/events, never
Secrets. Controlled transport/conflict tests are not real server outage claims.

`make benchmark-smoke` replays one immutable trace through all profiles and writes
independent JSONL observations and summaries. A watch gap is evidence, not a hidden
skip; unobserved CPU remains null. Full benchmarks require another host with the
specified budget. The T01–T60 matrix and exact checklist identify incomplete cases.

Historical failed attempts are kept separately from passed logs. Latest source
changes after a deployment/test need re-verification; no green historical log proves
an untested final binary. Docker’s disk-journal failure was recovered through an explicitly authorized data
reset; fresh setup, smoke and all integration scenarios have passing evidence. The
fresh aggregate-quota assertion required a focused fix/rerun, recorded transparently. Remote CI is unverified until executed.
