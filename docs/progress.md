# Progress and continuation checkpoint

Release remains incomplete. The original build specification is unchanged. The
[T01–T60 matrix](requirements-matrix.md), [smoke report](benchmark-report.md) and
[exact checklist](release-checklist.md) distinguish executed evidence from full
release acceptance.

Verified locally:

- M0/M1: eight pinned native Kueue compatibility scenarios; structural API/defaults,
  immutable validation, TLS dry-run/fail-closed behavior, exact-UID materialization,
  real collision/AlreadyExists/conflict/concurrent reconciliation and worker smoke.
- M2: actual failure/backoff Pod attempts, execution/absolute deadlines, missing and
  inactive queues, cancellation/deletion GC, JobLost, crash boundaries, two-replica
  Lease failover, parallel quota=500m, terminal/gauge reconstruction after restart,
  new-UID isolation, SIGTERM interrupted logs and unchanged idle resourceVersions.
- M3: isolated one-Running/250m versus shared three-Running/750m demand, within-CQ
  preemption and nominal-owner cohort reclaim. Replacement Pods retain the original
  Job UID. Three exact victim Pod CPU samples total 0.12092 CPU-seconds before
  confirmed eviction: a lower bound with missing final samples.
- M4: production registration and exact-UID prerequisite trust, all four decisions,
  bounded reservation hold/Retry/backoff/quota release, deadline/rejection cleanup,
  inactive-policy restoration, readiness latch across cleanup/preemption, atomic
  Ready crash recovery, unrelated-entry preservation and real API conflict/trust tests.
- M5: CLI commands/formats/strict parsing/exit codes/signals/reconnect, real tenant
  permissions and forbidden writes, Restricted Pods, webhook outage and blocked-Pod
  diagnostics. Private pinned monitoring, healthy scrapes, all dashboard expressions,
  actual worker CPU, bounded labels and reconstructed current-state gauges passed.
- M6 partial: five immutable 12-run smoke mixes across three profiles: 180 created,
  180 succeeded, zero unfinished, zero collector-gap reports, and one independently
  observed logical Job per run. Closed requested-CPU integrals and observed CPU lower
  bounds are recorded; exact actual CPU/waste remain unknown. The 100-object controller
  run projected/queued all 100 with zero Pods/completions. Final 500-object management/cleanup verification passed: 500 projected/queued,
  zero Pods/completions, 109.2 seconds projection and 187.4 seconds cleanup. Full five-repetition 60–120-run acceptance is blocked.
- M7 partial: all five host binaries, unit/race/vet/lint/generated/guard checks,
  real envtest/TLS/conflict tests, source coverage and API server validation of every
  rendered profile/monitoring manifest passed. Fresh setup from empty Docker storage
  passed using existing pinned tools. CI/docs/ADRs/source study/claim ledger exist;
  remote CI, tool-free-machine setup and final release acceptance remain unverified.

Docker recovery and setup:

The user explicitly authorized clearing all Docker data because no other project
used Docker. The corrupted Docker app/VM was stopped; only its exact `Docker.raw`
data disk was removed. Restart verification recorded zero containers, images,
volumes and build cache with a healthy Docker 29.0.1 engine. About 46 GiB host disk
was available, and source/tools/credentials/raw evidence remained intact. Fresh
`scripts/dev-up.sh` completed all node/dependency/manager/TLS/monitoring/fault-image
steps. Heavy builds retain an 8 GiB free-disk requirement. Doctor reads are bounded
and recognize a deleted project cluster even if kind leaves an empty kubeconfig.

Failures retained and fixes verified:

- Old `:dev` rebuilds did not force Deployment rollout: actual image-ID annotation
  now does. Install and fault setup wait for real TLS server dry-run readiness.
- Kueue resets check messages on eviction: the same optimistic Ready patch persists
  an owned Workload snapshot condition. Retry also persists supported requeueAt.
- Queued Ready projection toggled a condition timestamp: semantic status no-op is
  tested. Idle integration now waits for startup to settle before its three-second
  unchanged Run/Workload version assertion; it passed.
- Fresh kind suite: 470.521 seconds, every scenario except quota summary timing
  passed. The assertion now awaits exact 500m convergence; focused rerun passed in
  10.156 seconds. This is passing scenario coverage across two runs, not a single
  green full-suite invocation. Fresh worker smoke passed in 16.183 seconds.
- Sandbox denied envtest loopback binds. Its permitted rerun passed in 24.363 seconds;
  the failure is retained. Integration vet/lint passed with zero issues.
- The initial independent collector used a 15-second transport timeout. Its gaps
  remain archived. Current collection establishes 15 watches before replay and
  resumes closed streams at the last resourceVersion; real relists remain flagged.
- Initial 500-object controller load projected all 500 with zero Pods but exceeded
  a 60-second cleanup budget. Cleanup now preflights one list per namespace and
  retains exact API UID preconditions on every foreground DELETE. Race/unit tests
  preserve replacements and unsubmitted runs; real API envtest proved replacement
  between list and delete remains intact. A 155-second budget still expired just
  before native GC finished; that attempt is also safety-stopped and retained.
  The final bounded budget is 310 seconds for 500, with separate cleanup timing.

Current external limits:

This host has 8 GiB physical RAM and Docker approximately 6.77 GiB. The full guard
correctly refuses below 8 Docker CPUs/16 GiB memory before mutation. Do not assign
Docker more memory than the host can provide. The public source repository is
https://github.com/Dhyey0201/KubeFleet; successful remote CI remains unverified at
publication. Historical local manifests predate the initial commit and retain null
commit values and an explicit snapshot flag. No images were published and no paid
infrastructure was provisioned.

Guarded cluster-down, pinned bootstrap, fresh dev-up and public demo were reproduced.
Demo worker smoke passed in 18.146 seconds; profiles/gate/CLI passed in 118.042 seconds.
Final source/API/schema checks passed. Extended envtest lint found a logging-only
style issue; its focused Logf fix/rerun and refreshed coverage passed. Compact proof
and the reproducible development checkpoint package are recorded in
`evidence/verified/checkpoint.json`. Remaining acceptance requires a qualifying full host and
actual remote CI/tested commit. Failed attempts and original trace bytes
must remain preserved; no smoke result substitutes for full repetitions.
