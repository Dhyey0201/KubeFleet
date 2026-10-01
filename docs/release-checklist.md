# Release status: incomplete



Use this exact checklist in the release record. Each checked item must link to implementation and executed evidence.

- [x] Compatibility matrix and exact dependency/image locks exist and were exercised. ([lock](../versions.lock.yaml); [compatibility](../test/compatibility/README.md))
- [x] BatchRun and PrerequisiteGatePolicy schemas, defaults, conditions, printer columns, and generated code are complete. ([API](../api/v1alpha1/batchrun_types.go); [envtest](../evidence/verified/envtest-after-reset.log))
- [x] Create/update/dry-run validation and immutable-spec behavior are verified through the real API server. ([validator](../internal/webhook/batchrun.go); [envtest](../evidence/verified/envtest-after-reset.log))
- [x] One deterministic owned Job per run UID; collisions and stale ownership are handled safely. ([materializer](../internal/materialize/job.go); [envtest](../evidence/verified/envtest-after-reset.log))
- [x] Real Kueue native Job admission works; KubeFleet never fights suspension or creates duplicate Workloads. ([controller](../internal/controller/batchrun.go); [worker smoke](../evidence/verified/worker-smoke.log))
- [x] Status projection distinguishes quota reservation, admission, Pod scheduling, and running. ([projection](../internal/controller/batchrun.go); [kind scenarios](../evidence/verified/all-e2e-attempt.log))
- [x] Job backoff, execution deadline, total deadline, cancellation, JobLost, and terminal persistence are verified. ([lifecycle](../internal/controller/batchrun.go); [kind scenarios](../evidence/verified/all-e2e-attempt.log))
- [x] Durable stop intent is tested across the process-crash boundary. ([stop intent](../internal/controller/batchrun.go); [crash evidence](../evidence/verified/all-e2e-attempt.log))
- [x] Foreground cleanup, owner references, garbage collection, and quota release are proven in kind. ([cleanup](../internal/experiment/cleanup.go); [native GC evidence](../evidence/verified/controller-load.json))
- [x] Three queue profiles are complete; baseline no-borrow, shared borrowing, priority preemption, and reclaim are demonstrated. ([profiles](../config/kueue/profiles); [demo](../evidence/verified/setup-demo.log))
- [x] Prerequisite UID/approval policy, check Active registration, all four decisions, bounded hold, and backoff wakeup are verified. ([gate](../internal/controller/gate.go); [demo](../evidence/verified/setup-demo.log))
- [x] Unrelated check entries and Kueue-owned status fields survive concurrent updates. ([gate patch](../internal/controller/gate.go); [API conflict evidence](../evidence/verified/envtest-after-reset.log))
- [x] Readiness snapshot recovery and new-UID isolation are verified. ([snapshot recovery](../internal/controller/gate.go); [persistence evidence](../evidence/verified/all-e2e-attempt.log))
- [x] Two-replica leader election and restart convergence have actual measurements. ([manager](../cmd/manager/main.go); [failover evidence](../evidence/verified/all-e2e-attempt.log))
- [x] CLI commands, strict validation, outputs, exit codes, watch recovery, and cancellation are tested. ([CLI](../internal/cli); [demo](../evidence/verified/setup-demo.log))
- [x] Tenant roles, namespace boundaries, webhook fail-closed behavior, and worker security defaults are tested. ([tenant roles](../config/development/tenants.yaml); [security evidence](../evidence/verified/all-e2e-attempt.log))
- [x] Metrics scrape, bounded labels, real dashboards, and runbooks are verified. ([monitoring](../config/monitoring/resources.yaml); [kind evidence](../evidence/verified/all-e2e-attempt.log))
- [x] Worker, trace generator, collector, raw schemas, quality flags, and comparison summaries are implemented. ([experiment tools](../internal/experiment); [smoke evidence](../evidence/verified/smoke-suite.json))
- [ ] Main profiles have at least five valid repetitions in the full benchmark; actual findings and limits are reported.
- [x] Controller load is distinguished from executed workload throughput; host resource limits and stop thresholds are recorded. ([management load](../internal/experiment/controllerload.go); [actual results](../evidence/verified/controller-load.json))
- [x] T01 through T60 are mapped to actual tests and their results, with no silent skips. ([matrix](requirements-matrix.md); [verification ledger](../evidence/verified/README.md))
- [x] Formatting, vet, lint, race tests, envtest, webhook tests, end-to-end tests, and generated-file checks pass where required for release. ([test guide](testing.md); [final checks](../evidence/verified/final-source-checks.log))
- [x] Fresh setup/quickstart, smoke, full demo, diagnostics, and cleanup were reproduced. ([setup](../scripts/dev-up.sh); [reproduced demo](../evidence/verified/setup-demo.log))
- [ ] Architecture decisions, API guide, operations/security/testing docs, benchmark report, interview guide, and evidence ledger are complete.
- [x] Upstream source study is accurate; proposed contribution is genuine or honestly absent. ([upstream study](upstream-study.md); [version-specific native tests](../test/compatibility/README.md))
- [ ] Final package checksums, commit, lock hash, check statuses, and blockers are recorded.
- [x] Resume bullets contain only verified completed work and actual measured numbers. ([claim ledger](resume-ledger.md); [measured smoke report](benchmark-report.md))

Remaining acceptance: five valid full-host repetitions, the full benchmark report,
a tested commit and actual remote CI. The source package is a development checkpoint.
The fresh full kind invocation retained one timing failure; the corrected focused
rerun and public demo passed. All scenario coverage is recorded across those runs,
not represented as a single green full kind invocation.
