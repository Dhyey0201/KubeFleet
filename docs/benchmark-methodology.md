# Benchmark methodology

An immutable versioned trace records its seed, exact image, relative submission
times, namespace/name, CPU/memory, bounded worker duration, mode, priority,
parallelism and optional concrete prerequisite reference. The generator uses a
local seeded RNG. Replay keeps the same names on retries and records actual versus
intended submission time, attempts, errors and created UIDs. Existing objects are
accepted only after an ambiguous Create in the same replay and when trace identity
and the complete immutable spec match. A first-attempt AlreadyExists is a submission
error, never a newly created run. Unresolved ambiguous commits are counted separately
and flagged; they are not silently classified as failed creates.

The independent collector watches BatchRuns, Jobs, Pods, Workloads and Events in
the three tenant namespaces. Selected fields include UIDs, owners, resourceVersion,
observation/source timestamps, conditions and outcome. Raw Pod specifications,
environment, commands and Secrets are excluded. It flags relist/watch gaps;
resource versions deduplicate observations, while distinct Pod UIDs preserve
repeated starts. It does not use operator Prometheus counters as an outcome ledger.

The report reconciles created = succeeded + failed + cancelled + unfinished.
Submission failures are separate. It computes observed first-admission and terminal
latencies, median/p95, and complete-trace makespan/throughput. Unobserved runs remain
unfinished; missing intervals and metrics remain null with quality flags. The
reporter now reconstructs closed admitted requested-CPU intervals from source
conditions and can import per-Pod Prometheus counters as explicitly observed lower
bounds. Watch gaps or unclosed intervals make the integral null; missing final
samples leave exact actual CPU/waste null. Source tests pass; five recovered-cluster smoke mixes now pass across all three profiles with no
collector gaps and closed requested-CPU intervals. [The measured smoke report](benchmark-report.md)
records counts, makespans and explicit CPU sampling limits. Quantitative release measurements remain
incomplete.
Requested CPU is never substituted for actual CPU consumed or preemption waste.

`bash scripts/benchmark.sh smoke [balanced|asymmetric|arrival|mixed|priority]`
runs a separate untimed warmup, waits for all 15 collector watches, and replays an
immutable 12-run trace unchanged through
isolated/shared/priority once, collects independently, saves hashes and rendered
profile, waits for terminal observations, and cleans only exact submitted UIDs.
Its results are smoke evidence. Balanced demand need not benefit from borrowing;
keep slower results too. The first collector attempt exposed a 15-second client
transport timeout on watches; its relist gaps remain recorded. The collector now
uses bounded list calls and a streaming client without that transport timeout. Normal
watch expiry resumes the last observed resourceVersion; real relists remain flagged.
Each sidecar manifest records trace/configuration/lock hashes, actual local worker
image ID, bounded Docker capacity, outcome summary and observation limits. An absent
commit is null and explicitly flagged; local image IDs are not registry digests.
`make benchmark-smoke-suite` runs all five mixes. `make benchmark-full` uses the
same mixes with five repetitions each on a qualifying host. Full durations are 15
seconds, with 5–30 seconds for the mixed trace, and a bounded 50-minute drain.

The full starting profile uses nominal 2 CPU/4 GiB per team and borrowing limits
4 CPU/8 GiB. It requires at least 8 Docker CPUs and 16 GiB Docker memory. This
host has 8 GiB physical RAM and approximately 6.77 GiB allocated to Docker, so
full application/replay refuse before mutation. Full five-repetition comparisons,
quantitative full-host CPU utilization/waste and statistical interpretation remain
unverified. Five smoke mixes have actual results; they do not replace full repetitions.

Do not compare different trace bytes, worker images, resource requests, quota scale,
host settings or pacing as if they were one controlled experiment. Preserve raw
observations and quality flags; no local smoke result establishes production
capacity or a statistically significant performance improvement.

The separate controller-load command accepts 100/500, or 1000 only with the full
host budget. It uses a deliberately missing queue to keep active Pods at zero,
records created/projected/queued versus completed counts, and probes live manager
RSS, CPU, API errors, host load and free disk before/during the burst. Missing
safety data stops the run. Thresholds are 220 MiB RSS, 1 CPU, 1 API error/s, load 16
and 8 GiB free disk. Exact UID cleanup follows even on a safety stop. Live snapshots also record reconcile rate and histogram p50/p95, workqueue depth,
API request/patch counters and explicit missing-measurement flags. The 100-object
and 500-object runs passed with all requested objects projected/queued, zero
completed and zero Pods. The 500-object projection took 109.2 seconds; foreground
cleanup took 187.4 seconds within its recorded 310-second budget. Earlier 60- and
155-second cleanup attempts are retained as safety-stopped results. These are controller management measurements, not production
capacity or completed workload throughput.
