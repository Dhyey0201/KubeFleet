# Local smoke experiment report

These are one-repetition local smoke results, recorded after the authorized Docker
reset. Each mix replays the identical versioned trace bytes across profiles, with
an untimed warmup, independent watches established before submission, exact-UID
cleanup and recorded image/configuration/lock/host provenance.

All 15 comparisons created and succeeded 12 runs each: 180 created, 180 succeeded,
zero unfinished and zero collector-gap reports. An independent Job owner-UID audit
observed exactly one logical Job per run, with no missing or duplicate Job identities.

Measured complete-trace makespan in seconds:

| Trace mix | Isolated | Shared | Priority |
|---|---:|---:|---:|
| balanced | 19 | 20 | 19 |
| asymmetric | 55 | 20 | 19 |
| arrival | 35 | 20 | 21 |
| mixed | 20 | 20 | 20 |
| priority | 53 | 19 | 26 |

The asymmetric smoke trace drained faster with available cohort borrowing. The
balanced trace showed essentially equal makespan; borrowing is not expected to
help balanced demand. The priority trace recorded one eviction and 13 Pod starts
for 12 logical runs under the priority profile. Shared completed that trace faster
in this repetition. These small samples establish working behavior, not statistical
significance or production performance.

Closed admitted requested-CPU integrals were reconstructed for every comparison.
Some per-Pod cumulative CPU samples were observed, but final samples and complete
scrape coverage are unproven. Exact actual CPU and preemption-waste totals remain
null with explicit quality flags. A separate confirmed reclaim scenario measured
0.12092 CPU-seconds before eviction across three exact victim Pods: a lower bound
with missing final samples, not a full comparison measurement.

[Compact manifests and measurements](../evidence/verified/smoke-suite.json) preserve
per-profile counts, timing distributions, team results, hashes, measured CPU lower
bounds and limits. Raw observations, submissions, query exports and warmups remain
in the local `evidence/experiments` directories. The historical balanced experiment
with client watch gaps remains separately preserved.

Full acceptance is incomplete. The required 60–120-run, five-repetition comparison
needs at least 8 Docker CPUs and 16 GiB Docker memory; this host has 8 GiB physical
RAM. Full replay correctly refuses before mutation. No remote CI run or tested
commit exists; manifests explicitly flag the uncommitted source snapshot. No smoke
number is promoted to a full-host or resume improvement claim.

Controller management measurements:

| Batch | Created / projected / queued | Completed / Pods | Projection seconds | Cleanup seconds |
|---|---:|---:|---:|---:|
| 100 | 100 / 100 / 100 | 0 / 0 | 22.4 | Not separately recorded |
| 500 | 500 / 500 / 500 | 0 / 0 | 109.2 | 187.4 |

The deliberately missing queue keeps active worker Pods at zero. Live snapshots
record manager RSS/CPU, API errors and request/patch counters, reconcile rate and
histogram p50/p95, workqueue depth, host load and disk. Snapshots are sampled
observations, not continuous peak guarantees or exact event-ledger counts. Both
passing runs stayed within their declared safety thresholds. The 500-object
foreground cleanup needed longer than the original budgets: two earlier attempts
are explicitly safety-stopped, with all finalizers respected and residual UIDs
subsequently cleaned. [Raw measurements and stopped attempts](../evidence/verified/controller-load.json)
are retained. The 1000-object experiment was not run because the full host budget
is unavailable. These counts measure controller work, not successful Job throughput.
