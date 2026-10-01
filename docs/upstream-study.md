# Upstream source study

Kueue is pinned to v0.13.4. Relevant local module sources are
`pkg/workload/admissionchecks.go`, `pkg/controller/core/workload_controller.go`, and
`pkg/controller/admissionchecks/provisioning/controller.go` in the pinned module.
Official source: https://github.com/kubernetes-sigs/kueue/tree/v0.13.4 .

A real compatibility test found that Retry alone releases and immediately
re-reserves quota. The provisioning controller persists `status.requeueState.requeueAt`
with Retry; workload reconciliation honors that clock, clears it after backoff,
and resets admission checks to Pending on eviction. KubeFleet follows those supported
semantics and verifies actual quota counters rather than assuming a check state
proves release. The gate’s five-second local backoff is chosen configuration.

Another source finding: resetChecksOnEviction replaces each non-Pending check entry
and its message. A Ready message cannot be the sole readiness snapshot. KubeFleet
therefore commits a separate owned Workload condition with Ready, preserves other
conditions, and tests recovery after process death before BatchRun projection.

Finished Workloads can retain historical admission assignments. Completed quota
release is observed through ClusterQueue reservation counters and conditions,
not through the disappearance of every historical assignment field.

No upstream issue, PR, accepted contribution or maintainer endorsement was created.
A possible documentation clarification about the supported Retry/backoff contract
would require checking current upstream documentation and opening a separately
authorized contribution. This implementation is evidence of source study only.
