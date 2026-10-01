# Prerequisite gate

The fixed AdmissionCheck is `kubefleet-prerequisite`; its controller name is
`batch.kubefleet.dev/prerequisite-gate`. Parameters must reference the same
administrator-selected `PrerequisiteGatePolicy` as the validating webhook.
Registration becomes inactive when that policy is missing or invalid. Persisted
Active does not prove a controller is online; inspect manager health, the leader
Lease, and pending age.

Correlation follows Workload controller owner → exact Job UID → exact BatchRun
UID, including the deterministic Job name and materializer identity labels.
Copied Workload annotations do not authorize execution. Plain preparation Jobs
must be in the same namespace, carry
`batch.kubefleet.dev/prerequisite=approved`, and have neither a Kueue queue label
nor any BatchRun owner. Tenant roles cannot create or modify those Jobs.

The wait deadline is creationTimestamp plus the immutable wait timeout. It never
resets on a new reservation or restart. Missing/incomplete preparation is Pending;
after the policy hold limit from Kueue's QuotaReserved transition it becomes Retry.
Retry atomically persists a five-second `status.requeueState.requeueAt`, preserving
any later existing backoff. Kueue v0.13.4 resets the check to Pending on eviction
and releases reservation; Retry alone immediately re-reserves in this release.
Ready waits for Kueue's backoff clock. An expired deadline is Rejected even during
backoff. Transient reads return errors and never become Ready or terminal reject.

Ready and the custom `KubeFleetPrerequisiteReady` Workload condition are committed
in one optimistic status patch. That condition contains a small JSON snapshot of
the exact run UID and prerequisite UID. Kueue resets AdmissionCheck messages on
eviction, so a message alone cannot preserve the authorization snapshot. This
controller owns only its named check, this condition, and Retry's supported requeue
clock; it leaves other conditions, checks, admission assignments, and counters
unchanged. Fresh reads and resourceVersion preconditions preserve competing writes.

The gate projects the BatchRun decision after that commit. Test-only builds can
pause between the two writes. A new process recovers the snapshot even if the
preparation Job was cleaned up. The same run retains readiness on re-admission;
a new run UID or changed prerequisite UID cannot inherit it. Policy updates govern
new approvals, while already committed snapshots retain their authorization.

Cancellation cannot be compared atomically with a Workload write across two API
objects. Fresh stop checks narrow that race; durable stop intent and exact-UID
foreground deletion ensure a concurrent Ready cannot create a second logical Job.

Verified locally: real Ready admission, bounded Retry and quota release, success
wakeup after backoff, rejection causes, inactive policy restoration, preemption
following preparation cleanup, and the Ready-before-projection crash boundary.
See `test/e2e/gate_test.go` and `internal/controller/gate_test.go`. Concurrent
status preservation also passed a real-API stale resourceVersion conflict while
another check changed; quota admission and the competing entry remained intact.
