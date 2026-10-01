# Native compatibility spike

Run `make cluster-up` then `make compatibility`. Both commands use the dedicated
project kubeconfig and refuse any context except `kind-kubefleet`. The test checks
the project identity label before writing. Tests use temporary namespaces and
unique queue, flavor, priority, and AdmissionCheck names; cleanup uses those exact
objects. Interrupted processes can leave temporary resources: inspect their
identity before deleting them. No finalizers are force-removed.

The integration build tag is deliberate: ordinary `go test ./...` runs no cluster
mutation. A compatibility run writes the test log and timestamped raw API
snapshots under `evidence/compatibility/`. These are local evidence, not generated
fixtures or benchmark data. Every scenario uses real API objects and the real
Kueue manager.

Verified scenarios: native suspended Job/UID-owned Workload, admission and
unsuspension, successful Job and zero queue quota counters; isolated excess
request stays suspended; two Jobs consume twice one team's nominal CPU through
cohort borrowing; lower-priority running Pods are evicted for within-queue
preemption and cohort reclaim; an Active check gates execution while Pending;
Ready admits, Retry releases quota during persisted backoff and eventually
re-admits, and Rejected deactivates and releases quota.

Version-specific behavior in v0.13.4:

- Finished Workloads retain historical admission fields. Verify current quota
  accounting through ClusterQueue counters and usage, not by requiring removal
  of historical fields.
- `AdmissionCheck.spec.retryDelayMinutes` is deprecated and does not establish
  backoff for this custom driver. Set Retry and persist `status.requeueState`
  atomically using fresh-read optimistic concurrency, following the upstream
  provisioning controller. The driver uses a three-second backoff. Kueue resets
  the check to Pending on eviction and clears `requeueAt` after the deadline;
  the driver waits for that lifecycle before marking Ready.
- Queue status updates are asynchronous. Wait for observed counters rather than
  interpreting the first read as the final accounting result.

Source references:

- [Workload API](https://github.com/kubernetes-sigs/kueue/blob/v0.13.4/apis/kueue/v1beta1/workload_types.go)
- [Workload controller](https://github.com/kubernetes-sigs/kueue/blob/v0.13.4/pkg/controller/core/workload_controller.go)
- [Check reset utilities](https://github.com/kubernetes-sigs/kueue/blob/v0.13.4/pkg/workload/admissionchecks.go)
- [Provisioning controller Retry and requeue state](https://github.com/kubernetes-sigs/kueue/blob/v0.13.4/pkg/controller/admissionchecks/provisioning/controller.go)

This driver is a compatibility probe, not the production prerequisite controller.
It does not establish full prerequisite trust, tenant RBAC, cancellation,
leader-failover, or quantitative efficiency claims.
