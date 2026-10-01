# Interview walkthrough

Explain the product as a small batch admission/lifecycle layer built on Kubernetes
and Kueue. Show immutable BatchRun input, one deterministic native Job, Kueue’s
Workload/quota, optional prerequisite gate, restricted execution, and durable result.
Demonstrate queued versus reserved versus admitted versus Running and a real cancel.

Use architecture/ADRs and the matrix for discussion:

- How do repeats, stale cache, AlreadyExists and competing status writes preserve one logical Job? Follow materializer identity and fresh optimistic patches.
- What survives process death? Show stop intent, terminal result and atomically stored prerequisite snapshot, then boundary logs and recovered UIDs.
- Who owns suspension/quota/scheduling/retries? Point to native Kueue, Kubernetes scheduler and Job controller rather than claiming a custom scheduler.
- How does Pending avoid indefinite quota hold? Show QuotaReserved source timestamp, Retry plus persisted requeueAt and observed release/wakeup.
- Why does preparation cleanup not revoke a running run? Explain UID-scoped authorization snapshot and new-UID isolation requirements.
- What does failover prove? Show actual Lease/leader and original child UID; do not describe application execution as exactly once.
- Why can an admitted Pod remain blocked? Use the real Unschedulable/ImagePullFailure diagnostics.
- What does namespace RBAC prevent? Use impersonated tests, fail-closed admission and restricted worker defaults.
- How are CPU/throughput claims measured? Compare immutable trace, independent UID observations and actual cAdvisor samples; show null/gap flags where unavailable.
- What remains incomplete? Use the release checklist: full-host experiments, quantitative CPU/waste, remaining integration cases and remote CI validation.

Do not imply production deployment, statistical improvement, external upstream
contribution or full release completion. The evidence ledger is the claim boundary.
