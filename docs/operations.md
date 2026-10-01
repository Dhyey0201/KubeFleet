# Operations runbooks

Use the project environment first: `source scripts/env.sh` in bash. Queries below
are read-only; do not remove finalizers or bypass validation to make a test pass.
Events may expire, and persisted Active alone does not prove an online gate.

| Symptom | Read-only check | Interpretation and next step |
|---|---|---|
| Installation fails | `make doctor`; `kubectl get nodes`; `kubectl get pods -A` | Verify exact versions, free disk and dependency rollout before retrying. |
| Webhook certificate/endpoint failure | `kubectl -n kubefleet-system get certificate,service,endpointslice`; `kubectl get validatingwebhookconfiguration kubefleet-batchrun-validation -o yaml` | Check Certificate Ready, service DNS, endpoints and injected CA; keep failurePolicy Fail. |
| Manager not Ready | `kubectl -n kubefleet-system describe deployment kubefleet-manager`; `kubectl -n kubefleet-system logs deployment/kubefleet-manager` | Check TLS mounts, read-only filesystem, requests and startup errors. |
| No active leader | `kubectl -n kubefleet-system get lease kubefleet.batch.kubefleet.dev -o yaml` | Compare holder/renewTime with live Pods; inspect leader-election logs. |
| Queue inactive | `kubectl get clusterqueue team-a -o yaml`; `kubectl get admissioncheck kubefleet-prerequisite -o yaml` | Check flavors, namespace selector and registered check parameters/policy. |
| Gate stuck Pending/Retry | `bin/kubefleet diagnose NAME -n team-a -o json`; `kubectl -n team-a get job PREPARATION -o yaml` | Verify approval, UID, creation-based deadline, hold limit and persisted requeueAt. |
| Quota reserved without admission | `kubectl -n team-a get workload WORKLOAD -o yaml` | Inspect each check independently; Pending can hold quota briefly before Retry. |
| Admitted but unschedulable | `kubectl -n team-a describe pod POD`; `kubectl get nodes -o yaml` | Kueue admission does not create physical node capacity. |
| Image pull failure | `kubectl -n team-a describe pod POD` | Check immutable image availability, registry access and local kind loading. |
| Worker failure | `kubectl -n team-a get job JOB -o yaml`; `kubectl -n team-a logs POD` | Check exit code, bounded backoff and terminal cause; another logical attempt needs a new run. |
| High reconcile/API writes | Query manager reconcile/API counters in Prometheus; inspect manager transition logs | Look for condition timestamp churn or a configuration error loop; preserve workload state. |
| Cancellation cleanup stuck | `bin/kubefleet diagnose NAME -n team-a`; `kubectl -n team-a get job,pod,workload -o yaml` | Follow exact recorded UID ownership and foreground finalization; inspect termination. |
| Deadline enforcement delayed | `bin/kubefleet status NAME -n team-a -o json`; inspect manager health/Lease | Compare absolute creation-based total deadline against controller availability. |
| JobLost | `bin/kubefleet diagnose NAME -n team-a` | No automatic reexecution. Preserve the failed run and deliberately submit a new name. |
| Metrics absent | `kubectl -n kubefleet-monitoring get pods`; inspect Prometheus targets and manager metrics snapshot success | Check discovery/RBAC/TLS and cache health; missing CPU samples remain unknown. |
| Invalid benchmark comparison | Compare trace/profile/lock hashes and JSON quality flags | Different inputs, watch gaps, missing samples or safety stops invalidate particular comparisons; rerun without discarding failed evidence. |

Local services remain ClusterIP-only. Use loopback port-forwards for Prometheus
(9090) or Grafana (3000). Grafana credentials are in its dedicated Secret; keep them
out of command logs and artifacts. Dashboards use max over manager replicas for
current-state gauges; process counters/histograms are approximate observations.

A full host disk can also abort Docker's VM storage journal. Free only known project
caches, retain evidence, inspect API /readyz and Docker's storage log, and obtain
approval before restarting Docker if other projects' containers may be interrupted.
Never delete Docker volumes or factory-reset Docker as an implicit repair.
