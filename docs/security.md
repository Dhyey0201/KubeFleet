# Security boundaries

BatchRun validation fails closed on TLS endpoint or policy lookup failure. All
execution fields are immutable; cancellation only moves false → true. The
webhook intercepts BatchRun creates/updates only. Status uses its own subresource.
Release validation requires an allowed registry and immutable image digest. The
local `kubefleet-worker:compat` exception is explicit manager configuration.

Tenant service accounts use namespace RoleBindings to `kubefleet-tenant`.
They can submit/read/cancel BatchRuns and read Jobs, Pods/logs, events, Workloads,
and LocalQueues. They cannot forge status, mutate prerequisite Jobs or Workloads,
read Secrets, exec into Pods, write global queues or policy, or submit in another
namespace. Optional `kubefleet-global-diagnostics` grants global read-only queue
inspection when an administrator deliberately binds it. Without that binding,
diagnose reports InsufficientReadPermissions rather than interpreting Forbidden
as an absent resource.

The manager creates and deletes Jobs, reads Pods and policy, patches BatchRun and
its owned admission status, records Events, and manages its leader Lease. It has
no Secret read or Pod exec permission. Certificate contents are mounted by the
kubelet. Execution Pods use the fixed worker service account without token mounts,
non-root identity, RuntimeDefault seccomp, dropped capabilities, read-only root
filesystem, and no privilege escalation. Tenant namespaces enforce Restricted
PodSecurity. Kubernetes Job backoff handles Pod attempts; a lost logical Job never
causes the operator to recreate execution.

Monitoring is ClusterIP-only. Prometheus reads discovery and node metrics through
the API proxy; it does not read Secrets. Kube-state-metrics explicitly excludes
Secrets. Grafana requires a generated secret password, with no anonymous login.
The Kueue metrics scrape uses its internal self-signed certificate with TLS
verification disabled only for that local metrics job; it still authenticates with
a service-account token. External exposure and production PKI are not implemented.
For local access use a loopback-only kubectl port-forward. The Grafana administrator
password remains in `kubefleet-monitoring/grafana-auth`; do not include it in logs
or evidence bundles.

The real integration suite impersonates a tenant, checks authorization decisions,
attempts immutable/status/Job writes, rejects a privileged Pod, and verifies a
manager outage blocks BatchRun admission while an unrelated ConfigMap succeeds.
See `test/e2e/security_test.go` and `test/e2e/diagnostics_test.go`.
