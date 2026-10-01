# Verified claim ledger

Supported local-development statements:

- Built a Go Kubernetes operator around native Kueue Job admission with immutable API validation, exact-UID materialization, cancellation/deadline cleanup and bounded Job retries.
- Verified process death at three durable boundaries and two-replica Lease failover within the local 60-second acceptance target, preserving the original logical Job identity.
- Implemented a UID-approved prerequisite admission check with bounded reservation hold, supported Retry/backoff and durable readiness recovery.
- Verified namespace tenant permissions, fail-closed TLS webhook outage, restricted worker Pods, CLI signal/terminal behavior and read-only blocked-execution diagnostics.
- Deployed private Prometheus/Grafana/kube-state-metrics; executed dashboard queries and observed actual worker CPU metrics.
- Replayed five immutable 12-run smoke mixes across three profiles: 180 created and succeeded, no collector gaps, and exactly one observed logical Job per run. Requested CPU integrals and actual CPU lower bounds retain sampling limitations. Historical watch-gap evidence remains preserved.

Unsupported claims: complete release, production scalability, statistically significant
sharing improvement, exact CPU waste/utilization, successful full 16 GiB experiments,
remote CI success, exactly-once application processing, or accepted upstream work.
Update only from the actual matrix/evidence after verification; this file is not a
resume endorsement or fabricated performance result.
