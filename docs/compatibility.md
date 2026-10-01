# Compatibility discovery

Selected 2026-09-30. Native compatibility gate verified; the lock is not a complete release lock yet.

Kueue v0.13.4 uses Kubernetes Go modules v0.33.3 and controller-runtime v0.21.0, requiring Go >=1.24. Choose that aligned module family and Kubernetes 1.33.1 kind image. Go 1.26.8 is an available supported toolchain from official download metadata. This intentionally selects an older released Kueue baseline; it is a local demonstration, not a security-maintained production recommendation.

Sources:
- https://github.com/kubernetes-sigs/kueue/blob/v0.13.4/go.mod
- https://github.com/kubernetes-sigs/controller-runtime/blob/v0.21.0/go.mod
- https://github.com/kubernetes-sigs/kind/releases/tag/v0.29.0
- https://go.dev/dl/?mode=json

Docker reports 8 CPUs and 6.77 GiB memory. Full-profile benchmarks are blocked by the specified 16 GiB budget. Smoke work uses 0.25 CPU and 512 MiB nominal per team (CPU and memory uniformly scaled by 1/8), bounded workload durations and no host memory stress. Real smoke admission, parallel quota aggregation and bounded profile behavior were
measured in the cluster. This does not qualify the host for full experiments.

Native compatibility gate passed all eight scenarios on the real kind cluster.
Commands and raw artifacts: `make cluster-up`, `make compatibility`,
`evidence/cluster-up.log`, `evidence/compatibility/test.log`, timestamped JSON
snapshots. See `test/compatibility/README.md` for exact version-specific semantics.
Kueue's registration retry delay does not enforce custom-controller backoff:
Retry must also persist `requeueState.requeueAt`, as its provisioning controller
does. The implemented prerequisite controller preserves this verified behavior.

The final smoke quota is uniformly one eighth of the full starting configuration:
250m CPU and 512Mi memory nominal per team; requests 250m CPU and 64Mi memory per
single-Pod Job. The preliminary 256Mi quota has been replaced to align CPU and
memory scale factors. This is compatibility evidence, not the full benchmark.
Worker allocatable resources are recorded in `evidence/worker-capacity.json`.
Kind reports each worker's host-shared capacity; do not sum those values and
pretend the Docker VM has twice its physical memory or CPU.

Official release selections for later milestones:
- https://github.com/prometheus/prometheus/releases/tag/v3.5.0
- https://github.com/grafana/grafana/releases/tag/v12.1.1
- https://github.com/kubernetes/kube-state-metrics/releases/tag/v2.16.0
- https://github.com/golangci/golangci-lint/releases/tag/v2.4.0
- https://github.com/kubernetes-sigs/controller-tools/releases/tag/envtest-v1.33.0

Image manifest digests were resolved from the official registries with
`docker buildx imagetools inspect`; the lock records them. Pinned monitoring images and
cert-manager have been deployed and verified, including fresh Docker setup, TLS
admission, healthy scrapes, Grafana health and all dashboard expressions. envtest v1.33.0
Darwin/arm64 was downloaded and verified against the official SHA512 sidecar;
controller-gen v0.18.0 is installed project-locally. The local worker builds from
scratch without a mutable base image. No registry upload has occurred.

The core api/apimachinery/client-go dependencies resolve to v0.33.3; transitive
apiextensions-apiserver resolves to v0.33.1. `go.mod` and `go.sum` record exact
module versions; an aligned minor family does not imply identical patch versions.
