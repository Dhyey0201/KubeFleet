# KubeFleet

A Go Kubernetes batch admission and lifecycle operator built on native Kueue Job
integration. Immutable BatchRuns produce one exact-UID suspended Job; Kueue owns
quota/admission/preemption, and Kubernetes owns Pod execution and Job retries.
An administrator-approved prerequisite Job can gate admission through a bounded,
durable AdmissionCheck. CLI, restricted tenant roles, private monitoring and
independent smoke trace collection are implemented and locally exercised.

**Release status: incomplete.** See [progress](docs/progress.md), the
[T01–T60 matrix](docs/requirements-matrix.md) and exact
[release checklist](docs/release-checklist.md). Full benchmark measurements require
at least 8 Docker CPUs/16 GiB memory; this host has 8 GiB physical RAM. After disk exhaustion corrupted Docker's storage journal, an explicitly authorized
Docker data reset restored a healthy empty engine and adequate free disk. Fresh setup, all integration scenarios, five smoke mixes and 100/500-object
controller management/cleanup checks passed locally. The public demo and guarded
teardown/rebuild were reproduced. Historical evidence
remains intact.

## Local development

Docker Desktop/Linux Docker must be healthy. Reserve at least 8 GiB free disk for
builds/images/tests. The dedicated kind cluster is named `kubefleet`; every mutating
script checks context and project identity. No cloud account or registry publication
is required for local development.

```bash
make bootstrap
make doctor
make dev-up
make smoke
make e2e
make benchmark-smoke
make benchmark-smoke-suite # all five smoke workload mixes
```

`make dev-up` builds/loads worker and manager images, creates the dedicated cluster,
installs pinned Kueue/cert-manager/CRDs/TLS validation, creates the selected gate
policy/check, installs namespace roles and private monitoring, and builds the test
fault image. It was reproduced from completely empty Docker storage using the existing pinned
local tool installation. A tool-free machine bootstrap remains a separate check. Use `make help` for individual steps. Source-only checks are `make verify`;
kind checks need Docker and the development stack.

```bash
source scripts/env.sh # bash; sets project tools and kubeconfig
bin/kubefleet validate -f config/samples/smoke.yaml --server
bin/kubefleet submit -f config/samples/smoke.yaml
bin/kubefleet wait smoke-sample -n team-a --timeout 120s
bin/kubefleet diagnose smoke-sample -n team-a --output json
```

Use a new name for another attempt. Cancelling is irreversible; a terminal/lost Job
is never automatically reexecuted. The local image exception is explicit and must
be disabled in release configuration. External deployment/publication is absent.

Read [architecture](docs/architecture.md), [API](docs/api.md), [CLI](docs/cli.md),
[gate semantics](docs/prerequisite-gate.md), [security](docs/security.md),
[runbooks](docs/operations.md), [testing](docs/testing.md),
[benchmark methodology](docs/benchmark-methodology.md), and [CI status](docs/ci.md).
Raw evidence stays local; compact verified records are prepared separately. No
smoke result establishes production scalability or statistical performance gains.
