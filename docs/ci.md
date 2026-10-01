# CI

The workflow uses ordinary pull_request/push events, read-only contents permission,
per-ref cancellation and bounded job timeouts. It does not use pull_request_target,
registry secrets or external publication. Official checkout v4 and upload-artifact
v4 tag refs were resolved through GitHub's repository API on 2026-10-01 and pinned
to immutable commits in `.github/workflows/verify.yaml`; checkout retains no token.

The source job bootstraps exact tools, installs the pinned linter, checks formatting,
builds all five binaries, runs unit/race/vet/lint/generated/envtest/TLS checks,
collects review coverage and builds scratch images. The kind job creates only the
project cluster, loads the worker, installs dependencies and the manager/gate,
installs private monitoring, builds debug hooks, and runs smoke/e2e and an independent
smoke replay. Artifact paths exclude kubeconfigs, certs and Grafana passwords.

The public repository is https://github.com/Dhyey0201/KubeFleet. Pushes to it trigger
this workflow; successful remote execution remains unverified at publication.
Local tests have their own recorded evidence. The runner's capacity does not imply
that the full 16 GiB benchmark was executed; CI runs the smoke profile only.
Dependency caching is keyed by exact lock/go.sum hashes and excludes kubeconfigs
and credentials. Pure kustomize render checks produce hashed profile artifacts;
the kind job validates every profile and monitoring manifest through API server
dry-run after CRD installation (`make validate-manifests`). This check also passed
locally on the recreated cluster.
