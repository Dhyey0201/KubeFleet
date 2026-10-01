# Repository work

Preserve `KubeFleet_Codex_Build_Specification.md`. Track actual progress and blockers
in `docs/progress.md`, the T01–T60 matrix, and the exact release checklist.

Use `scripts/env.sh` from bash for the pinned project tools and isolated kubeconfig.
Run formatting, unit/race tests and vet for Go changes; run envtest for API/webhook
or status concurrency changes, and relevant kind scenarios for admission/lifecycle.
Do not count source-only implementations as passed integrations. Record failures
and fixes; no skipped assertion or invented benchmark replaces evidence.

Cluster mutations and cleanup must verify `kind-kubefleet` and the project identity.
Delete experiment resources only by verified UID. Never remove finalizers to hide
cleanup bugs, disable fail-closed admission, or grant broad tenant privileges.
Do not publish images, send external messages, or provision paid infrastructure
without explicit task authorization. Test-only fault controls stay out of release
images. Keep kubeconfigs, tokens, certificates, generated passwords and Secrets out
of logs and committed evidence. Full benchmarks require the documented host budget.
