# API guide

Both resources use `batch.kubefleet.dev/v1alpha1`. BatchRun is namespaced with a
status subresource; PrerequisiteGatePolicy is cluster scoped and administrator owned.
The generated CRDs are authoritative for bounds/defaults and printer columns.

BatchRun requires queueName, image, and positive CPU/memory requests and limits.
Limits must cover requests; other resources/claims are unsupported. Parallelism
is 1–16, completions at least parallelism and at most 128, backoff 0–6, total timeout
30–86400 seconds, execution timeout positive and at most total, and prerequisite
wait 5–600 seconds and at most total. Command/args and strings are bounded. Priority
and image must match policy, with immutable image digests outside explicit local mode.
All execution fields are immutable. Cancel only changes false→true.

Optional prerequisite contains a same-namespace Job name, exact immutable UID and
wait timeout. The CLI can resolve an omitted UID for online submission, but the
server requires the concrete UID. Administrator policy controls hold time (1–30,
default 10), maximum wait (5–600), the fixed approval label, ungated allowance,
unmanaged rejection and nonempty registry/priority allowlists.

Status records observedGeneration, phase, eight conditions (Valid, JobCreated,
QuotaReserved, Admitted, PrerequisiteReady, Running, Stopping, Complete), exact Job
and Workload references, durable stop intent, terminal outcome/cause/time, observed
first admission/Running times and prerequisite UID decision. Unknown distinguishes
unobserved state from a false observation. Quota reservation, admission and actual
Running are separate. Succeeded/Failed/Cancelled are terminal and cannot resume.
A new attempt needs a new UID; deleting an owned Job yields JobLost.

Development examples use a loaded local image, never a invented registry digest.
Release examples require a build-produced published/loaded immutable digest and
matching administrator allowlist. See `config/samples/smoke.yaml` and CLI guide.
