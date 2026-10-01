# CLI

Build with `make build`. Standard kubeconfig credentials/context/namespace apply;
use `--kubeconfig`, `--context`, `--namespace`/`-n` to override them. No default
cross-namespace lookup or credential elevation occurs.

```
bin/kubefleet version
bin/kubefleet validate -f config/samples/smoke.yaml
bin/kubefleet validate -f config/samples/smoke.yaml --server
bin/kubefleet submit -f config/samples/smoke.yaml
bin/kubefleet list -n team-a --output json
bin/kubefleet status NAME -n team-a --output yaml
bin/kubefleet wait NAME -n team-a --timeout 120s
bin/kubefleet diagnose NAME -n team-a --output json
bin/kubefleet cancel NAME -n team-a --wait --timeout 60s
```

The manifest parser accepts one document, rejects duplicate/unknown fields and
server-owned status, and applies defaults only to omitted fields. Explicit zero
values remain available for validation. Submit/server validation can resolve an
omitted prerequisite UID by reading the same-namespace approved plain Job; local
validation requires a concrete UID. AlreadyExists reports identity and never
replaces the existing object.

List/status/diagnose support table/json/yaml. Machine output is stdout; resolution
messages, retained-outcome notices and errors are stderr. Wait returns 0 for
Succeeded, 2 for Failed, 3 for Cancelled, 4 for its timeout, and 1 for client/server
errors or interrupted operations. A closed watch triggers relist/reconnect, including
410 expiry. A recreated name with another UID is an error. Cancelling an already
terminal run is a successful no-op and retains its outcome.

Diagnose performs bounded read-only UID correlation and explains queue/gate state,
quota versus admission, suspension, physical scheduling, startup failures, deadlines,
cleanup and lost Jobs. Each finding includes its source and observation time;
Forbidden reads degrade explicitly and events may have expired.

Real tests verify submit/dry-run/wait/terminal codes, timeout, SIGINT, cancellation,
AlreadyExists, outputs and blocked-Pod findings. Reconnect/410 use controlled dynamic
client tests; they are not claims of a real API-server outage.
