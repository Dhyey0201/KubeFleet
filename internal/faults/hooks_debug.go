//go:build faultinject

package faults

import (
	"encoding/json"
	"os"
	"time"
)

const Enabled = true

// Hit blocks the targeted reconciliation after publishing a durable boundary
// observation. Tests kill the Pod; tenant inputs cannot enable the build or env.
func Hit(point, identity string) {
	if os.Getenv("KUBEFLEET_FAULT_POINT") != point || os.Getenv("KUBEFLEET_FAULT_RUN") != identity {
		return
	}
	b, _ := json.Marshal(map[string]any{"event": "fault_boundary_reached", "point": point, "identity": identity, "time": time.Now().UTC()})
	_, _ = os.Stdout.Write(append(b, '\n'))
	select {}
}
