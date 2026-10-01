package experiment

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"testing"
	"time"
)

func TestRequestedIntegralRetainsFinishedHistoryButClosesTime(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Second)
	w := Observation{Kind: "Workload", UID: "workload", Owners: []meta.OwnerReference{{Kind: "Job", APIVersion: "batch/v1", UID: "job"}}, Status: map[string]any{"conditions": []any{map[string]any{"type": "Admitted", "status": "True", "lastTransitionTime": start.Format(time.RFC3339)}}, "admission": map[string]any{"podSetAssignments": []any{map[string]any{"resourceUsage": map[string]any{"cpu": "250m"}}}}}}
	done := w
	done.Status = map[string]any{"conditions": []any{map[string]any{"type": "Admitted", "status": "True", "lastTransitionTime": start.Format(time.RFC3339)}, map[string]any{"type": "Finished", "status": "True", "lastTransitionTime": start.Add(10 * time.Second).Format(time.RFC3339)}}, "admission": w.Status["admission"]}
	observations := []Observation{w, w, done}
	integral, quality := RequestedCPUIntegral(observations, map[types.UID]types.UID{"job": "run"})
	if integral == nil || *integral != 2.5 || len(quality) != 0 {
		t.Fatalf("integral %v quality %v", integral, quality)
	}
	observations = append(observations, Observation{EventType: "QUALITY"})
	integral, quality = RequestedCPUIntegral(observations, map[types.UID]types.UID{"job": "run"})
	if integral != nil || len(quality) == 0 {
		t.Fatal("gap fabricated a complete interval")
	}
}
