package experiment

import (
	"bytes"
	"encoding/json"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"testing"
	"time"
)

func TestCPUImportIsLowerBoundAndDoesNotCountCompletedWorkAsWaste(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Second)
	eviction := start.Add(5 * time.Second)
	observations := []Observation{
		{Kind: "Pod", UID: "victim", Name: "victim", Namespace: "team-a", CreatedAt: start, DeletedAt: &meta.Time{Time: eviction.Add(time.Second)}, Owners: []meta.OwnerReference{{Kind: "Job", APIVersion: "batch/v1", UID: "job"}}, Status: map[string]any{"phase": "Failed"}},
		{Kind: "Pod", UID: "completed", Name: "completed", Namespace: "team-a", CreatedAt: start, Owners: []meta.OwnerReference{{Kind: "Job", APIVersion: "batch/v1", UID: "job"}}, Status: map[string]any{"phase": "Succeeded"}},
		{Kind: "Workload", UID: "workload", Owners: []meta.OwnerReference{{Kind: "Job", APIVersion: "batch/v1", UID: "job"}}, Status: map[string]any{"conditions": []any{map[string]any{"type": "Evicted", "status": "True", "reason": "Preempted", "lastTransitionTime": eviction.Format(time.RFC3339Nano)}}}},
	}
	matrix := map[string]any{"schemaVersion": "v1", "prometheus": map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": []any{
		map[string]any{"metric": map[string]string{"pod": "victim", "namespace": "team-a"}, "values": []any{[]any{float64(start.Add(time.Second).Unix()), "0.5"}, []any{float64(start.Add(4 * time.Second).Unix()), "1.0"}}},
		map[string]any{"metric": map[string]string{"pod": "completed", "namespace": "team-a"}, "values": []any{[]any{float64(start.Add(time.Second).Unix()), "0.7"}}},
	}}}}
	b, _ := json.Marshal(matrix)
	cpu, waste, quality, err := ImportCPU(bytes.NewReader(b), observations, map[types.UID]types.UID{"job": "run"})
	if err != nil || cpu == nil || *cpu != 1.7 || waste == nil || *waste != 1 || len(quality) == 0 {
		t.Fatalf("cpu=%v waste=%v quality=%v err=%v", cpu, waste, quality, err)
	}
}
