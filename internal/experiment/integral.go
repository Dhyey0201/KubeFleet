package experiment

import (
	"fmt"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"time"
)

// RequestedCPUIntegral reconstructs closed admitted intervals from source
// condition times. It measures requested CPU, never actual CPU consumption.
func RequestedCPUIntegral(observations []Observation, jobs map[types.UID]types.UID) (*float64, []string) {
	type interval struct {
		active bool
		start  time.Time
		cpu    float64
	}
	states := map[types.UID]interval{}
	total := 0.0
	seen := false
	flags := map[string]bool{}
	for _, o := range observations {
		if o.EventType == "QUALITY" {
			flags["requested_cpu_integral_watch_gap"] = true
		}
		if o.Kind != "Workload" {
			continue
		}
		matched := false
		for _, owner := range o.Owners {
			if owner.Kind == "Job" && owner.APIVersion == "batch/v1" && jobs[owner.UID] != "" {
				matched = true
			}
		}
		if !matched {
			continue
		}
		seen = true
		admitted, finished := false, false
		var admittedAt, finishedAt time.Time
		conditions, _ := o.Status["conditions"].([]any)
		for _, raw := range conditions {
			c, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			t, _ := time.Parse(time.RFC3339Nano, fmt.Sprint(c["lastTransitionTime"]))
			if c["type"] == "Admitted" {
				admitted = c["status"] == "True"
				admittedAt = t
			}
			if c["type"] == "Finished" && c["status"] == "True" {
				finished = true
				finishedAt = t
			}
		}
		active := admitted && !finished && o.DeletedAt == nil && o.EventType != "DELETED"
		cpu := 0.0
		if active {
			admission, _ := o.Status["admission"].(map[string]any)
			assignments, _ := admission["podSetAssignments"].([]any)
			for _, raw := range assignments {
				assignment, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				usage, _ := assignment["resourceUsage"].(map[string]any)
				if value, exists := usage["cpu"]; exists {
					q, err := resource.ParseQuantity(fmt.Sprint(value))
					if err != nil {
						flags["requested_cpu_quantity_invalid"] = true
					} else {
						cpu += q.AsApproximateFloat64()
					}
				}
			}
			if cpu <= 0 {
				flags["admitted_cpu_assignment_missing"] = true
			}
		}
		previous := states[o.UID]
		if previous.active == active && (!active || previous.cpu == cpu) {
			continue
		}
		boundary := admittedAt
		if finished {
			boundary = finishedAt
		}
		if o.DeletedAt != nil && !finished && (boundary.IsZero() || admitted) {
			boundary = o.DeletedAt.Time
		}
		if boundary.IsZero() {
			flags["requested_cpu_source_time_missing"] = true
			boundary = o.ObservedAt
		}
		if previous.active {
			if boundary.Before(previous.start) {
				flags["requested_cpu_source_time_reversed"] = true
			} else {
				total += previous.cpu * boundary.Sub(previous.start).Seconds()
			}
		}
		states[o.UID] = interval{active: active, start: boundary, cpu: cpu}
	}
	for _, state := range states {
		if state.active {
			flags["requested_cpu_interval_unclosed"] = true
		}
	}
	if !seen {
		flags["requested_cpu_workload_unobserved"] = true
	}
	quality := []string{}
	for flag := range flags {
		quality = append(quality, flag)
	}
	if len(quality) > 0 {
		return nil, quality
	}
	return &total, nil
}
