package experiment

import (
	"encoding/json"
	"fmt"
	"io"
	"k8s.io/apimachinery/pkg/types"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type CPUExport struct {
	SchemaVersion string       `json:"schemaVersion"`
	Query         string       `json:"query"`
	Start         time.Time    `json:"start"`
	End           time.Time    `json:"end"`
	Prometheus    PromResponse `json:"prometheus"`
}
type PromResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string   `json:"metric"`
			Values [][]json.RawMessage `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

var cgroupUID = regexp.MustCompile(`pod([0-9a-fA-F]{8}[-_][0-9a-fA-F]{4}[-_][0-9a-fA-F]{4}[-_][0-9a-fA-F]{4}[-_][0-9a-fA-F]{12})`)

// ImportCPU reports observed cumulative lower bounds. Prometheus query evaluation
// times are not proof of a final container counter or uninterrupted sampling.
func ImportCPU(input io.Reader, observations []Observation, jobs map[types.UID]types.UID) (*float64, *float64, []string, error) {
	var export CPUExport
	decoder := json.NewDecoder(io.LimitReader(input, 32*1024*1024))
	if err := decoder.Decode(&export); err != nil {
		return nil, nil, nil, err
	}
	if export.SchemaVersion != Version || export.Prometheus.Status != "success" || export.Prometheus.Data.ResultType != "matrix" {
		return nil, nil, nil, fmt.Errorf("unsupported or failed CPU export")
	}
	pods := map[types.UID]Observation{}
	aliases := map[string][]types.UID{}
	evictions := map[types.UID][]time.Time{}
	for _, o := range observations {
		if o.Kind == "Pod" {
			for _, owner := range o.Owners {
				if owner.Kind == "Job" && owner.APIVersion == "batch/v1" && jobs[owner.UID] != "" {
					pods[o.UID] = o
				}
			}
		}
		if o.Kind == "Workload" {
			var jobUID types.UID
			for _, owner := range o.Owners {
				if owner.Kind == "Job" && jobs[owner.UID] != "" {
					jobUID = owner.UID
				}
			}
			if jobUID == "" {
				continue
			}
			conditions, _ := o.Status["conditions"].([]any)
			for _, raw := range conditions {
				c, ok := raw.(map[string]any)
				if ok && c["type"] == "Evicted" && c["status"] == "True" && c["reason"] == "Preempted" {
					t, _ := time.Parse(time.RFC3339Nano, fmt.Sprint(c["lastTransitionTime"]))
					if !t.IsZero() {
						evictions[jobUID] = append(evictions[jobUID], t)
					}
				}
			}
		}
	}
	for uid, pod := range pods {
		aliases[pod.Namespace+"/"+pod.Name] = append(aliases[pod.Namespace+"/"+pod.Name], uid)
	}
	flags := map[string]bool{"cpu_values_are_observed_cumulative_lower_bounds": true, "cpu_final_sample_and_gaps_not_proven": true}
	max := map[types.UID]float64{}
	waste := map[types.UID]float64{}
	for _, series := range export.Prometheus.Data.Result {
		uid := types.UID("")
		if match := cgroupUID.FindStringSubmatch(series.Metric["id"]); len(match) > 1 {
			uid = types.UID(strings.ReplaceAll(match[1], "_", "-"))
		} else {
			candidates := aliases[series.Metric["namespace"]+"/"+series.Metric["pod"]]
			if len(candidates) == 1 {
				uid = candidates[0]
			} else if len(candidates) > 1 {
				flags["cpu_pod_name_ambiguous"] = true
			}
		}
		pod, ok := pods[uid]
		if !ok {
			continue
		}
		if series.Metric["namespace"] != pod.Namespace || series.Metric["pod"] != pod.Name {
			flags["cpu_uid_label_mismatch"] = true
			continue
		}
		previous := -1.0
		for _, pair := range series.Values {
			if len(pair) != 2 {
				return nil, nil, nil, fmt.Errorf("malformed CPU sample")
			}
			var timestamp float64
			var raw string
			if json.Unmarshal(pair[0], &timestamp) != nil || json.Unmarshal(pair[1], &raw) != nil {
				return nil, nil, nil, fmt.Errorf("malformed CPU sample")
			}
			value, err := strconv.ParseFloat(raw, 64)
			if err != nil || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
				flags["cpu_sample_invalid"] = true
				continue
			}
			if previous >= 0 && value < previous {
				flags["cpu_counter_reset"] = true
			}
			previous = value
			if current, exists := max[uid]; !exists || value > current {
				max[uid] = value
			}
			observed := time.Unix(0, int64(timestamp*1e9)).UTC()
			for _, owner := range pod.Owners {
				for _, eviction := range evictions[owner.UID] {
					if interruptedAt(pod, eviction) && !pod.CreatedAt.After(eviction) && !observed.After(eviction) && value > waste[uid] {
						waste[uid] = value
					}
				}
			}
		}
	}
	total, lost := 0.0, 0.0
	for uid, pod := range pods {
		phase, _ := pod.Status["phase"].(string)
		if phase != "Running" && phase != "Succeeded" && phase != "Failed" {
			continue
		}
		if value, ok := max[uid]; ok {
			total += value
		} else {
			flags["cpu_started_pod_samples_missing"] = true
		}
	}
	for _, value := range waste {
		lost += value
	}
	quality := []string{}
	for flag := range flags {
		quality = append(quality, flag)
	}
	if len(max) == 0 {
		quality = append(quality, "cpu_samples_absent")
		return nil, nil, quality, nil
	}
	var wasteValue *float64
	if len(evictions) > 0 && len(waste) > 0 {
		wasteValue = &lost
	} else if len(evictions) > 0 {
		quality = append(quality, "preemption_cpu_samples_missing")
	}
	return &total, wasteValue, quality, nil
}

func interruptedAt(pod Observation, eviction time.Time) bool {
	if pod.Status["phase"] == "Succeeded" {
		return false
	}
	if pod.DeletedAt != nil && !pod.DeletedAt.Time.Before(eviction) {
		return true
	}
	containers, _ := pod.Status["containerStatuses"].([]any)
	for _, raw := range containers {
		container, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		state, _ := container["state"].(map[string]any)
		termination, _ := state["terminated"].(map[string]any)
		finished, _ := time.Parse(time.RFC3339Nano, fmt.Sprint(termination["finishedAt"]))
		if !finished.IsZero() && !finished.Before(eviction) && fmt.Sprint(termination["exitCode"]) != "0" {
			return true
		}
	}
	return false
}
