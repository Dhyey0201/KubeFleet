package experiment

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"k8s.io/apimachinery/pkg/types"
	"math"
	"sort"
	"time"
)

type Summary struct {
	AmbiguousUnresolvedCreates                  int                     `json:"ambiguousUnresolvedCreates"`
	Teams                                       map[string]*TeamSummary `json:"teams"`
	CPUObservedLowerBoundSeconds                *float64                `json:"cpuObservedLowerBoundSeconds"`
	PreemptionCPUWasteObservedLowerBoundSeconds *float64                `json:"preemptionCPUWasteObservedLowerBoundSeconds"`
	SchemaVersion                               string                  `json:"schemaVersion"`
	TraceSHA256                                 string                  `json:"traceSHA256"`
	Created                                     int                     `json:"created"`
	SubmissionFailures                          int                     `json:"submissionFailures"`
	Succeeded                                   int                     `json:"succeeded"`
	Failed                                      int                     `json:"failed"`
	Cancelled                                   int                     `json:"cancelled"`
	Unfinished                                  int                     `json:"unfinished"`
	AdmissionWaitSeconds                        Distribution            `json:"admissionWaitSeconds"`
	CompletionSeconds                           Distribution            `json:"completionSeconds"`
	MakespanSeconds                             *float64                `json:"makespanSeconds"`
	CompletedThroughputPerSecond                *float64                `json:"completedThroughputPerSecond"`
	AdmittedRequestedCPUSeconds                 *float64                `json:"admittedRequestedCPUSeconds"`
	ActualCPUSeconds                            *float64                `json:"actualCPUSeconds"`
	PreemptionCPUWasteSeconds                   *float64                `json:"preemptionCPUWasteSeconds"`
	PodStarts                                   int                     `json:"podStarts"`
	Evictions                                   int                     `json:"evictions"`
	Quality                                     []string                `json:"quality"`
	Outcomes                                    map[types.UID]string    `json:"outcomes"`
}
type TeamSummary struct {
	Created                       int          `json:"created"`
	Succeeded                     int          `json:"succeeded"`
	Failed                        int          `json:"failed"`
	Cancelled                     int          `json:"cancelled"`
	Unfinished                    int          `json:"unfinished"`
	AdmissionWaitSeconds          Distribution `json:"admissionWaitSeconds"`
	CompletionSeconds             Distribution `json:"completionSeconds"`
	SuccessfulThroughputPerSecond *float64     `json:"successfulThroughputPerSecond"`
}
type Distribution struct {
	Samples int      `json:"samples"`
	Median  *float64 `json:"median"`
	P95     *float64 `json:"p95"`
}

func distribution(values []float64) Distribution {
	d := Distribution{Samples: len(values)}
	if len(values) == 0 {
		return d
	}
	sort.Float64s(values)
	mid := values[len(values)/2]
	if len(values)%2 == 0 {
		mid = (values[len(values)/2-1] + values[len(values)/2]) / 2
	}
	p95 := values[int(math.Ceil(float64(len(values))*.95))-1]
	d.Median = &mid
	d.P95 = &p95
	return d
}
func ReadSubmissions(r io.Reader) (*Submissions, error) {
	var s Submissions
	d := json.NewDecoder(io.LimitReader(r, 4*1024*1024))
	d.DisallowUnknownFields()
	if err := d.Decode(&s); err != nil {
		return nil, err
	}
	if s.SchemaVersion != Version {
		return nil, fmt.Errorf("unsupported submission version")
	}
	return &s, nil
}

// Summarize uses independently collected UID observations and leaves unavailable
// CPU and interval measurements null instead of substituting requested resources.
func Summarize(input io.Reader, submissions *Submissions) (*Summary, error) {
	return SummarizeWithCPU(input, submissions, nil)
}
func SummarizeWithCPU(input io.Reader, submissions *Submissions, cpu io.Reader) (*Summary, error) {
	result := &Summary{SchemaVersion: Version, TraceSHA256: submissions.TraceSHA256, Quality: []string{}, Outcomes: map[types.UID]string{}, Teams: map[string]*TeamSummary{}}
	expected := map[types.UID]bool{}
	namespaces := map[types.UID]string{}
	teamWaits, teamCompletions := map[string][]float64{}, map[string][]float64{}
	teamFirst, teamLast := map[string]time.Time{}, map[string]time.Time{}
	for _, s := range submissions.Results {
		if s.Created {
			if s.UID == "" {
				return nil, fmt.Errorf("created submission lacks UID")
			}
			if expected[s.UID] {
				return nil, fmt.Errorf("duplicate created UID")
			}
			expected[s.UID] = true
			result.Created++
			namespaces[s.UID] = s.Namespace
			if result.Teams[s.Namespace] == nil {
				result.Teams[s.Namespace] = &TeamSummary{}
			}
			result.Teams[s.Namespace].Created++
		} else {
			if s.AmbiguousCommit {
				result.AmbiguousUnresolvedCreates++
			} else {
				result.SubmissionFailures++
			}
		}
	}
	scan := bufio.NewScanner(input)
	scan.Buffer(make([]byte, 4096), 2*1024*1024)
	latest := map[types.UID]Observation{}
	all := []Observation{}
	flags := map[string]bool{}
	if result.AmbiguousUnresolvedCreates > 0 {
		flags["ambiguous_create_outcome_unresolved"] = true
	}
	for scan.Scan() {
		var o Observation
		if err := json.Unmarshal(scan.Bytes(), &o); err != nil {
			return nil, err
		}
		if o.SchemaVersion != Version {
			return nil, fmt.Errorf("unsupported observation version")
		}
		if o.EventType == "QUALITY" {
			flags["collector_watch_or_read_gap"] = true
		}
		all = append(all, o)
		if o.UID != "" {
			latest[o.UID] = o
		}
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	waits, completions := []float64{}, []float64{}
	var earliest, latestFinish time.Time
	for uid := range expected {
		o, exists := latest[uid]
		ns := namespaces[uid]
		team := result.Teams[ns]
		outcome := "Unfinished"
		if exists && o.Kind == "BatchRun" {
			if r, ok := o.Status["result"].(map[string]any); ok {
				if v, ok := r["outcome"].(string); ok {
					switch v {
					case "Succeeded", "Failed", "Cancelled":
						outcome = v
					default:
						flags["unknown_terminal_outcome"] = true
					}
				}
			}
			if t := statusTime(o.Status, "firstAdmittedAt"); !t.IsZero() {
				if !o.CreatedAt.IsZero() && t.Before(o.CreatedAt) {
					flags["invalid_source_time"] = true
				} else if !o.CreatedAt.IsZero() {
					waits = append(waits, t.Sub(o.CreatedAt).Seconds())
					teamWaits[ns] = append(teamWaits[ns], t.Sub(o.CreatedAt).Seconds())
				}
			}
			if t := statusTime(o.Status, "finishedAt"); !t.IsZero() && outcome != "Unfinished" && !o.CreatedAt.IsZero() {
				if t.Before(o.CreatedAt) {
					flags["invalid_source_time"] = true
				} else {
					completions = append(completions, t.Sub(o.CreatedAt).Seconds())
					teamCompletions[ns] = append(teamCompletions[ns], t.Sub(o.CreatedAt).Seconds())
					if teamLast[ns].IsZero() || t.After(teamLast[ns]) {
						teamLast[ns] = t
					}
					if latestFinish.IsZero() || t.After(latestFinish) {
						latestFinish = t
					}
				}
			}
			if !o.CreatedAt.IsZero() && (earliest.IsZero() || o.CreatedAt.Before(earliest)) {
				earliest = o.CreatedAt
			}
			if !o.CreatedAt.IsZero() && (teamFirst[ns].IsZero() || o.CreatedAt.Before(teamFirst[ns])) {
				teamFirst[ns] = o.CreatedAt
			}
		} else {
			flags["created_run_not_observed"] = true
		}
		result.Outcomes[uid] = outcome
		switch outcome {
		case "Succeeded":
			result.Succeeded++
			team.Succeeded++
		case "Failed":
			result.Failed++
			team.Failed++
		case "Cancelled":
			result.Cancelled++
			team.Cancelled++
		default:
			result.Unfinished++
			team.Unfinished++
		}
	}
	jobs := map[types.UID]types.UID{}
	for uid, o := range latest {
		if o.Kind != "Job" {
			continue
		}
		for _, owner := range o.Owners {
			if owner.Kind == "BatchRun" && owner.APIVersion == "batch.kubefleet.dev/v1alpha1" && owner.Controller != nil && *owner.Controller && expected[owner.UID] {
				jobs[uid] = owner.UID
			}
		}
	}
	pods := map[types.UID]bool{}
	evictions := map[string]bool{}
	for _, o := range all {
		if o.Kind != "Pod" && o.Kind != "Workload" {
			continue
		}
		matched := false
		for _, owner := range o.Owners {
			if owner.Kind == "Job" && owner.APIVersion == "batch/v1" && owner.Controller != nil && *owner.Controller && jobs[owner.UID] != "" {
				matched = true
			}
		}
		if !matched {
			continue
		}
		if o.Kind == "Pod" {
			if phase, ok := o.Status["phase"].(string); ok && (phase == "Running" || phase == "Succeeded" || phase == "Failed") {
				pods[o.UID] = true
			}
		}
		if o.Kind == "Workload" {
			conditions, _ := o.Status["conditions"].([]any)
			for _, raw := range conditions {
				c, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				if c["type"] == "Evicted" && c["status"] == "True" {
					key := string(o.UID) + "/" + fmt.Sprint(c["lastTransitionTime"])
					evictions[key] = true
				}
			}
		}
	}
	for ns, team := range result.Teams {
		team.AdmissionWaitSeconds = distribution(teamWaits[ns])
		team.CompletionSeconds = distribution(teamCompletions[ns])
		if team.Created != team.Succeeded+team.Failed+team.Cancelled+team.Unfinished {
			return nil, fmt.Errorf("team count equation violated")
		}
		if team.Unfinished == 0 && !teamFirst[ns].IsZero() && !teamLast[ns].IsZero() {
			seconds := teamLast[ns].Sub(teamFirst[ns]).Seconds()
			if seconds > 0 {
				value := float64(team.Succeeded) / seconds
				team.SuccessfulThroughputPerSecond = &value
			}
		}
	}
	result.PodStarts = len(pods)
	result.Evictions = len(evictions)
	result.AdmissionWaitSeconds = distribution(waits)
	result.CompletionSeconds = distribution(completions)
	if !earliest.IsZero() && !latestFinish.IsZero() && result.Unfinished == 0 {
		seconds := latestFinish.Sub(earliest).Seconds()
		if seconds > 0 {
			result.MakespanSeconds = &seconds
			throughput := float64(result.Succeeded) / seconds
			result.CompletedThroughputPerSecond = &throughput
		}
	} else {
		flags["makespan_incomplete"] = true
	}
	if cpu != nil {
		lower, wasteLower, quality, err := ImportCPU(cpu, all, jobs)
		if err != nil {
			return nil, err
		}
		result.CPUObservedLowerBoundSeconds = lower
		result.PreemptionCPUWasteObservedLowerBoundSeconds = wasteLower
		for _, flag := range quality {
			flags[flag] = true
		}
	} else {
		flags["actual_cpu_not_collected_by_object_watch"] = true
	}
	flags["preemption_cpu_waste_requires_per_pod_cpu_samples"] = true
	var integralQuality []string
	result.AdmittedRequestedCPUSeconds, integralQuality = RequestedCPUIntegral(all, jobs)
	for _, flag := range integralQuality {
		flags[flag] = true
	}
	for flag := range flags {
		result.Quality = append(result.Quality, flag)
	}
	sort.Strings(result.Quality)
	if result.Created != result.Succeeded+result.Failed+result.Cancelled+result.Unfinished {
		return nil, fmt.Errorf("count equation violated")
	}
	return result, nil
}
func statusTime(status map[string]any, key string) time.Time {
	value, ok := status[key].(string)
	if !ok {
		return time.Time{}
	}
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed
}
