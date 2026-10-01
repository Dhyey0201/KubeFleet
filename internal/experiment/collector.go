package experiment

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"sync"
	"time"
)

type Observation struct {
	SchemaVersion   string                `json:"schemaVersion"`
	ExperimentID    string                `json:"experimentID"`
	Profile         string                `json:"profile"`
	Repetition      int                   `json:"repetition"`
	ObservedAt      time.Time             `json:"observedAt"`
	EventType       string                `json:"eventType"`
	Kind            string                `json:"kind,omitempty"`
	Namespace       string                `json:"namespace,omitempty"`
	Name            string                `json:"name,omitempty"`
	UID             types.UID             `json:"uid,omitempty"`
	ResourceVersion string                `json:"resourceVersion,omitempty"`
	CreatedAt       time.Time             `json:"createdAt,omitempty"`
	DeletedAt       *meta.Time            `json:"deletedAt,omitempty"`
	Owners          []meta.OwnerReference `json:"owners,omitempty"`
	Status          map[string]any        `json:"status,omitempty"`
	Spec            map[string]any        `json:"spec,omitempty"`
	QualityReason   string                `json:"qualityReason,omitempty"`
}

var collected = []struct {
	kind     string
	resource schema.GroupVersionResource
}{
	{"BatchRun", schema.GroupVersionResource{Group: "batch.kubefleet.dev", Version: "v1alpha1", Resource: "batchruns"}},
	{"Job", schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}},
	{"Pod", schema.GroupVersionResource{Version: "v1", Resource: "pods"}},
	{"Workload", schema.GroupVersionResource{Group: "kueue.x-k8s.io", Version: "v1beta1", Resource: "workloads"}},
	{"Event", schema.GroupVersionResource{Version: "v1", Resource: "events"}},
}

// Collect watches bounded tenant namespaces and explicit resource kinds only.
// It writes selected fields, never raw Pod specs, environment, credentials or Secrets.
func Collect(ctx context.Context, c dynamic.Interface, out io.Writer, experimentID, profile string, repetition int) error {
	if experimentID == "" || repetition < 1 {
		return fmt.Errorf("experiment identity and positive repetition required")
	}
	switch profile {
	case "isolated", "shared", "priority", "controller":
	default:
		return fmt.Errorf("invalid profile")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	encoder := json.NewEncoder(out)
	var writeErr error
	seen := map[string]string{}
	emit := func(kind, ns, event, quality string, obj *unstructured.Unstructured) {
		mu.Lock()
		defer mu.Unlock()
		if writeErr != nil {
			return
		}
		obs := Observation{SchemaVersion: Version, ExperimentID: experimentID, Profile: profile, Repetition: repetition, ObservedAt: time.Now().UTC(), EventType: event, Kind: kind, Namespace: ns, QualityReason: quality}
		if obj != nil {
			key := kind + "/" + string(obj.GetUID())
			if event != "DELETED" && seen[key] == obj.GetResourceVersion() {
				return
			}
			seen[key] = obj.GetResourceVersion()
			obs.Name = obj.GetName()
			obs.UID = obj.GetUID()
			obs.ResourceVersion = obj.GetResourceVersion()
			obs.CreatedAt = obj.GetCreationTimestamp().Time
			obs.DeletedAt = obj.GetDeletionTimestamp()
			obs.Owners = obj.GetOwnerReferences()
			obs.Status, obs.Spec = selectedFields(kind, obj)
		}
		if err := encoder.Encode(obs); err != nil {
			writeErr = err
			cancel()
		}
	}
	emit("", "", "COLLECTOR_STARTED", "", nil)
	var wg sync.WaitGroup
	for _, ns := range []string{"team-a", "team-b", "team-c"} {
		for _, resource := range collected {
			wg.Add(1)
			go func(ns, kind string, gvr schema.GroupVersionResource) {
				defer wg.Done()
				rc := c.Resource(gvr).Namespace(ns)
				first := true
				needsList := true
				resourceVersion := ""
				for ctx.Err() == nil {
					if needsList {
						listCtx, stop := context.WithTimeout(ctx, 10*time.Second)
						list, err := rc.List(listCtx, meta.ListOptions{})
						stop()
						if err != nil {
							emit(kind, ns, "QUALITY", "list_error: "+err.Error(), nil)
							if !collectorPause(ctx) {
								return
							}
							continue
						}
						if !first {
							emit(kind, ns, "QUALITY", "watch_gap_relist: intermediate transitions may be unobserved", nil)
						}
						first = false
						for i := range list.Items {
							emit(kind, ns, "LIST", "", &list.Items[i])
						}
						resourceVersion = list.GetResourceVersion()
						needsList = false
					}
					timeout := int64(300)
					stream, err := rc.Watch(ctx, meta.ListOptions{ResourceVersion: resourceVersion, TimeoutSeconds: &timeout, AllowWatchBookmarks: true})
					if err != nil {
						if apiErrors.IsResourceExpired(err) || apiErrors.IsGone(err) {
							needsList = true
						}
						emit(kind, ns, "QUALITY", "watch_setup_error: "+err.Error(), nil)
						if !collectorPause(ctx) {
							return
						}
						continue
					}
					emit(kind, ns, "WATCH_READY", "", nil)
					disconnected := false
					for !disconnected {
						select {
						case <-ctx.Done():
							stream.Stop()
							return
						case event, ok := <-stream.ResultChan():
							if !ok {
								disconnected = true
								break
							}
							if event.Type != watch.Error {
								if accessor, err := apiMeta.Accessor(event.Object); err == nil && accessor.GetResourceVersion() != "" {
									resourceVersion = accessor.GetResourceVersion()
								}
							}
							if event.Type == watch.Bookmark {
								continue
							}
							if event.Type == watch.Error {
								emit(kind, ns, "QUALITY", "watch_error: server version may have expired", nil)
								needsList = true
								disconnected = true
								break
							}
							if obj, ok := event.Object.(*unstructured.Unstructured); ok {
								emit(kind, ns, string(event.Type), "", obj)
							} else {
								emit(kind, ns, "QUALITY", "unsupported watch payload", nil)
							}
						}
					}
					stream.Stop()
					if !collectorPause(ctx) {
						return
					}
				}
			}(ns, resource.kind, resource.resource)
		}
	}
	wg.Wait()
	emit("", "", "COLLECTOR_STOPPED", "collection ended; unobserved intervals remain unknown", nil)
	return writeErr
}
func collectorPause(ctx context.Context) bool {
	timer := time.NewTimer(250 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func selectedFields(kind string, u *unstructured.Unstructured) (map[string]any, map[string]any) {
	status, spec := map[string]any{}, map[string]any{}
	statusFields := []string{}
	specFields := []string{}
	switch kind {
	case "BatchRun":
		statusFields = []string{"observedGeneration", "phase", "conditions", "jobRef", "workloadRef", "stopReason", "stopRequestedAt", "result", "firstAdmittedAt", "firstRunningObservedAt", "finishedAt", "prerequisiteDecision", "reason"}
		specFields = []string{"queueName", "resources", "parallelism", "completions", "totalTimeoutSeconds", "executionTimeoutSeconds", "priorityClassName", "prerequisite", "cancel"}
	case "Job":
		statusFields = []string{"conditions", "active", "succeeded", "failed", "startTime", "completionTime"}
		specFields = []string{"suspend", "parallelism", "completions", "backoffLimit", "activeDeadlineSeconds"}
	case "Workload":
		statusFields = []string{"conditions", "admission", "admissionChecks", "requeueState", "accumulatedPastExexcutionTimeSeconds"}
		specFields = []string{"queueName", "priority", "active"}
	case "Pod":
		statusFields = []string{"phase", "conditions", "startTime"}
		specFields = []string{"nodeName"}
	case "Event":
		for _, name := range []string{"reason", "type", "count", "firstTimestamp", "lastTimestamp", "eventTime", "involvedObject"} {
			if v, ok, _ := unstructured.NestedFieldCopy(u.Object, name); ok {
				status[name] = v
			}
		}
	}
	for _, name := range statusFields {
		if v, ok, _ := unstructured.NestedFieldCopy(u.Object, "status", name); ok {
			status[name] = v
		}
	}
	for _, name := range specFields {
		if v, ok, _ := unstructured.NestedFieldCopy(u.Object, "spec", name); ok {
			spec[name] = v
		}
	}
	if kind == "Pod" {
		containers, _, _ := unstructured.NestedSlice(u.Object, "status", "containerStatuses")
		clean := []any{}
		for _, v := range containers {
			raw, ok := v.(map[string]any)
			if !ok {
				continue
			}
			item := map[string]any{"name": raw["name"], "restartCount": raw["restartCount"]}
			for _, field := range []string{"state", "lastState"} {
				if state, ok := raw[field].(map[string]any); ok {
					filtered := map[string]any{}
					for _, typ := range []string{"running", "waiting", "terminated"} {
						if value, ok := state[typ].(map[string]any); ok {
							keys := map[string]any{}
							for _, key := range []string{"startedAt", "finishedAt", "exitCode", "reason", "signal"} {
								if v, exists := value[key]; exists {
									keys[key] = v
								}
							}
							filtered[typ] = keys
						}
					}
					item[field] = filtered
				}
			}
			clean = append(clean, item)
		}
		status["containerStatuses"] = clean
	}
	return status, spec
}
