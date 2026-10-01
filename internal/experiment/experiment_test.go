package experiment

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestImmutableDeterministicTraceAndReplay(t *testing.T) {
	a, err := Generate(42, 12, "smoke", "mixed", "worker:dev")
	if err != nil {
		t.Fatal(err)
	}
	b, err := Generate(42, 12, "smoke", "mixed", "worker:dev")
	if err != nil {
		t.Fatal(err)
	}
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	if !bytes.Equal(left, right) {
		t.Fatal("seed did not reproduce trace")
	}
	b.Entries[0].CPU = "1"
	if b.Validate() == nil {
		t.Fatal("unsafe smoke CPU accepted")
	}
	dir := t.TempDir()
	if err := WriteJSONExclusive(dir+"/trace.json", a); err != nil {
		t.Fatal(err)
	}
	if WriteJSONExclusive(dir+"/trace.json", a) == nil {
		t.Fatal("trace overwritten")
	}
	trace, hash, err := ReadTrace(dir + "/trace.json")
	if err != nil {
		t.Fatal(err)
	}
	for i := range trace.Entries {
		trace.Entries[i].AtMilliseconds = 0
	}
	scheme := runtime.NewScheme()
	_ = api.AddToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	submitted, err := Replay(context.Background(), c, trace, hash, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range submitted.Results {
		if !result.Created || result.Attempts != 1 {
			t.Fatal("submission lost")
		}
	}
	second, err := Replay(context.Background(), c, trace, hash, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range second.Results {
		if result.Created || result.Error == "" {
			t.Fatalf("pre-existing run was counted as a fresh create: %+v", result)
		}
	}
}

func TestFullTracePreservesDurationMixAndAllowsSerialDrain(t *testing.T) {
	mixed, err := Generate(42, 60, "full", "mixed", "worker:dev")
	if err != nil {
		t.Fatal(err)
	}
	durations := map[int64]bool{}
	for _, entry := range mixed.Entries {
		durations[entry.DurationMilliseconds] = true
	}
	if len(durations) < 2 {
		t.Fatal("full trace flattened the short/long duration mix")
	}
	burst, err := Generate(42, 120, "full", "asymmetric", "worker:dev")
	if err != nil {
		t.Fatal(err)
	}
	var serialMillis int64
	for _, entry := range burst.Entries {
		serialMillis += entry.DurationMilliseconds
	}
	if serialMillis >= int64(burst.Run(burst.Entries[0]).Spec.TotalTimeoutSeconds)*1000 {
		t.Fatal("serial baseline cannot drain within the total deadline")
	}
}
func TestCollectorSanitizesPodEnvironment(t *testing.T) {
	pod := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"nodeName": "node", "containers": []any{map[string]any{"env": []any{map[string]any{"value": "TOP-SECRET"}}, "args": []any{"TOP-SECRET"}}}}, "status": map[string]any{"phase": "Running", "containerStatuses": []any{map[string]any{"name": "worker", "state": map[string]any{"terminated": map[string]any{"exitCode": float64(1), "message": "TOP-SECRET", "reason": "Error"}}}}}}}
	status, spec := selectedFields("Pod", pod)
	b, _ := json.Marshal([]any{status, spec})
	if strings.Contains(string(b), "TOP-SECRET") {
		t.Fatal("sensitive Pod fields collected")
	}
}

func TestCollectorReadinessCoversEveryTenantResourceWatch(t *testing.T) {
	listKinds := map[schema.GroupVersionResource]string{}
	for _, resource := range collected {
		listKinds[resource.resource] = resource.kind + "List"
	}
	c := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	done := make(chan error, 1)
	go func() {
		err := Collect(ctx, c, writer, "readiness", "isolated", 1)
		_ = writer.Close()
		done <- err
	}()
	ready := map[string]bool{}
	decoder := json.NewDecoder(reader)
	for {
		var observation Observation
		err := decoder.Decode(&observation)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if observation.EventType == "WATCH_READY" {
			ready[observation.Namespace+"/"+observation.Kind] = true
			if len(ready) == 15 {
				cancel()
			}
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(ready) != 15 {
		t.Fatalf("only %d resource watches became ready", len(ready))
	}
}

func TestCollectorResumesClosedWatchAtLastVersionWithoutRelist(t *testing.T) {
	listKinds := map[schema.GroupVersionResource]string{}
	for _, resource := range collected {
		listKinds[resource.resource] = resource.kind + "List"
	}
	c := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds)
	var lists, watches atomic.Int32
	resumed := make(chan string, 1)
	c.PrependReactor("list", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetNamespace() == "team-a" {
			lists.Add(1)
		}
		return false, nil, nil
	})
	c.PrependWatchReactor("pods", func(action ktesting.Action) (bool, watch.Interface, error) {
		if action.GetNamespace() != "team-a" {
			return false, nil, nil
		}
		stream := watch.NewRaceFreeFake()
		if watches.Add(1) == 1 {
			pod := &unstructured.Unstructured{}
			pod.SetUID("observed-pod")
			pod.SetResourceVersion("17")
			stream.Add(pod)
			stream.Stop()
		} else {
			resumed <- action.(ktesting.WatchAction).GetWatchRestrictions().ResourceVersion
		}
		return true, stream, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	done := make(chan error, 1)
	go func() {
		err := Collect(ctx, c, writer, "reconnect", "isolated", 1)
		_ = writer.Close()
		done <- err
	}()
	decoder := json.NewDecoder(reader)
	version := ""
	for {
		var observation Observation
		err := decoder.Decode(&observation)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		select {
		case version = <-resumed:
			cancel()
		default:
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if version != "17" || lists.Load() != 1 {
		t.Fatalf("resume version=%q list calls=%d", version, lists.Load())
	}
}
func TestSummaryCountEquationGapsAndRepeatedPodStarts(t *testing.T) {
	now := time.Now().UTC()
	sub := &Submissions{SchemaVersion: Version, TraceSHA256: strings.Repeat("a", 64), Results: []Submission{{Created: true, UID: "success"}, {Created: true, UID: "failed"}, {Created: true, UID: "cancelled"}, {Created: true, UID: "missing"}, {Created: false, Error: "create failed"}}}
	observations := []Observation{{SchemaVersion: Version, EventType: "QUALITY", QualityReason: "watch_gap"}}
	for _, v := range []struct {
		uid     types.UID
		outcome string
	}{{"success", "Succeeded"}, {"failed", "Failed"}, {"cancelled", "Cancelled"}} {
		observations = append(observations, Observation{SchemaVersion: Version, Kind: "BatchRun", UID: v.uid, CreatedAt: now, Status: map[string]any{"result": map[string]any{"outcome": v.outcome}, "finishedAt": now.Add(time.Second).Format(time.RFC3339Nano)}})
	}
	observations = append(observations, Observation{SchemaVersion: Version, Kind: "Job", UID: "job", Owners: []meta.OwnerReference{{Kind: "BatchRun", APIVersion: api.GroupVersion.String(), UID: "success", Controller: ptr.To(true)}}})
	for _, uid := range []types.UID{"pod-one", "pod-one", "pod-two"} {
		observations = append(observations, Observation{SchemaVersion: Version, Kind: "Pod", UID: uid, Owners: []meta.OwnerReference{{Kind: "Job", APIVersion: "batch/v1", UID: "job", Controller: ptr.To(true)}}, Status: map[string]any{"phase": "Running"}})
	}
	var buffer bytes.Buffer
	for _, o := range observations {
		_ = json.NewEncoder(&buffer).Encode(o)
	}
	result, err := Summarize(&buffer, sub)
	if err != nil {
		t.Fatal(err)
	}
	if result.Created != 4 || result.Succeeded != 1 || result.Failed != 1 || result.Cancelled != 1 || result.Unfinished != 1 || result.SubmissionFailures != 1 || result.PodStarts != 2 {
		t.Fatalf("incorrect accounting %+v", result)
	}
	if result.ActualCPUSeconds != nil || result.MakespanSeconds != nil {
		t.Fatal("fabricated incomplete measurement")
	}
	if !strings.Contains(strings.Join(result.Quality, " "), "collector_watch_or_read_gap") {
		t.Fatal("gap not flagged")
	}
}

func TestReplayRecoversACommittedTimeoutWithoutChangingName(t *testing.T) {
	trace, err := Generate(42, 1, "smoke", "balanced", "worker:dev")
	if err != nil {
		t.Fatal(err)
	}
	s := runtime.NewScheme()
	_ = api.AddToScheme(s)
	base := fake.NewClientBuilder().WithScheme(s).Build()
	calls := 0
	wrapped := interceptor.NewClient(base, interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		calls++
		obj.SetUID("committed-uid")
		if err := c.Create(ctx, obj, opts...); err != nil {
			return err
		}
		return context.DeadlineExceeded
	}})
	result, err := Replay(context.Background(), wrapped, trace, strings.Repeat("a", 64), 1)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || !result.Results[0].Created || result.Results[0].UID != "committed-uid" || !result.Results[0].AmbiguousCommit {
		t.Fatal("ambiguous create not recovered by exact intent")
	}
}
