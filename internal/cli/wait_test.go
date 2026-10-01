package cli

import (
	"context"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"testing"
	"time"
)

func TestWaitRelistsAfterCloseAndExpiredVersion(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = api.AddToScheme(scheme)
	run := &api.BatchRun{TypeMeta: meta.TypeMeta{APIVersion: api.GroupVersion.String(), Kind: "BatchRun"}, ObjectMeta: meta.ObjectMeta{Name: "run", Namespace: "team-a", UID: "uid", ResourceVersion: "1"}}
	gvr := api.GroupVersion.WithResource("batchruns")
	c := dynamicfake.NewSimpleDynamicClient(scheme, run)
	attempts := 0
	c.PrependWatchReactor("batchruns", func(ktesting.Action) (bool, watch.Interface, error) {
		attempts++
		w := watch.NewRaceFreeFake()
		switch attempts {
		case 1:
			w.Stop()
		case 2:
			w.Error(&meta.Status{Status: meta.StatusFailure, Code: 410, Reason: meta.StatusReasonExpired})
		default:
			done := run.DeepCopy()
			done.Status.Result = &api.RunResult{Outcome: "Failed", Reason: "WorkerFailed"}
			obj, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(done)
			w.Modify(&unstructured.Unstructured{Object: obj})
		}
		return true, w, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done, code, err := Wait(ctx, c.Resource(gvr).Namespace("team-a"), "run")
	if err != nil || code != 2 || done == nil || attempts != 3 {
		t.Fatalf("code %d attempts %d error %v", code, attempts, err)
	}
	listCount := 0
	for _, a := range c.Actions() {
		if a.GetVerb() == "list" {
			listCount++
		}
	}
	if listCount != 3 {
		t.Fatal("did not relist after disconnect/410")
	}
}
func TestWaitTimeoutAndCancel(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = api.AddToScheme(scheme)
	run := &api.BatchRun{TypeMeta: meta.TypeMeta{APIVersion: api.GroupVersion.String(), Kind: "BatchRun"}, ObjectMeta: meta.ObjectMeta{Name: "run", Namespace: "team-a", UID: "uid"}}
	for _, timeout := range []bool{false, true} {
		c := dynamicfake.NewSimpleDynamicClient(scheme, run)
		c.PrependWatchReactor("batchruns", func(ktesting.Action) (bool, watch.Interface, error) { return true, watch.NewRaceFreeFake(), nil })
		var ctx context.Context
		var cancel context.CancelFunc
		if timeout {
			ctx, cancel = context.WithTimeout(context.Background(), 30*time.Millisecond)
		} else {
			ctx, cancel = context.WithCancel(context.Background())
			cancel()
		}
		start := time.Now()
		_, code, err := Wait(ctx, c.Resource(api.GroupVersion.WithResource("batchruns")).Namespace("team-a"), "run")
		cancel()
		want := 1
		if timeout {
			want = 4
		}
		if code != want || err == nil || time.Since(start) > time.Second {
			t.Fatalf("code=%d err=%v", code, err)
		}
	}
}
