//go:build integration

package smoke

import (
	"context"
	"encoding/json"
	"fmt"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/materialize"
	"os"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"testing"
	"time"
)

func TestRealBatchRunLifecycle(t *testing.T) {
	raw, err := clientcmd.LoadFromFile(os.Getenv("KUBECONFIG"))
	if err != nil {
		t.Fatal(err)
	}
	if raw.CurrentContext != "kind-kubefleet" {
		t.Fatal("unexpected context")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	if err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{api.AddToScheme, batch.AddToScheme, core.AddToScheme, kueue.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var ns core.Namespace
	if err := c.Get(ctx, types.NamespacedName{Name: "kube-system"}, &ns); err != nil || ns.Labels["batch.kubefleet.dev/project"] != "kubefleet" {
		t.Fatal("project identity missing", err)
	}
	id := fmt.Sprintf("smoke-%d", time.Now().UnixNano())
	dir := os.Getenv("SMOKE_EVIDENCE")
	if dir == "" {
		t.Fatal("SMOKE_EVIDENCE required")
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	save := func(name string, obj any) {
		t.Helper()
		b, err := json.MarshalIndent(obj, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, id+"-"+name+".json"), append(b, '\n'), 0644); err != nil {
			t.Fatal(err)
		}
	}
	await := func(label string, predicate func() (bool, error)) {
		t.Helper()
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		timeout := time.NewTimer(90 * time.Second)
		defer timeout.Stop()
		for {
			ok, err := predicate()
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-timeout.C:
				t.Fatal("timeout " + label)
			case <-tick.C:
			}
		}
	}
	create := func(name, queue, duration string) *api.BatchRun {
		t.Helper()
		r := &api.BatchRun{ObjectMeta: meta.ObjectMeta{Name: id + "-" + name, Namespace: "team-a"}, Spec: api.BatchRunSpec{QueueName: queue, Image: "kubefleet-worker:compat", Command: []string{"/worker"}, Args: []string{"sleep", "--duration=" + duration}, TotalTimeoutSeconds: 300, ExecutionTimeoutSeconds: 180, Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("250m"), core.ResourceMemory: resource.MustParse("64Mi")}, Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("250m"), core.ResourceMemory: resource.MustParse("128Mi")}}}}
		if err := c.Create(ctx, r); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := c.Delete(cleanup, r); client.IgnoreNotFound(err) != nil {
				t.Error(err)
			}
		})
		save(name+"-created", r)
		return r
	}
	refresh := func(r *api.BatchRun) error { return c.Get(ctx, client.ObjectKeyFromObject(r), r) }
	stopped := func(r *api.BatchRun, outcome string) {
		t.Helper()
		await(outcome, func() (bool, error) {
			err := refresh(r)
			return r.Status.Result != nil && r.Status.Result.Outcome == outcome, err
		})
		save(r.Name+"-terminal", r)
	}
	cancelled := func(r *api.BatchRun) {
		t.Helper()
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			if err := refresh(r); err != nil {
				return err
			}
			before := r.DeepCopy()
			r.Spec.Cancel = true
			return c.Patch(ctx, r, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
		}); err != nil {
			t.Fatal(err)
		}
	}
	clean := func(r *api.BatchRun) {
		t.Helper()
		await("owned Job/Pods/Workload gone", func() (bool, error) {
			if r.Status.JobRef == nil {
				return true, nil
			}
			var job batch.Job
			err := c.Get(ctx, types.NamespacedName{Name: r.Status.JobRef.Name, Namespace: r.Namespace}, &job)
			if err != nil && !errors.IsNotFound(err) {
				return false, err
			}
			if err == nil && job.UID == r.Status.JobRef.UID {
				return false, nil
			}
			var pods core.PodList
			if err := c.List(ctx, &pods, client.InNamespace(r.Namespace), client.MatchingLabels{"batch.kubernetes.io/job-name": r.Status.JobRef.Name}); err != nil {
				return false, err
			}
			for _, p := range pods.Items {
				o := meta.GetControllerOf(&p)
				if o != nil && o.UID == r.Status.JobRef.UID {
					return false, nil
				}
			}
			if r.Status.WorkloadRef != nil {
				var w kueue.Workload
				err := c.Get(ctx, types.NamespacedName{Name: r.Status.WorkloadRef.Name, Namespace: r.Namespace}, &w)
				if err != nil && !errors.IsNotFound(err) {
					return false, err
				}
				if err == nil && w.UID == r.Status.WorkloadRef.UID {
					return false, nil
				}
			}
			return true, nil
		})
	}
	t.Run("successful-real-worker", func(t *testing.T) {
		r := create("success", "team-a", "2s")
		stopped(r, "Succeeded")
		if r.Status.JobRef == nil || r.Status.WorkloadRef == nil {
			t.Fatal("missing UID correlation")
		}
		var job batch.Job
		if err := c.Get(ctx, types.NamespacedName{Name: r.Status.JobRef.Name, Namespace: r.Namespace}, &job); err != nil {
			t.Fatal(err)
		}
		if !materialize.Owned(r, &job) || job.Status.Succeeded != 1 {
			t.Fatal("no real successful owned Job")
		}
		save("success-job", job)
		var jobs batch.JobList
		if err := c.List(ctx, &jobs, client.InNamespace(r.Namespace), client.MatchingLabels{materialize.RunUIDLabel: string(r.UID)}); err != nil || len(jobs.Items) != 1 {
			t.Fatal("duplicate Jobs", err)
		}
		bad := r.DeepCopy()
		bad.Spec.Args = []string{"new-execution"}
		if err := c.Update(ctx, bad); err == nil {
			t.Fatal("immutable intent changed")
		}
		save("success-run", r)
	})
	t.Run("cancel-running", func(t *testing.T) {
		r := create("cancel", "team-a", "120s")
		await("Running", func() (bool, error) { err := refresh(r); return r.Status.Phase == "Running", err })
		save("cancel-running", r)
		cancelled(r)
		stopped(r, "Cancelled")
		if r.Status.StopRequestedAt == nil {
			t.Fatal("stop intent not persisted")
		}
		clean(r)
	})
	t.Run("cancel-queued", func(t *testing.T) {
		r := create("queued", "missing-queue", "120s")
		await("queued Job persisted", func() (bool, error) {
			err := refresh(r)
			return r.Status.JobRef != nil && r.Status.Phase == "Queued", err
		})
		cancelled(r)
		stopped(r, "Cancelled")
		clean(r)
	})
	t.Run("lost-job", func(t *testing.T) {
		r := create("lost", "team-a", "120s")
		await("Running", func() (bool, error) { err := refresh(r); return r.Status.Phase == "Running", err })
		var job batch.Job
		if err := c.Get(ctx, types.NamespacedName{Name: r.Status.JobRef.Name, Namespace: r.Namespace}, &job); err != nil {
			t.Fatal(err)
		}
		uid := job.UID
		if err := c.Delete(ctx, &job, client.PropagationPolicy(meta.DeletePropagationForeground), client.Preconditions{UID: &uid}); err != nil {
			t.Fatal(err)
		}
		stopped(r, "Failed")
		if r.Status.Result.Reason != "JobLost" {
			t.Fatal("unexpected loss reason", r.Status.Result.Reason)
		}
		clean(r)
	})
}
