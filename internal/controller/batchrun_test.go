package controller

import (
	"context"
	"fmt"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/materialize"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"testing"
	"time"
)

func runFixture() *api.BatchRun {
	return &api.BatchRun{ObjectMeta: meta.ObjectMeta{Name: "sample", Namespace: "team-a", UID: "run-uid", CreationTimestamp: meta.NewTime(time.Now()), Generation: 1}, Spec: api.BatchRunSpec{QueueName: "team-a", Image: "worker:dev", Parallelism: 1, Completions: 1, TotalTimeoutSeconds: 600, ExecutionTimeoutSeconds: 60, PriorityClassName: "normal", Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("250m"), core.ResourceMemory: resource.MustParse("64Mi")}, Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("500m"), core.ResourceMemory: resource.MustParse("128Mi")}}}}
}
func fakeReconciler(t *testing.T, objects ...client.Object) *BatchRunReconciler {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{api.AddToScheme, batch.AddToScheme, core.AddToScheme, kueue.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&api.BatchRun{}, &batch.Job{}, &kueue.Workload{}).WithObjects(objects...).WithIndex(&kueue.Workload{}, workloadJobUIDIndex, func(o client.Object) []string {
		owner := meta.GetControllerOf(o)
		if owner != nil {
			return []string{string(owner.UID)}
		}
		return nil
	}).Build()
	return &BatchRunReconciler{Client: c, Reader: c, PolicyName: "default"}
}
func reconcileOnce(t *testing.T, r *BatchRunReconciler, run *api.BatchRun) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
}
func fetch(t *testing.T, r *BatchRunReconciler, run *api.BatchRun) {
	t.Helper()
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), run); err != nil {
		t.Fatal(err)
	}
}
func TestCollisionDoesNotAdoptOrDelete(t *testing.T) {
	run := runFixture()
	unrelated := &batch.Job{ObjectMeta: meta.ObjectMeta{Name: materialize.JobName(run.Name, run.UID), Namespace: run.Namespace, UID: "unrelated"}}
	r := fakeReconciler(t, run, unrelated)
	reconcileOnce(t, r, run)
	fetch(t, r, run)
	if run.Status.StopReason != "JobNameCollision" || run.Status.Result != nil {
		t.Fatal("stop intent not durably recorded before terminal cleanup")
	}
	reconcileOnce(t, r, run)
	fetch(t, r, run)
	if run.Status.Result == nil || run.Status.Result.Outcome != "Failed" {
		t.Fatal("collision did not converge to failure")
	}
	var retained batch.Job
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(unrelated), &retained); err != nil || retained.UID != "unrelated" {
		t.Fatal("unrelated Job modified/deleted")
	}
}
func TestDurableStopPrecedesLateCompletion(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "completion-before-stop", true: "stop-before-completion"}[committed], func(t *testing.T) {
			run := runFixture()
			job, err := materialize.Build(run)
			if err != nil {
				t.Fatal(err)
			}
			job.UID = "job-uid"
			run.Spec.Cancel = true
			run.Status.JobRef = &api.ObjectReference{Name: job.Name, UID: job.UID}
			if committed {
				run.Status.StopReason = "UserCancelled"
				now := meta.Now()
				run.Status.StopRequestedAt = &now
			}
			job.Status.Conditions = []batch.JobCondition{{Type: batch.JobComplete, Status: core.ConditionTrue}}
			r := fakeReconciler(t, run, job)
			reconcileOnce(t, r, run)
			if committed {
				reconcileOnce(t, r, run)
			}
			fetch(t, r, run)
			expected := "Succeeded"
			if committed {
				expected = "Cancelled"
			}
			if run.Status.Result == nil || run.Status.Result.Outcome != expected {
				t.Fatalf("result %+v; expected %s", run.Status.Result, expected)
			}
		})
	}
}
func TestRecordedJobLossNeverReexecutes(t *testing.T) {
	run := runFixture()
	run.Status.JobRef = &api.ObjectReference{Name: materialize.JobName(run.Name, run.UID), UID: "lost-job"}
	// A committed JobLost stop must never create work, even without policy access.
	run.Status.StopReason = "JobLost"
	now := meta.Now()
	run.Status.StopRequestedAt = &now
	r := fakeReconciler(t, run)
	reconcileOnce(t, r, run)
	fetch(t, r, run)
	if run.Status.Result == nil || run.Status.Result.Reason != "JobLost" {
		t.Fatal("lost Job result missing")
	}
	var job batch.Job
	if err := r.Get(context.Background(), types.NamespacedName{Name: run.Status.JobRef.Name, Namespace: run.Namespace}, &job); !errors.IsNotFound(err) {
		t.Fatal("lost Job recreated")
	}
}
func TestReplacedChildIsNeverDeleted(t *testing.T) {
	run := runFixture()
	job, err := materialize.Build(run)
	if err != nil {
		t.Fatal(err)
	}
	job.UID = "replacement"
	run.Status.JobRef = &api.ObjectReference{Name: job.Name, UID: "original"}
	r := fakeReconciler(t, run, job)
	reconcileOnce(t, r, run)
	reconcileOnce(t, r, run)
	fetch(t, r, run)
	if run.Status.Result == nil || run.Status.Result.Reason != "JobLost" {
		t.Fatal("replacement failed to converge safely")
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(job), &batch.Job{}); err != nil {
		t.Fatal("replacement deleted")
	}
}

func TestStatusConflictReReadsAndPreservesStop(t *testing.T) {
	run := runFixture()
	r := fakeReconciler(t, run)
	base := r.Client.(client.WithWatch)
	calls := 0
	r.Client = interceptor.NewClient(base, interceptor.Funcs{SubResourcePatch: func(ctx context.Context, c client.Client, name string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
		calls++
		if calls == 1 {
			var fresh api.BatchRun
			if err := base.Get(ctx, client.ObjectKeyFromObject(run), &fresh); err != nil {
				return err
			}
			fresh.Status.StopReason = "UserCancelled"
			now := meta.Now()
			fresh.Status.StopRequestedAt = &now
			fresh.Status.Phase = "Stopping"
			if err := base.Status().Update(ctx, &fresh); err != nil {
				return err
			}
			return errors.NewConflict(api.GroupVersion.WithResource("batchruns").GroupResource(), run.Name, fmt.Errorf("injected competing stop"))
		}
		return c.SubResource(name).Patch(ctx, obj, p, opts...)
	}})
	if err := r.status(context.Background(), run, func(s *api.BatchRunStatus) { s.Phase = "Running" }); err != nil {
		t.Fatal(err)
	}
	fetch(t, r, run)
	if calls != 1 || run.Status.StopReason != "UserCancelled" || run.Status.Phase != "Stopping" {
		t.Fatal("fresh-read retry overwrote committed stop")
	}
}

func TestTransientReadsDoNotMutateOrFailOpen(t *testing.T) {
	for name, injected := range map[string]error{"throttled": errors.NewTooManyRequests("injected", 1), "forbidden": errors.NewForbidden(api.GroupVersion.WithResource("batchruns").GroupResource(), "sample", fmt.Errorf("injected")), "timeout": context.DeadlineExceeded, "absent": errors.NewNotFound(api.GroupVersion.WithResource("batchruns").GroupResource(), "sample")} {
		t.Run(name, func(t *testing.T) {
			run := runFixture()
			r := fakeReconciler(t, run)
			base := r.Client.(client.WithWatch)
			r.Reader = interceptor.NewClient(base, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return injected
			}})
			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
			if name == "absent" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err != injected {
				t.Fatalf("read error incorrectly transformed: %v", err)
			}
			var actual api.BatchRun
			mustErr := base.Get(context.Background(), client.ObjectKeyFromObject(run), &actual)
			if mustErr != nil {
				t.Fatal(mustErr)
			}
			if actual.Status.Phase != "" || actual.Status.Result != nil {
				t.Fatal("transient read emitted a positive or terminal decision")
			}
			var jobs batch.JobList
			if err := base.List(context.Background(), &jobs); err != nil || len(jobs.Items) != 0 {
				t.Fatal("transient read created execution", err)
			}
		})
	}
}

func TestQueuedReadyProjectionDoesNotToggleConditionTimestamp(t *testing.T) {
	run := runFixture()
	run.Spec.Prerequisite = &api.PrerequisiteReference{JobName: "prepare", JobUID: "prepare", WaitTimeoutSeconds: 120}
	run.Status.PrerequisiteDecision = &api.PrerequisiteDecision{JobUID: "prepare", State: "Ready", Reason: "PrerequisiteComplete", LastDecisionTime: meta.Now()}
	job, err := materialize.Build(run)
	if err != nil {
		t.Fatal(err)
	}
	job.UID = "job"
	w := &kueue.Workload{ObjectMeta: meta.ObjectMeta{Name: "workload", Namespace: run.Namespace, UID: "workload", OwnerReferences: []meta.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: boolPointer(true)}}}}
	r := fakeReconciler(t, run, job, w)
	reconcileOnce(t, r, run)
	fetch(t, r, run)
	version := run.ResourceVersion
	for i := 0; i < 3; i++ {
		reconcileOnce(t, r, run)
		fetch(t, r, run)
		if run.ResourceVersion != version {
			t.Fatal("Ready status rewrote transition timestamp while queued")
		}
	}
}
func boolPointer(v bool) *bool { return &v }
