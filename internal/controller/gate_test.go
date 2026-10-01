package controller

import (
	"context"
	"fmt"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/gate"
	"kubefleet.local/kubefleet/internal/materialize"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"testing"
	"time"
)

func gateFixture(t *testing.T) (*GateReconciler, *api.BatchRun, *batch.Job, *kueue.Workload) {
	t.Helper()
	run := runFixture()
	run.Spec.Prerequisite = &api.PrerequisiteReference{JobName: "prepare", JobUID: "prepare-uid", WaitTimeoutSeconds: 120}
	job, err := materialize.Build(run)
	if err != nil {
		t.Fatal(err)
	}
	job.UID = "worker-job-uid"
	prep := &batch.Job{ObjectMeta: meta.ObjectMeta{Name: "prepare", Namespace: run.Namespace, UID: "prepare-uid", Labels: map[string]string{"batch.kubefleet.dev/prerequisite": "approved"}}, Status: batch.JobStatus{Conditions: []batch.JobCondition{{Type: batch.JobComplete, Status: core.ConditionTrue}}}}
	w := &kueue.Workload{ObjectMeta: meta.ObjectMeta{Name: "workload", Namespace: run.Namespace, UID: "workload-uid", OwnerReferences: []meta.OwnerReference{{Kind: "Job", APIVersion: "batch/v1", Name: job.Name, UID: job.UID, Controller: ptr.To(true)}}}, Status: kueue.WorkloadStatus{AdmissionChecks: []kueue.AdmissionCheckState{{Name: gate.CheckName, State: kueue.CheckStatePending, LastTransitionTime: meta.Now()}, {Name: "unrelated", State: kueue.CheckStatePending, Message: "owned by another controller", LastTransitionTime: meta.Now()}}}}
	policy := &api.PrerequisiteGatePolicy{ObjectMeta: meta.ObjectMeta{Name: "default"}, Spec: api.PrerequisiteGatePolicySpec{MaxReservationHoldSeconds: 1, MaxWaitTimeoutSeconds: 600, RequiredJobLabel: "batch.kubefleet.dev/prerequisite=approved", AllowUngatedBatchRuns: ptr.To(true), RejectUnmanagedWorkloads: ptr.To(true), AllowedImageRegistries: []string{"example.test"}, AllowedPriorityClassNames: []string{"normal"}}}
	check := &kueue.AdmissionCheck{ObjectMeta: meta.ObjectMeta{Name: gate.CheckName}, Spec: kueue.AdmissionCheckSpec{ControllerName: gate.ControllerName, Parameters: &kueue.AdmissionCheckParametersReference{APIGroup: api.GroupVersion.Group, Kind: "PrerequisiteGatePolicy", Name: "default"}}, Status: kueue.AdmissionCheckStatus{Conditions: []meta.Condition{{Type: "Active", Status: meta.ConditionTrue, Reason: "Valid", LastTransitionTime: meta.Now()}}}}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{api.AddToScheme, batch.AddToScheme, core.AddToScheme, kueue.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.BatchRun{}, &kueue.Workload{}, &kueue.AdmissionCheck{}, &batch.Job{}).WithObjects(run, job, prep, w, policy, check).Build()
	return &GateReconciler{Client: c, Reader: c, PolicyName: "default"}, run, prep, w
}
func TestGateReadySnapshotSurvivesCleanupAndReset(t *testing.T) {
	r, run, prep, w := gateFixture(t)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, req.NamespacedName, w); err != nil {
		t.Fatal(err)
	}
	snap := apiMeta.FindStatusCondition(w.Status.Conditions, gate.SnapshotCondition)
	if namedCheck(w).State != kueue.CheckStateReady || snap == nil || !gate.Matches(snap.Message, run) {
		t.Fatal("no durable Ready snapshot")
	}
	if err := r.Delete(ctx, prep); err != nil {
		t.Fatal(err)
	}
	// Simulate Kueue reset and projection lost before a process crash.
	namedCheck(w).State = kueue.CheckStatePending
	namedCheck(w).Message = "Reset to Pending after eviction"
	if err := r.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(run), run); err != nil {
		t.Fatal(err)
	}
	run.Status.PrerequisiteDecision = nil
	if err := r.Status().Update(ctx, run); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, req.NamespacedName, w); err != nil {
		t.Fatal(err)
	}
	if namedCheck(w).State != kueue.CheckStateReady {
		t.Fatal("lost readiness after cleanup/reset")
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(run), run); err != nil {
		t.Fatal(err)
	}
	if run.Status.PrerequisiteDecision == nil || run.Status.PrerequisiteDecision.State != "Ready" {
		t.Fatal("projection not recovered")
	}
}
func TestGateConflictPreservesOtherEntry(t *testing.T) {
	r, _, _, w := gateFixture(t)
	ctx := context.Background()
	base := r.Client
	conflicted := false
	r.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
		if sub == "status" && obj.GetName() == w.Name && !conflicted {
			conflicted = true
			var fresh kueue.Workload
			if err := base.Get(ctx, client.ObjectKeyFromObject(w), &fresh); err != nil {
				return err
			}
			fresh.Status.AdmissionChecks[1].State = kueue.CheckStateReady
			fresh.Status.AdmissionChecks[1].Message = "concurrent owner update"
			if err := base.Status().Update(ctx, &fresh); err != nil {
				return err
			}
			return errors.NewConflict(kueue.GroupVersion.WithResource("workloads").GroupResource(), w.Name, fmt.Errorf("injected real competitor"))
		}
		return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
	}})
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)}); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(ctx, client.ObjectKeyFromObject(w), w); err != nil {
		t.Fatal(err)
	}
	if !conflicted || w.Status.AdmissionChecks[1].Message != "concurrent owner update" || w.Status.AdmissionChecks[1].State != kueue.CheckStateReady {
		t.Fatal("unrelated entry lost")
	}
}
func TestGateTransientReadNeverApproves(t *testing.T) {
	for _, kind := range []string{"forbidden", "timeout", "throttled"} {
		t.Run(kind, func(t *testing.T) {
			r, _, prep, w := gateFixture(t)
			base := r.Client
			r.Reader = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if key == client.ObjectKeyFromObject(prep) {
					switch kind {
					case "forbidden":
						return errors.NewForbidden(batch.SchemeGroupVersion.WithResource("jobs").GroupResource(), key.Name, fmt.Errorf("denied"))
					case "timeout":
						return context.DeadlineExceeded
					default:
						return errors.NewTooManyRequests("slow down", 1)
					}
				}
				return c.Get(ctx, key, obj, opts...)
			}})
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)}); err == nil {
				t.Fatal("transient error hidden")
			}
			if err := base.Get(context.Background(), client.ObjectKeyFromObject(w), w); err != nil {
				t.Fatal(err)
			}
			if namedCheck(w).State != kueue.CheckStatePending || apiMeta.FindStatusCondition(w.Status.Conditions, gate.SnapshotCondition) != nil {
				t.Fatal("fail-open approval")
			}
		})
	}
}
func TestGateRetryPersistsBackoffAndPreservesQuota(t *testing.T) {
	r, _, prep, w := gateFixture(t)
	ctx := context.Background()
	prep.Status.Conditions = nil
	if err := r.Status().Update(ctx, prep); err != nil {
		t.Fatal(err)
	}
	w.Status.Conditions = []meta.Condition{{Type: "QuotaReserved", Status: meta.ConditionTrue, Reason: "Reserved", LastTransitionTime: meta.NewTime(time.Now().Add(-2 * time.Second))}}
	if err := r.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)}); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(w), w); err != nil {
		t.Fatal(err)
	}
	if namedCheck(w).State != kueue.CheckStateRetry || w.Status.RequeueState == nil || w.Status.RequeueState.RequeueAt == nil {
		t.Fatal("Retry did not persist delay")
	}
	if !apiMeta.IsStatusConditionTrue(w.Status.Conditions, "QuotaReserved") {
		t.Fatal("gate removed Kueue reservation condition")
	}
}
