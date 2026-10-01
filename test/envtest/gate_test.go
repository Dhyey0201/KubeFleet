//go:build envtest

package envtest

import (
	"context"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/controller"
	"kubefleet.local/kubefleet/internal/gate"
	"kubefleet.local/kubefleet/internal/materialize"
	"path/filepath"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"testing"
)

func TestGateRealAPIConflictAndTrust(t *testing.T) {
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases"), "."}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{api.AddToScheme, batch.AddToScheme, core.AddToScheme, kueue.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	c, err := client.NewWithWatch(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	mustCreate := func(o client.Object) {
		t.Helper()
		if err := c.Create(ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	mustCreate(&core.Namespace{ObjectMeta: meta.ObjectMeta{Name: "gate-tenant"}})
	p := &api.PrerequisiteGatePolicy{ObjectMeta: meta.ObjectMeta{Name: "default"}, Spec: api.PrerequisiteGatePolicySpec{AllowedImageRegistries: []string{"registry.example.test"}, AllowedPriorityClassNames: []string{"normal"}}}
	mustCreate(p)
	registration := &kueue.AdmissionCheck{ObjectMeta: meta.ObjectMeta{Name: gate.CheckName}, Spec: kueue.AdmissionCheckSpec{ControllerName: gate.ControllerName, Parameters: &kueue.AdmissionCheckParametersReference{APIGroup: api.GroupVersion.Group, Kind: "PrerequisiteGatePolicy", Name: "default"}}}
	mustCreate(registration)
	registration.Status.Conditions = []meta.Condition{{Type: "Active", Status: meta.ConditionTrue, Reason: "PolicyReady", Message: "Valid test policy", LastTransitionTime: meta.Now()}}
	if err := c.Status().Update(ctx, registration); err != nil {
		t.Fatal(err)
	}
	resources := core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("250m"), core.ResourceMemory: resource.MustParse("64Mi")}, Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("250m"), core.ResourceMemory: resource.MustParse("128Mi")}}
	run := &api.BatchRun{ObjectMeta: meta.ObjectMeta{Name: "gate-run", Namespace: "gate-tenant"}, Spec: api.BatchRunSpec{QueueName: "team-a", Image: "registry.example.test/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Resources: resources, TotalTimeoutSeconds: 300, ExecutionTimeoutSeconds: 30}}
	mustCreate(run)
	job, err := materialize.Build(run)
	if err != nil {
		t.Fatal(err)
	}
	mustCreate(job)
	prep := &batch.Job{ObjectMeta: meta.ObjectMeta{Name: "prepare", Namespace: run.Namespace, Labels: map[string]string{"batch.kubefleet.dev/prerequisite": "approved"}}, Spec: batch.JobSpec{Template: job.Spec.Template}}
	prep.Spec.Template.Labels = nil
	mustCreate(prep)
	prep.Status.Conditions = []batch.JobCondition{{Type: batch.JobComplete, Status: core.ConditionTrue, Reason: "Complete", LastTransitionTime: meta.Now()}}
	prep.Status.Conditions = append(prep.Status.Conditions, batch.JobCondition{Type: batch.JobSuccessCriteriaMet, Status: core.ConditionTrue, Reason: "Complete", LastTransitionTime: meta.Now()})
	prep.Status.Succeeded = 1
	now := meta.Now()
	prep.Status.StartTime = &now
	prep.Status.CompletionTime = &now
	if err := c.Status().Update(ctx, prep); err != nil {
		t.Fatal(err)
	}
	// No webhook is installed here; API isolation deliberately supplies an immutable
	// prerequisite reference before invoking the gate. Kind tests cover validation.
	run.Spec.Prerequisite = &api.PrerequisiteReference{JobName: prep.Name, JobUID: prep.UID, WaitTimeoutSeconds: 120}
	if err := c.Update(ctx, run); err != nil {
		t.Fatal(err)
	}
	w := &kueue.Workload{ObjectMeta: meta.ObjectMeta{Name: "gate-workload", Namespace: run.Namespace, OwnerReferences: []meta.OwnerReference{{Kind: "Job", APIVersion: "batch/v1", Name: job.Name, UID: job.UID, Controller: ptr.To(true)}}}, Spec: kueue.WorkloadSpec{QueueName: "team-a", PodSets: []kueue.PodSet{{Name: "main", Count: 1, Template: job.Spec.Template}}}}
	mustCreate(w)
	w.Status.AdmissionChecks = []kueue.AdmissionCheckState{{Name: gate.CheckName, State: kueue.CheckStatePending, Message: "waiting", LastTransitionTime: meta.Now()}, {Name: "other-check", State: kueue.CheckStatePending, Message: "original", LastTransitionTime: meta.Now()}}
	w.Status.Conditions = []meta.Condition{{Type: "QuotaReserved", Status: meta.ConditionTrue, Reason: "Reserved", Message: "API preservation test", LastTransitionTime: meta.Now()}}
	w.Status.Admission = &kueue.Admission{ClusterQueue: "team-a", PodSetAssignments: []kueue.PodSetAssignment{{Name: "main", Count: ptr.To(int32(1)), ResourceUsage: resources.Requests, Flavors: map[core.ResourceName]kueue.ResourceFlavorReference{core.ResourceCPU: "workers", core.ResourceMemory: "workers"}}}}
	if err := c.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	injected, conflict := false, false
	wrapped := interceptor.NewClient(c, interceptor.Funcs{SubResourcePatch: func(ctx context.Context, base client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
		if sub == "status" && obj.GetName() == w.Name && !injected {
			injected = true
			var competitor kueue.Workload
			if err := c.Get(ctx, client.ObjectKeyFromObject(w), &competitor); err != nil {
				return err
			}
			competitor.Status.AdmissionChecks[1].State = kueue.CheckStateReady
			competitor.Status.AdmissionChecks[1].Message = "concurrent update"
			if err := c.Status().Update(ctx, &competitor); err != nil {
				return err
			}
		}
		err := base.SubResource(sub).Patch(ctx, obj, patch, opts...)
		if errors.IsConflict(err) {
			conflict = true
		}
		return err
	}})
	reconciler := &controller.GateReconciler{Client: wrapped, Reader: c, PolicyName: "default"}
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(w), w); err != nil {
		t.Fatal(err)
	}
	if !conflict || w.Status.AdmissionChecks[1].State != kueue.CheckStateReady || w.Status.AdmissionChecks[1].Message != "concurrent update" || w.Status.Admission == nil || len(w.Status.Conditions) < 2 {
		t.Fatal("real stale patch lost unrelated status")
	}
	t.Log("real resourceVersion conflict preserved another check and quota admission")
	// Reset only this controller's decision/snapshot, then supply disallowed trust
	// relationships. A fresh reconciler must reject both without executing a Pod.
	for _, kind := range []string{"queued", "BatchRun-owned"} {
		t.Run(kind, func(t *testing.T) {
			if err := c.Get(ctx, client.ObjectKeyFromObject(w), w); err != nil {
				t.Fatal(err)
			}
			w.Status.Conditions = w.Status.Conditions[:1]
			w.Status.AdmissionChecks[0].State = kueue.CheckStatePending
			w.Status.AdmissionChecks[0].Message = "waiting"
			if err := c.Status().Update(ctx, w); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(ctx, client.ObjectKeyFromObject(run), run); err != nil {
				t.Fatal(err)
			}
			run.Status.PrerequisiteDecision = nil
			if err := c.Status().Update(ctx, run); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(ctx, client.ObjectKeyFromObject(prep), prep); err != nil {
				t.Fatal(err)
			}
			prep.Labels = map[string]string{"batch.kubefleet.dev/prerequisite": "approved"}
			prep.OwnerReferences = nil
			if kind == "queued" {
				prep.Labels["kueue.x-k8s.io/queue-name"] = "team-a"
			} else {
				prep.OwnerReferences = []meta.OwnerReference{{Kind: "BatchRun", APIVersion: api.GroupVersion.String(), Name: run.Name, UID: run.UID}}
			}
			if err := c.Update(ctx, prep); err != nil {
				t.Fatal(err)
			}
			if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)}); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(ctx, client.ObjectKeyFromObject(w), w); err != nil {
				t.Fatal(err)
			}
			if w.Status.AdmissionChecks[0].State != kueue.CheckStateRejected || w.Status.AdmissionChecks[0].Message != "PrerequisiteNotAllowed" {
				t.Fatal("untrusted preparation approved")
			}
		})
	}
}
