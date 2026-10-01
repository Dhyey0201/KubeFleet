//go:build envtest

package envtest

import (
	"context"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/controller"
	"kubefleet.local/kubefleet/internal/materialize"
	"path/filepath"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metrics "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"sync"
	"testing"
	"time"
)

func TestReconciliationAndStatusWatches(t *testing.T) {
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
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metrics.Options{BindAddress: "0"}, HealthProbeBindAddress: "0"})
	if err != nil {
		t.Fatal(err)
	}
	r := &controller.BatchRunReconciler{PolicyName: "default"}
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	await := func(label string, predicate func() (bool, error)) {
		t.Helper()
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		timeout := time.NewTimer(10 * time.Second)
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
			case <-tick.C:
			case <-timeout.C:
				t.Fatal("timeout: " + label)
			}
		}
	}
	if err := c.Create(ctx, &core.Namespace{ObjectMeta: meta.ObjectMeta{Name: "tenant"}}); err != nil {
		t.Fatal(err)
	}
	p := &api.PrerequisiteGatePolicy{ObjectMeta: meta.ObjectMeta{Name: "default"}, Spec: api.PrerequisiteGatePolicySpec{AllowedImageRegistries: []string{"registry.example.test"}, AllowedPriorityClassNames: []string{"normal"}}}
	if err := c.Create(ctx, p); err != nil {
		t.Fatal(err)
	}
	run := &api.BatchRun{ObjectMeta: meta.ObjectMeta{Name: "watch-test", Namespace: "tenant"}, Spec: api.BatchRunSpec{QueueName: "team-a", Image: "registry.example.test/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("250m"), core.ResourceMemory: resource.MustParse("64Mi")}, Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("500m"), core.ResourceMemory: resource.MustParse("128Mi")}}}}
	if err := c.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(run)
	await("one Job and durable reference", func() (bool, error) {
		if err := c.Get(ctx, key, run); err != nil {
			return false, err
		}
		return run.Status.JobRef != nil, nil
	})
	var job batch.Job
	if err := c.Get(ctx, types.NamespacedName{Name: run.Status.JobRef.Name, Namespace: run.Namespace}, &job); err != nil {
		t.Fatal(err)
	}
	if !materialize.Owned(run, &job) || !*job.Spec.Suspend {
		t.Fatal("unowned or initially active Job")
	}
	// Concurrent/repeated reconciles must converge on the same UID-owned Job.
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); errs <- err }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var jobs batch.JobList
	if err := c.List(ctx, &jobs, client.InNamespace("tenant")); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 1 || jobs.Items[0].UID != job.UID {
		t.Fatal("duplicate logical Jobs")
	}
	wl := &kueue.Workload{ObjectMeta: meta.ObjectMeta{Name: "owned-workload", Namespace: "tenant", OwnerReferences: []meta.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: ptr.To(true)}}}, Spec: kueue.WorkloadSpec{QueueName: "team-a", PodSets: []kueue.PodSet{{Name: "main", Count: 1, Template: job.Spec.Template}}}}
	if err := c.Create(ctx, wl); err != nil {
		t.Fatal(err)
	}
	wl.Status.Conditions = []meta.Condition{{Type: "Admitted", Status: meta.ConditionTrue, Reason: "TestAdmission", Message: "API watch test", LastTransitionTime: meta.Now()}}
	if err := c.Status().Update(ctx, wl); err != nil {
		t.Fatal(err)
	}
	await("status-only Workload event projects Admitted without Running", func() (bool, error) {
		err := c.Get(ctx, key, run)
		return run.Status.Phase == "Admitted" && run.Status.FirstRunningObservedAt == nil, err
	})
	now := meta.Now()
	job.Status.StartTime = &now
	job.Status.CompletionTime = &now
	job.Status.Succeeded = 1
	job.Status.Conditions = []batch.JobCondition{{Type: batch.JobComplete, Status: core.ConditionTrue, Reason: "Completed", LastTransitionTime: now}, {Type: batch.JobSuccessCriteriaMet, Status: core.ConditionTrue, Reason: "Completed", LastTransitionTime: now}}
	if err := c.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	await("status-only Job event persists terminal result", func() (bool, error) {
		err := c.Get(ctx, key, run)
		return run.Status.Result != nil && run.Status.Result.Outcome == "Succeeded", err
	})
	before := run.ResourceVersion
	for i := 0; i < 3; i++ {
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(ctx, key, run); err != nil {
		t.Fatal(err)
	}
	if before != run.ResourceVersion {
		t.Fatal("terminal status was rewritten")
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "absent", Namespace: "tenant"}}); err != nil {
		t.Fatal(err)
	}
	t.Logf("one Job UID %s; Workload and Job status-only watches verified", job.UID)
	// This API-server test supplies status observations. It does not run Kueue,
	// scheduler, Job controller, kubelets, or GC; kind proves actual execution.
}
