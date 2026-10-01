//go:build integration

package spike

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-logr/logr"
	"os"
	"path/filepath"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"testing"
	"time"

	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

type fixture struct {
	t                           *testing.T
	ctx                         context.Context
	c                           client.Client
	id, nsA, nsB, flavor, check string
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func (f *fixture) save(name string, v any) {
	f.t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	must(f.t, err)
	dir := os.Getenv("SPIKE_EVIDENCE")
	if dir == "" {
		f.t.Fatal("SPIKE_EVIDENCE is required")
	}
	must(f.t, os.MkdirAll(dir, 0755))
	must(f.t, os.WriteFile(filepath.Join(dir, f.id+"-"+name+".json"), append(b, '\n'), 0644))
}
func (f *fixture) await(label string, predicate func() (bool, error)) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 90*time.Second)
	defer cancel()
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		ok, err := predicate()
		must(f.t, err)
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			f.t.Fatalf("timeout waiting for %s", label)
		case <-tick.C:
		}
	}
}
func newFixture(t *testing.T, shared, preemption, gated bool) *fixture {
	t.Helper()
	path := os.Getenv("KUBECONFIG")
	cfg, err := clientcmd.LoadFromFile(path)
	must(t, err)
	if cfg.CurrentContext != "kind-kubefleet" {
		t.Fatal("refusing context other than kind-kubefleet")
	}
	rest, err := clientcmd.BuildConfigFromFlags("", path)
	must(t, err)
	s := runtime.NewScheme()
	must(t, core.AddToScheme(s))
	must(t, batch.AddToScheme(s))
	must(t, kueue.AddToScheme(s))
	c, err := client.New(rest, client.Options{Scheme: s})
	must(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	var system core.Namespace
	must(t, c.Get(ctx, types.NamespacedName{Name: "kube-system"}, &system))
	if system.Labels["batch.kubefleet.dev/project"] != "kubefleet" {
		t.Fatal("project identity mismatch")
	}
	id := fmt.Sprintf("spike-%d", time.Now().UnixNano())
	f := &fixture{t: t, ctx: ctx, c: c, id: id, nsA: id + "-a", nsB: id + "-b", flavor: id, check: id}
	var objects []client.Object
	// Delete namespaces first and wait for Job/Workload GC before deleting queue policy.
	t.Cleanup(func() {
		clean, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		for i := len(objects) - 1; i >= 0; i-- {
			if err := c.Delete(clean, objects[i]); client.IgnoreNotFound(err) != nil {
				t.Errorf("cleanup %s: %v", objects[i].GetName(), err)
			}
		}
	})
	create := func(o client.Object) { must(t, c.Create(ctx, o)); objects = append(objects, o) }
	create(&kueue.ResourceFlavor{ObjectMeta: meta.ObjectMeta{Name: f.flavor}, Spec: kueue.ResourceFlavorSpec{NodeLabels: map[string]string{"batch.kubefleet.dev/pool": "workers"}}})
	if gated {
		ac := &kueue.AdmissionCheck{ObjectMeta: meta.ObjectMeta{Name: f.check}, Spec: kueue.AdmissionCheckSpec{ControllerName: "batch.kubefleet.dev/compatibility-spike", RetryDelayMinutes: ptr.To[int64](1)}}
		create(ac)
		ac.Status.Conditions = []meta.Condition{{Type: "Active", Status: meta.ConditionTrue, Reason: "CompatibilitySpike", Message: "Test driver controls this check", LastTransitionTime: meta.Now(), ObservedGeneration: ac.Generation}}
		must(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
			var fresh kueue.AdmissionCheck
			if err := c.Get(ctx, client.ObjectKeyFromObject(ac), &fresh); err != nil {
				return err
			}
			fresh.Status.Conditions = ac.Status.Conditions
			return c.Status().Update(ctx, &fresh)
		}))
		f.save("active-check", ac)
	}
	for _, ns := range []string{f.nsA, f.nsB} {
		cq := &kueue.ClusterQueue{ObjectMeta: meta.ObjectMeta{Name: ns}, Spec: kueue.ClusterQueueSpec{NamespaceSelector: &meta.LabelSelector{MatchLabels: map[string]string{"batch.kubefleet.dev/spike": id}}, QueueingStrategy: kueue.BestEffortFIFO, ResourceGroups: []kueue.ResourceGroup{{CoveredResources: []core.ResourceName{core.ResourceCPU, core.ResourceMemory}, Flavors: []kueue.FlavorQuotas{{Name: kueue.ResourceFlavorReference(f.flavor), Resources: []kueue.ResourceQuota{{Name: core.ResourceCPU, NominalQuota: resource.MustParse("250m")}, {Name: core.ResourceMemory, NominalQuota: resource.MustParse("512Mi")}}}}}}}}
		if shared {
			cq.Spec.Cohort = kueue.CohortReference(id)
			for i := range cq.Spec.ResourceGroups[0].Flavors[0].Resources {
				r := &cq.Spec.ResourceGroups[0].Flavors[0].Resources[i]
				q := r.NominalQuota.DeepCopy()
				r.BorrowingLimit = &q
				l := q.DeepCopy()
				r.LendingLimit = &l
			}
		}
		if preemption {
			cq.Spec.Preemption = &kueue.ClusterQueuePreemption{WithinClusterQueue: kueue.PreemptionPolicyLowerPriority, ReclaimWithinCohort: kueue.PreemptionPolicyLowerPriority}
		}
		if gated {
			cq.Spec.AdmissionChecks = []kueue.AdmissionCheckReference{kueue.AdmissionCheckReference(f.check)}
		}
		create(cq)
		create(&core.Namespace{ObjectMeta: meta.ObjectMeta{Name: ns, Labels: map[string]string{"batch.kubefleet.dev/spike": id}}})
		create(&kueue.LocalQueue{ObjectMeta: meta.ObjectMeta{Name: "queue", Namespace: ns}, Spec: kueue.LocalQueueSpec{ClusterQueue: kueue.ClusterQueueReference(ns)}})
		f.await("ClusterQueue Active", func() (bool, error) {
			var q kueue.ClusterQueue
			err := c.Get(ctx, types.NamespacedName{Name: ns}, &q)
			return apiMeta.IsStatusConditionTrue(q.Status.Conditions, "Active"), err
		})
	}
	for name, value := range map[string]int32{"low": -10, "high": 10} {
		create(&kueue.WorkloadPriorityClass{ObjectMeta: meta.ObjectMeta{Name: id + "-" + name}, Value: value})
	}
	return f
}
func (f *fixture) job(ns, name, cpu, priority, duration string) *batch.Job {
	f.t.Helper()
	j := &batch.Job{ObjectMeta: meta.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{"kueue.x-k8s.io/queue-name": "queue", "kueue.x-k8s.io/priority-class": f.id + "-" + priority}}, Spec: batch.JobSpec{Suspend: ptr.To(true), BackoffLimit: ptr.To[int32](0), Template: core.PodTemplateSpec{Spec: core.PodSpec{RestartPolicy: core.RestartPolicyNever, AutomountServiceAccountToken: ptr.To(false), SecurityContext: &core.PodSecurityContext{RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To[int64](65532), SeccompProfile: &core.SeccompProfile{Type: core.SeccompProfileTypeRuntimeDefault}}, Containers: []core.Container{{Name: "worker", Image: "kubefleet-worker:compat", ImagePullPolicy: core.PullNever, Args: []string{"sleep", "--duration=" + duration}, Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse(cpu), core.ResourceMemory: resource.MustParse("64Mi")}, Limits: core.ResourceList{core.ResourceCPU: resource.MustParse(cpu), core.ResourceMemory: resource.MustParse("128Mi")}}, SecurityContext: &core.SecurityContext{AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true), Capabilities: &core.Capabilities{Drop: []core.Capability{"ALL"}}}}}}}}}
	must(f.t, f.c.Create(f.ctx, j))
	f.save(name+"-created", j)
	return j
}
func (f *fixture) workload(j *batch.Job) *kueue.Workload {
	f.t.Helper()
	var found *kueue.Workload
	f.await("one UID-owned Workload", func() (bool, error) {
		var list kueue.WorkloadList
		err := f.c.List(f.ctx, &list, client.InNamespace(j.Namespace))
		if err != nil {
			return false, err
		}
		for i := range list.Items {
			w := &list.Items[i]
			owner := meta.GetControllerOf(w)
			if owner != nil && owner.UID == j.UID && owner.Kind == "Job" {
				if found != nil && found.Name != w.Name {
					return false, fmt.Errorf("multiple owned Workloads")
				}
				found = w.DeepCopy()
			}
		}
		return found != nil, nil
	})
	f.save(j.Name+"-workload", found)
	return found
}
func (f *fixture) refresh(w *kueue.Workload) error {
	return f.c.Get(f.ctx, client.ObjectKeyFromObject(w), w)
}
func (f *fixture) admitted(j *batch.Job, w *kueue.Workload) {
	f.t.Helper()
	f.await("Admitted and unsuspended", func() (bool, error) {
		if err := f.refresh(w); err != nil {
			return false, err
		}
		if err := f.c.Get(f.ctx, client.ObjectKeyFromObject(j), j); err != nil {
			return false, err
		}
		return apiMeta.IsStatusConditionTrue(w.Status.Conditions, "Admitted") && !ptr.Deref(j.Spec.Suspend, true), nil
	})
	f.save(j.Name+"-admitted", w)
}
func (f *fixture) complete(j *batch.Job, w *kueue.Workload) {
	f.t.Helper()
	f.await("Job success, Workload Finished and released reservation", func() (bool, error) {
		if err := f.refresh(w); err != nil {
			return false, err
		}
		if err := f.c.Get(f.ctx, client.ObjectKeyFromObject(j), j); err != nil {
			return false, err
		}
		if j.Status.Succeeded != 1 || !apiMeta.IsStatusConditionTrue(w.Status.Conditions, "Finished") {
			return false, nil
		}
		var q kueue.ClusterQueue
		if err := f.c.Get(f.ctx, types.NamespacedName{Name: j.Namespace}, &q); err != nil {
			return false, err
		}
		f.save(j.Name+"-released-queue", q)
		return q.Status.ReservingWorkloads == 0 && q.Status.AdmittedWorkloads == 0, nil
	})
	f.save(j.Name+"-finished", w)
	f.save(j.Name+"-completed-job", j)
}
func (f *fixture) setCheck(w *kueue.Workload, state kueue.CheckState) {
	f.t.Helper()
	must(f.t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := f.refresh(w); err != nil {
			return err
		}
		for i := range w.Status.AdmissionChecks {
			check := &w.Status.AdmissionChecks[i]
			if string(check.Name) == f.check {
				check.State = state
				check.Message = "compatibility test decision " + string(state)
				check.LastTransitionTime = meta.Now()
				// Follow the pinned provisioning AdmissionCheck controller: Retry
				// must persist requeueAt; the deprecated registration delay is ignored.
				if state == kueue.CheckStateRetry {
					at := meta.NewTime(time.Now().Add(3 * time.Second))
					w.Status.RequeueState = &kueue.RequeueState{Count: ptr.To[int32](1), RequeueAt: &at}
				}
				return f.c.Status().Update(f.ctx, w)
			}
		}
		return fmt.Errorf("named check missing")
	}))
	f.save(string(state)+"-decision", w)
}
func TestNativeCompatibility(t *testing.T) {
	ctrllog.SetLogger(logr.Discard())
	t.Run("completion-releases-quota", func(t *testing.T) {
		f := newFixture(t, false, false, false)
		j := f.job(f.nsA, "complete", "250m", "low", "2s")
		w := f.workload(j)
		f.admitted(j, w)
		f.complete(j, w)
	})
	t.Run("isolated-blocks-borrowing", func(t *testing.T) {
		f := newFixture(t, false, false, false)
		j1 := f.job(f.nsA, "occupy", "250m", "low", "120s")
		w1 := f.workload(j1)
		f.admitted(j1, w1)
		j2 := f.job(f.nsA, "blocked", "250m", "low", "120s")
		w2 := f.workload(j2)
		f.await("second Job waits for quota", func() (bool, error) {
			err := f.refresh(w2)
			c := apiMeta.FindStatusCondition(w2.Status.Conditions, "QuotaReserved")
			return c != nil && c.Status == meta.ConditionFalse && c.Reason == "Pending", err
		})
		must(t, f.c.Get(f.ctx, client.ObjectKeyFromObject(j2), j2))
		if !ptr.Deref(j2.Spec.Suspend, false) {
			t.Fatal("isolated quota allowed excess workload")
		}
		f.save("isolated-blocked", w2)
	})
	t.Run("cohort-borrowing", func(t *testing.T) {
		f := newFixture(t, true, false, false)
		j1 := f.job(f.nsA, "borrow-one", "250m", "low", "120s")
		w1 := f.workload(j1)
		f.admitted(j1, w1)
		j2 := f.job(f.nsA, "borrow-two", "250m", "low", "120s")
		w2 := f.workload(j2)
		f.admitted(j2, w2)
		var q kueue.ClusterQueue
		f.await("two admitted workloads and 500m CPU usage", func() (bool, error) {
			err := f.c.Get(f.ctx, types.NamespacedName{Name: f.nsA}, &q)
			if err != nil {
				return false, err
			}
			if q.Status.AdmittedWorkloads != 2 {
				return false, nil
			}
			for _, flavor := range q.Status.FlavorsUsage {
				for _, r := range flavor.Resources {
					if r.Name == core.ResourceCPU {
						return r.Total.Cmp(resource.MustParse("500m")) == 0, nil
					}
				}
			}
			return false, nil
		})
		f.save("borrowed-queue", q)
	})
	for _, cohort := range []bool{false, true} {
		name := "within-queue-preemption"
		if cohort {
			name = "cohort-reclaim"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, cohort, true, false)
			cpu := "250m"
			if cohort {
				cpu = "500m"
			}
			low := f.job(f.nsA, "low", cpu, "low", "120s")
			lw := f.workload(low)
			f.admitted(low, lw)
			f.await("low worker running before preemption", func() (bool, error) {
				var pods core.PodList
				err := f.c.List(f.ctx, &pods, client.InNamespace(f.nsA), client.MatchingLabels{"batch.kubernetes.io/job-name": "low"})
				for _, p := range pods.Items {
					if p.Status.Phase == core.PodRunning {
						f.save("victim-running-pod", p)
						return true, err
					}
				}
				return false, err
			})
			ns := f.nsA
			if cohort {
				ns = f.nsB
			}
			high := f.job(ns, "high", "250m", "high", "120s")
			hw := f.workload(high)
			f.await("low evicted by preemption", func() (bool, error) {
				err := f.refresh(lw)
				condition := apiMeta.FindStatusCondition(lw.Status.Conditions, "Evicted")
				return condition != nil && condition.Status == meta.ConditionTrue && condition.Reason == "Preempted", err
			})
			f.save("victim-evicted", lw)
			f.admitted(high, hw)
		})
	}
	t.Run("check-ready", func(t *testing.T) {
		f := newFixture(t, false, false, true)
		j := f.job(f.nsA, "ready", "250m", "low", "2s")
		w := f.workload(j)
		f.pending(j, w)
		f.setCheck(w, kueue.CheckStateReady)
		f.admitted(j, w)
		f.complete(j, w)
	})
	t.Run("check-retry", func(t *testing.T) {
		f := newFixture(t, false, false, true)
		j := f.job(f.nsA, "retry", "250m", "low", "2s")
		w := f.workload(j)
		f.pending(j, w)
		f.setCheck(w, kueue.CheckStateRetry)
		f.await("Retry eviction and reservation release", func() (bool, error) {
			err := f.refresh(w)
			return w.Status.Admission == nil && apiMeta.IsStatusConditionTrue(w.Status.Conditions, "Evicted"), err
		})
		f.save("retry-quota-released", w)
		var q kueue.ClusterQueue
		f.await("reservation counter zero during backoff", func() (bool, error) {
			err := f.c.Get(f.ctx, types.NamespacedName{Name: f.nsA}, &q)
			return q.Status.ReservingWorkloads == 0, err
		})
		f.save("retry-released-queue", q)
		if w.Status.RequeueState == nil || w.Status.RequeueState.RequeueAt == nil {
			t.Fatal("missing persisted Retry backoff")
		}
		backoffUntil := w.Status.RequeueState.RequeueAt.Time
		f.await("Kueue finishes persisted backoff", func() (bool, error) {
			err := f.refresh(w)
			done := apiMeta.IsStatusConditionTrue(w.Status.Conditions, "Requeued")
			if time.Now().Before(backoffUntil) && done {
				return false, fmt.Errorf("Kueue requeued before persisted backoff elapsed")
			}
			return done, err
		})
		f.save("retry-backoff-finished", w)
		f.await("Kueue resets Retry to Pending", func() (bool, error) {
			err := f.refresh(w)
			for _, s := range w.Status.AdmissionChecks {
				if string(s.Name) == f.check && s.State == kueue.CheckStatePending {
					return true, err
				}
			}
			return false, err
		})
		f.save("retry-reset-pending", w)
		f.setCheck(w, kueue.CheckStateReady)
		f.admitted(j, w)
		f.complete(j, w)
	})
	t.Run("check-rejected", func(t *testing.T) {
		f := newFixture(t, false, false, true)
		j := f.job(f.nsA, "rejected", "250m", "low", "120s")
		w := f.workload(j)
		f.pending(j, w)
		f.setCheck(w, kueue.CheckStateRejected)
		f.await("Rejected deactivation and reservation release", func() (bool, error) {
			err := f.refresh(w)
			return !ptr.Deref(w.Spec.Active, true) && w.Status.Admission == nil, err
		})
		f.save("rejected-deactivated", w)
	})
}
func (f *fixture) pending(j *batch.Job, w *kueue.Workload) {
	f.t.Helper()
	f.await("Pending with quota reserved", func() (bool, error) {
		err := f.refresh(w)
		for _, s := range w.Status.AdmissionChecks {
			if string(s.Name) == f.check && s.State == kueue.CheckStatePending && apiMeta.IsStatusConditionTrue(w.Status.Conditions, "QuotaReserved") {
				return true, err
			}
		}
		return false, err
	})
	must(f.t, f.c.Get(f.ctx, client.ObjectKeyFromObject(j), j))
	if !ptr.Deref(j.Spec.Suspend, false) || apiMeta.IsStatusConditionTrue(w.Status.Conditions, "Admitted") {
		f.t.Fatal("Pending check allowed execution")
	}
	var pods core.PodList
	must(f.t, f.c.List(f.ctx, &pods, client.InNamespace(j.Namespace), client.MatchingLabels{"batch.kubernetes.io/job-name": j.Name}))
	if len(pods.Items) != 0 {
		f.t.Fatal("Pending check produced Pods")
	}
	f.save(j.Name+"-pending-reserved", w)
}
