//go:build integration

package e2e

import (
	"context"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/gate"
	"kubefleet.local/kubefleet/internal/materialize"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"testing"
	"time"
)

func (f *suite) preparation(name, mode string, duration time.Duration, approved bool) *batch.Job {
	labels := map[string]string{}
	if approved {
		labels["batch.kubefleet.dev/prerequisite"] = "approved"
	}
	prototype := f.runTemplate()
	job := &batch.Job{ObjectMeta: meta.ObjectMeta{Name: f.id + "-" + name, Namespace: "team-a", Labels: labels}, Spec: batch.JobSpec{BackoffLimit: int32ptr(0), Template: prototype}}
	job.Spec.Template.Spec.Containers[0].Args = []string{mode, "--duration=" + duration.String()}
	must(f.t, f.c.Create(f.ctx, job))
	f.save(name+"-preparation", job)
	f.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := f.c.Delete(ctx, job, client.PropagationPolicy(meta.DeletePropagationForeground)); client.IgnoreNotFound(err) != nil {
			f.t.Error(err)
		}
	})
	return job
}
func int32ptr(v int32) *int32 { return &v }
func (f *suite) runTemplate() core.PodTemplateSpec {
	// Use the production materializer's restricted template, removing all identity
	// labels and BatchRun ownership from the administrator-created preparation Job.
	r := &api.BatchRun{ObjectMeta: meta.ObjectMeta{Name: "template", Namespace: "team-a", UID: "template"}, Spec: api.BatchRunSpec{Image: "kubefleet-worker:compat", QueueName: "team-a", Parallelism: 1, Completions: 1, ExecutionTimeoutSeconds: 120, TotalTimeoutSeconds: 300, PriorityClassName: "normal"}}
	// An existing fixture supplies verified resource/security choices.
	r.Spec.Resources = core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("100m"), core.ResourceMemory: resource.MustParse("64Mi")}, Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("250m"), core.ResourceMemory: resource.MustParse("128Mi")}}
	j, err := materialize.Build(r)
	must(f.t, err)
	j.Spec.Template.Labels = nil
	return j.Spec.Template
}
func (f *suite) completePreparation(j *batch.Job) {
	f.await("preparation complete", func() (bool, error) {
		err := f.c.Get(f.ctx, client.ObjectKeyFromObject(j), j)
		for _, c := range j.Status.Conditions {
			if c.Type == batch.JobComplete && c.Status == core.ConditionTrue {
				return true, err
			}
		}
		return false, err
	})
}
func (f *suite) gated(name string, j *batch.Job, wait int32) *api.BatchRun {
	return f.run(name, "sleep", func(r *api.BatchRun) {
		r.Spec.Prerequisite = &api.PrerequisiteReference{JobName: j.Name, JobUID: j.UID, WaitTimeoutSeconds: wait}
	})
}
func (f *suite) workload(r *api.BatchRun) *kueue.Workload {
	f.await("Workload ref", func() (bool, error) { err := f.refresh(r); return r.Status.WorkloadRef != nil, err })
	var w kueue.Workload
	must(f.t, f.c.Get(f.ctx, types.NamespacedName{Namespace: r.Namespace, Name: r.Status.WorkloadRef.Name}, &w))
	return &w
}
func checkState(w *kueue.Workload) kueue.CheckState {
	for _, c := range w.Status.AdmissionChecks {
		if c.Name == gate.CheckName {
			return c.State
		}
	}
	return ""
}
func TestM4PrerequisiteGate(t *testing.T) {
	t.Run("completed-and-latched", func(t *testing.T) {
		f := newSuite(t)
		f.profile("isolated")
		prep := f.preparation("completed", "prepare", 100*time.Millisecond, true)
		f.completePreparation(prep)
		r := f.gated("dependent", prep, 120)
		f.runningUID(r)
		w := f.workload(r)
		if checkState(w) != kueue.CheckStateReady {
			t.Fatal("not Ready")
		}
		snap := apiMeta.FindStatusCondition(w.Status.Conditions, gate.SnapshotCondition)
		if snap == nil || !gate.Matches(snap.Message, r) {
			t.Fatal("snapshot missing")
		}
		must(t, f.c.Delete(f.ctx, prep, client.PropagationPolicy(meta.DeletePropagationForeground)))
		// Cause a real preemption and re-admission after prerequisite cleanup.
		var cq kueue.ClusterQueue
		must(t, f.c.Get(f.ctx, types.NamespacedName{Name: "team-a"}, &cq))
		cq.Spec.Preemption = &kueue.ClusterQueuePreemption{WithinClusterQueue: kueue.PreemptionPolicyLowerPriority}
		must(t, f.c.Update(f.ctx, &cq))
		high := f.run("latched-preemptor", "sleep", func(r *api.BatchRun) { r.Spec.PriorityClassName = "high" })
		f.runningUID(high)
		f.cancel(high)
		f.terminal(high, "Cancelled", "")
		f.clean(high)
		f.runningUID(r)
		must(t, f.refresh(r))
		if r.Status.PrerequisiteDecision == nil || r.Status.PrerequisiteDecision.State != "Ready" {
			t.Fatal("latched readiness lost")
		}
		f.save("latched-readmission", r)
		f.stopAll(r)
	})
	t.Run("pending-retry-backoff-success", func(t *testing.T) {
		f := newSuite(t)
		f.profile("isolated")
		var p api.PrerequisiteGatePolicy
		must(t, f.c.Get(f.ctx, types.NamespacedName{Name: "default"}, &p))
		p.Spec.MaxReservationHoldSeconds = 1
		must(t, f.c.Update(f.ctx, &p))
		t.Cleanup(func() { p.Spec.MaxReservationHoldSeconds = 10; must(t, f.c.Update(f.ctx, &p)) })
		prep := f.preparation("slow", "prepare", 8*time.Second, true)
		r := f.gated("waiting", prep, 60)
		f.await("Pending reservation with no Pods", func() (bool, error) {
			w := f.workload(r)
			if apiMeta.IsStatusConditionTrue(w.Status.Conditions, "QuotaReserved") && checkState(w) == kueue.CheckStatePending {
				if len(f.pods(r)) != 0 {
					t.Fatal("gate ran worker before Ready")
				}
				f.save("pending-reservation", w)
				return true, nil
			}
			return false, nil
		})
		f.await("Retry reservation released", func() (bool, error) {
			w := f.workload(r)
			var cq kueue.ClusterQueue
			err := f.c.Get(f.ctx, types.NamespacedName{Name: "team-a"}, &cq)
			if w.Status.RequeueState != nil && w.Status.RequeueState.RequeueAt != nil && !apiMeta.IsStatusConditionTrue(w.Status.Conditions, "QuotaReserved") && cq.Status.ReservingWorkloads == 0 {
				f.save("retry-released", w)
				f.save("retry-queue", cq)
				return true, err
			}
			return false, err
		})
		f.completePreparation(prep)
		start := time.Now()
		f.runningUID(r)
		f.save("wakeup", map[string]any{"observedCompletionToRunningSeconds": time.Since(start).Seconds(), "run": r})
		f.stopAll(r)
	})
	for _, reason := range []string{"PrerequisiteUIDMismatch", "PrerequisiteNotAllowed", "PrerequisiteFailed", "PrerequisiteDeadlineExceeded"} {
		t.Run(reason, func(t *testing.T) {
			f := newSuite(t)
			f.profile("isolated")
			var prep *batch.Job
			switch reason {
			case "PrerequisiteFailed":
				prep = f.preparation("failed", "fail", 100*time.Millisecond, true)
				f.await("failed preparation", func() (bool, error) {
					err := f.c.Get(f.ctx, client.ObjectKeyFromObject(prep), prep)
					for _, c := range prep.Status.Conditions {
						if c.Type == batch.JobFailed && c.Status == core.ConditionTrue {
							return true, err
						}
					}
					return false, err
				})
			case "PrerequisiteDeadlineExceeded":
				prep = &batch.Job{ObjectMeta: meta.ObjectMeta{Name: f.id + "-absent", Namespace: "team-a", UID: "absent-uid"}}
			default:
				prep = f.preparation("prepare", "prepare", 100*time.Millisecond, reason != "PrerequisiteNotAllowed")
				f.completePreparation(prep)
			}
			if reason == "PrerequisiteUIDMismatch" {
				prep.UID = "old-uid"
			}
			wait := int32(60)
			if reason == "PrerequisiteDeadlineExceeded" {
				wait = 5
			}
			r := f.gated("reject", prep, wait)
			f.terminal(r, "Failed", reason)
			if len(f.pods(r)) != 0 {
				t.Fatal("rejected gate executed")
			}
			f.clean(r)
		})
	}
	t.Run("policy-inactive-and-restored", func(t *testing.T) {
		f := newSuite(t)
		var p api.PrerequisiteGatePolicy
		must(t, f.c.Get(f.ctx, types.NamespacedName{Name: "default"}, &p))
		original := p.DeepCopy()
		must(t, f.c.Delete(f.ctx, &p))
		t.Cleanup(func() {
			var current api.PrerequisiteGatePolicy
			if errors.IsNotFound(f.c.Get(f.ctx, types.NamespacedName{Name: "default"}, &current)) {
				original.ResourceVersion = ""
				original.UID = ""
				must(t, f.c.Create(f.ctx, original))
			}
		})
		f.await("inactive registration", func() (bool, error) {
			var ac kueue.AdmissionCheck
			err := f.c.Get(f.ctx, types.NamespacedName{Name: gate.CheckName}, &ac)
			return apiMeta.IsStatusConditionFalse(ac.Status.Conditions, "Active"), err
		})
		original.ResourceVersion = ""
		original.UID = ""
		must(t, f.c.Create(f.ctx, original))
		f.await("Active registration restored", func() (bool, error) {
			var ac kueue.AdmissionCheck
			err := f.c.Get(f.ctx, types.NamespacedName{Name: gate.CheckName}, &ac)
			return apiMeta.IsStatusConditionTrue(ac.Status.Conditions, "Active"), err
		})
	})
}
func TestM4ReadyCrashRecovery(t *testing.T) {
	f := newSuite(t)
	f.profile("isolated")
	prep := f.preparation("ready", "prepare", 100*time.Millisecond, true)
	f.completePreparation(prep)
	identity := "team-a/" + f.id + "-boundary"
	f.configureManager("kubefleet-manager:faults", "after-gate-ready", identity, 1)
	t.Cleanup(func() { f.configureManager("kubefleet-manager:dev", "", "", 1) })
	r := f.gated("boundary", prep, 120)
	pod := f.boundary("after-gate-ready", identity)
	must(t, f.refresh(r))
	if r.Status.PrerequisiteDecision != nil && r.Status.PrerequisiteDecision.State == "Ready" {
		t.Fatal("hook was after projection")
	}
	w := f.workload(r)
	if checkState(w) != kueue.CheckStateReady || !apiMeta.IsStatusConditionTrue(w.Status.Conditions, gate.SnapshotCondition) {
		t.Fatal("hook before durable Ready")
	}
	f.save("ready-boundary", w)
	must(t, f.c.Delete(f.ctx, prep, client.PropagationPolicy(meta.DeletePropagationForeground)))
	f.kill(pod)
	f.await("snapshot projection recovered", func() (bool, error) {
		err := f.refresh(r)
		return r.Status.PrerequisiteDecision != nil && r.Status.PrerequisiteDecision.State == "Ready", err
	})
	f.runningUID(r)
	f.save("ready-recovered", r)
	f.stopAll(r)
}
