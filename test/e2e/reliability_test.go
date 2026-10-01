//go:build integration

package e2e

import (
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/materialize"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"testing"
	"time"
)

func TestM2LifecycleFailures(t *testing.T) {
	for _, backoff := range []int32{0, 1} {
		t.Run("worker-failure-backoff-"+string(rune('0'+backoff)), func(t *testing.T) {
			f := newSuite(t)
			r := f.run("failure", "fail", func(r *api.BatchRun) {
				r.Spec.Args = []string{"fail", "--duration=100ms", "--exit-code=7"}
				r.Spec.BackoffLimit = backoff
			})
			f.terminal(r, "Failed", "BackoffLimitExceeded")
			job := f.child(r)
			var pods core.PodList
			must(t, f.c.List(f.ctx, &pods, client.InNamespace(r.Namespace), client.MatchingLabels{"batch.kubernetes.io/job-name": job.Name}))
			f.save("failed-pods", pods)
			if len(pods.Items) != int(backoff+1) {
				t.Fatalf("expected %d Pod attempts; got %d", backoff+1, len(pods.Items))
			}
			for _, p := range pods.Items {
				if p.Status.Phase != core.PodFailed {
					t.Fatal("non-failed attempt")
				}
			}
		})
	}
	t.Run("execution-deadline", func(t *testing.T) {
		f := newSuite(t)
		r := f.run("execution-timeout", "sleep", func(r *api.BatchRun) { r.Spec.ExecutionTimeoutSeconds = 2 })
		f.terminal(r, "Failed", "DeadlineExceeded")
	})
	t.Run("absolute-queued-deadline", func(t *testing.T) {
		f := newSuite(t)
		r := f.run("queued-timeout", "sleep", func(r *api.BatchRun) {
			r.Spec.QueueName = "missing-queue"
			r.Spec.TotalTimeoutSeconds = 30
			r.Spec.ExecutionTimeoutSeconds = 20
		})
		f.await("missing-queue diagnostic", func() (bool, error) { err := f.refresh(r); return r.Status.Reason == "LocalQueueMissing", err })
		f.terminal(r, "Failed", "TotalDeadlineExceeded")
		f.clean(r)
	})
	t.Run("deletion-garbage-collection", func(t *testing.T) {
		f := newSuite(t)
		r := f.run("delete", "sleep", nil)
		f.await("Running", func() (bool, error) { err := f.refresh(r); return r.Status.Phase == "Running", err })
		f.save("delete-running", r)
		must(t, f.c.Delete(f.ctx, r, client.PropagationPolicy(meta.DeletePropagationForeground)))
		f.clean(r)
	})
	t.Run("inactive-queue-deadline", func(t *testing.T) {
		f := newSuite(t)
		f.profile("isolated")
		var cq kueue.ClusterQueue
		must(t, f.c.Get(f.ctx, types.NamespacedName{Name: "team-a"}, &cq))
		before := cq.DeepCopy()
		cq.Spec.ResourceGroups[0].Flavors[0].Name = "missing-test-flavor"
		must(t, f.c.Patch(f.ctx, &cq, client.MergeFrom(before)))
		t.Cleanup(func() { f.profile("isolated") })
		f.await("inactive ClusterQueue", func() (bool, error) {
			err := f.c.Get(f.ctx, types.NamespacedName{Name: "team-a"}, &cq)
			return apiMeta.IsStatusConditionFalse(cq.Status.Conditions, "Active"), err
		})
		r := f.run("inactive-timeout", "sleep", func(r *api.BatchRun) {
			r.Spec.TotalTimeoutSeconds = 30
			r.Spec.ExecutionTimeoutSeconds = 20
		})
		f.await("inactive-queue diagnostic", func() (bool, error) {
			err := f.refresh(r)
			return r.Status.Reason == "ClusterQueueInactive", err
		})
		f.terminal(r, "Failed", "TotalDeadlineExceeded")
		f.clean(r)
		if len(f.pods(r)) != 0 {
			t.Fatal("inactive queue created worker Pods")
		}
	})
}
func TestM2CrashBoundaries(t *testing.T) {
	for _, point := range []string{"after-job-create", "after-stop-intent"} {
		t.Run(point, func(t *testing.T) {
			f := newSuite(t)
			name := f.id + "-crash"
			identity := "team-a/" + name
			f.configureManager("kubefleet-manager:faults", point, identity, 1)
			t.Cleanup(func() { f.configureManager("kubefleet-manager:dev", "", "", 1) })
			r := f.run("crash", "sleep", nil)
			if point == "after-stop-intent" {
				f.await("Running", func() (bool, error) { err := f.refresh(r); return r.Status.Phase == "Running", err })
				f.cancel(r)
			}
			pod := f.boundary(point, identity)
			must(t, f.refresh(r))
			f.save("at-boundary-run", r)
			if point == "after-job-create" && r.Status.JobRef != nil {
				t.Fatal("hook occurred after Job ref persistence")
			}
			if point == "after-stop-intent" && r.Status.StopReason != "UserCancelled" {
				t.Fatal("stop hook before durable intent")
			}
			var jobs batch.JobList
			must(t, f.c.List(f.ctx, &jobs, client.InNamespace(r.Namespace), client.MatchingLabels{materialize.RunUIDLabel: string(r.UID)}))
			if len(jobs.Items) != 1 {
				t.Fatal("boundary did not have exactly one child")
			}
			original := jobs.Items[0].UID
			f.save("boundary-job", jobs.Items[0])
			start := time.Now()
			f.kill(pod)
			if point == "after-job-create" {
				f.await("recover original child reference", func() (bool, error) { err := f.refresh(r); return r.Status.JobRef != nil, err })
				if r.Status.JobRef.UID != original {
					t.Fatal("duplicate execution after crash")
				}
				f.cancel(r)
			}
			f.terminal(r, "Cancelled", "UserCancelled")
			f.clean(r)
			f.save("recovery", map[string]any{"killToConvergenceSeconds": time.Since(start).Seconds(), "originalJobUID": original, "oldLeader": pod.Name})
		})
	}
}
func TestM2LeaderFailover(t *testing.T) {
	f := newSuite(t)
	f.configureManager("kubefleet-manager:dev", "", "", 2)
	t.Cleanup(func() { f.configureManager("kubefleet-manager:dev", "", "", 1) })
	running := f.run("running", "sleep", nil)
	f.await("Running", func() (bool, error) { err := f.refresh(running); return running.Status.Phase == "Running", err })
	queued := f.run("queued", "sleep", nil)
	job := f.child(queued)
	old := f.leader()
	f.save("backlog", []*api.BatchRun{running, queued})
	start := time.Now()
	f.kill(old)
	var leaderName string
	f.await("new leader", func() (bool, error) {
		leader := f.leader()
		leaderName = leader.Name
		return leader.UID != old.UID, nil
	})
	leaderDelay := time.Since(start)
	f.cancel(running)
	f.terminal(running, "Cancelled", "")
	f.clean(running)
	f.await("queued run begins", func() (bool, error) { err := f.refresh(queued); return queued.Status.Phase == "Running", err })
	if queued.Status.JobRef.UID != job.UID {
		t.Fatal("failover created duplicate child")
	}
	f.save("failover", map[string]any{"oldLeader": old.Name, "newLeader": leaderName, "killToLeaderSeconds": leaderDelay.Seconds(), "killToConvergenceSeconds": time.Since(start).Seconds(), "queuedJobUID": job.UID})
	if time.Since(start) > 60*time.Second {
		t.Fatal("failover convergence exceeded 60s")
	}
	f.cancel(queued)
	f.terminal(queued, "Cancelled", "")
	f.clean(queued)
}
