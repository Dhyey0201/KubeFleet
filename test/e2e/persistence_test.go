//go:build integration

package e2e

import (
	"fmt"
	core "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"strconv"
	"testing"
	"time"
)

func TestM2PersistenceIdentityAndIdle(t *testing.T) {
	f := newSuite(t)
	f.profile("isolated")
	prep := f.preparation("identity", "prepare", 100*time.Millisecond, true)
	f.completePreparation(prep)
	r := f.gated("identity", prep, 60)
	f.runningUID(r)
	f.await("Ready projection", func() (bool, error) {
		err := f.refresh(r)
		return r.Status.Phase == "Running" && r.Status.PrerequisiteDecision != nil && r.Status.PrerequisiteDecision.State == "Ready", err
	})
	w := f.workload(r)
	version, workloadVersion := r.ResourceVersion, w.ResourceVersion
	stableSince := time.Now()
	f.await("startup observations settled", func() (bool, error) {
		if err := f.refresh(r); err != nil {
			return false, err
		}
		if err := f.c.Get(f.ctx, client.ObjectKeyFromObject(w), w); err != nil {
			return false, err
		}
		if r.ResourceVersion != version || w.ResourceVersion != workloadVersion {
			version, workloadVersion = r.ResourceVersion, w.ResourceVersion
			stableSince = time.Now()
		}
		return time.Since(stableSince) >= time.Second, nil
	})
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		must(t, f.refresh(r))
		must(t, f.c.Get(f.ctx, client.ObjectKeyFromObject(w), w))
		if r.ResourceVersion != version || w.ResourceVersion != workloadVersion {
			t.Fatal("unchanged execution caused repeated API writes")
		}
		time.Sleep(100 * time.Millisecond)
	}
	f.cancel(r)
	f.terminal(r, "Cancelled", "")
	f.clean(r)
	saved := r.DeepCopy()
	leader := f.leader()
	f.kill(leader)
	f.await("new leader", func() (bool, error) { p := f.leader(); return p.UID != leader.UID, nil })
	must(t, f.refresh(r))
	if r.ResourceVersion != saved.ResourceVersion || r.Status.Result.Outcome != "Cancelled" {
		t.Fatal("terminal status changed after restart")
	}
	f.save("terminal-restart", r)
	newLeader := f.leader()
	var currentRuns api.BatchRunList
	must(t, f.c.List(f.ctx, &currentRuns, client.InNamespace(r.Namespace)))
	wantCancelled := 0
	for _, current := range currentRuns.Items {
		if current.Status.Phase == "Cancelled" {
			wantCancelled++
		}
	}
	query := fmt.Sprintf(`kubefleet_batchruns_current{namespace=%q,phase="Cancelled",pod=%q}`, r.Namespace, newLeader.Name)
	f.await("new process reconstructs terminal gauge", func() (bool, error) {
		j, err := f.query(query)
		if err != nil || j["status"] != "success" {
			return false, nil
		}
		values := j["data"].(map[string]any)["result"].([]any)
		if len(values) != 1 {
			return false, nil
		}
		value := values[0].(map[string]any)["value"].([]any)[1].(string)
		got, err := strconv.ParseFloat(value, 64)
		if err != nil || got != float64(wantCancelled) {
			return false, nil
		}
		f.save("terminal-gauge-after-restart", map[string]any{"newManagerPodUID": newLeader.UID, "expectedFromAPI": wantCancelled, "query": query, "source": j})
		return true, nil
	})
	must(t, f.c.Delete(f.ctx, prep, client.PropagationPolicy(meta.DeletePropagationForeground)))
	must(t, f.c.Delete(f.ctx, r, client.PropagationPolicy(meta.DeletePropagationForeground)))
	f.await("old run gone", func() (bool, error) {
		var current api.BatchRun
		err := f.c.Get(f.ctx, client.ObjectKeyFromObject(r), &current)
		return errors.IsNotFound(err), client.IgnoreNotFound(err)
	})
	fresh := saved.DeepCopy()
	fresh.ObjectMeta = meta.ObjectMeta{Name: saved.Name, Namespace: saved.Namespace}
	fresh.Status = api.BatchRunStatus{}
	fresh.Spec.Cancel = false
	fresh.Spec.Prerequisite.WaitTimeoutSeconds = 5
	must(t, f.c.Create(f.ctx, fresh))
	if fresh.UID == saved.UID {
		t.Fatal("new run reused UID")
	}
	f.terminal(fresh, "Failed", "PrerequisiteDeadlineExceeded")
	f.clean(fresh)
	if fresh.Status.JobRef != nil && saved.Status.JobRef != nil && fresh.Status.JobRef.Name == saved.Status.JobRef.Name {
		t.Fatal("new UID reused child identity")
	}
	if len(f.pods(fresh)) > 0 {
		t.Fatal("new run inherited Ready authorization")
	}
	must(t, f.c.Delete(f.ctx, fresh))
}
func TestM2AggregateParallelism(t *testing.T) {
	f := newSuite(t)
	f.profile("shared")
	r := f.run("parallel", "sleep", func(r *api.BatchRun) { r.Spec.Parallelism = 2; r.Spec.Completions = 2 })
	f.await("two running Pods", func() (bool, error) {
		n := 0
		for _, p := range f.pods(r) {
			if p.Status.Phase == core.PodRunning && p.DeletionTimestamp.IsZero() {
				n++
			}
		}
		return n == 2, nil
	})
	var cq kueue.ClusterQueue
	f.await("aggregate quota status converges to 500m", func() (bool, error) {
		if err := f.c.Get(f.ctx, types.NamespacedName{Name: "team-a"}, &cq); err != nil {
			return false, err
		}
		cpu := resource.Quantity{}
		for _, flavor := range cq.Status.FlavorsReservation {
			for _, usage := range flavor.Resources {
				if usage.Name == core.ResourceCPU {
					cpu.Add(usage.Total)
				}
			}
		}
		return cpu.Cmp(resource.MustParse("500m")) == 0, nil
	})
	f.save("aggregate-parallelism", cq)
	f.stopAll(r)
	f.profile("isolated")
}
