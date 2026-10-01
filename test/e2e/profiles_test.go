//go:build integration

package e2e

import (
	"fmt"
	core "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"os/exec"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"strconv"
	"testing"
	"time"
)

func (f *suite) profile(name string) {
	f.t.Helper()
	root, err := filepath.Abs("../..")
	must(f.t, err)
	cmd := exec.CommandContext(f.ctx, "bash", filepath.Join(root, "scripts/apply-profile.sh"), name, "smoke")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	f.save("profile-"+name, map[string]any{"output": string(out), "profile": name, "scale": "smoke"})
	if err != nil {
		f.t.Fatalf("apply profile: %v\n%s", err, out)
	}
}
func (f *suite) pods(r *api.BatchRun) []core.Pod {
	f.t.Helper()
	var list core.PodList
	must(f.t, f.c.List(f.ctx, &list, client.InNamespace(r.Namespace), client.MatchingLabels{"batch.kubefleet.dev/run-uid": string(r.UID)}))
	return list.Items
}
func (f *suite) runningUID(r *api.BatchRun) types.UID {
	f.t.Helper()
	var uid types.UID
	f.await("Running Pod", func() (bool, error) {
		for _, p := range f.pods(r) {
			if p.Status.Phase == core.PodRunning && p.DeletionTimestamp.IsZero() {
				uid = p.UID
				return true, nil
			}
		}
		return false, nil
	})
	return uid
}
func (f *suite) stopAll(runs ...*api.BatchRun) {
	for _, r := range runs {
		f.cancel(r)
		f.terminal(r, "Cancelled", "")
		f.clean(r)
	}
}
func TestM3Profiles(t *testing.T) {
	for _, profile := range []string{"isolated", "shared"} {
		t.Run(profile, func(t *testing.T) {
			f := newSuite(t)
			f.profile(profile)
			runs := make([]*api.BatchRun, 3)
			for i := range runs {
				runs[i] = f.run(fmt.Sprintf("asymmetric-%d", i), "cpu", nil)
			}
			want := 1
			if profile == "shared" {
				want = 3
			}
			f.await("expected admitted demand", func() (bool, error) {
				n := 0
				for _, r := range runs {
					if err := f.refresh(r); err != nil {
						return false, err
					}
					if r.Status.Phase == "Running" {
						n++
					}
				}
				return n == want, nil
			})
			// Verify a stable interval, rather than a transient initial count.
			until := time.Now().Add(3 * time.Second)
			for time.Now().Before(until) {
				n := 0
				for _, r := range runs {
					must(t, f.refresh(r))
					if r.Status.Phase == "Running" {
						n++
					}
				}
				if n != want {
					t.Fatalf("expected %d running, got %d", want, n)
				}
				time.Sleep(100 * time.Millisecond)
			}
			var cq kueue.ClusterQueue
			must(t, f.c.Get(f.ctx, types.NamespacedName{Name: "team-a"}, &cq))
			cpu := resource.Quantity{}
			for _, flavor := range cq.Status.FlavorsReservation {
				for _, usage := range flavor.Resources {
					if usage.Name == core.ResourceCPU {
						cpu.Add(usage.Total)
					}
				}
			}
			if cpu.Cmp(resource.MustParse(fmt.Sprintf("%dm", 250*want))) != 0 {
				t.Fatalf("unexpected reserved CPU %s", cpu.String())
			}
			f.save("asymmetric-queue", cq)
			f.save("asymmetric-runs", runs)
			f.stopAll(runs...)
		})
	}
	t.Run("within-queue-preemption", func(t *testing.T) {
		f := newSuite(t)
		f.profile("priority")
		// Remove cohort capacity from this scenario to isolate within-queue policy.
		var cq kueue.ClusterQueue
		must(t, f.c.Get(f.ctx, types.NamespacedName{Name: "team-a"}, &cq))
		cq.Spec.Cohort = ""
		for gi := range cq.Spec.ResourceGroups {
			for fi := range cq.Spec.ResourceGroups[gi].Flavors {
				for ri := range cq.Spec.ResourceGroups[gi].Flavors[fi].Resources {
					q := &cq.Spec.ResourceGroups[gi].Flavors[fi].Resources[ri]
					q.BorrowingLimit = nil
					q.LendingLimit = nil
				}
			}
		}
		must(t, f.c.Update(f.ctx, &cq))
		f.save("within-queue-config", cq)
		low := f.run("low", "cpu", func(r *api.BatchRun) { r.Spec.PriorityClassName = "low" })
		oldPod := f.runningUID(low)
		job := f.child(low)
		f.save("victim-before", f.pods(low))
		high := f.run("high", "sleep", func(r *api.BatchRun) { r.Spec.PriorityClassName = "high" })
		f.runningUID(high)
		f.await("victim preempted", func() (bool, error) {
			var w kueue.Workload
			if err := f.c.Get(f.ctx, types.NamespacedName{Namespace: low.Namespace, Name: low.Status.WorkloadRef.Name}, &w); err != nil {
				return false, err
			}
			if apiMeta.IsStatusConditionTrue(w.Status.Conditions, "Evicted") {
				f.save("victim-evicted", w)
				return true, nil
			}
			return false, nil
		})
		f.save("victim-after", f.pods(low))
		f.cancel(high)
		f.terminal(high, "Cancelled", "")
		f.clean(high)
		replacement := f.runningUID(low)
		must(t, f.refresh(low))
		if low.Status.JobRef.UID != job.UID || replacement == oldPod {
			t.Fatal("preemption did not restart a Pod under the same logical Job")
		}
		f.save("replacement", map[string]any{"jobUID": job.UID, "oldPodUID": oldPod, "newPodUID": replacement})
		f.stopAll(low)
	})
	t.Run("cohort-reclaim", func(t *testing.T) {
		f := newSuite(t)
		f.profile("priority")
		borrower := f.run("borrower", "cpu", func(r *api.BatchRun) {
			r.Spec.Parallelism = 3
			r.Spec.Completions = 3
			r.Spec.PriorityClassName = "low"
		})
		f.await("three borrower Pods", func() (bool, error) {
			n := 0
			for _, p := range f.pods(borrower) {
				if p.Status.Phase == core.PodRunning && p.DeletionTimestamp.IsZero() {
					n++
				}
			}
			return n == 3, nil
		})
		old := f.pods(borrower)
		job := f.child(borrower)
		f.save("borrower-before", old)
		// Sample actual cumulative CPU while these exact Pods still exist. The
		// last pre-eviction scrape is a lower bound; the missing tail is explicit.
		cpuSamples := make([]map[string]any, 0, len(old))
		cpuLowerBound := 0.0
		for _, pod := range old {
			query := fmt.Sprintf(`max(container_cpu_usage_seconds_total{namespace=%q,pod=%q,container="worker"})`, pod.Namespace, pod.Name)
			f.await("borrower CPU sample "+pod.Name, func() (bool, error) {
				var current core.Pod
				if err := f.c.Get(f.ctx, client.ObjectKeyFromObject(&pod), &current); err != nil {
					return false, err
				}
				if current.UID != pod.UID {
					return false, fmt.Errorf("borrower Pod identity changed before measurement")
				}
				j, err := f.query(query)
				if err != nil || j["status"] != "success" {
					return false, nil
				}
				values := j["data"].(map[string]any)["result"].([]any)
				if len(values) != 1 {
					return false, nil
				}
				value := values[0].(map[string]any)["value"].([]any)[1].(string)
				seconds, err := strconv.ParseFloat(value, 64)
				if err != nil || seconds <= 0 {
					return false, nil
				}
				cpuLowerBound += seconds
				cpuSamples = append(cpuSamples, map[string]any{"podUID": pod.UID, "podName": pod.Name, "cpuSeconds": seconds, "query": query, "observedAt": time.Now().UTC(), "source": j})
				return true, nil
			})
		}
		owner := f.run("owner", "sleep", func(r *api.BatchRun) {
			r.Namespace = "team-b"
			r.Spec.QueueName = "team-b"
			r.Spec.PriorityClassName = "high"
		})
		f.runningUID(owner)
		f.await("borrower eviction", func() (bool, error) {
			var w kueue.Workload
			err := f.c.Get(f.ctx, types.NamespacedName{Namespace: borrower.Namespace, Name: borrower.Status.WorkloadRef.Name}, &w)
			ok := apiMeta.IsStatusConditionTrue(w.Status.Conditions, "Evicted")
			if ok {
				f.save("borrower-evicted", w)
			}
			return ok, err
		})
		f.save("borrower-after", f.pods(borrower))
		f.save("reclaim-cpu-lower-bound", map[string]any{
			"jobUID": job.UID, "samples": cpuSamples, "observedLostCPUSecondsLowerBound": cpuLowerBound,
			"quality": []string{"actual_cumulative_cpu_before_confirmed_preemption", "missing_final_sample_lower_bound", "not_total_cluster_cpu_or_full_benchmark"},
		})
		var events core.EventList
		must(t, f.c.List(f.ctx, &events, client.InNamespace("team-a")))
		f.save("reclaim-events", events)
		f.cancel(owner)
		f.terminal(owner, "Cancelled", "")
		f.clean(owner)
		f.await("borrower restarted", func() (bool, error) {
			n := 0
			for _, p := range f.pods(borrower) {
				if p.Status.Phase == core.PodRunning && p.DeletionTimestamp.IsZero() {
					for _, before := range old {
						if before.UID == p.UID {
							return false, nil
						}
					}
					n++
				}
			}
			return n == 3, nil
		})
		must(t, f.refresh(borrower))
		if borrower.Status.JobRef.UID != job.UID {
			t.Fatal("reclaim changed logical Job")
		}
		f.save("borrower-restarted", f.pods(borrower))
		f.stopAll(borrower)
	})
	f := newSuite(t)
	f.profile("isolated")
}
