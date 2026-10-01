//go:build integration

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	apps "k8s.io/api/apps/v1"
	batch "k8s.io/api/batch/v1"
	coordination "k8s.io/api/coordination/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"os"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"strings"
	"testing"
	"time"
)

type suite struct {
	t       *testing.T
	c       client.Client
	k       *kubernetes.Clientset
	cfg     *rest.Config
	ctx     context.Context
	id, dir string
}

func newSuite(t *testing.T) *suite {
	t.Helper()
	path := os.Getenv("KUBECONFIG")
	raw, err := clientcmd.LoadFromFile(path)
	must(t, err)
	if raw.CurrentContext != "kind-kubefleet" {
		t.Fatal("unexpected context")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	must(t, err)
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{api.AddToScheme, apps.AddToScheme, batch.AddToScheme, core.AddToScheme, coordination.AddToScheme, kueue.AddToScheme} {
		must(t, add(s))
	}
	c, err := client.New(cfg, client.Options{Scheme: s})
	must(t, err)
	k, err := kubernetes.NewForConfig(cfg)
	must(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	var ns core.Namespace
	must(t, c.Get(ctx, types.NamespacedName{Name: "kube-system"}, &ns))
	if ns.Labels["batch.kubefleet.dev/project"] != "kubefleet" {
		t.Fatal("project identity mismatch")
	}
	dir := os.Getenv("E2E_EVIDENCE")
	if dir == "" {
		t.Fatal("E2E_EVIDENCE required")
	}
	must(t, os.MkdirAll(dir, 0755))
	f := &suite{t: t, c: c, k: k, cfg: cfg, ctx: ctx, id: fmt.Sprintf("e2e-%d", time.Now().UnixNano()), dir: dir}
	t.Cleanup(func() {
		if t.Failed() {
			f.diagnostics()
		}
	})
	return f
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func (f *suite) save(name string, obj any) {
	f.t.Helper()
	b, err := json.MarshalIndent(obj, "", "  ")
	must(f.t, err)
	must(f.t, os.WriteFile(filepath.Join(f.dir, f.id+"-"+name+".json"), append(b, '\n'), 0644))
}
func (f *suite) await(label string, p func() (bool, error)) {
	f.t.Helper()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	timer := time.NewTimer(90 * time.Second)
	defer timer.Stop()
	for {
		ok, err := p()
		must(f.t, err)
		if ok {
			return
		}
		select {
		case <-f.ctx.Done():
			f.t.Fatal(f.ctx.Err())
		case <-timer.C:
			f.diagnostics()
			f.t.Fatal("timeout: " + label)
		case <-tick.C:
		}
	}
}
func (f *suite) diagnostics() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for name, obj := range map[string]client.ObjectList{"runs": &api.BatchRunList{}, "jobs": &batch.JobList{}, "pods": &core.PodList{}, "workloads": &kueue.WorkloadList{}, "queues": &kueue.ClusterQueueList{}, "events": &core.EventList{}} {
		if err := f.c.List(ctx, obj); err == nil {
			f.save("diagnostic-"+name, obj)
		}
	}
}
func (f *suite) run(name, mode string, changes func(*api.BatchRun)) *api.BatchRun {
	f.t.Helper()
	r := &api.BatchRun{ObjectMeta: meta.ObjectMeta{Name: f.id + "-" + name, Namespace: "team-a"}, Spec: api.BatchRunSpec{QueueName: "team-a", Image: "kubefleet-worker:compat", Command: []string{"/worker"}, Args: []string{mode, "--duration=120s"}, TotalTimeoutSeconds: 300, ExecutionTimeoutSeconds: 180, Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("250m"), core.ResourceMemory: resource.MustParse("64Mi")}, Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("250m"), core.ResourceMemory: resource.MustParse("128Mi")}}}}
	if changes != nil {
		changes(r)
	}
	must(f.t, f.c.Create(f.ctx, r))
	f.save(name+"-created", r)
	f.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := f.c.Delete(ctx, r, client.PropagationPolicy(meta.DeletePropagationForeground))
		if client.IgnoreNotFound(err) != nil {
			f.t.Error(err)
		}
	})
	return r
}
func (f *suite) refresh(r *api.BatchRun) error {
	return f.c.Get(f.ctx, client.ObjectKeyFromObject(r), r)
}
func (f *suite) terminal(r *api.BatchRun, outcome, reason string) {
	f.t.Helper()
	f.await("terminal "+outcome, func() (bool, error) { err := f.refresh(r); return r.Status.Result != nil, err })
	f.save(r.Name+"-terminal", r)
	if r.Status.Result.Outcome != outcome || reason != "" && r.Status.Result.Reason != reason {
		f.t.Fatalf("unexpected result %+v", r.Status.Result)
	}
}
func (f *suite) cancel(r *api.BatchRun) {
	must(f.t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := f.refresh(r); err != nil {
			return err
		}
		if r.Status.Result != nil {
			return nil
		}
		before := r.DeepCopy()
		r.Spec.Cancel = true
		return f.c.Patch(f.ctx, r, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	}))
}
func (f *suite) child(r *api.BatchRun) *batch.Job {
	f.t.Helper()
	f.await("durable Job ref", func() (bool, error) { err := f.refresh(r); return r.Status.JobRef != nil, err })
	var j batch.Job
	must(f.t, f.c.Get(f.ctx, types.NamespacedName{Name: r.Status.JobRef.Name, Namespace: r.Namespace}, &j))
	return &j
}
func (f *suite) clean(r *api.BatchRun) {
	f.t.Helper()
	f.await("UID-owned execution cleanup", func() (bool, error) {
		var jobs batch.JobList
		if err := f.c.List(f.ctx, &jobs, client.InNamespace(r.Namespace)); err != nil {
			return false, err
		}
		for _, j := range jobs.Items {
			o := meta.GetControllerOf(&j)
			if o != nil && o.UID == r.UID {
				return false, nil
			}
		}
		var pods core.PodList
		if err := f.c.List(f.ctx, &pods, client.InNamespace(r.Namespace)); err != nil {
			return false, err
		}
		for _, p := range pods.Items {
			if p.Labels["batch.kubefleet.dev/run-uid"] == string(r.UID) {
				return false, nil
			}
		}
		var workloads kueue.WorkloadList
		if err := f.c.List(f.ctx, &workloads, client.InNamespace(r.Namespace)); err != nil {
			return false, err
		}
		for _, w := range workloads.Items {
			o := meta.GetControllerOf(&w)
			if o != nil && r.Status.JobRef != nil && o.UID == r.Status.JobRef.UID {
				return false, nil
			}
		}
		return true, nil
	})
}
func (f *suite) configureManager(image, point, target string, replicas int32) {
	f.t.Helper()
	key := types.NamespacedName{Name: "kubefleet-manager", Namespace: "kubefleet-system"}
	must(f.t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var d apps.Deployment
		if err := f.c.Get(f.ctx, key, &d); err != nil {
			return err
		}
		d.Spec.Replicas = ptr.To(replicas)
		d.Spec.Template.Spec.Containers[0].Image = image
		d.Spec.Template.Spec.Containers[0].Env = []core.EnvVar{{Name: "KUBEFLEET_FAULT_POINT", Value: point}, {Name: "KUBEFLEET_FAULT_RUN", Value: target}}
		return f.c.Update(f.ctx, &d)
	}))
	f.await("manager rollout", func() (bool, error) {
		var d apps.Deployment
		err := f.c.Get(f.ctx, key, &d)
		return d.Status.ObservedGeneration == d.Generation && d.Status.UpdatedReplicas == replicas && d.Status.AvailableReplicas == replicas && d.Status.Replicas == replicas, err
	})
	probe := &api.BatchRun{ObjectMeta: meta.ObjectMeta{Name: f.id + "-admission-probe", Namespace: "team-a"}, Spec: api.BatchRunSpec{QueueName: "team-a", Image: "kubefleet-worker:compat", Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("250m"), core.ResourceMemory: resource.MustParse("64Mi")}, Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("250m"), core.ResourceMemory: resource.MustParse("128Mi")}}}}
	f.await("TLS admission endpoint ready", func() (bool, error) {
		err := f.c.Create(f.ctx, probe.DeepCopy(), client.DryRunAll)
		return err == nil, nil
	})

}
func (f *suite) leader() *core.Pod {
	f.t.Helper()
	var found *core.Pod
	f.await("leader Pod identity", func() (bool, error) {
		var lease coordination.Lease
		if err := f.c.Get(f.ctx, types.NamespacedName{Name: "kubefleet.batch.kubefleet.dev", Namespace: "kubefleet-system"}, &lease); err != nil {
			return false, err
		}
		if lease.Spec.HolderIdentity == nil {
			return false, nil
		}
		var pods core.PodList
		if err := f.c.List(f.ctx, &pods, client.InNamespace("kubefleet-system"), client.MatchingLabels{"app": "kubefleet-manager"}); err != nil {
			return false, err
		}
		for _, p := range pods.Items {
			if p.DeletionTimestamp.IsZero() && strings.HasPrefix(*lease.Spec.HolderIdentity, p.Name+"_") {
				found = p.DeepCopy()
				f.save("lease", lease)
				return true, nil
			}
		}
		return false, nil
	})
	return found
}
func (f *suite) kill(p *core.Pod) {
	uid := p.UID
	must(f.t, f.c.Delete(f.ctx, p, client.Preconditions{UID: &uid}, client.GracePeriodSeconds(0)))
}
func (f *suite) boundary(point, identity string) *core.Pod {
	f.t.Helper()
	var hit *core.Pod
	f.await("fault boundary "+point, func() (bool, error) {
		var pods core.PodList
		if err := f.c.List(f.ctx, &pods, client.InNamespace("kubefleet-system"), client.MatchingLabels{"app": "kubefleet-manager"}); err != nil {
			return false, err
		}
		for _, p := range pods.Items {
			if p.DeletionTimestamp != nil {
				continue
			}
			b, err := f.k.CoreV1().Pods(p.Namespace).GetLogs(p.Name, &core.PodLogOptions{Container: "manager"}).DoRaw(f.ctx)
			if err != nil {
				continue
			}
			if strings.Contains(string(b), `"point":"`+point+`"`) && strings.Contains(string(b), `"identity":"`+identity+`"`) {
				must(f.t, os.WriteFile(filepath.Join(f.dir, f.id+"-boundary.log"), b, 0644))
				hit = p.DeepCopy()
				return true, nil
			}
		}
		return false, nil
	})
	return hit
}
