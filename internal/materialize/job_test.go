package materialize

import (
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"strings"
	"testing"
)

func sample() *api.BatchRun {
	return &api.BatchRun{ObjectMeta: meta.ObjectMeta{Name: "catalog", Namespace: "team-a", UID: types.UID("run-uid-one")}, Spec: api.BatchRunSpec{QueueName: "team-a", Image: "kubefleet-worker:compat", Command: []string{"/worker"}, Args: []string{"sleep", "--duration=2s"}, Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("250m"), core.ResourceMemory: resource.MustParse("64Mi")}, Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("500m"), core.ResourceMemory: resource.MustParse("128Mi")}}, Parallelism: 2, Completions: 4, TotalTimeoutSeconds: 600, ExecutionTimeoutSeconds: 60, PriorityClassName: "normal"}}
}
func TestNameAndOwnership(t *testing.T) {
	run := sample()
	run.Name = strings.Repeat("a", 253)
	name := JobName(run.Name, run.UID)
	if len(name) > 63 || len(validation.IsDNS1123Label(name)) > 0 {
		t.Fatalf("invalid name %q", name)
	}
	if name != JobName(run.Name, run.UID) || name == JobName(run.Name, "another-uid") {
		t.Fatal("name does not encode immutable UID deterministically")
	}
	j, err := Build(run)
	if err != nil {
		t.Fatal(err)
	}
	if !Owned(run, j) {
		t.Fatal("own child rejected")
	}
	j.OwnerReferences[0].UID = "stale-uid"
	if Owned(run, j) {
		t.Fatal("stale child adopted")
	}
	j.OwnerReferences[0].UID = run.UID
	j.Labels[RunUIDLabel] = "copied-uid"
	if Owned(run, j) {
		t.Fatal("unverified labels adopted")
	}
}
func TestSecurityAndIntent(t *testing.T) {
	run := sample()
	j, err := Build(run)
	if err != nil {
		t.Fatal(err)
	}
	pod := j.Spec.Template.Spec
	container := pod.Containers[0]
	if !*j.Spec.Suspend || *j.Spec.Parallelism != 2 || *j.Spec.Completions != 4 || *j.Spec.ActiveDeadlineSeconds != 60 || j.Spec.TTLSecondsAfterFinished != nil {
		t.Fatal("incorrect Job execution intent")
	}
	if pod.ServiceAccountName != ServiceAccount || *pod.AutomountServiceAccountToken || !*pod.SecurityContext.RunAsNonRoot || *pod.SecurityContext.RunAsUser == 0 || *container.SecurityContext.AllowPrivilegeEscalation || !*container.SecurityContext.ReadOnlyRootFilesystem || container.SecurityContext.Capabilities.Drop[0] != "ALL" || pod.SecurityContext.SeccompProfile.Type != core.SeccompProfileTypeRuntimeDefault {
		t.Fatal("unsafe security defaults")
	}
	container.Args[0] = "changed"
	container.Resources.Requests[core.ResourceCPU] = resource.MustParse("2")
	if run.Spec.Args[0] != "sleep" || run.Spec.Resources.Requests.Cpu().Cmp(resource.MustParse("250m")) != 0 {
		t.Fatal("builder aliases mutable input")
	}
	for _, stop := range []func(*api.BatchRun){func(r *api.BatchRun) { r.Spec.Cancel = true }, func(r *api.BatchRun) { r.Status.StopReason = "deadline" }, func(r *api.BatchRun) { r.Status.Result = &api.RunResult{Outcome: "Succeeded"} }, func(r *api.BatchRun) { now := meta.Now(); r.DeletionTimestamp = &now }} {
		r := sample()
		stop(r)
		if _, err := Build(r); err == nil {
			t.Fatal("stopped run materialized")
		}
	}
}
