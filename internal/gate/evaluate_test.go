package gate

import (
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"testing"
	"time"
)

func TestTrustAndDeadline(t *testing.T) {
	now := time.Now()
	p := &api.PrerequisiteGatePolicy{Spec: api.PrerequisiteGatePolicySpec{AllowUngatedBatchRuns: ptr.To(true)}}
	run := &api.BatchRun{ObjectMeta: meta.ObjectMeta{Namespace: "team-a", UID: "run", CreationTimestamp: meta.NewTime(now.Add(-time.Second))}, Spec: api.BatchRunSpec{Prerequisite: &api.PrerequisiteReference{JobName: "prepare", JobUID: "approved-uid", WaitTimeoutSeconds: 60}}}
	job := &batch.Job{ObjectMeta: meta.ObjectMeta{Name: "prepare", Namespace: "team-a", UID: "approved-uid", Labels: map[string]string{"batch.kubefleet.dev/prerequisite": "approved"}}, Status: batch.JobStatus{Conditions: []batch.JobCondition{{Type: batch.JobComplete, Status: core.ConditionTrue}}}}
	cases := []struct {
		name, reason string
		state        kueue.CheckState
		mutate       func(*api.BatchRun, *batch.Job)
		missing      bool
	}{
		{"complete", "PrerequisiteComplete", kueue.CheckStateReady, func(*api.BatchRun, *batch.Job) {}, false},
		{"missing", "PrerequisiteMissing", kueue.CheckStatePending, func(*api.BatchRun, *batch.Job) {}, true},
		{"running", "PrerequisiteIncomplete", kueue.CheckStatePending, func(_ *api.BatchRun, j *batch.Job) { j.Status.Conditions = nil }, false},
		{"failed", "PrerequisiteFailed", kueue.CheckStateRejected, func(_ *api.BatchRun, j *batch.Job) {
			j.Status.Conditions = []batch.JobCondition{{Type: batch.JobFailed, Status: core.ConditionTrue}}
		}, false},
		{"replacement", "PrerequisiteUIDMismatch", kueue.CheckStateRejected, func(_ *api.BatchRun, j *batch.Job) { j.UID = "new-uid" }, false},
		{"unapproved", "PrerequisiteNotAllowed", kueue.CheckStateRejected, func(_ *api.BatchRun, j *batch.Job) { j.Labels = nil }, false},
		{"queued", "PrerequisiteNotAllowed", kueue.CheckStateRejected, func(_ *api.BatchRun, j *batch.Job) { j.Labels["kueue.x-k8s.io/queue-name"] = "team-a" }, false},
		{"managed", "PrerequisiteNotAllowed", kueue.CheckStateRejected, func(_ *api.BatchRun, j *batch.Job) {
			j.OwnerReferences = []meta.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: "BatchRun", Name: "another", UID: "another"}}
		}, false},
		{"cross-namespace", "PrerequisiteNotAllowed", kueue.CheckStateRejected, func(_ *api.BatchRun, j *batch.Job) { j.Namespace = "team-b" }, false},
		{"deadline", "PrerequisiteDeadlineExceeded", kueue.CheckStateRejected, func(r *api.BatchRun, _ *batch.Job) { r.CreationTimestamp = meta.NewTime(now.Add(-60 * time.Second)) }, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, j := run.DeepCopy(), job.DeepCopy()
			c.mutate(r, j)
			if c.missing {
				j = nil
			}
			got := Evaluate(r, p, j, now)
			if got.State != c.state || got.Reason != c.reason {
				t.Fatalf("got %+v", got)
			}
		})
	}
}
func TestSnapshotIdentity(t *testing.T) {
	run := &api.BatchRun{ObjectMeta: meta.ObjectMeta{UID: "run"}, Spec: api.BatchRunSpec{Prerequisite: &api.PrerequisiteReference{JobUID: "prepare"}}}
	snapshot := Encode(run, "PrerequisiteComplete")
	if !Matches(snapshot, run) {
		t.Fatal("same UID snapshot lost")
	}
	other := run.DeepCopy()
	other.UID = "new-run"
	if Matches(snapshot, other) {
		t.Fatal("new run inherited snapshot")
	}
	other = run.DeepCopy()
	other.Spec.Prerequisite.JobUID = "new-prepare"
	if Matches(snapshot, other) {
		t.Fatal("new prerequisite inherited snapshot")
	}
	if Matches("malformed", run) {
		t.Fatal("malformed snapshot accepted")
	}
}
