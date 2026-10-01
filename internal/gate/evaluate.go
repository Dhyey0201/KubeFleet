// Package gate evaluates the one-level prerequisite trust contract without I/O.
package gate

import (
	"encoding/json"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"time"
)

const ControllerName = "batch.kubefleet.dev/prerequisite-gate"
const CheckName = "kubefleet-prerequisite"
const SnapshotCondition = "KubeFleetPrerequisiteReady"

// Snapshot is stored with Ready in the same optimistic Workload status patch.
// A separate condition survives Kueue resetting AdmissionCheck messages on eviction.
type Snapshot struct {
	RunUID          types.UID `json:"runUID"`
	PrerequisiteUID types.UID `json:"prerequisiteUID,omitempty"`
	Reason          string    `json:"reason"`
}
type Decision struct {
	State  kueue.CheckState
	Reason string
}

func Encode(run *api.BatchRun, reason string) string {
	s := Snapshot{RunUID: run.UID, Reason: reason}
	if run.Spec.Prerequisite != nil {
		s.PrerequisiteUID = run.Spec.Prerequisite.JobUID
	}
	b, _ := json.Marshal(s)
	return string(b)
}
func Matches(raw string, run *api.BatchRun) bool {
	var s Snapshot
	if json.Unmarshal([]byte(raw), &s) != nil || s.RunUID != run.UID {
		return false
	}
	if run.Spec.Prerequisite == nil {
		return s.PrerequisiteUID == "" && s.Reason == "NotRequired"
	}
	return s.PrerequisiteUID == run.Spec.Prerequisite.JobUID && s.Reason == "PrerequisiteComplete"
}

// Evaluate requires callers to distinguish missing Jobs from all read errors.
func Evaluate(run *api.BatchRun, policy *api.PrerequisiteGatePolicy, prerequisite *batch.Job, now time.Time) Decision {
	reject := func(reason string) Decision { return Decision{kueue.CheckStateRejected, reason} }
	if run.Spec.Prerequisite == nil {
		if policy.Spec.AllowUngatedBatchRuns != nil && *policy.Spec.AllowUngatedBatchRuns {
			return Decision{kueue.CheckStateReady, "NotRequired"}
		}
		return reject("PrerequisiteRequired")
	}
	ref := run.Spec.Prerequisite
	if !now.Before(run.CreationTimestamp.Add(time.Duration(ref.WaitTimeoutSeconds) * time.Second)) {
		return reject("PrerequisiteDeadlineExceeded")
	}
	if prerequisite == nil {
		return Decision{kueue.CheckStatePending, "PrerequisiteMissing"}
	}
	if prerequisite.UID != ref.JobUID {
		return reject("PrerequisiteUIDMismatch")
	}
	owned := false
	for _, owner := range prerequisite.OwnerReferences {
		if owner.Kind == "BatchRun" && owner.APIVersion == api.GroupVersion.String() {
			owned = true
		}
	}
	if prerequisite.Namespace != run.Namespace || prerequisite.Labels["batch.kubefleet.dev/prerequisite"] != "approved" || prerequisite.Labels["kueue.x-k8s.io/queue-name"] != "" || owned {
		return reject("PrerequisiteNotAllowed")
	}
	for _, c := range prerequisite.Status.Conditions {
		if c.Type == batch.JobFailed && c.Status == core.ConditionTrue {
			return reject("PrerequisiteFailed")
		}
	}
	for _, c := range prerequisite.Status.Conditions {
		if c.Type == batch.JobComplete && c.Status == core.ConditionTrue {
			return Decision{kueue.CheckStateReady, "PrerequisiteComplete"}
		}
	}
	return Decision{kueue.CheckStatePending, "PrerequisiteIncomplete"}
}
