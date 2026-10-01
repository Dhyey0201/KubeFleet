// Package materialize builds the single deterministic child Job for a BatchRun UID.
package materialize

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/validation"
	"strings"
)

const RunUIDLabel = "batch.kubefleet.dev/run-uid"
const RunNameLabel = "batch.kubefleet.dev/run-name"
const QueueLabel = "kueue.x-k8s.io/queue-name"
const PriorityLabel = "kueue.x-k8s.io/priority-class"
const ServiceAccount = "kubefleet-worker"

func JobName(name string, uid types.UID) string {
	sum := sha256.Sum256([]byte(uid))
	prefix := name
	if len(prefix) > 46 {
		prefix = prefix[:46]
	}
	prefix = strings.TrimRight(prefix, "-.")
	return prefix + "-" + hex.EncodeToString(sum[:8])
}
func Build(run *api.BatchRun) (*batch.Job, error) {
	if run.UID == "" || run.Name == "" || run.Namespace == "" {
		return nil, fmt.Errorf("persisted run identity required")
	}
	if run.Spec.Cancel || run.Status.StopReason != "" || run.Status.Result != nil || !run.DeletionTimestamp.IsZero() {
		return nil, fmt.Errorf("run has stopped or is deleting")
	}
	if errs := validation.ValidateSpec(run.Spec); len(errs) > 0 {
		return nil, errs.ToAggregate()
	}
	s := run.Spec
	labels := map[string]string{RunUIDLabel: string(run.UID), RunNameLabel: run.Name, QueueLabel: s.QueueName, PriorityLabel: s.PriorityClassName}
	// The run name may exceed a label value's bound; UID is the authoritative identity.
	if len(run.Name) > 63 {
		delete(labels, RunNameLabel)
	}
	owner := meta.OwnerReference{APIVersion: api.GroupVersion.String(), Kind: "BatchRun", Name: run.Name, UID: run.UID, Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true)}
	return &batch.Job{
		ObjectMeta: meta.ObjectMeta{Name: JobName(run.Name, run.UID), Namespace: run.Namespace, Labels: labels, OwnerReferences: []meta.OwnerReference{owner}},
		Spec: batch.JobSpec{
			Suspend: ptr.To(true), Parallelism: ptr.To(s.Parallelism), Completions: ptr.To(s.Completions), BackoffLimit: ptr.To(s.BackoffLimit), ActiveDeadlineSeconds: ptr.To(int64(s.ExecutionTimeoutSeconds)),
			Template: core.PodTemplateSpec{
				ObjectMeta: meta.ObjectMeta{Labels: map[string]string{RunUIDLabel: string(run.UID)}},
				Spec: core.PodSpec{
					ServiceAccountName: ServiceAccount, AutomountServiceAccountToken: ptr.To(false), RestartPolicy: core.RestartPolicyNever, TerminationGracePeriodSeconds: ptr.To[int64](10),
					SecurityContext: &core.PodSecurityContext{RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To[int64](65532), RunAsGroup: ptr.To[int64](65532), SeccompProfile: &core.SeccompProfile{Type: core.SeccompProfileTypeRuntimeDefault}},
					Containers: []core.Container{{
						Name: "worker", Image: s.Image, Command: append([]string(nil), s.Command...), Args: append([]string(nil), s.Args...), Resources: *s.Resources.DeepCopy(), Env: []core.EnvVar{{Name: "RUN_UID", Value: string(run.UID)}},
						SecurityContext: &core.SecurityContext{AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true), Capabilities: &core.Capabilities{Drop: []core.Capability{"ALL"}}},
					}},
				},
			},
		},
	}, nil
}

// Owned verifies both controller UID and immutable identity before adoption/deletion.
func Owned(run *api.BatchRun, job *batch.Job) bool {
	owner := meta.GetControllerOf(job)
	return run.UID != "" && job.Namespace == run.Namespace && job.Name == JobName(run.Name, run.UID) && owner != nil && owner.UID == run.UID && owner.Name == run.Name && owner.Kind == "BatchRun" && owner.APIVersion == api.GroupVersion.String() && job.Labels[RunUIDLabel] == string(run.UID) && job.Labels[QueueLabel] == run.Spec.QueueName && job.Labels[PriorityLabel] == run.Spec.PriorityClassName
}
