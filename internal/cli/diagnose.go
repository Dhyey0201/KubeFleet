package cli

import (
	"context"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/materialize"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"time"
)

type Finding struct {
	Code        string `json:"code"`
	Source      string `json:"source"`
	Reason      string `json:"reason"`
	Action      string `json:"action"`
	Uncertainty string `json:"uncertainty,omitempty"`
}
type Diagnosis struct {
	Namespace  string    `json:"namespace"`
	Name       string    `json:"name"`
	UID        types.UID `json:"uid"`
	ObservedAt time.Time `json:"observedAt"`
	Findings   []Finding `json:"findings"`
}

func Diagnose(ctx context.Context, c client.Client, r *api.BatchRun) (*Diagnosis, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	d := &Diagnosis{Namespace: r.Namespace, Name: r.Name, UID: r.UID, ObservedAt: time.Now().UTC(), Findings: []Finding{}}
	add := func(code, source, reason, action, uncertainty string) {
		d.Findings = append(d.Findings, Finding{code, source, reason, action, uncertainty})
	}
	readError := func(source string, err error) error {
		if errors.IsForbidden(err) || errors.IsUnauthorized(err) {
			add("InsufficientReadPermissions", source, err.Error(), "Ask the administrator for the documented read-only diagnostic role", "Resource state was not inspected")
			return nil
		}
		return err
	}
	source := "BatchRun/" + r.Name
	if r.Status.ObservedGeneration < r.Generation || len(r.Status.Conditions) == 0 {
		add("ControllerUnavailable", source, "current intent has not been observed", "Inspect manager health and leader Lease", "Status lag alone cannot prove the controller is offline")
	}
	if r.Status.Result != nil {
		code := r.Status.Result.Reason
		if code == "DeadlineExceeded" {
			code = "ExecutionDeadlineExceeded"
		}
		add(code, source, r.Status.Result.Message, "Inspect the terminal Job and worker logs", "Events may have expired")
	}
	if r.Status.StopReason != "" && r.Status.Result == nil {
		add("CleanupPending", source, r.Status.StopReason, "Inspect exact-owned Pods and Workload finalization", "Cleanup can take time while termination and garbage collection proceed")
	}
	if r.Status.PrerequisiteDecision != nil {
		p := r.Status.PrerequisiteDecision
		code := p.Reason
		if p.State == "Pending" || p.State == "Retry" {
			code = "WaitingForPrerequisite"
		}
		add(code, source, p.State+": "+p.Reason, "Inspect the same-namespace approved Job and immutable UID", "")
	}
	if time.Now().After(r.CreationTimestamp.Add(time.Duration(r.Spec.TotalTimeoutSeconds)*time.Second)) && r.Status.Result == nil {
		add("TotalDeadlineExceeded", source, "absolute total deadline has elapsed", "Inspect manager health and cleanup state", "Controller enforcement may be delayed")
	}
	var lq kueue.LocalQueue
	err := c.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: r.Spec.QueueName}, &lq)
	switch {
	case errors.IsNotFound(err):
		add("LocalQueueMissing", "LocalQueue/"+r.Spec.QueueName, "queue does not exist", "Ask the administrator to restore the selected LocalQueue", "")
	case err != nil:
		if e := readError("LocalQueue/"+r.Spec.QueueName, err); e != nil {
			return nil, e
		}
	default:
		var cq kueue.ClusterQueue
		err = c.Get(ctx, types.NamespacedName{Name: string(lq.Spec.ClusterQueue)}, &cq)
		if errors.IsNotFound(err) {
			add("ClusterQueueInactive", "ClusterQueue/"+string(lq.Spec.ClusterQueue), "queue does not exist", "Inspect the administrator queue configuration", "")
		} else if err != nil {
			if e := readError("ClusterQueue/"+string(lq.Spec.ClusterQueue), err); e != nil {
				return nil, e
			}
		} else {
			if !apiMeta.IsStatusConditionTrue(cq.Status.Conditions, "Active") {
				reason := "no Active condition"
				if condition := apiMeta.FindStatusCondition(cq.Status.Conditions, "Active"); condition != nil {
					reason = condition.Reason + ": " + condition.Message
				}
				add("ClusterQueueInactive", "ClusterQueue/"+cq.Name, reason, "Inspect resource flavors and required AdmissionChecks", "")
			}
			for _, name := range cq.Spec.AdmissionChecks {
				var check kueue.AdmissionCheck
				err := c.Get(ctx, types.NamespacedName{Name: string(name)}, &check)
				if errors.IsNotFound(err) || err == nil && !apiMeta.IsStatusConditionTrue(check.Status.Conditions, "Active") {
					add("AdmissionCheckInactive", "AdmissionCheck/"+string(name), "required check is unavailable or inactive", "Inspect gate registration, policy and manager health", "")
				} else if err != nil {
					if e := readError("AdmissionCheck/"+string(name), err); e != nil {
						return nil, e
					}
				}
			}
		}
	}
	var job batch.Job
	if r.Status.JobRef != nil {
		err = c.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: r.Status.JobRef.Name}, &job)
		if errors.IsNotFound(err) || err == nil && (job.UID != r.Status.JobRef.UID || !materialize.Owned(r, &job)) {
			add("JobLost", "Job/"+r.Status.JobRef.Name, "recorded exact-owned Job is absent or replaced", "Create a new BatchRun for another attempt", "Cleanup after a committed stop can also remove the Job")
		} else if err != nil {
			if e := readError("Job/"+r.Status.JobRef.Name, err); e != nil {
				return nil, e
			}
		} else {
			if job.Spec.Suspend != nil && *job.Spec.Suspend {
				add("JobSuspended", "Job/"+job.Name, "Kueue has not started this Job", "Inspect quota and admission checks", "")
			}
			var pods core.PodList
			err = c.List(ctx, &pods, client.InNamespace(r.Namespace), client.MatchingLabels{"batch.kubernetes.io/job-name": job.Name})
			if err != nil {
				if e := readError("Pods for Job/"+job.Name, err); e != nil {
					return nil, e
				}
			} else {
				for _, p := range pods.Items {
					owner := meta.GetControllerOf(&p)
					if owner == nil || owner.Kind != "Job" || owner.UID != job.UID || owner.APIVersion != "batch/v1" {
						continue
					}
					for _, condition := range p.Status.Conditions {
						if condition.Type == core.PodScheduled && condition.Status == core.ConditionFalse && condition.Reason == "Unschedulable" {
							add("PodUnschedulable", "Pod/"+p.Name, condition.Message, "Inspect node capacity and Pod placement constraints", "Reservation does not guarantee physical placement")
						}
					}
					for _, container := range p.Status.ContainerStatuses {
						if waiting := container.State.Waiting; waiting != nil {
							code := "ContainerStartupPending"
							switch waiting.Reason {
							case "ErrImagePull", "ImagePullBackOff":
								code = "ImagePullFailure"
							case "CrashLoopBackOff":
								code = "ContainerCrash"
							}
							add(code, "Pod/"+p.Name, waiting.Reason+": "+waiting.Message, "Inspect Pod events and worker image/logs", "Events may have expired")
						}
						if terminated := container.State.Terminated; terminated != nil && terminated.ExitCode != 0 {
							add("ContainerCrash", "Pod/"+p.Name, terminated.Reason, "Inspect the worker termination exit code and logs", "")
						}
					}
				}
			}
		}
	}
	if r.Status.WorkloadRef != nil {
		var w kueue.Workload
		err = c.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: r.Status.WorkloadRef.Name}, &w)
		if err != nil && !errors.IsNotFound(err) {
			if e := readError("Workload/"+r.Status.WorkloadRef.Name, err); e != nil {
				return nil, e
			}
		} else if err == nil && w.UID == r.Status.WorkloadRef.UID {
			owner := meta.GetControllerOf(&w)
			if owner != nil && owner.Kind == "Job" && owner.APIVersion == "batch/v1" && r.Status.JobRef != nil && owner.UID == r.Status.JobRef.UID {
				reserved := apiMeta.IsStatusConditionTrue(w.Status.Conditions, "QuotaReserved")
				admitted := apiMeta.IsStatusConditionTrue(w.Status.Conditions, "Admitted")
				if reserved && !admitted {
					add("QuotaReservedNotAdmitted", "Workload/"+w.Name, "quota is reserved while admission is blocked", "Inspect Pending or Retry admission checks", "")
				} else if !reserved && !admitted && !apiMeta.IsStatusConditionTrue(w.Status.Conditions, "Finished") {
					add("WaitingForQuota", "Workload/"+w.Name, "no current quota reservation", "Inspect quota, competing Workloads, and backoff", "This observation alone does not establish starvation")
				}
			}
		}
	}
	if len(d.Findings) == 0 {
		add("ExecutionObserved", source, r.Status.Phase+": "+r.Status.Reason, "Use status or wait to observe progress", "This is a point-in-time observation")
	}
	return d, nil
}
