package controller

import (
	"context"
	"fmt"
	"time"

	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	errors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/faults"
	"kubefleet.local/kubefleet/internal/materialize"
	"kubefleet.local/kubefleet/internal/observability"
	"kubefleet.local/kubefleet/internal/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

const workloadJobUIDIndex = "kubefleet.workloadJobUID"

// BatchRunReconciler treats API state as the durable source of execution intent.
type BatchRunReconciler struct {
	client.Client
	Reader      client.Reader
	PolicyName  string
	Options     validation.Options
	Concurrency int
	Recorder    record.EventRecorder
}

func (r *BatchRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var run api.BatchRun
	if err := r.Reader.Get(ctx, req.NamespacedName, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !run.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	if run.Status.Result != nil {
		return ctrl.Result{}, nil
	}
	deadline := run.CreationTimestamp.Add(time.Duration(run.Spec.TotalTimeoutSeconds) * time.Second)
	next := 30 * time.Second
	if left := time.Until(deadline); left > 0 && left < next {
		next = left
	}
	result := ctrl.Result{RequeueAfter: next}
	name := materialize.JobName(run.Name, run.UID)
	var job batch.Job
	err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: name}, &job)
	exists := err == nil
	if err != nil && !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	if exists && !materialize.Owned(&run, &job) {
		if run.Status.StopReason != "" {
			return r.cleanup(ctx, &run, &job, false, nil)
		}
		return result, r.stop(ctx, &run, "JobNameCollision", "The deterministic name is occupied by an unrelated Job")
	}
	if exists && run.Status.JobRef != nil && run.Status.JobRef.UID != job.UID {
		if run.Status.StopReason != "" {
			return r.cleanup(ctx, &run, &job, false, nil)
		}
		return result, r.stop(ctx, &run, "JobLost", "Recorded Job UID was replaced")
	}
	var wl *kueue.Workload
	if exists {
		wl, err = r.workload(ctx, &job)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	if run.Status.StopReason != "" {
		return r.cleanup(ctx, &run, &job, exists, wl)
	}
	if exists {
		for _, c := range job.Status.Conditions {
			if c.Status == core.ConditionTrue && (c.Type == batch.JobComplete || c.Type == batch.JobFailed) {
				outcome := "Failed"
				reason := c.Reason
				if c.Type == batch.JobComplete {
					outcome = "Succeeded"
					reason = "JobComplete"
				}
				if reason == "" {
					reason = "JobFailed"
				}
				return ctrl.Result{}, r.status(ctx, &run, func(s *api.BatchRunStatus) {
					now := meta.Now()
					s.JobRef = &api.ObjectReference{Name: job.Name, UID: job.UID}
					s.Phase = outcome
					s.Result = &api.RunResult{Outcome: outcome, Reason: reason, Message: bounded(c.Message)}
					s.FinishedAt = &now
					s.Reason = reason
					s.Message = bounded(c.Message)
					condition(s, run.Generation, "Complete", meta.ConditionTrue, reason, c.Message)
					condition(s, run.Generation, "Running", meta.ConditionFalse, "JobTerminal", "Job execution is terminal")
				})
			}
		}
	}
	if run.Spec.Cancel {
		return result, r.stop(ctx, &run, "UserCancelled", "User requested cancellation")
	}
	if !time.Now().Before(deadline) {
		return result, r.stop(ctx, &run, "TotalDeadlineExceeded", "Absolute total deadline expired")
	}
	if invalid := validation.ValidateSpec(run.Spec); len(invalid) > 0 {
		return result, r.stop(ctx, &run, "InvalidSpec", "Spec violates API validation")
	}
	if !exists && run.Status.JobRef != nil {
		return result, r.stop(ctx, &run, "JobLost", "Recorded Job disappeared; execution will not be recreated")
	}
	if !exists {
		var policy api.PrerequisiteGatePolicy
		if err := r.Reader.Get(ctx, types.NamespacedName{Name: r.PolicyName}, &policy); err != nil {
			return ctrl.Result{}, err
		}
		if invalid := validation.ValidateCreate(run.Spec, &policy, r.Options); len(invalid) > 0 {
			return result, r.stop(ctx, &run, "InvalidSpec", "Spec violates the active administrator policy")
		}
	}
	if wl != nil {
		if check := namedCheck(wl); check != nil && check.State == kueue.CheckStateRejected {
			return result, r.stop(ctx, &run, check.Message, "Prerequisite admission rejected: "+check.Message)
		}
	}
	if wl != nil && !isTrue(wl, "Finished") && !ptrActive(wl) {
		return result, r.stop(ctx, &run, "AdmissionRejected", "Kueue deactivated the Workload")
	}
	if !exists {
		if run.Spec.Prerequisite != nil {
			if err := r.requireGate(ctx, &run); err != nil {
				return ctrl.Result{}, err
			}
		}
		desired, err := materialize.Build(&run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err = r.Create(ctx, desired); err != nil {
			observability.CreationErrors.WithLabelValues(errorClass(err)).Inc()
			if !errors.IsAlreadyExists(err) {
				return ctrl.Result{}, err
			}
			// Bypass cache on a concurrent create; never adopt from name alone.
			if err = r.Reader.Get(ctx, client.ObjectKeyFromObject(desired), &job); err != nil {
				return ctrl.Result{}, err
			}
			if !materialize.Owned(&run, &job) {
				return result, r.stop(ctx, &run, "JobNameCollision", "Concurrent creation occupied the deterministic name")
			}
		} else {
			job = *desired
			faults.Hit("after-job-create", run.Namespace+"/"+run.Name)
		}
	}
	running, err := r.running(ctx, &job)
	if err != nil {
		return ctrl.Result{}, err
	}
	queueReason, queueMessage, err := r.queueDiagnostic(ctx, &run)
	if err != nil {
		return ctrl.Result{}, err
	}
	return result, r.status(ctx, &run, func(s *api.BatchRunStatus) {
		s.JobRef = &api.ObjectReference{Name: job.Name, UID: job.UID}
		s.Phase = "Queued"
		s.Reason = "WaitingForQuota"
		s.Message = "Waiting for Kueue admission"
		if queueReason != "" {
			s.Reason = queueReason
			s.Message = queueMessage
		}
		condition(s, run.Generation, "Valid", meta.ConditionTrue, "Validated", "Spec accepted by active policy")
		condition(s, run.Generation, "JobCreated", meta.ConditionTrue, "JobCreated", "One deterministic owned Job exists")
		condition(s, run.Generation, "Stopping", meta.ConditionFalse, "NotStopping", "No stop intent committed")
		condition(s, run.Generation, "Complete", meta.ConditionFalse, "Nonterminal", "Execution is not terminal")
		if run.Spec.Prerequisite == nil {
			condition(s, run.Generation, "PrerequisiteReady", meta.ConditionTrue, "NotRequired", "No prerequisite requested")
		}

		reserved, admitted := false, false
		if wl != nil {
			s.WorkloadRef = &api.ObjectReference{Name: wl.Name, UID: wl.UID}
			reserved = isTrue(wl, "QuotaReserved") && !isTrue(wl, "Finished")
			admitted = isTrue(wl, "Admitted") && !isTrue(wl, "Finished")
		}
		condition(s, run.Generation, "QuotaReserved", truth(reserved), "Observed", "Current Kueue quota reservation")
		condition(s, run.Generation, "Admitted", truth(admitted), "Observed", "Current Kueue admission")
		condition(s, run.Generation, "Running", truth(running), "Observed", "UID-owned Pod observed in Running phase")
		if run.Spec.Prerequisite != nil && !admitted {
			s.Phase = "WaitingForPrerequisite"
			s.Reason = "PrerequisitePending"
			s.Message = "Waiting for the prerequisite AdmissionCheck"
			if d := s.PrerequisiteDecision; d != nil {
				s.Reason = d.Reason
				s.Message = "Prerequisite check: " + d.State
				ready := d.State == "Ready"
				condition(s, run.Generation, "PrerequisiteReady", truth(ready), d.Reason, s.Message)
				if ready {
					s.Phase = "Queued"
				}
			} else {
				condition(s, run.Generation, "PrerequisiteReady", meta.ConditionUnknown, "AwaitingCheck", "Prerequisite controller has not projected a decision")
			}
		}
		if d := s.PrerequisiteDecision; d != nil && d.State == "Ready" {
			condition(s, run.Generation, "PrerequisiteReady", meta.ConditionTrue, d.Reason, "Durable prerequisite readiness snapshot")
		}
		if admitted {
			s.Phase = "Admitted"
			s.Reason = "Admitted"
			s.Message = "Admitted; waiting for a Running Pod"
			if s.FirstAdmittedAt == nil {
				now := meta.Now()
				s.FirstAdmittedAt = &now
				observability.QueueWait.WithLabelValues(observability.Tenant(run.Spec.QueueName)).Observe(time.Since(run.CreationTimestamp.Time).Seconds())
			}
		}
		if running {
			s.Phase = "Running"
			s.Reason = "Running"
			s.Message = "An owned worker Pod is Running"
			if s.FirstRunningObservedAt == nil {
				now := meta.Now()
				s.FirstRunningObservedAt = &now
			}
		}
	})
}
func bounded(s string) string {
	r := []rune(s)
	if len(r) > 1024 {
		return string(r[:1024])
	}
	return s
}
func truth(v bool) meta.ConditionStatus {
	if v {
		return meta.ConditionTrue
	}
	return meta.ConditionFalse
}
func condition(s *api.BatchRunStatus, generation int64, typ string, status meta.ConditionStatus, reason, message string) {
	apiMeta.SetStatusCondition(&s.Conditions, meta.Condition{Type: typ, Status: status, Reason: reason, Message: bounded(message), ObservedGeneration: generation, LastTransitionTime: meta.Now()})
}
func ptrActive(w *kueue.Workload) bool { return w.Spec.Active == nil || *w.Spec.Active }
func isTrue(w *kueue.Workload, typ string) bool {
	return apiMeta.IsStatusConditionTrue(w.Status.Conditions, typ)
}
func (r *BatchRunReconciler) status(ctx context.Context, run *api.BatchRun, mutate func(*api.BatchRunStatus)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh api.BatchRun
		if err := r.Reader.Get(ctx, client.ObjectKeyFromObject(run), &fresh); err != nil {
			return err
		}
		if fresh.UID != run.UID {
			return fmt.Errorf("BatchRun UID changed")
		}
		if fresh.Generation != run.Generation {
			return errors.NewConflict(api.GroupVersion.WithResource("batchruns").GroupResource(), run.Name, fmt.Errorf("generation changed; reconcile fresh intent"))
		}
		// A competing reconciliation may have committed a terminal or stop decision.
		if fresh.Status.Result != nil || fresh.Status.StopReason != "" && run.Status.StopReason == "" {
			return nil
		}
		before := fresh.DeepCopy()
		for _, typ := range []string{"Valid", "JobCreated", "QuotaReserved", "Admitted", "PrerequisiteReady", "Running", "Stopping", "Complete"} {
			if apiMeta.FindStatusCondition(fresh.Status.Conditions, typ) == nil {
				condition(&fresh.Status, fresh.Generation, typ, meta.ConditionUnknown, "NotObserved", "No reliable observation yet")
			}
		}
		mutate(&fresh.Status)
		fresh.Status.ObservedGeneration = fresh.Generation
		if equality.Semantic.DeepEqual(before.Status, fresh.Status) {
			return nil
		}
		if err := r.Status().Patch(ctx, &fresh, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		if before.Status.Phase != fresh.Status.Phase || before.Status.Reason != fresh.Status.Reason || before.Status.StopReason != fresh.Status.StopReason {
			eventType := core.EventTypeNormal
			if fresh.Status.Phase == "Failed" {
				eventType = core.EventTypeWarning
			}
			if r.Recorder != nil {
				r.Recorder.Event(&fresh, eventType, fresh.Status.Reason, bounded(fresh.Status.Message))
			}
			ctrl.LoggerFrom(ctx).Info("run transition", "namespace", fresh.Namespace, "name", fresh.Name, "uid", fresh.UID, "phase", fresh.Status.Phase, "reason", fresh.Status.Reason)
		}
		return nil
	})
}
func (r *BatchRunReconciler) stop(ctx context.Context, run *api.BatchRun, reason, message string) error {
	committed := false
	err := r.status(ctx, run, func(s *api.BatchRunStatus) {
		if s.StopReason != "" {
			return
		}
		now := meta.Now()
		s.StopReason = reason
		committed = true
		s.StopRequestedAt = &now
		s.Phase = "Stopping"
		s.Reason = reason
		s.Message = bounded(message)
		condition(s, run.Generation, "Stopping", meta.ConditionTrue, reason, message)
	})
	if err == nil {
		if committed {
			observability.StopRequests.WithLabelValues(observability.Reason(reason)).Inc()
		}
		faults.Hit("after-stop-intent", run.Namespace+"/"+run.Name)
	}
	return err
}
func (r *BatchRunReconciler) workload(ctx context.Context, job *batch.Job) (*kueue.Workload, error) {
	var list kueue.WorkloadList
	if err := r.List(ctx, &list, client.InNamespace(job.Namespace), client.MatchingFields{workloadJobUIDIndex: string(job.UID)}); err != nil {
		return nil, err
	}
	var found *kueue.Workload
	for i := range list.Items {
		w := &list.Items[i]
		owner := meta.GetControllerOf(w)
		if owner == nil || owner.UID != job.UID || owner.Kind != "Job" || owner.APIVersion != "batch/v1" || owner.Name != job.Name {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("multiple Workloads own Job UID %s", job.UID)
		}
		found = w.DeepCopy()
	}
	return found, nil
}
func (r *BatchRunReconciler) running(ctx context.Context, job *batch.Job) (bool, error) {
	var pods core.PodList
	if err := r.List(ctx, &pods, client.InNamespace(job.Namespace), client.MatchingLabels{"batch.kubernetes.io/job-name": job.Name}); err != nil {
		return false, err
	}
	for _, p := range pods.Items {
		owner := meta.GetControllerOf(&p)
		if owner != nil && owner.UID == job.UID && owner.Kind == "Job" && p.DeletionTimestamp.IsZero() && p.Status.Phase == core.PodRunning {
			return true, nil
		}
	}
	return false, nil
}
func (r *BatchRunReconciler) cleanup(ctx context.Context, run *api.BatchRun, job *batch.Job, exists bool, wl *kueue.Workload) (ctrl.Result, error) {
	result := ctrl.Result{RequeueAfter: time.Second}
	if exists {
		if !materialize.Owned(run, job) {
			return result, fmt.Errorf("cleanup ownership mismatch")
		}
		if job.DeletionTimestamp.IsZero() {
			uid := job.UID
			err := r.Delete(ctx, job, client.PropagationPolicy(meta.DeletePropagationForeground), client.Preconditions{UID: &uid})
			return result, client.IgnoreNotFound(err)
		}
		return result, nil
	}
	// A foreground Job deletion normally includes Workload and Pods. Confirm the
	// recorded Workload no longer reserves quota, even when GC is delayed.
	if run.Status.WorkloadRef != nil {
		var w kueue.Workload
		err := r.Reader.Get(ctx, types.NamespacedName{Name: run.Status.WorkloadRef.Name, Namespace: run.Namespace}, &w)
		if err != nil && !errors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		if err == nil && w.UID == run.Status.WorkloadRef.UID && !isTrue(&w, "Finished") && isTrue(&w, "QuotaReserved") {
			return result, nil
		}
	}
	if run.Status.JobRef != nil {
		var pods core.PodList
		if err := r.List(ctx, &pods, client.InNamespace(run.Namespace), client.MatchingLabels{"batch.kubernetes.io/job-name": run.Status.JobRef.Name}); err != nil {
			return ctrl.Result{}, err
		}
		for _, p := range pods.Items {
			o := meta.GetControllerOf(&p)
			if o != nil && o.UID == run.Status.JobRef.UID {
				return result, nil
			}
		}
	}
	outcome := "Failed"
	if run.Status.StopReason == "UserCancelled" {
		outcome = "Cancelled"
	}
	return ctrl.Result{}, r.status(ctx, run, func(s *api.BatchRunStatus) {
		now := meta.Now()
		s.Phase = outcome
		s.FinishedAt = &now
		s.Result = &api.RunResult{Outcome: outcome, Reason: s.StopReason, Message: s.Message}
		condition(s, run.Generation, "Stopping", meta.ConditionFalse, "CleanupComplete", "Owned execution resources stopped")
		condition(s, run.Generation, "Running", meta.ConditionFalse, "CleanupComplete", "Owned Pods are gone")
		condition(s, run.Generation, "QuotaReserved", meta.ConditionFalse, "CleanupComplete", "Owned reservation released")
		condition(s, run.Generation, "Admitted", meta.ConditionFalse, "CleanupComplete", "Owned execution is stopped")
		condition(s, run.Generation, "Complete", meta.ConditionTrue, s.StopReason, s.Message)
	})
}
func (r *BatchRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("batchrun")
	}
	if r.Reader == nil {
		r.Reader = mgr.GetAPIReader()
	}
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}
	if r.Concurrency == 0 {
		r.Concurrency = 4
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &kueue.Workload{}, workloadJobUIDIndex, func(obj client.Object) []string {
		o := meta.GetControllerOf(obj)
		if o != nil && o.Kind == "Job" && o.APIVersion == "batch/v1" {
			return []string{string(o.UID)}
		}
		return nil
	}); err != nil {
		return err
	}
	mapChild := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
		owner := meta.GetControllerOf(obj)
		if owner == nil || owner.Kind != "Job" || owner.APIVersion != "batch/v1" {
			return nil
		}
		var job batch.Job
		if err := r.Reader.Get(ctx, types.NamespacedName{Name: owner.Name, Namespace: obj.GetNamespace()}, &job); err != nil || job.UID != owner.UID {
			return nil
		}
		runOwner := meta.GetControllerOf(&job)
		if runOwner == nil || runOwner.Kind != "BatchRun" || runOwner.APIVersion != api.GroupVersion.String() {
			return nil
		}
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: runOwner.Name, Namespace: job.Namespace}}}
	})
	if err := r.queueIndexes(mgr); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).For(&api.BatchRun{}).Owns(&batch.Job{}).Watches(&kueue.Workload{}, mapChild).Watches(&core.Pod{}, mapChild).Watches(&kueue.LocalQueue{}, handler.EnqueueRequestsFromMapFunc(r.localQueueRuns)).Watches(&kueue.ClusterQueue{}, handler.EnqueueRequestsFromMapFunc(r.clusterQueueRuns)).WithOptions(controller.Options{MaxConcurrentReconciles: r.Concurrency}).Complete(r)
}

// requireGate prevents first materialization of prerequisite work in a queue
// lacking this controller's registered check. Kueue remains the admission owner.
func (r *BatchRunReconciler) requireGate(ctx context.Context, run *api.BatchRun) error {
	var lq kueue.LocalQueue
	if err := r.Reader.Get(ctx, types.NamespacedName{Name: run.Spec.QueueName, Namespace: run.Namespace}, &lq); err != nil {
		return err
	}
	var cq kueue.ClusterQueue
	if err := r.Reader.Get(ctx, types.NamespacedName{Name: string(lq.Spec.ClusterQueue)}, &cq); err != nil {
		return err
	}
	for _, name := range cq.Spec.AdmissionChecks {
		var check kueue.AdmissionCheck
		if err := r.Reader.Get(ctx, types.NamespacedName{Name: string(name)}, &check); err != nil {
			return err
		}
		p := check.Spec.Parameters
		if check.Spec.ControllerName == "batch.kubefleet.dev/prerequisite-gate" && p != nil && p.APIGroup == api.GroupVersion.Group && p.Kind == "PrerequisiteGatePolicy" && p.Name == r.PolicyName && apiMeta.IsStatusConditionTrue(check.Status.Conditions, "Active") {
			return nil
		}
	}
	return fmt.Errorf("queue lacks an Active prerequisite check bound to policy %s", r.PolicyName)
}

func errorClass(err error) string {
	switch {
	case errors.IsAlreadyExists(err):
		return "already_exists"
	case errors.IsForbidden(err):
		return "forbidden"
	case errors.IsTooManyRequests(err):
		return "throttled"
	case errors.IsConflict(err):
		return "conflict"
	case errors.IsTimeout(err) || errors.IsServerTimeout(err):
		return "timeout"
	case errors.IsInvalid(err):
		return "invalid"
	default:
		return "other"
	}
}
