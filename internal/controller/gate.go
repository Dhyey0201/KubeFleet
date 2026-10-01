package controller

import (
	"context"
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
	"kubefleet.local/kubefleet/internal/gate"
	"kubefleet.local/kubefleet/internal/materialize"
	"kubefleet.local/kubefleet/internal/observability"
	"kubefleet.local/kubefleet/internal/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"time"
)

const gateCheckIndex = "kubefleet.gateCheck"
const prerequisiteIndex = "kubefleet.prerequisiteName"
const RetryBackoff = 5 * time.Second

type GateReconciler struct {
	client.Client
	Reader      client.Reader
	PolicyName  string
	Concurrency int
	Recorder    record.EventRecorder
}

func namedCheck(w *kueue.Workload) *kueue.AdmissionCheckState {
	for i := range w.Status.AdmissionChecks {
		if w.Status.AdmissionChecks[i].Name == gate.CheckName {
			return &w.Status.AdmissionChecks[i]
		}
	}
	return nil
}
func validParameters(c *kueue.AdmissionCheck, policy string) bool {
	p := c.Spec.Parameters
	return c.Spec.ControllerName == gate.ControllerName && p != nil && p.APIGroup == api.GroupVersion.Group && p.Kind == "PrerequisiteGatePolicy" && p.Name == policy
}
func (r *GateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var w kueue.Workload
	if err := r.Reader.Get(ctx, req.NamespacedName, &w); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !w.DeletionTimestamp.IsZero() || isTrue(&w, "Finished") || !ptrActive(&w) || namedCheck(&w) == nil {
		return ctrl.Result{}, nil
	}
	var registration kueue.AdmissionCheck
	if err := r.Reader.Get(ctx, types.NamespacedName{Name: gate.CheckName}, &registration); err != nil {
		return ctrl.Result{}, err
	}
	if !validParameters(&registration, r.PolicyName) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	run, err := r.managedRun(ctx, &w)
	if err != nil {
		return ctrl.Result{}, err
	}
	if run != nil && (run.Spec.Cancel || run.Status.StopReason != "" || run.Status.Result != nil || !run.DeletionTimestamp.IsZero()) {
		return ctrl.Result{}, nil
	}
	// An atomically committed snapshot is authoritative for this exact run/reference.
	if run != nil {
		snapshot := apiMeta.FindStatusCondition(w.Status.Conditions, gate.SnapshotCondition)
		latched := snapshot != nil && snapshot.Status == meta.ConditionTrue && gate.Matches(snapshot.Message, run)
		projected := run.Status.PrerequisiteDecision
		if !latched && projected != nil && projected.State == "Ready" && run.Spec.Prerequisite != nil && projected.JobUID == run.Spec.Prerequisite.JobUID {
			latched = true
		}
		if latched {
			if inBackoff(&w, time.Now()) {
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			reason := "PrerequisiteComplete"
			if run.Spec.Prerequisite == nil {
				reason = "NotRequired"
			}
			return r.decide(ctx, &w, run, gate.Decision{State: kueue.CheckStateReady, Reason: reason})
		}
	}
	var policy api.PrerequisiteGatePolicy
	if err := r.Reader.Get(ctx, types.NamespacedName{Name: r.PolicyName}, &policy); err != nil {
		return ctrl.Result{}, err
	}
	if err := validation.ValidatePolicy(&policy); err != nil {
		return ctrl.Result{}, err
	}
	if !apiMeta.IsStatusConditionTrue(registration.Status.Conditions, "Active") {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if run == nil {
		if *policy.Spec.RejectUnmanagedWorkloads {
			return r.decide(ctx, &w, nil, gate.Decision{State: kueue.CheckStateRejected, Reason: "UnmanagedWorkload"})
		}
		return ctrl.Result{}, nil
	}
	now := time.Now()
	var prerequisite *batch.Job
	if ref := run.Spec.Prerequisite; ref != nil {
		var j batch.Job
		err := r.Reader.Get(ctx, types.NamespacedName{Name: ref.JobName, Namespace: run.Namespace}, &j)
		if err != nil && !errors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		if err == nil {
			prerequisite = &j
		}
	}
	decision := gate.Evaluate(run, &policy, prerequisite, now)
	// Rejection of an expired deadline remains valid during backoff. Ready must wait
	// until Kueue's own backoff state machine permits requeue.
	if decision.State != kueue.CheckStateRejected && inBackoff(&w, now) {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if decision.State == kueue.CheckStatePending {
		reserved := apiMeta.FindStatusCondition(w.Status.Conditions, "QuotaReserved")
		if reserved != nil && reserved.Status == meta.ConditionTrue && now.Sub(reserved.LastTransitionTime.Time) >= time.Duration(policy.Spec.MaxReservationHoldSeconds)*time.Second {
			decision = gate.Decision{State: kueue.CheckStateRetry, Reason: "ReservationHoldExceeded"}
		}
	}
	return r.decide(ctx, &w, run, decision)
}
func inBackoff(w *kueue.Workload, now time.Time) bool {
	return namedCheck(w).State == kueue.CheckStateRetry || (w.Status.RequeueState != nil && w.Status.RequeueState.RequeueAt != nil && now.Before(w.Status.RequeueState.RequeueAt.Time))
}
func (r *GateReconciler) managedRun(ctx context.Context, w *kueue.Workload) (*api.BatchRun, error) {
	owner := meta.GetControllerOf(w)
	if owner == nil || owner.Kind != "Job" || owner.APIVersion != "batch/v1" {
		return nil, nil
	}
	var job batch.Job
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: owner.Name}, &job); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if job.UID != owner.UID {
		return nil, nil
	}
	parent := meta.GetControllerOf(&job)
	if parent == nil || parent.Kind != "BatchRun" || parent.APIVersion != api.GroupVersion.String() {
		return nil, nil
	}
	var run api.BatchRun
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: parent.Name}, &run); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if run.UID != parent.UID || !materialize.Owned(&run, &job) {
		return nil, nil
	}
	return &run, nil
}
func (r *GateReconciler) decide(ctx context.Context, w *kueue.Workload, run *api.BatchRun, d gate.Decision) (ctrl.Result, error) {
	changed := false
	persisted := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh kueue.Workload
		if err := r.Reader.Get(ctx, client.ObjectKeyFromObject(w), &fresh); err != nil {
			return client.IgnoreNotFound(err)
		}
		if fresh.UID != w.UID || !fresh.DeletionTimestamp.IsZero() || isTrue(&fresh, "Finished") || !ptrActive(&fresh) {
			return nil
		}
		check := namedCheck(&fresh)
		if check == nil {
			return nil
		}
		// Recheck stop and ownership on each conflict retry. A concurrent cancel cannot
		// be atomically compared across two objects; the stop coordinator deletes the
		// sole Job, so a transient Ready cannot create a replacement execution.
		current, err := r.managedRun(ctx, &fresh)
		if err != nil {
			return err
		}
		if run != nil && (current == nil || current.UID != run.UID || current.Spec.Cancel || current.Status.StopReason != "" || current.Status.Result != nil || !current.DeletionTimestamp.IsZero()) {
			return nil
		}
		if d.State == kueue.CheckStateReady && inBackoff(&fresh, time.Now()) {
			return nil
		}
		if check.State == kueue.CheckStateRejected {
			return nil
		}
		before := fresh.DeepCopy()
		if check.State != d.State {
			check.LastTransitionTime = meta.Now()
		}
		check.State = d.State
		check.Message = d.Reason
		if d.State == kueue.CheckStateReady && run != nil {
			apiMeta.SetStatusCondition(&fresh.Status.Conditions, meta.Condition{Type: gate.SnapshotCondition, Status: meta.ConditionTrue, ObservedGeneration: fresh.Generation, Reason: "ReadySnapshot", Message: gate.Encode(run, d.Reason), LastTransitionTime: meta.Now()})
		}
		if d.State == kueue.CheckStateRetry && namedCheck(before).State != kueue.CheckStateRetry {
			at := meta.NewTime(time.Now().Add(RetryBackoff))
			if fresh.Status.RequeueState == nil {
				fresh.Status.RequeueState = &kueue.RequeueState{}
			}
			if fresh.Status.RequeueState.RequeueAt == nil || fresh.Status.RequeueState.RequeueAt.Before(&at) {
				fresh.Status.RequeueState.RequeueAt = &at
			}
		}
		if equality.Semantic.DeepEqual(before.Status, fresh.Status) {
			persisted = true
			return nil
		}
		if err := r.Status().Patch(ctx, &fresh, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		changed = true
		persisted = true
		return nil
	})
	if err == nil && changed {
		if r.Recorder != nil {
			r.Recorder.Event(w, core.EventTypeNormal, d.Reason, "Prerequisite decision: "+string(d.State))
		}
		observability.GateDecisions.WithLabelValues(string(d.State), observability.Reason(d.Reason)).Inc()
		if d.State == kueue.CheckStateReady && run != nil && run.Spec.Prerequisite != nil {
			observability.PrerequisiteWait.WithLabelValues(observability.Tenant(run.Namespace)).Observe(time.Since(run.CreationTimestamp.Time).Seconds())
		}
		ctrl.LoggerFrom(ctx).Info("prerequisite decision", "namespace", w.Namespace, "name", w.Name, "uid", w.UID, "state", d.State, "reason", d.Reason)
		if d.State == kueue.CheckStateReady && run != nil {
			faults.Hit("after-gate-ready", run.Namespace+"/"+run.Name)
		}
	}
	if err == nil && persisted && run != nil {
		err = r.project(ctx, run, d)
	}
	return ctrl.Result{RequeueAfter: time.Second}, err
}
func (r *GateReconciler) project(ctx context.Context, run *api.BatchRun, d gate.Decision) error {
	br := &BatchRunReconciler{Client: r.Client, Reader: r.Reader, Recorder: r.Recorder}
	return br.status(ctx, run, func(s *api.BatchRunStatus) {
		if s.PrerequisiteDecision != nil && s.PrerequisiteDecision.State == "Ready" {
			return
		}
		uid := types.UID("")
		if run.Spec.Prerequisite != nil {
			uid = run.Spec.Prerequisite.JobUID
		}
		if old := s.PrerequisiteDecision; old != nil && old.State == string(d.State) && old.Reason == d.Reason && old.JobUID == uid {
			return
		}
		s.PrerequisiteDecision = &api.PrerequisiteDecision{JobUID: uid, State: string(d.State), Reason: d.Reason, LastDecisionTime: meta.Now()}
	})
}
func (r *GateReconciler) allChecks(ctx context.Context, _ client.Object) []reconcile.Request {
	var list kueue.WorkloadList
	if err := r.List(ctx, &list, client.MatchingFields{gateCheckIndex: gate.CheckName}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "map gate configuration")
		return nil
	}
	out := make([]reconcile.Request, 0, len(list.Items))
	for _, w := range list.Items {
		out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&w)})
	}
	return out
}
func (r *GateReconciler) jobWorkloads(ctx context.Context, obj client.Object) []reconcile.Request {
	job := obj.(*batch.Job)
	runs := []api.BatchRun{}
	var dependent api.BatchRunList
	if err := r.List(ctx, &dependent, client.InNamespace(job.Namespace), client.MatchingFields{prerequisiteIndex: job.Name}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "map prerequisite")
		return nil
	}
	runs = append(runs, dependent.Items...)
	if parent := meta.GetControllerOf(job); parent != nil && parent.Kind == "BatchRun" {
		var run api.BatchRun
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: parent.Name}, &run); err == nil && run.UID == parent.UID {
			runs = append(runs, run)
		}
	}
	out := []reconcile.Request{}
	for _, run := range runs {
		var jobs batch.JobList
		if err := r.List(ctx, &jobs, client.InNamespace(run.Namespace), client.MatchingLabels{materialize.RunUIDLabel: string(run.UID)}); err != nil {
			continue
		}
		for _, j := range jobs.Items {
			if !materialize.Owned(&run, &j) {
				continue
			}
			var list kueue.WorkloadList
			if err := r.List(ctx, &list, client.InNamespace(j.Namespace), client.MatchingFields{workloadJobUIDIndex: string(j.UID)}); err != nil {
				continue
			}
			for _, w := range list.Items {
				out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&w)})
			}
		}
	}
	return out
}
func (r *GateReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Client = mgr.GetClient()
	r.Reader = mgr.GetAPIReader()
	r.Recorder = mgr.GetEventRecorderFor("prerequisite-gate")
	if r.Concurrency == 0 {
		r.Concurrency = 4
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &kueue.Workload{}, gateCheckIndex, func(obj client.Object) []string {
		w := obj.(*kueue.Workload)
		if namedCheck(w) != nil {
			return []string{gate.CheckName}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &api.BatchRun{}, prerequisiteIndex, func(obj client.Object) []string {
		run := obj.(*api.BatchRun)
		if run.Spec.Prerequisite != nil {
			return []string{run.Spec.Prerequisite.JobName}
		}
		return nil
	}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).Named("prerequisite-gate").For(&kueue.Workload{}).Watches(&batch.Job{}, handler.EnqueueRequestsFromMapFunc(r.jobWorkloads)).Watches(&kueue.AdmissionCheck{}, handler.EnqueueRequestsFromMapFunc(r.allChecks)).Watches(&api.PrerequisiteGatePolicy{}, handler.EnqueueRequestsFromMapFunc(r.allChecks)).WithOptions(controller.Options{MaxConcurrentReconciles: r.Concurrency}).Complete(r)
}

type GateRegistrationReconciler struct {
	client.Client
	Reader     client.Reader
	PolicyName string
	Recorder   record.EventRecorder
}

func (r *GateRegistrationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if req.Name != gate.CheckName {
		return ctrl.Result{}, nil
	}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var check kueue.AdmissionCheck
		if err := r.Reader.Get(ctx, req.NamespacedName, &check); err != nil {
			return client.IgnoreNotFound(err)
		}
		before := check.DeepCopy()
		valid := validParameters(&check, r.PolicyName)
		reason, message := "PolicyReady", "Selected prerequisite policy is valid"
		if !valid {
			reason, message = "InvalidParameters", "AdmissionCheck parameters do not match the selected policy"
		} else {
			var p api.PrerequisiteGatePolicy
			err := r.Reader.Get(ctx, types.NamespacedName{Name: r.PolicyName}, &p)
			if err != nil || validation.ValidatePolicy(&p) != nil {
				valid = false
				reason = "PolicyUnavailable"
				message = "Selected prerequisite policy is missing, unreadable, or invalid"
			}
		}
		apiMeta.SetStatusCondition(&check.Status.Conditions, meta.Condition{Type: "Active", Status: truth(valid), Reason: reason, Message: message, ObservedGeneration: check.Generation, LastTransitionTime: meta.Now()})
		if equality.Semantic.DeepEqual(before.Status, check.Status) {
			return nil
		}
		if err := r.Status().Patch(ctx, &check, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		if r.Recorder != nil {
			r.Recorder.Event(&check, core.EventTypeNormal, reason, message)
		}
		return nil
	})
	return ctrl.Result{RequeueAfter: 30 * time.Second}, err
}
func (r *GateRegistrationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Client = mgr.GetClient()
	r.Reader = mgr.GetAPIReader()
	r.Recorder = mgr.GetEventRecorderFor("gate-registration")
	return ctrl.NewControllerManagedBy(mgr).Named("gate-registration").For(&kueue.AdmissionCheck{}).Watches(&api.PrerequisiteGatePolicy{}, handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: gate.CheckName}}}
	})).Complete(r)
}
