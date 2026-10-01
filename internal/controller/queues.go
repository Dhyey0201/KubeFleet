package controller

import (
	"context"
	errors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"time"
)

const runQueueIndex = "kubefleet.runQueue"
const localClusterQueueIndex = "kubefleet.localClusterQueue"

func (r *BatchRunReconciler) queueIndexes(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &api.BatchRun{}, runQueueIndex, func(o client.Object) []string {
		run := o.(*api.BatchRun)
		return []string{run.Namespace + "/" + run.Spec.QueueName}
	}); err != nil {
		return err
	}
	return mgr.GetFieldIndexer().IndexField(context.Background(), &kueue.LocalQueue{}, localClusterQueueIndex, func(o client.Object) []string { return []string{string(o.(*kueue.LocalQueue).Spec.ClusterQueue)} })
}
func (r *BatchRunReconciler) localQueueRuns(ctx context.Context, o client.Object) []reconcile.Request {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var runs api.BatchRunList
	if err := r.List(ctx, &runs, client.MatchingFields{runQueueIndex: o.GetNamespace() + "/" + o.GetName()}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "queue wakeup failed")
		return nil
	}
	requests := make([]reconcile.Request, 0, len(runs.Items))
	for _, run := range runs.Items {
		if run.Status.Result == nil {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&run)})
		}
	}
	return requests
}
func (r *BatchRunReconciler) clusterQueueRuns(ctx context.Context, o client.Object) []reconcile.Request {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var queues kueue.LocalQueueList
	if err := r.List(ctx, &queues, client.MatchingFields{localClusterQueueIndex: o.GetName()}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "cluster queue wakeup failed")
		return nil
	}
	var requests []reconcile.Request
	for i := range queues.Items {
		requests = append(requests, r.localQueueRuns(ctx, &queues.Items[i])...)
	}
	return requests
}
func (r *BatchRunReconciler) queueDiagnostic(ctx context.Context, run *api.BatchRun) (string, string, error) {
	var lq kueue.LocalQueue
	err := r.Reader.Get(ctx, types.NamespacedName{Name: run.Spec.QueueName, Namespace: run.Namespace}, &lq)
	if errors.IsNotFound(err) {
		return "LocalQueueMissing", "LocalQueue is missing; no quota admission is possible", nil
	}
	if err != nil {
		return "", "", err
	}
	var cq kueue.ClusterQueue
	err = r.Reader.Get(ctx, types.NamespacedName{Name: string(lq.Spec.ClusterQueue)}, &cq)
	if errors.IsNotFound(err) {
		return "ClusterQueueInactive", "Referenced ClusterQueue is missing", nil
	}
	if err != nil {
		return "", "", err
	}
	if !apiMeta.IsStatusConditionTrue(cq.Status.Conditions, "Active") {
		return "ClusterQueueInactive", "ClusterQueue is not Active; inspect its conditions", nil
	}
	for _, name := range cq.Spec.AdmissionChecks {
		var ac kueue.AdmissionCheck
		err = r.Reader.Get(ctx, types.NamespacedName{Name: string(name)}, &ac)
		if errors.IsNotFound(err) {
			return "AdmissionCheckInactive", "Required AdmissionCheck is missing", nil
		}
		if err != nil {
			return "", "", err
		}
		if !apiMeta.IsStatusConditionTrue(ac.Status.Conditions, "Active") {
			return "AdmissionCheckInactive", "Required AdmissionCheck is not Active", nil
		}
	}
	return "", "", nil
}
