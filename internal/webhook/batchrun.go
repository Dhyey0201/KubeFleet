package webhook

import (
	"context"
	"fmt"
	errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"time"
)

// BatchRunValidator reads administrator policy directly from the API server.
type BatchRunValidator struct {
	Reader     client.Reader
	PolicyName string
	Options    validation.Options
}

var _ admission.CustomValidator = (*BatchRunValidator)(nil)

func (v *BatchRunValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	run, ok := obj.(*api.BatchRun)
	if !ok {
		return nil, errors.NewBadRequest("expected BatchRun")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var policy api.PrerequisiteGatePolicy
	if v.Reader == nil || v.PolicyName == "" {
		return nil, errors.NewServiceUnavailable("active policy is not configured")
	}
	if err := v.Reader.Get(ctx, types.NamespacedName{Name: v.PolicyName}, &policy); err != nil {
		return nil, errors.NewServiceUnavailable("active policy could not be read")
	}
	if errs := validation.ValidateCreate(run.Spec, &policy, v.Options); len(errs) > 0 {
		return nil, errors.NewInvalid(api.GroupVersion.WithKind("BatchRun").GroupKind(), run.Name, errs)
	}
	return nil, nil
}
func (v *BatchRunValidator) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	old, ok := oldObj.(*api.BatchRun)
	if !ok {
		return nil, errors.NewBadRequest("expected old BatchRun")
	}
	run, ok := newObj.(*api.BatchRun)
	if !ok {
		return nil, errors.NewBadRequest("expected BatchRun")
	}
	// Updates cannot revise execution intent. Cancellation remains possible after
	// administrator policy changes or while the policy is temporarily unreadable.
	if errs := validation.ValidateUpdate(old.Spec, run.Spec); len(errs) > 0 {
		return nil, errors.NewInvalid(api.GroupVersion.WithKind("BatchRun").GroupKind(), run.Name, errs)
	}
	return nil, nil
}
func (v *BatchRunValidator) ValidateDelete(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	if _, ok := obj.(*api.BatchRun); !ok {
		return nil, fmt.Errorf("expected BatchRun")
	}
	return nil, nil
}
