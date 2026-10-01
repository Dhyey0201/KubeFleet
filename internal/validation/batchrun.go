// Package validation shares fail-closed API rules between admission, CLI, and controllers.
package validation

import (
	_ "crypto/sha256"
	"fmt"
	"slices"
	"strings"

	"github.com/distribution/reference"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	api "kubefleet.local/kubefleet/api/v1alpha1"
)

// Options permits one explicit local development image. Empty disables the exception.
type Options struct{ LocalImage string }

func ValidateSpec(s api.BatchRunSpec) field.ErrorList {
	var errors field.ErrorList
	root := field.NewPath("spec")
	invalid := func(name string, value any, msg string) {
		errors = append(errors, field.Invalid(root.Child(name), value, msg))
	}
	if msgs := validation.IsDNS1123Label(s.QueueName); len(msgs) > 0 || len(s.QueueName) > 63 {
		invalid("queueName", s.QueueName, "must be a DNS name of at most 63 characters")
	}
	if s.Image == "" || len(s.Image) > 512 {
		invalid("image", s.Image, "image must contain 1..512 characters")
	}
	for name, list := range map[string][]string{"command": s.Command, "args": s.Args} {
		bound := 64
		if name == "command" {
			bound = 32
		}
		if len(list) > bound {
			invalid(name, len(list), "too many entries")
		}
		for _, arg := range list {
			if len(arg) > 4096 || strings.ContainsRune(arg, 0) {
				invalid(name, "redacted", "entry is too long or contains NUL")
			}
		}
	}
	if len(s.Command) > 0 && s.Command[0] == "" {
		invalid("command", "", "executable must not be empty")
	}
	for name, list := range map[string]core.ResourceList{"requests": s.Resources.Requests, "limits": s.Resources.Limits} {
		path := root.Child("resources", name)
		for _, r := range []core.ResourceName{core.ResourceCPU, core.ResourceMemory} {
			q, ok := list[r]
			if !ok || q.Sign() <= 0 {
				errors = append(errors, field.Invalid(path.Key(string(r)), q.String(), "positive quantity required"))
			}
		}
		for r := range list {
			if r != core.ResourceCPU && r != core.ResourceMemory {
				errors = append(errors, field.Forbidden(path.Key(string(r)), "only CPU and memory are supported"))
			}
		}
	}
	if len(s.Resources.Claims) > 0 {
		errors = append(errors, field.Forbidden(root.Child("resources", "claims"), "resource claims are not supported"))
	}
	for _, r := range []core.ResourceName{core.ResourceCPU, core.ResourceMemory} {
		request, limit := s.Resources.Requests[r], s.Resources.Limits[r]
		if limit.Cmp(request) < 0 {
			errors = append(errors, field.Invalid(root.Child("resources", "limits").Key(string(r)), limit.String(), "limit must be at least request"))
		}
	}
	if s.Parallelism < 1 || s.Parallelism > 16 {
		invalid("parallelism", s.Parallelism, "must be 1..16")
	}
	if s.Completions < s.Parallelism || s.Completions > 128 {
		invalid("completions", s.Completions, "must be at least parallelism and at most 128")
	}
	if s.BackoffLimit < 0 || s.BackoffLimit > 6 {
		invalid("backoffLimit", s.BackoffLimit, "must be 0..6")
	}
	if s.TotalTimeoutSeconds < 30 || s.TotalTimeoutSeconds > 86400 {
		invalid("totalTimeoutSeconds", s.TotalTimeoutSeconds, "must be 30..86400")
	}
	if s.ExecutionTimeoutSeconds < 1 || s.ExecutionTimeoutSeconds > s.TotalTimeoutSeconds {
		invalid("executionTimeoutSeconds", s.ExecutionTimeoutSeconds, "must be positive and at most total timeout")
	}
	if s.PriorityClassName == "" || len(validation.IsDNS1123Subdomain(s.PriorityClassName)) > 0 {
		invalid("priorityClassName", s.PriorityClassName, "valid priority class name required")
	}
	if p := s.Prerequisite; p != nil {
		if len(validation.IsDNS1123Label(p.JobName)) > 0 || len(p.JobName) > 63 {
			invalid("prerequisite.jobName", p.JobName, "valid Job name required")
		}
		if p.JobUID == "" || len(p.JobUID) > 128 {
			invalid("prerequisite.jobUID", p.JobUID, "immutable UID required")
		}
		if p.WaitTimeoutSeconds < 5 || p.WaitTimeoutSeconds > 600 || p.WaitTimeoutSeconds > s.TotalTimeoutSeconds {
			invalid("prerequisite.waitTimeoutSeconds", p.WaitTimeoutSeconds, "must be 5..600 and at most total timeout")
		}
	}
	return errors
}
func ValidateUpdate(old, new api.BatchRunSpec) field.ErrorList {
	var errors field.ErrorList
	if old.Cancel && !new.Cancel {
		errors = append(errors, field.Forbidden(field.NewPath("spec", "cancel"), "cancellation is irreversible"))
	}
	old.Cancel = false
	new.Cancel = false
	if !equality.Semantic.DeepEqual(old, new) {
		errors = append(errors, field.Forbidden(field.NewPath("spec"), "all fields except cancel are immutable"))
	}
	return errors
}
func ValidatePolicy(p *api.PrerequisiteGatePolicy) error {
	if p == nil {
		return fmt.Errorf("policy is required")
	}
	s := p.Spec
	if s.MaxReservationHoldSeconds < 1 || s.MaxReservationHoldSeconds > 30 || s.MaxWaitTimeoutSeconds < 5 || s.MaxWaitTimeoutSeconds > 600 {
		return fmt.Errorf("policy timing bounds are invalid")
	}
	if s.RequiredJobLabel != "batch.kubefleet.dev/prerequisite=approved" {
		return fmt.Errorf("invalid prerequisite approval label")
	}
	if s.AllowUngatedBatchRuns == nil || s.RejectUnmanagedWorkloads == nil {
		return fmt.Errorf("policy defaults are missing")
	}
	if len(s.AllowedImageRegistries) == 0 || len(s.AllowedImageRegistries) > 32 || len(s.AllowedPriorityClassNames) == 0 || len(s.AllowedPriorityClassNames) > 32 {
		return fmt.Errorf("nonempty bounded registry and priority allowlists required")
	}
	for _, registry := range s.AllowedImageRegistries {
		if registry == "" || len(registry) > 253 || strings.ContainsAny(registry, "/@ \t\n") {
			return fmt.Errorf("invalid registry entry")
		}
	}
	for _, priority := range s.AllowedPriorityClassNames {
		if len(validation.IsDNS1123Subdomain(priority)) > 0 {
			return fmt.Errorf("invalid priority entry")
		}
	}
	return nil
}
func ValidateCreate(s api.BatchRunSpec, p *api.PrerequisiteGatePolicy, o Options) field.ErrorList {
	errors := ValidateSpec(s)
	path := field.NewPath("spec")
	if err := ValidatePolicy(p); err != nil {
		return append(errors, field.Forbidden(path, "active policy unavailable or invalid: "+err.Error()))
	}
	if !slices.Contains(p.Spec.AllowedPriorityClassNames, s.PriorityClassName) {
		errors = append(errors, field.Forbidden(path.Child("priorityClassName"), "not in policy allowlist"))
	}
	if s.Prerequisite == nil && !*p.Spec.AllowUngatedBatchRuns {
		errors = append(errors, field.Required(path.Child("prerequisite"), "policy requires a prerequisite"))
	}
	if s.Prerequisite != nil && s.Prerequisite.WaitTimeoutSeconds > p.Spec.MaxWaitTimeoutSeconds {
		errors = append(errors, field.Forbidden(path.Child("prerequisite", "waitTimeoutSeconds"), "exceeds policy limit"))
	}
	if o.LocalImage != "" && s.Image == o.LocalImage {
		return errors
	}
	named, err := reference.ParseNormalizedNamed(s.Image)
	if err != nil {
		return append(errors, field.Invalid(path.Child("image"), s.Image, "invalid image reference"))
	}
	if !slices.Contains(p.Spec.AllowedImageRegistries, reference.Domain(named)) {
		errors = append(errors, field.Forbidden(path.Child("image"), "registry not in policy allowlist"))
	}
	if _, ok := named.(reference.Digested); !ok {
		errors = append(errors, field.Invalid(path.Child("image"), s.Image, "immutable image digest required; local image exception is explicit"))
	}
	return errors
}
