package validation

import (
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"strings"
	"testing"
)

func validSpec() api.BatchRunSpec {
	return api.BatchRunSpec{QueueName: "team-a", Image: "registry.example.test/work/worker@sha256:" + strings.Repeat("a", 64), Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("250m"), core.ResourceMemory: resource.MustParse("64Mi")}, Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("500m"), core.ResourceMemory: resource.MustParse("128Mi")}}, Parallelism: 1, Completions: 1, TotalTimeoutSeconds: 600, ExecutionTimeoutSeconds: 60, PriorityClassName: "normal"}
}
func validPolicy() *api.PrerequisiteGatePolicy {
	return &api.PrerequisiteGatePolicy{Spec: api.PrerequisiteGatePolicySpec{MaxReservationHoldSeconds: 10, MaxWaitTimeoutSeconds: 600, RequiredJobLabel: "batch.kubefleet.dev/prerequisite=approved", AllowUngatedBatchRuns: ptr.To(true), RejectUnmanagedWorkloads: ptr.To(true), AllowedImageRegistries: []string{"registry.example.test"}, AllowedPriorityClassNames: []string{"normal"}}}
}
func TestValidationBoundaries(t *testing.T) {
	if e := ValidateCreate(validSpec(), validPolicy(), Options{}); len(e) > 0 {
		t.Fatalf("valid spec: %v", e)
	}
	cases := map[string]func(*api.BatchRunSpec){
		"zero-cpu":            func(s *api.BatchRunSpec) { s.Resources.Requests[core.ResourceCPU] = resource.MustParse("0") },
		"negative-memory":     func(s *api.BatchRunSpec) { s.Resources.Requests[core.ResourceMemory] = resource.MustParse("-1Mi") },
		"limit-below-request": func(s *api.BatchRunSpec) { s.Resources.Limits[core.ResourceCPU] = resource.MustParse("100m") },
		"parallelism-zero":    func(s *api.BatchRunSpec) { s.Parallelism = 0 },
		"completions-too-low": func(s *api.BatchRunSpec) { s.Parallelism = 2 },
		"backoff-too-high":    func(s *api.BatchRunSpec) { s.BackoffLimit = 7 },
		"execution-too-long":  func(s *api.BatchRunSpec) { s.ExecutionTimeoutSeconds = 601 },
		"extra-resource": func(s *api.BatchRunSpec) {
			s.Resources.Requests[core.ResourceName("example.test/gpu")] = resource.MustParse("1")
		},
		"registry-prefix-attack": func(s *api.BatchRunSpec) {
			s.Image = "registry.example.test.evil.test/work@sha256:" + strings.Repeat("a", 64)
		},
		"mutable-tag":      func(s *api.BatchRunSpec) { s.Image = "registry.example.test/work:latest" },
		"unknown-priority": func(s *api.BatchRunSpec) { s.PriorityClassName = "system-node-critical" },
		"missing-prerequisite-uid": func(s *api.BatchRunSpec) {
			s.Prerequisite = &api.PrerequisiteReference{JobName: "prepare", WaitTimeoutSeconds: 120}
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			s := validSpec()
			change(&s)
			if e := ValidateCreate(s, validPolicy(), Options{}); len(e) == 0 {
				t.Fatal("unsafe spec accepted")
			}
		})
	}
	if len(ValidateCreate(validSpec(), nil, Options{})) == 0 {
		t.Fatal("missing policy failed open")
	}
	s := validSpec()
	s.Image = "kubefleet-worker:compat"
	if len(ValidateCreate(s, validPolicy(), Options{LocalImage: s.Image})) != 0 {
		t.Fatal("explicit development image rejected")
	}
	if len(ValidateCreate(s, validPolicy(), Options{})) == 0 {
		t.Fatal("development image allowed in release mode")
	}
}
func TestImmutableAndCancellation(t *testing.T) {
	old := validSpec()
	next := old
	next.Cancel = true
	if len(ValidateUpdate(old, next)) != 0 || len(ValidateUpdate(next, next)) != 0 {
		t.Fatal("cancellation is not idempotent")
	}
	if len(ValidateUpdate(next, old)) == 0 {
		t.Fatal("cancel reversal accepted")
	}
	next = old
	next.Image = "another-image"
	if len(ValidateUpdate(old, next)) == 0 {
		t.Fatal("spec mutation accepted")
	}
	next = *old.DeepCopy()
	next.Resources.Requests[core.ResourceCPU] = resource.MustParse("0.25")
	if len(ValidateUpdate(old, next)) != 0 {
		t.Fatal("equivalent quantities treated as mutation")
	}
}
