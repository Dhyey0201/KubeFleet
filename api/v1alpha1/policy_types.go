package v1alpha1

import meta "k8s.io/apimachinery/pkg/apis/meta/v1"

type PrerequisiteGatePolicySpec struct {
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=30
	MaxReservationHoldSeconds int32 `json:"maxReservationHoldSeconds,omitempty"`
	// +kubebuilder:default=600
	// +kubebuilder:validation:Minimum=5
	// +kubebuilder:validation:Maximum=600
	MaxWaitTimeoutSeconds int32 `json:"maxWaitTimeoutSeconds,omitempty"`
	// +kubebuilder:default="batch.kubefleet.dev/prerequisite=approved"
	// +kubebuilder:validation:Enum="batch.kubefleet.dev/prerequisite=approved"
	RequiredJobLabel string `json:"requiredJobLabel,omitempty"`
	// +kubebuilder:default=true
	AllowUngatedBatchRuns *bool `json:"allowUngatedBatchRuns,omitempty"`
	// +kubebuilder:default=true
	RejectUnmanagedWorkloads *bool `json:"rejectUnmanagedWorkloads,omitempty"`
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=253
	AllowedImageRegistries []string `json:"allowedImageRegistries"`
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=253
	AllowedPriorityClassNames []string `json:"allowedPriorityClassNames"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
type PrerequisiteGatePolicy struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            PrerequisiteGatePolicySpec `json:"spec"`
}

// +kubebuilder:object:root=true
type PrerequisiteGatePolicyList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	Items         []PrerequisiteGatePolicy `json:"items"`
}

func init() { SchemeBuilder.Register(&PrerequisiteGatePolicy{}, &PrerequisiteGatePolicyList{}) }
