package v1alpha1

import (
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type PrerequisiteReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	JobName string `json:"jobName"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Type=string
	JobUID types.UID `json:"jobUID"`
	// +kubebuilder:default=120
	// +kubebuilder:validation:Minimum=5
	// +kubebuilder:validation:Maximum=600
	WaitTimeoutSeconds int32 `json:"waitTimeoutSeconds,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="self.completions >= self.parallelism",message="completions must be at least parallelism"
// +kubebuilder:validation:XValidation:rule="self.executionTimeoutSeconds <= self.totalTimeoutSeconds",message="execution timeout exceeds total timeout"
// +kubebuilder:validation:XValidation:rule="!has(self.prerequisite) || self.prerequisite.waitTimeoutSeconds <= self.totalTimeoutSeconds",message="prerequisite timeout exceeds total timeout"
type BatchRunSpec struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	QueueName string `json:"queueName"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	Image string `json:"image"`
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:items:MaxLength=4096
	// +optional
	Command []string `json:"command,omitempty"`
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=4096
	// +optional
	Args      []string                  `json:"args,omitempty"`
	Resources core.ResourceRequirements `json:"resources"`
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=16
	Parallelism int32 `json:"parallelism,omitempty"`
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=128
	Completions int32 `json:"completions,omitempty"`
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=6
	BackoffLimit int32 `json:"backoffLimit,omitempty"`
	// +kubebuilder:default=3600
	// +kubebuilder:validation:Minimum=30
	// +kubebuilder:validation:Maximum=86400
	TotalTimeoutSeconds int32 `json:"totalTimeoutSeconds,omitempty"`
	// +kubebuilder:default=600
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=86400
	ExecutionTimeoutSeconds int32 `json:"executionTimeoutSeconds,omitempty"`
	// +kubebuilder:default=normal
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	PriorityClassName string `json:"priorityClassName,omitempty"`
	// +optional
	Prerequisite *PrerequisiteReference `json:"prerequisite,omitempty"`
	// +kubebuilder:default=false
	Cancel bool `json:"cancel,omitempty"`
}
type ObjectReference struct {
	Name string `json:"name"`
	// +kubebuilder:validation:Type=string
	UID types.UID `json:"uid"`
}
type RunResult struct {
	// +kubebuilder:validation:Enum=Succeeded;Failed;Cancelled
	Outcome string `json:"outcome"`
	// +kubebuilder:validation:MaxLength=128
	Reason string `json:"reason"`
	// +kubebuilder:validation:MaxLength=1024
	Message string `json:"message,omitempty"`
}
type PrerequisiteDecision struct {
	// +kubebuilder:validation:Type=string
	JobUID types.UID `json:"jobUID"`
	// +kubebuilder:validation:Enum=Pending;Ready;Retry;Rejected
	State            string    `json:"state"`
	LastDecisionTime meta.Time `json:"lastDecisionTime"`
	Reason           string    `json:"reason"`
}
type BatchRunStatus struct {
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +kubebuilder:validation:Enum=Pending;WaitingForPrerequisite;Queued;Admitted;Running;Stopping;Succeeded;Failed;Cancelled
	Phase string `json:"phase,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions             []meta.Condition      `json:"conditions,omitempty"`
	JobRef                 *ObjectReference      `json:"jobRef,omitempty"`
	WorkloadRef            *ObjectReference      `json:"workloadRef,omitempty"`
	StopReason             string                `json:"stopReason,omitempty"`
	StopRequestedAt        *meta.Time            `json:"stopRequestedAt,omitempty"`
	Result                 *RunResult            `json:"result,omitempty"`
	FirstAdmittedAt        *meta.Time            `json:"firstAdmittedAt,omitempty"`
	FirstRunningObservedAt *meta.Time            `json:"firstRunningObservedAt,omitempty"`
	FinishedAt             *meta.Time            `json:"finishedAt,omitempty"`
	PrerequisiteDecision   *PrerequisiteDecision `json:"prerequisiteDecision,omitempty"`
	// +kubebuilder:validation:MaxLength=128
	Reason string `json:"reason,omitempty"`
	// +kubebuilder:validation:MaxLength=1024
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=brun
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Queue",type=string,JSONPath=`.spec.queueName`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.reason`
type BatchRun struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`
	Spec            BatchRunSpec   `json:"spec"`
	Status          BatchRunStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type BatchRunList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	Items         []BatchRun `json:"items"`
}

func init() { SchemeBuilder.Register(&BatchRun{}, &BatchRunList{}) }
