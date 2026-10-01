// Package observability exposes operational metrics; counters are process-local
// observations and are never used as an execution or benchmark ledger.
package observability

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimemetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"time"
)

var StopRequests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "kubefleet_stop_requests_total", Help: "Durable stop decisions observed by this process."}, []string{"reason"})
var GateDecisions = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "kubefleet_gate_decisions_total", Help: "Gate status decisions emitted by this process."}, []string{"state", "reason"})
var CreationErrors = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "kubefleet_job_creation_errors_total", Help: "Job creation errors by bounded API class."}, []string{"class"})
var QueueWait = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "kubefleet_queue_wait_observed_seconds", Help: "Approximate observed creation-to-first-admission latency; observations can repeat across conflicts/restarts.", Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600}}, []string{"queue"})
var PrerequisiteWait = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "kubefleet_prerequisite_wait_observed_seconds", Help: "Approximate observed prerequisite readiness delay.", Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600}}, []string{"namespace"})

func init() {
	runtimemetrics.Registry.MustRegister(StopRequests, GateDecisions, CreationErrors, QueueWait, PrerequisiteWait)
}
func Tenant(name string) string {
	switch name {
	case "team-a", "team-b", "team-c":
		return name
	default:
		return "other"
	}
}
func Reason(name string) string {
	switch name {
	case "UserCancelled", "JobLost", "JobNameCollision", "InvalidSpec", "AdmissionRejected", "TotalDeadlineExceeded", "NotRequired", "PrerequisiteRequired", "PrerequisiteComplete", "PrerequisiteMissing", "PrerequisiteIncomplete", "PrerequisiteUIDMismatch", "PrerequisiteNotAllowed", "PrerequisiteFailed", "PrerequisiteDeadlineExceeded", "ReservationHoldExceeded", "UnmanagedWorkload":
		return name
	default:
		return "Other"
	}
}

type Collector struct {
	Client                          client.Client
	runs, pending, cleanup, success *prometheus.Desc
}

func NewCollector(c client.Client) *Collector {
	return &Collector{c, prometheus.NewDesc("kubefleet_batchruns_current", "Current API/cache BatchRuns; use max across replicas.", []string{"namespace", "phase"}, nil), prometheus.NewDesc("kubefleet_gate_pending_current", "Current Pending/Retry prerequisite decisions; use max across replicas.", []string{"namespace"}, nil), prometheus.NewDesc("kubefleet_cleanup_pending_current", "Current durable stops awaiting cleanup; use max across replicas.", []string{"namespace"}, nil), prometheus.NewDesc("kubefleet_metrics_snapshot_success", "Whether the last collection read succeeded.", nil, nil)}
}
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.runs
	ch <- c.pending
	ch <- c.cleanup
	ch <- c.success
}
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var list api.BatchRunList
	if err := c.Client.List(ctx, &list); err != nil {
		ch <- prometheus.MustNewConstMetric(c.success, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.success, prometheus.GaugeValue, 1)
	phases := []string{"Pending", "WaitingForPrerequisite", "Queued", "Admitted", "Running", "Stopping", "Succeeded", "Failed", "Cancelled"}
	counts := map[string]map[string]int{}
	pending, cleanup := map[string]int{}, map[string]int{}
	for _, ns := range []string{"team-a", "team-b", "team-c", "other"} {
		counts[ns] = map[string]int{}
	}
	for _, r := range list.Items {
		ns := Tenant(r.Namespace)
		phase := r.Status.Phase
		if phase == "" {
			phase = "Pending"
		}
		counts[ns][phase]++
		if r.Status.StopReason != "" && r.Status.Result == nil {
			cleanup[ns]++
		}
		if d := r.Status.PrerequisiteDecision; d != nil && (d.State == "Pending" || d.State == "Retry") && r.Status.Result == nil {
			pending[ns]++
		}
	}
	for _, ns := range []string{"team-a", "team-b", "team-c", "other"} {
		for _, phase := range phases {
			ch <- prometheus.MustNewConstMetric(c.runs, prometheus.GaugeValue, float64(counts[ns][phase]), ns, phase)
		}
		ch <- prometheus.MustNewConstMetric(c.pending, prometheus.GaugeValue, float64(pending[ns]), ns)
		ch <- prometheus.MustNewConstMetric(c.cleanup, prometheus.GaugeValue, float64(cleanup[ns]), ns)
	}
}
func Register(c client.Client) error { return runtimemetrics.Registry.Register(NewCollector(c)) }
