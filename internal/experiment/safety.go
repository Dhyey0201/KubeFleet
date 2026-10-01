package experiment

import (
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/sys/unix"
	"k8s.io/client-go/kubernetes"
	"math"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type SafetySnapshot struct {
	ObservedAt          time.Time `json:"observedAt"`
	ManagerMemoryBytes  float64   `json:"managerMemoryBytes"`
	ManagerCPU          float64   `json:"managerCPU"`
	APIErrorsPerSecond  float64   `json:"apiErrorsPerSecond"`
	HostLoad1           float64   `json:"hostLoad1"`
	HostFreeDiskBytes   uint64    `json:"hostFreeDiskBytes"`
	ReconcilePerSecond  *float64  `json:"reconcilePerSecond"`
	ReconcileP50Seconds *float64  `json:"reconcileP50Seconds"`
	ReconcileP95Seconds *float64  `json:"reconcileP95Seconds"`
	WorkqueueDepth      *float64  `json:"workqueueDepth"`
	APIRequestsTotal    *float64  `json:"apiRequestsTotal"`
	APIPatchesTotal     *float64  `json:"apiPatchesTotal"`
	MissingMeasurements []string  `json:"missingMeasurements,omitempty"`
}
type SafetyProbe func(context.Context) (SafetySnapshot, error)

func NewSafetyProbe(k *kubernetes.Clientset) SafetyProbe {
	return func(ctx context.Context) (SafetySnapshot, error) {
		snapshot := SafetySnapshot{ObservedAt: time.Now().UTC()}
		query := func(q string) (float64, error) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			b, err := k.CoreV1().Services("kubefleet-monitoring").ProxyGet("http", "prometheus", "9090", "api/v1/query", map[string]string{"query": q}).DoRaw(ctx)
			if err != nil {
				return 0, err
			}
			var result struct {
				Status string
				Data   struct {
					Result []struct{ Value []json.RawMessage }
				}
			}
			if err := json.Unmarshal(b, &result); err != nil {
				return 0, err
			}
			if result.Status != "success" || len(result.Data.Result) != 1 || len(result.Data.Result[0].Value) != 2 {
				return 0, fmt.Errorf("safety metric unavailable")
			}
			var value string
			if err := json.Unmarshal(result.Data.Result[0].Value[1], &value); err != nil {
				return 0, err
			}
			return strconv.ParseFloat(value, 64)
		}
		up, err := query(`min(up{job="kubefleet-manager"})`)
		if err != nil || up != 1 {
			return snapshot, fmt.Errorf("manager scrape unavailable: %v", err)
		}
		snapshot.ManagerMemoryBytes, err = query(`max(process_resident_memory_bytes{job="kubefleet-manager"})`)
		if err != nil {
			return snapshot, err
		}
		snapshot.ManagerCPU, err = query(`sum(rate(process_cpu_seconds_total{job="kubefleet-manager"}[1m]))`)
		if err != nil {
			return snapshot, err
		}
		snapshot.APIErrorsPerSecond, err = query(`sum(rate(rest_client_requests_total{job="kubefleet-manager",code=~"5..|429"}[1m])) or vector(0)`)
		if err != nil {
			return snapshot, err
		}
		// These process/scrape measurements describe controller management work,
		// separately from independent created/queued/completed resource counts.
		for _, metric := range []struct {
			name, expression string
			value            **float64
		}{
			{"reconcilePerSecond", `sum(rate(controller_runtime_reconcile_total{job="kubefleet-manager"}[1m]))`, &snapshot.ReconcilePerSecond},
			{"reconcileP50Seconds", `histogram_quantile(0.5,sum by(le)(rate(controller_runtime_reconcile_time_seconds_bucket{job="kubefleet-manager"}[1m])))`, &snapshot.ReconcileP50Seconds},
			{"reconcileP95Seconds", `histogram_quantile(0.95,sum by(le)(rate(controller_runtime_reconcile_time_seconds_bucket{job="kubefleet-manager"}[1m])))`, &snapshot.ReconcileP95Seconds},
			{"workqueueDepth", `sum(workqueue_depth{job="kubefleet-manager"})`, &snapshot.WorkqueueDepth},
			{"apiRequestsTotal", `sum(rest_client_requests_total{job="kubefleet-manager"})`, &snapshot.APIRequestsTotal},
			{"apiPatchesTotal", `sum(rest_client_requests_total{job="kubefleet-manager",method="PATCH"})`, &snapshot.APIPatchesTotal},
		} {
			value, err := query(metric.expression)
			if err != nil || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
				snapshot.MissingMeasurements = append(snapshot.MissingMeasurements, metric.name)
				continue
			}
			*metric.value = &value
		}
		var stat unix.Statfs_t
		if err := unix.Statfs(".", &stat); err != nil {
			return snapshot, err
		}
		snapshot.HostFreeDiskBytes = uint64(stat.Bavail) * uint64(stat.Bsize)
		switch runtime.GOOS {
		case "linux":
			b, err := os.ReadFile("/proc/loadavg")
			if err != nil {
				return snapshot, err
			}
			fields := strings.Fields(string(b))
			snapshot.HostLoad1, err = strconv.ParseFloat(fields[0], 64)
			if err != nil {
				return snapshot, err
			}
		case "darwin":
			call, cancel := context.WithTimeout(ctx, 2*time.Second)
			b, err := exec.CommandContext(call, "sysctl", "-n", "vm.loadavg").Output()
			cancel()
			if err != nil {
				return snapshot, err
			}
			fields := strings.Fields(strings.Trim(string(b), "{} \n"))
			snapshot.HostLoad1, err = strconv.ParseFloat(fields[0], 64)
			if err != nil {
				return snapshot, err
			}
		default:
			return snapshot, fmt.Errorf("unsupported safety host")
		}
		return snapshot, nil
	}
}
func (s SafetySnapshot) Check() error {
	for _, v := range []float64{s.ManagerMemoryBytes, s.ManagerCPU, s.APIErrorsPerSecond, s.HostLoad1} {
		if v < 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("invalid safety measurement")
		}
	}
	switch {
	case s.HostFreeDiskBytes < 8*1024*1024*1024:
		return fmt.Errorf("free disk below 8 GiB")
	case s.ManagerMemoryBytes > 220*1024*1024:
		return fmt.Errorf("manager memory above 220 MiB")
	case s.ManagerCPU > 1.0:
		return fmt.Errorf("manager CPU above 1 CPU")
	case s.APIErrorsPerSecond > 1:
		return fmt.Errorf("manager API error rate above 1/s")
	case s.HostLoad1 > 16:
		return fmt.Errorf("host load above 16")
	}
	return nil
}
