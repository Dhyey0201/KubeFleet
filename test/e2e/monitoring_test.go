//go:build integration

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func (f *suite) query(query string) (map[string]any, error) {
	b, err := f.k.CoreV1().Services("kubefleet-monitoring").ProxyGet("http", "prometheus", "9090", "api/v1/query", map[string]string{"query": query}).DoRaw(f.ctx)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = json.Unmarshal(b, &result)
	return result, err
}
func TestM5Monitoring(t *testing.T) {
	f := newSuite(t)
	f.profile("isolated")
	r := f.run("metrics-cpu", "cpu", nil)
	f.runningUID(r)
	queued := f.run("metrics-queued", "sleep", nil)
	f.child(queued)
	f.await("actual worker CPU metrics", func() (bool, error) {
		j, err := f.query(`sum(rate(container_cpu_usage_seconds_total{namespace="team-a",container="worker"}[1m]))`)
		if err != nil {
			return false, nil
		}
		data, ok := j["data"].(map[string]any)
		if !ok {
			return false, nil
		}
		values, ok := data["result"].([]any)
		if !ok || len(values) == 0 {
			return false, nil
		}
		value := values[0].(map[string]any)["value"].([]any)[1].(string)
		n, err := strconv.ParseFloat(value, 64)
		if err != nil || n <= 0 {
			return false, nil
		}
		f.save("actual-worker-cpu", j)
		return true, nil
	})
	f.cancel(r)
	f.terminal(r, "Cancelled", "")
	f.clean(r)
	f.runningUID(queued)
	root, err := filepath.Abs("../..")
	must(t, err)
	b, err := os.ReadFile(filepath.Join(root, "config/monitoring/queries.json"))
	must(t, err)
	var queries []struct {
		Title string `json:"title"`
		Query string `json:"query"`
	}
	must(t, json.Unmarshal(b, &queries))
	for _, q := range queries {
		f.await("dashboard: "+q.Title, func() (bool, error) {
			j, err := f.query(q.Query)
			if err != nil {
				return false, nil
			}
			if j["status"] != "success" {
				return false, fmt.Errorf("query failed: %v", j)
			}
			data := j["data"].(map[string]any)
			values := data["result"].([]any)
			if len(values) == 0 {
				return false, nil
			}
			f.save("dashboard-"+fmt.Sprint(len(q.Title))+"-"+q.Title, j)
			return true, nil
		})
	}
	for _, job := range []string{"kubefleet-manager", "kueue", "kube-state-metrics", "kubelet-cadvisor"} {
		f.await("healthy scrape "+job, func() (bool, error) {
			j, err := f.query(fmt.Sprintf(`min(up{job=%q})`, job))
			if err != nil {
				return false, nil
			}
			values := j["data"].(map[string]any)["result"].([]any)
			if len(values) == 0 {
				return false, nil
			}
			return values[0].(map[string]any)["value"].([]any)[1] == "1", nil
		})
	}
	health, err := f.k.CoreV1().Services("kubefleet-monitoring").ProxyGet("http", "grafana", "3000", "api/health", nil).DoRaw(f.ctx)
	must(t, err)
	var grafana map[string]any
	must(t, json.Unmarshal(health, &grafana))
	if grafana["database"] != "ok" {
		t.Fatal("Grafana unhealthy")
	}
	f.save("grafana-health", grafana)
	f.stopAll(queued)
	// API-derived terminal counts remain observable after execution stops.
}
