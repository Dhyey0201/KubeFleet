package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

func TestGaugesRecomputeAndBoundTenantLabels(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = api.AddToScheme(scheme)
	run := &api.BatchRun{ObjectMeta: meta.ObjectMeta{Name: "one", Namespace: "arbitrary-user-string"}, Status: api.BatchRunStatus{Phase: "Running"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).Build()
	registry := prometheus.NewRegistry()
	registry.MustRegister(NewCollector(c))
	for i := 0; i < 2; i++ {
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, family := range families {
			if family.GetName() != "kubefleet_batchruns_current" {
				continue
			}
			for _, metric := range family.Metric {
				ns, phase := "", ""
				for _, label := range metric.Label {
					if label.GetName() == "namespace" {
						ns = label.GetValue()
					}
					if label.GetName() == "phase" {
						phase = label.GetValue()
					}
				}
				if ns == "arbitrary-user-string" {
					t.Fatal("unbounded tenant label")
				}
				if ns == "other" && phase == "Running" {
					found = true
					if metric.GetGauge().GetValue() != 1 {
						t.Fatal("scrape incremented current state")
					}
				}
			}
		}
		if !found {
			t.Fatal("current run missing")
		}
	}
}
