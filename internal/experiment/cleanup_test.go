package experiment

import (
	"context"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

func TestCleanupPreflightsAllIdentitiesBeforeDeleting(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	a := &api.BatchRun{ObjectMeta: meta.ObjectMeta{Name: "a", Namespace: "team-a", UID: "original-a"}}
	b := &api.BatchRun{ObjectMeta: meta.ObjectMeta{Name: "b", Namespace: "team-a", UID: "replacement-b"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(a, b).Build()
	submissions := &Submissions{Results: []Submission{
		{Created: true, Namespace: a.Namespace, Name: a.Name, UID: a.UID},
		{Created: true, Namespace: b.Namespace, Name: b.Name, UID: "original-b"},
	}}
	if err := Cleanup(context.Background(), c, submissions); err == nil {
		t.Fatal("replacement accepted")
	}
	for _, run := range []*api.BatchRun{a, b} {
		var current api.BatchRun
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &current); err != nil {
			t.Fatal("preflight failure deleted a run:", err)
		}
		if current.UID != run.UID {
			t.Fatal("run identity changed")
		}
	}
}

func TestCleanupLeavesUnsubmittedRuns(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	selected := &api.BatchRun{ObjectMeta: meta.ObjectMeta{Name: "selected", Namespace: "team-a", UID: "selected"}}
	unsubmitted := &api.BatchRun{ObjectMeta: meta.ObjectMeta{Name: "unsubmitted", Namespace: "team-a", UID: "unsubmitted"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(selected, unsubmitted).Build()
	submissions := &Submissions{Results: []Submission{{Created: true, Namespace: selected.Namespace, Name: selected.Name, UID: selected.UID}}}
	if err := Cleanup(context.Background(), c, submissions); err != nil {
		t.Fatal(err)
	}
	var remaining api.BatchRun
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(unsubmitted), &remaining); err != nil {
		t.Fatal(err)
	}
	if remaining.UID != unsubmitted.UID {
		t.Fatal("unsubmitted run changed")
	}
}
