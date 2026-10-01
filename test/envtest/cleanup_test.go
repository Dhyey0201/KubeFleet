//go:build envtest

package envtest

import (
	"context"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/experiment"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"testing"
	"time"
)

func TestCleanupUIDPreconditionSurvivesReplacementAfterList(t *testing.T) {
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{api.AddToScheme, core.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	c, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.Create(ctx, &core.Namespace{ObjectMeta: meta.ObjectMeta{Name: "team-a"}}); err != nil {
		t.Fatal(err)
	}
	trace, err := experiment.Generate(42, 1, "smoke", "balanced", "kubefleet-worker:compat")
	if err != nil {
		t.Fatal(err)
	}
	original := trace.Run(trace.Entries[0])
	if err := c.Create(ctx, original); err != nil {
		t.Fatal(err)
	}
	fresh := original.DeepCopy()
	fresh.ObjectMeta = meta.ObjectMeta{Name: original.Name, Namespace: original.Namespace}
	wrapped := interceptor.NewClient(c, interceptor.Funcs{Delete: func(ctx context.Context, base client.WithWatch, obj client.Object, options ...client.DeleteOption) error {
		if err := base.Delete(ctx, original); err != nil {
			return err
		}
		if err := base.Create(ctx, fresh); err != nil {
			return err
		}
		return base.Delete(ctx, obj, options...)
	}})
	err = experiment.Cleanup(ctx, wrapped, &experiment.Submissions{Results: []experiment.Submission{{Created: true, Namespace: original.Namespace, Name: original.Name, UID: original.UID}}})
	if !apiErrors.IsConflict(err) {
		t.Fatalf("expected API UID precondition conflict, got %v", err)
	}
	var current api.BatchRun
	if err := c.Get(ctx, client.ObjectKeyFromObject(fresh), &current); err != nil {
		t.Fatal(err)
	}
	if current.UID == original.UID || current.UID != fresh.UID {
		t.Fatal("replacement was adopted or deleted")
	}
	t.Log("real API rejected old-UID deletion after list; replacement remained intact")
}
