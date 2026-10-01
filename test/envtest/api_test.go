//go:build envtest

package envtest

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/controller"
	"kubefleet.local/kubefleet/internal/materialize"
	"kubefleet.local/kubefleet/internal/validation"
	validator "kubefleet.local/kubefleet/internal/webhook"
	"net"
	"os"
	"path/filepath"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metrics "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"strings"
	"testing"
	"time"
)

func TestAPIAndTLSAdmission(t *testing.T) {
	root := filepath.Join("..", "..")
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join(root, "config", "crd", "bases"), "."}, ErrorIfCRDPathMissing: true, WebhookInstallOptions: envtest.WebhookInstallOptions{Paths: []string{filepath.Join(root, "config", "webhook")}}}
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
	if err := core.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kueue.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batch.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	opts := env.WebhookInstallOptions
	server := webhook.NewServer(webhook.Options{Host: opts.LocalServingHost, Port: opts.LocalServingPort, CertDir: opts.LocalServingCertDir})
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, WebhookServer: server, Metrics: metrics.Options{BindAddress: "0"}, HealthProbeBindAddress: "0"})
	if err != nil {
		t.Fatal(err)
	}
	v := &validator.BatchRunValidator{Reader: mgr.GetAPIReader(), PolicyName: "default", Options: validation.Options{}}
	if err := ctrl.NewWebhookManagedBy(mgr).For(&api.BatchRun{}).WithValidator(v).Complete(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("manager did not stop")
		}
	})
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(opts.LocalServingCAData) {
		t.Fatal("missing webhook CA")
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", fmt.Sprintf("%s:%d", opts.LocalServingHost, opts.LocalServingPort), &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("TLS webhook did not start:", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	policy := &api.PrerequisiteGatePolicy{ObjectMeta: meta.ObjectMeta{Name: "default"}, Spec: api.PrerequisiteGatePolicySpec{AllowedImageRegistries: []string{"registry.example.test"}, AllowedPriorityClassNames: []string{"normal"}}}
	if err := c.Create(ctx, policy); err != nil {
		t.Fatal(err)
	}
	if policy.Spec.MaxReservationHoldSeconds != 10 || policy.Spec.RequiredJobLabel != "batch.kubefleet.dev/prerequisite=approved" || policy.Spec.AllowUngatedBatchRuns == nil {
		t.Fatal("policy defaults not applied")
	}
	if err := c.Create(ctx, &core.Namespace{ObjectMeta: meta.ObjectMeta{Name: "tenant"}}); err != nil {
		t.Fatal(err)
	}
	makeRun := func(name string) *api.BatchRun {
		return &api.BatchRun{ObjectMeta: meta.ObjectMeta{Name: name, Namespace: "tenant"}, Spec: api.BatchRunSpec{QueueName: "team-a", Image: "registry.example.test/worker@sha256:" + strings.Repeat("a", 64), Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("250m"), core.ResourceMemory: resource.MustParse("64Mi")}, Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("500m"), core.ResourceMemory: resource.MustParse("128Mi")}}}}
	}
	good := makeRun("defaulted")
	if err := c.Create(ctx, good); err != nil {
		t.Fatal(err)
	}
	if good.Spec.Parallelism != 1 || good.Spec.Completions != 1 || good.Spec.TotalTimeoutSeconds != 3600 || good.Spec.ExecutionTimeoutSeconds != 600 || good.Spec.PriorityClassName != "normal" {
		t.Fatalf("defaults: %+v", good.Spec)
	}
	dry := makeRun("dry")
	if err := c.Create(ctx, dry, client.DryRunAll); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(dry), &api.BatchRun{}); !errors.IsNotFound(err) {
		t.Fatalf("dry run persisted: %v", err)
	}
	cases := []struct {
		name   string
		change func(*api.BatchRun)
	}{
		{"zero-cpu", func(r *api.BatchRun) { r.Spec.Resources.Requests[core.ResourceCPU] = resource.MustParse("0") }},
		{"negative-memory", func(r *api.BatchRun) { r.Spec.Resources.Requests[core.ResourceMemory] = resource.MustParse("-1Mi") }},
		{"limit-below-request", func(r *api.BatchRun) { r.Spec.Resources.Limits[core.ResourceCPU] = resource.MustParse("100m") }},
		{"invalid-parallelism", func(r *api.BatchRun) { r.Spec.Parallelism = 17 }},
		{"invalid-completions", func(r *api.BatchRun) { r.Spec.Parallelism = 2; r.Spec.Completions = 1 }},
		{"invalid-timeouts", func(r *api.BatchRun) { r.Spec.TotalTimeoutSeconds = 30; r.Spec.ExecutionTimeoutSeconds = 31 }},
		{"unapproved-registry", func(r *api.BatchRun) { r.Spec.Image = "evil.test/worker@sha256:" + strings.Repeat("a", 64) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := makeRun(tc.name)
			tc.change(run)
			if err := c.Create(ctx, run); err == nil {
				t.Fatal("invalid create accepted")
			}
			if err := c.Create(ctx, run, client.DryRunAll); err == nil {
				t.Fatal("invalid dry run accepted")
			}
		})
	}
	changed := good.DeepCopy()
	changed.Spec.Args = []string{"new-intent"}
	if err := c.Update(ctx, changed); err == nil {
		t.Fatal("immutable spec changed before admission")
	}
	good.Spec.Cancel = true
	if err := c.Update(ctx, good); err != nil {
		t.Fatal(err)
	}
	if err := c.Update(ctx, good); err != nil {
		t.Fatal(err)
	}
	good.Spec.Cancel = false
	if err := c.Update(ctx, good); err == nil {
		t.Fatal("cancellation reversed")
	}

	t.Run("real-name-collision", func(t *testing.T) {
		run := makeRun("collision")
		if err := c.Create(ctx, run); err != nil {
			t.Fatal(err)
		}
		unrelated, err := materialize.Build(run)
		if err != nil {
			t.Fatal(err)
		}
		unrelated.OwnerReferences = nil
		unrelated.Labels = nil
		if err := c.Create(ctx, unrelated); err != nil {
			t.Fatal(err)
		}
		reconciler := &controller.BatchRunReconciler{Client: c, Reader: c, PolicyName: "default"}
		req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}
		for i := 0; i < 2; i++ {
			if _, err := reconciler.Reconcile(ctx, req); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.Get(ctx, client.ObjectKeyFromObject(run), run); err != nil {
			t.Fatal(err)
		}
		if run.Status.Result == nil || run.Status.Result.Reason != "JobNameCollision" {
			t.Fatal("collision failed to converge")
		}
		var retained batch.Job
		if err := c.Get(ctx, client.ObjectKeyFromObject(unrelated), &retained); err != nil || retained.UID != unrelated.UID || len(retained.OwnerReferences) != 0 {
			t.Fatal("unrelated Job adopted or deleted", err)
		}
	})
	t.Run("real-already-exists-recovery", func(t *testing.T) {
		run := makeRun("create-race")
		if err := c.Create(ctx, run); err != nil {
			t.Fatal(err)
		}
		existing, err := materialize.Build(run)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Create(ctx, existing); err != nil {
			t.Fatal(err)
		}
		miss := true
		reader := interceptor.NewClient(c, interceptor.Funcs{Get: func(ctx context.Context, base client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*batch.Job); ok && miss {
				miss = false
				return errors.NewNotFound(batch.Resource("jobs"), key.Name)
			}
			return base.Get(ctx, key, obj, opts...)
		}})
		reconciler := &controller.BatchRunReconciler{Client: c, Reader: reader, PolicyName: "default"}
		if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(ctx, client.ObjectKeyFromObject(run), run); err != nil {
			t.Fatal(err)
		}
		if run.Status.JobRef == nil || run.Status.JobRef.UID != existing.UID {
			t.Fatal("server AlreadyExists recovery did not verify existing UID")
		}
		var jobs batch.JobList
		if err := c.List(ctx, &jobs, client.InNamespace(run.Namespace), client.MatchingLabels{materialize.RunUIDLabel: string(run.UID)}); err != nil || len(jobs.Items) != 1 {
			t.Fatal("duplicate Jobs after create race", err)
		}
	})

	t.Run("real-status-conflict-preserves-stop", func(t *testing.T) {
		run := makeRun("status-conflict")
		if err := c.Create(ctx, run); err != nil {
			t.Fatal(err)
		}
		injected := false
		observedConflict := false
		writing := interceptor.NewClient(c, interceptor.Funcs{SubResourcePatch: func(ctx context.Context, base client.Client, name string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if name == "status" && !injected {
				injected = true
				var competing api.BatchRun
				if err := base.Get(ctx, client.ObjectKeyFromObject(run), &competing); err != nil {
					return err
				}
				now := meta.Now()
				competing.Status.StopReason = "UserCancelled"
				competing.Status.StopRequestedAt = &now
				competing.Status.Phase = "Stopping"
				if err := base.Status().Update(ctx, &competing); err != nil {
					return err
				}
			}
			err := base.SubResource(name).Patch(ctx, obj, patch, opts...)
			observedConflict = observedConflict || errors.IsConflict(err)
			return err
		}})
		reconciler := &controller.BatchRunReconciler{Client: writing, Reader: c, PolicyName: "default"}
		if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(ctx, client.ObjectKeyFromObject(run), run); err != nil {
			t.Fatal(err)
		}
		if !observedConflict || run.Status.StopReason != "UserCancelled" || run.Status.Phase != "Stopping" {
			t.Fatal("real API conflict retry overwrote stop intent")
		}
	})
	// Admission service failure must reject BatchRun writes, while core resources remain usable.
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	done <- nil
	if err := c.Create(context.Background(), makeRun("webhook-down")); err == nil {
		t.Fatal("webhook outage failed open")
	}
	if err := c.Create(context.Background(), &core.ConfigMap{ObjectMeta: meta.ObjectMeta{Name: "unaffected", Namespace: "tenant"}}); err != nil {
		t.Fatal("unrelated resource affected:", err)
	}
	if dir := os.Getenv("ENVTEST_EVIDENCE"); dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "api-admission.txt"), []byte("PASS: real API defaulting, TLS validation, valid/invalid dry runs, immutable pre-admission intent, irreversible cancellation, webhook fail closed, core resource unaffected.\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}
}
