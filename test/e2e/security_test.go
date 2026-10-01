//go:build integration

package e2e

import (
	auth "k8s.io/api/authorization/v1"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/cli"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

func TestM5TenantSecurity(t *testing.T) {
	f := newSuite(t)
	cfg := rest.CopyConfig(f.cfg)
	cfg.Impersonate = rest.ImpersonationConfig{UserName: "system:serviceaccount:team-a:kubefleet-tenant", Groups: []string{"system:serviceaccounts", "system:serviceaccounts:team-a", "system:authenticated"}}
	tenant, err := client.New(cfg, client.Options{Scheme: f.c.Scheme()})
	must(t, err)
	k, err := kubernetes.NewForConfig(cfg)
	must(t, err)
	cases := []struct {
		name, verb, group, resource, sub, namespace string
		allowed                                     bool
	}{
		{"create-own", "create", api.GroupVersion.Group, "batchruns", "", "team-a", true},
		{"cancel-own", "patch", api.GroupVersion.Group, "batchruns", "", "team-a", true},
		{"cross-namespace", "create", api.GroupVersion.Group, "batchruns", "", "team-b", false},
		{"forge-status", "patch", api.GroupVersion.Group, "batchruns", "status", "team-a", false},
		{"create-job", "create", "batch", "jobs", "", "team-a", false},
		{"modify-prerequisite", "patch", "batch", "jobs", "", "team-a", false},
		{"workload-status", "patch", "kueue.x-k8s.io", "workloads", "status", "team-a", false},
		{"read-secrets", "get", "", "secrets", "", "team-a", false},
		{"read-policy", "get", api.GroupVersion.Group, "prerequisitegatepolicies", "", "", false},
		{"write-policy", "patch", api.GroupVersion.Group, "prerequisitegatepolicies", "", "", false},
		{"write-queue", "patch", "kueue.x-k8s.io", "clusterqueues", "", "", false},
		{"pod-exec", "create", "", "pods", "exec", "team-a", false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			review, err := k.AuthorizationV1().SelfSubjectAccessReviews().Create(f.ctx, &auth.SelfSubjectAccessReview{Spec: auth.SelfSubjectAccessReviewSpec{ResourceAttributes: &auth.ResourceAttributes{Verb: test.verb, Group: test.group, Resource: test.resource, Subresource: test.sub, Namespace: test.namespace}}}, meta.CreateOptions{})
			must(t, err)
			f.save(test.name, review.Status)
			if review.Status.Allowed != test.allowed {
				t.Fatalf("RBAC result %+v", review.Status)
			}
		})
	}
	r := f.run("security", "sleep", nil)
	f.child(r)
	must(t, tenant.Get(f.ctx, client.ObjectKeyFromObject(r), r))
	before := r.DeepCopy()
	r.Spec.Image = "unapproved/worker:latest"
	if err := tenant.Patch(f.ctx, r, client.MergeFrom(before)); !errors.IsInvalid(err) {
		t.Fatalf("immutable spec was not rejected: %v", err)
	}
	must(t, f.refresh(r))
	r.Status.Phase = "Succeeded"
	if err := tenant.Status().Update(f.ctx, r); !errors.IsForbidden(err) {
		t.Fatalf("status forgery: %v", err)
	}
	if err := tenant.Create(f.ctx, &batch.Job{ObjectMeta: meta.ObjectMeta{Name: f.id + "-forged", Namespace: "team-a"}}); !errors.IsForbidden(err) {
		t.Fatalf("Job create: %v", err)
	}
	must(t, f.refresh(r))
	d, err := cli.Diagnose(f.ctx, tenant, r)
	must(t, err)
	found := false
	for _, x := range d.Findings {
		if x.Code == "InsufficientReadPermissions" {
			found = true
		}
	}
	if !found {
		t.Fatal("Forbidden global queue reads did not degrade diagnostics")
	}
	f.save("tenant-diagnosis", d)
	// Restricted PodSecurity rejects privileged administrator submissions too.
	privileged := true
	err = f.c.Create(f.ctx, &core.Pod{ObjectMeta: meta.ObjectMeta{Name: f.id + "-privileged", Namespace: "team-a"}, Spec: core.PodSpec{Containers: []core.Container{{Name: "bad", Image: "kubefleet-worker:compat", SecurityContext: &core.SecurityContext{Privileged: &privileged}}}}}, client.DryRunAll)
	if !errors.IsForbidden(err) {
		t.Fatalf("PodSecurity did not reject privileged Pod: %v", err)
	}
	before = r.DeepCopy()
	r.Spec.Cancel = true
	must(t, tenant.Patch(f.ctx, r, client.MergeFrom(before)))
	f.terminal(r, "Cancelled", "")
	f.clean(r)
	var manager core.ServiceAccount
	must(t, f.c.Get(f.ctx, types.NamespacedName{Namespace: "kubefleet-system", Name: "kubefleet-manager"}, &manager))
}
