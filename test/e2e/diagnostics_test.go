//go:build integration

package e2e

import (
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/cli"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"testing"
)

func TestM5WebhookOutage(t *testing.T) {
	f := newSuite(t)
	r := f.run("outage-running", "sleep", nil)
	f.runningUID(r)
	key := types.NamespacedName{Namespace: "kubefleet-system", Name: "kubefleet-manager"}
	var d apps.Deployment
	must(t, f.c.Get(f.ctx, key, &d))
	d.Spec.Replicas = ptr.To(int32(0))
	must(t, f.c.Update(f.ctx, &d))
	t.Cleanup(func() { f.configureManager("kubefleet-manager:dev", "", "", 1) })
	f.await("no manager Pods", func() (bool, error) {
		var pods core.PodList
		err := f.c.List(f.ctx, &pods, client.InNamespace(key.Namespace), client.MatchingLabels{"app": "kubefleet-manager"})
		return len(pods.Items) == 0, err
	})
	probe := r.DeepCopy()
	probe.Name = f.id + "-outage-probe"
	probe.ResourceVersion = ""
	probe.UID = ""
	probe.Status = api.BatchRunStatus{}
	err := f.c.Create(f.ctx, probe, client.DryRunAll)
	if !errors.IsInternalError(err) {
		t.Fatalf("webhook did not fail closed: %v", err)
	}
	config := &core.ConfigMap{}
	config.Name = f.id + "-unrelated"
	config.Namespace = "team-a"
	must(t, f.c.Create(f.ctx, config))
	must(t, f.c.Delete(f.ctx, config))
	f.configureManager("kubefleet-manager:dev", "", "", 1)
	f.stopAll(r)
}
func TestM5PodDiagnostics(t *testing.T) {
	for _, code := range []string{"PodUnschedulable", "ImagePullFailure"} {
		t.Run(code, func(t *testing.T) {
			f := newSuite(t)
			f.profile("isolated")
			if code == "PodUnschedulable" {
				var cq kueue.ClusterQueue
				must(t, f.c.Get(f.ctx, types.NamespacedName{Name: "team-a"}, &cq))
				cq.Spec.ResourceGroups[0].Flavors[0].Resources[0].NominalQuota = resource.MustParse("20")
				must(t, f.c.Update(f.ctx, &cq))
			}
			r := f.run("diagnostic", "sleep", func(r *api.BatchRun) {
				if code == "PodUnschedulable" {
					r.Spec.Resources.Requests[core.ResourceCPU] = resource.MustParse("20")
					r.Spec.Resources.Limits[core.ResourceCPU] = resource.MustParse("20")
				} else {
					r.Spec.Image = "registry.example.test/unavailable@sha256:3b37ef8ae1855065fbbe3fffe0b8860434557668cd8cae43c9040f1b561b0c9d"
				}
			})
			f.await(code, func() (bool, error) {
				must(t, f.refresh(r))
				d, err := cli.Diagnose(f.ctx, f.c, r)
				if err != nil {
					return false, err
				}
				for _, finding := range d.Findings {
					if finding.Code == code {
						f.save(code, d)
						return true, nil
					}
				}
				return false, nil
			})
			if r.Status.Phase == "Running" {
				t.Fatal("admitted but blocked Pod falsely projected Running")
			}
			f.stopAll(r)
		})
	}
	f := newSuite(t)
	f.profile("isolated")
}
