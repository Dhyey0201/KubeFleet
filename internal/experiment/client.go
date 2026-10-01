package experiment

import (
	"context"
	"fmt"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

func Client(kubeconfig, contextName string) (client.Client, *rest.Config, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = kubeconfig
	loading := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: contextName})
	raw, err := loading.RawConfig()
	if err != nil {
		return nil, nil, err
	}
	selected := contextName
	if selected == "" {
		selected = raw.CurrentContext
	}
	if selected != "kind-kubefleet" {
		return nil, nil, fmt.Errorf("experiments require dedicated kind-kubefleet context")
	}
	cfg, err := loading.ClientConfig()
	if err != nil {
		return nil, nil, err
	}
	cfg.Timeout = 15 * time.Second
	scheme := runtime.NewScheme()
	_ = core.AddToScheme(scheme)
	_ = api.AddToScheme(scheme)
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, nil, err
	}
	var ns core.Namespace
	if err := c.Get(context.Background(), types.NamespacedName{Name: "kube-system"}, &ns); err != nil {
		return nil, nil, err
	}
	if ns.Labels["batch.kubefleet.dev/project"] != "kubefleet" {
		return nil, nil, fmt.Errorf("project cluster identity mismatch")
	}
	return c, cfg, nil
}
