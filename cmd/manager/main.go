package main

import (
	"flag"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/controller"
	"kubefleet.local/kubefleet/internal/observability"
	"kubefleet.local/kubefleet/internal/validation"
	validator "kubefleet.local/kubefleet/internal/webhook"
	"os"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metrics "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

func main() {
	var certDir, policy, localImage, metricsAddr, probeAddr string
	var leader bool
	var concurrency int
	flag.StringVar(&certDir, "webhook-cert-dir", "/tmp/k8s-webhook-server/serving-certs", "TLS serving certificate directory")
	flag.StringVar(&policy, "policy", "default", "administrator-selected policy name")
	flag.StringVar(&localImage, "development-local-image", "", "explicit local kind image; empty in release")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics listener")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health listener")
	flag.BoolVar(&leader, "leader-elect", true, "enable Kubernetes leader election")
	flag.IntVar(&concurrency, "concurrency", 4, "maximum concurrent reconciles")
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))
	log := ctrl.Log.WithName("setup")
	fatal := func(err error) {
		if err != nil {
			log.Error(err, "manager startup failed")
			os.Exit(1)
		}
	}
	if concurrency < 1 || concurrency > 64 {
		log.Error(nil, "concurrency must be 1..64")
		os.Exit(1)
	}
	scheme := runtime.NewScheme()
	fatal(clientgoscheme.AddToScheme(scheme))
	fatal(api.AddToScheme(scheme))
	fatal(kueue.AddToScheme(scheme))
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: scheme, LeaderElection: leader, LeaderElectionID: "kubefleet.batch.kubefleet.dev", Metrics: metrics.Options{BindAddress: metricsAddr}, HealthProbeBindAddress: probeAddr, WebhookServer: webhook.NewServer(webhook.Options{Port: 9443, CertDir: certDir})})
	fatal(err)
	fatal(observability.Register(mgr.GetClient()))
	options := validation.Options{LocalImage: localImage}
	reconciler := &controller.BatchRunReconciler{PolicyName: policy, Options: options, Concurrency: concurrency}
	fatal(reconciler.SetupWithManager(mgr))
	fatal((&controller.GateReconciler{PolicyName: policy, Concurrency: concurrency}).SetupWithManager(mgr))
	fatal((&controller.GateRegistrationReconciler{PolicyName: policy}).SetupWithManager(mgr))
	fatal(ctrl.NewWebhookManagedBy(mgr).For(&api.BatchRun{}).WithValidator(&validator.BatchRunValidator{Reader: mgr.GetAPIReader(), PolicyName: policy, Options: options}).Complete())
	fatal(mgr.AddHealthzCheck("healthz", healthz.Ping))
	fatal(mgr.AddReadyzCheck("webhook", mgr.GetWebhookServer().StartedChecker()))
	fatal(mgr.Start(ctrl.SetupSignalHandler()))
}
