package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/cli"
	"kubefleet.local/kubefleet/internal/gate"
	"os"
	"os/signal"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"sigs.k8s.io/yaml"
	"syscall"
	"text/tabwriter"
	"time"
)

var version = "dev"
var commit = "uncommitted"

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	code, err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(code)
}
func run(ctx context.Context, args []string, out, stderr io.Writer) (int, error) {
	if len(args) == 0 {
		return 1, fmt.Errorf("usage: kubefleet version|validate|submit|list|status|wait|cancel|diagnose [options]")
	}
	command := args[0]
	if command == "version" {
		fmt.Fprintf(out, "kubefleet %s commit=%s api=%s Kubernetes=1.33 Kueue=0.13.4\n", version, commit, api.GroupVersion)
		return 0, nil
	}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	var path, kubeconfig, contextName, namespace, output string
	var server, cancelWait bool
	var timeout time.Duration
	fs.StringVar(&path, "f", "", "BatchRun manifest")
	fs.StringVar(&kubeconfig, "kubeconfig", "", "kubeconfig path")
	fs.StringVar(&contextName, "context", "", "context override")
	fs.StringVar(&namespace, "namespace", "", "namespace override")
	fs.StringVar(&namespace, "n", "", "namespace override")
	fs.StringVar(&output, "output", "table", "table, json or yaml")
	fs.StringVar(&output, "o", "table", "table, json or yaml")
	fs.BoolVar(&server, "server", false, "validate by server dry-run")
	fs.BoolVar(&cancelWait, "wait", false, "wait after cancellation")
	fs.DurationVar(&timeout, "timeout", 120*time.Second, "operation or wait timeout")
	// Standard flag stops at positional NAME; allow options on either side of it.
	positional := []string{}
	flags := []string{}
	for i := 1; i < len(args); i++ {
		a := args[i]
		if len(a) > 0 && a[0] == '-' {
			flags = append(flags, a)
			if a == "-f" || a == "-n" || a == "-o" || a == "--kubeconfig" || a == "--context" || a == "--namespace" || a == "--output" || a == "--timeout" {
				i++
				if i >= len(args) {
					return 1, fmt.Errorf("missing value for %s", a)
				}
				flags = append(flags, args[i])
			}
		} else {
			positional = append(positional, a)
		}
	}
	if err := fs.Parse(flags); err != nil {
		return 1, err
	}
	if timeout <= 0 || timeout > 24*time.Hour {
		return 1, fmt.Errorf("timeout must be positive and at most 24h")
	}
	if output != "table" && output != "json" && output != "yaml" {
		return 1, fmt.Errorf("unknown output %s", output)
	}
	var submitted *api.BatchRun
	if command == "validate" || command == "submit" {
		if path == "" {
			return 1, fmt.Errorf("-f FILE is required")
		}
		var err error
		submitted, err = cli.Parse(path)
		if err != nil {
			return 1, err
		}
		if command == "validate" && !server {
			if err := cli.Validate(submitted); err != nil {
				return 1, err
			}
			fmt.Fprintln(out, "valid (local schema; administrator policy is checked by server)")
			return 0, nil
		}
	}
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		loading.ExplicitPath = kubeconfig
	}
	overrides := &clientcmd.ConfigOverrides{CurrentContext: contextName}
	overrides.Context.Namespace = namespace
	config := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, overrides)
	cfg, err := config.ClientConfig()
	if err != nil {
		return 1, err
	}
	cfg.Timeout = 15 * time.Second
	ns, _, err := config.Namespace()
	if err != nil {
		return 1, err
	}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{api.AddToScheme, batch.AddToScheme, core.AddToScheme, kueue.AddToScheme} {
		if err := add(scheme); err != nil {
			return 1, err
		}
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return 1, err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if submitted != nil {
		if namespace != "" && submitted.Namespace != "" && submitted.Namespace != ns {
			return 1, fmt.Errorf("manifest namespace conflicts with override")
		}
		if submitted.Namespace == "" {
			submitted.Namespace = ns
		}
		if ref := submitted.Spec.Prerequisite; ref != nil && ref.JobUID == "" {
			var prep batch.Job
			if err := c.Get(ctx, types.NamespacedName{Namespace: submitted.Namespace, Name: ref.JobName}, &prep); err != nil {
				return 1, err
			}
			ref.JobUID = prep.UID
			policy := &api.PrerequisiteGatePolicy{Spec: api.PrerequisiteGatePolicySpec{AllowUngatedBatchRuns: boolptr(true)}}
			// Evaluate trust without rejecting a still-incomplete approved preparation.
			d := gate.Evaluate(submitted, policy, &prep, time.Time{})
			if d.State == kueue.CheckStateRejected {
				return 1, fmt.Errorf("prerequisite cannot be resolved: %s", d.Reason)
			}
			fmt.Fprintf(stderr, "resolved prerequisite %s/%s UID=%s\n", prep.Namespace, prep.Name, prep.UID)
		}
		if err := cli.Validate(submitted); err != nil {
			return 1, err
		}
		options := []client.CreateOption{}
		if command == "validate" {
			options = append(options, client.DryRunAll)
		}
		if err := c.Create(ctx, submitted, options...); err != nil {
			if errors.IsAlreadyExists(err) {
				var existing api.BatchRun
				if read := c.Get(ctx, client.ObjectKeyFromObject(submitted), &existing); read == nil {
					fmt.Fprintf(stderr, "existing %s/%s UID=%s; choose a new name\n", existing.Namespace, existing.Name, existing.UID)
				}
			}
			return 1, err
		}
		if command == "validate" {
			fmt.Fprintln(out, "valid (server dry-run)")
		} else {
			fmt.Fprintf(out, "%s/%s UID=%s\n", submitted.Namespace, submitted.Name, submitted.UID)
		}
		return 0, nil
	}
	if command == "list" {
		var list api.BatchRunList
		if err := c.List(ctx, &list, client.InNamespace(ns)); err != nil {
			return 1, err
		}
		return 0, printObject(out, output, &list)
	}
	if len(positional) != 1 {
		return 1, fmt.Errorf("%s requires one BatchRun NAME", command)
	}
	key := types.NamespacedName{Namespace: ns, Name: positional[0]}
	var r api.BatchRun
	if err := c.Get(ctx, key, &r); err != nil {
		return 1, err
	}
	switch command {
	case "status":
		return 0, printObject(out, output, &r)
	case "diagnose":
		d, err := cli.Diagnose(ctx, c, &r)
		if err != nil {
			return 1, err
		}
		return 0, printObject(out, output, d)
	case "cancel":
		if r.Status.Result != nil {
			fmt.Fprintf(stderr, "retained terminal outcome %s\n", r.Status.Result.Outcome)
			return 0, printObject(out, output, &r)
		}
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			if err := c.Get(ctx, key, &r); err != nil {
				return err
			}
			if r.Status.Result != nil || r.Spec.Cancel {
				return nil
			}
			before := r.DeepCopy()
			r.Spec.Cancel = true
			return c.Patch(ctx, &r, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
		})
		if err != nil {
			return 1, err
		}
		if !cancelWait {
			return 0, printObject(out, output, &r)
		}
		fallthrough
	case "wait":
		watchCfg := *cfg
		watchCfg.Timeout = 0
		dyn, err := dynamic.NewForConfig(&watchCfg)
		if err != nil {
			return 1, err
		}
		terminal, code, err := cli.Wait(ctx, dyn.Resource(api.GroupVersion.WithResource("batchruns")).Namespace(ns), r.Name)
		if terminal != nil {
			if p := printObject(out, output, terminal); p != nil {
				return 1, p
			}
		}
		return code, err
	default:
		return 1, fmt.Errorf("unknown command %q", command)
	}
}
func boolptr(v bool) *bool { return &v }
func printObject(out io.Writer, format string, obj any) error {
	switch format {
	case "json":
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(obj)
	case "yaml":
		b, err := yaml.Marshal(obj)
		if err != nil {
			return err
		}
		_, err = out.Write(b)
		return err
	}
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	row := func(r *api.BatchRun) {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", r.Name, r.Status.Phase, r.Spec.QueueName, r.Spec.PriorityClassName, time.Since(r.CreationTimestamp.Time).Round(time.Second), r.Status.Reason)
	}
	switch v := obj.(type) {
	case *api.BatchRunList:
		fmt.Fprintln(w, "NAME\tPHASE\tQUEUE\tPRIORITY\tAGE\tREASON")
		for i := range v.Items {
			row(&v.Items[i])
		}
	case *api.BatchRun:
		fmt.Fprintln(w, "NAME\tPHASE\tQUEUE\tPRIORITY\tAGE\tREASON")
		row(v)
		for _, c := range v.Status.Conditions {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", c.Type, c.Status, c.Reason, c.Message)
		}
		fmt.Fprintf(w, "Total deadline\t%s\n", v.CreationTimestamp.Add(time.Duration(v.Spec.TotalTimeoutSeconds)*time.Second).Format(time.RFC3339))
		if v.Status.JobRef != nil {
			fmt.Fprintf(w, "Job\t%s\t%s\n", v.Status.JobRef.Name, v.Status.JobRef.UID)
		}
		if v.Status.WorkloadRef != nil {
			fmt.Fprintf(w, "Workload\t%s\t%s\n", v.Status.WorkloadRef.Name, v.Status.WorkloadRef.UID)
		}
	default:
		b, err := yaml.Marshal(obj)
		if err != nil {
			return err
		}
		_, err = out.Write(b)
		return err
	}
	return w.Flush()
}
