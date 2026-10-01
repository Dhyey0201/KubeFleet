package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/experiment"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: loadgen generate|replay [flags]")
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ContinueOnError)
	prerequisiteName := fs.String("prerequisite-name", "", "approved preparation Job name for prerequisite mix")
	prerequisiteUID := fs.String("prerequisite-uid", "", "exact preparation Job UID")
	prerequisiteWait := fs.Int("prerequisite-wait", 120, "absolute prerequisite wait seconds")
	seed := fs.Int64("seed", 42, "deterministic seed")
	count := fs.Int("count", 12, "bounded entries")
	size := fs.String("size", "smoke", "smoke or full")
	mix := fs.String("mix", "balanced", "balanced, asymmetric, arrival, mixed or priority")
	image := fs.String("image", "kubefleet-worker:compat", "explicit worker image")
	out := fs.String("out", "", "exclusive output path")
	cpuPath := fs.String("cpu", "", "optional versioned per-Pod Prometheus CPU export")
	observations := fs.String("observations", "", "independent JSONL observations")
	submissions := fs.String("submissions", "", "submission result JSON")
	trace := fs.String("trace", "", "immutable trace path")
	kube := fs.String("kubeconfig", "", "kubeconfig")
	kcontext := fs.String("context", "", "context override")
	concurrency := fs.Int("concurrency", 4, "bounded create concurrency")
	timeout := fs.Duration("timeout", 20*time.Minute, "replay deadline")
	if err := fs.Parse(os.Args[2:]); err != nil {
		return err
	}
	if *out == "" && os.Args[1] != "cleanup" && os.Args[1] != "wait" {
		return fmt.Errorf("--out is required")
	}
	if os.Args[1] == "controller" {
		c, cfg, err := experiment.Client(*kube, *kcontext)
		if err != nil {
			return err
		}
		outputFile, err := os.OpenFile(*out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer outputFile.Close()
		signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		ctx, cancel := context.WithTimeout(signalCtx, *timeout)
		defer cancel()
		k, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			return err
		}
		result, runErr := experiment.ControllerBurst(ctx, c, *count, *image, experiment.NewSafetyProbe(k))
		if result != nil {
			if err := json.NewEncoder(outputFile).Encode(result); err != nil {
				return err
			}
		}
		if runErr == nil && result != nil && result.SafetyStopped {
			return fmt.Errorf("controller load safety stopped: %s", result.StopReason)
		}
		return runErr
	}
	if os.Args[1] == "generate" {
		t, err := experiment.Generate(*seed, *count, *size, *mix, *image)
		if err != nil {
			return err
		}
		if *mix == "prerequisite" {
			if *prerequisiteName == "" || *prerequisiteUID == "" || *prerequisiteWait < 5 || *prerequisiteWait > 600 {
				return fmt.Errorf("prerequisite mix requires a concrete Job name/UID and bounded wait")
			}
			for i := range t.Entries {
				t.Entries[i].Prerequisite = &api.PrerequisiteReference{JobName: *prerequisiteName, JobUID: types.UID(*prerequisiteUID), WaitTimeoutSeconds: int32(*prerequisiteWait)}
			}
			if err := t.Validate(); err != nil {
				return err
			}
		}
		return experiment.WriteJSONExclusive(*out, t)
	}
	if os.Args[1] == "report" || os.Args[1] == "cleanup" || os.Args[1] == "wait" {
		f, err := os.Open(*submissions)
		if err != nil {
			return err
		}
		defer f.Close()
		submitted, err := experiment.ReadSubmissions(f)
		if err != nil {
			return err
		}
		if os.Args[1] == "report" {
			observationsFile, err := os.Open(*observations)
			if err != nil {
				return err
			}
			defer observationsFile.Close()
			var cpuFile *os.File
			if *cpuPath != "" {
				cpuFile, err = os.Open(*cpuPath)
				if err != nil {
					return err
				}
				defer cpuFile.Close()
			}
			var cpuReader io.Reader
			if cpuFile != nil {
				cpuReader = cpuFile
			}
			report, err := experiment.SummarizeWithCPU(observationsFile, submitted, cpuReader)
			if err != nil {
				return err
			}
			return experiment.WriteJSONExclusive(*out, report)
		}
		c, _, err := experiment.Client(*kube, *kcontext)
		if err != nil {
			return err
		}
		signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		ctx, cancel := context.WithTimeout(signalCtx, *timeout)
		defer cancel()
		if os.Args[1] == "wait" {
			return experiment.WaitAll(ctx, c, submitted)
		}
		return experiment.Cleanup(ctx, c, submitted)
	}
	if os.Args[1] != "replay" {
		return fmt.Errorf("unknown command")
	}
	t, hash, err := experiment.ReadTrace(*trace)
	if err != nil {
		return err
	}
	if t.Size == "full" {
		if err := experiment.FullBudget(); err != nil {
			return err
		}
	}
	c, _, err := experiment.Client(*kube, *kcontext)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	outputFile, err := os.OpenFile(*out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer outputFile.Close()
	result, replayErr := experiment.Replay(ctx, c, t, hash, *concurrency)
	if result != nil {
		if err := json.NewEncoder(outputFile).Encode(result); err != nil {
			return err
		}
		for _, entry := range result.Results {
			if !entry.Created && replayErr == nil {
				replayErr = fmt.Errorf("one or more submissions failed; inspect results")
			}
		}
	}
	return replayErr
}
