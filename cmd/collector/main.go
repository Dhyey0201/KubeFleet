package main

import (
	"context"
	"flag"
	"fmt"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
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
	kube := flag.String("kubeconfig", "", "kubeconfig path")
	kcontext := flag.String("context", "", "context override")
	out := flag.String("out", "", "exclusive JSONL output path")
	id := flag.String("experiment", "", "experiment ID")
	profile := flag.String("profile", "isolated", "quota profile")
	repetition := flag.Int("repetition", 1, "repetition index")
	duration := flag.Duration("duration", 2*time.Minute, "bounded collection interval")
	flag.Parse()
	if *out == "" || *id == "" || *duration <= 0 || *duration > time.Hour {
		return fmt.Errorf("output, experiment ID and bounded duration (<=1h) required")
	}
	_, cfg, err := experiment.Client(*kube, *kcontext)
	if err != nil {
		return err
	}
	watchCfg := rest.CopyConfig(cfg)
	watchCfg.Timeout = 0
	c, err := dynamic.NewForConfig(watchCfg)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(*out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()
	if err := experiment.Collect(ctx, c, f, *id, *profile, *repetition); err != nil {
		return err
	}
	return f.Sync()
}
