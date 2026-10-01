package main

import (
	"context"
	"flag"
	"kubefleet.local/kubefleet/internal/worker"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if len(os.Args) < 2 {
		log.Error("mode required")
		os.Exit(1)
	}
	f := flag.NewFlagSet("worker", flag.ExitOnError)
	d := f.Duration("duration", 20*time.Second, "bounded work duration")
	seed := f.Uint64("seed", 42, "deterministic seed")
	b := f.Int("bytes", 16<<20, "memory bytes (max 256 MiB)")
	code := f.Int("exit-code", 7, "fail mode exit code")
	_ = f.Parse(os.Args[2:])
	c := worker.Config{Mode: os.Args[1], Duration: *d, Seed: *seed, Bytes: *b, ExitCode: *code}
	log = log.With("mode", c.Mode, "seed", c.Seed, "run", os.Getenv("RUN_UID"))
	if err := c.Validate(); err != nil {
		log.Error("invalid workload", "error", err)
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	start := time.Now()
	log.Info("start", "durationSeconds", c.Duration.Seconds(), "memoryBytes", c.Bytes)
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				log.Info("heartbeat", "elapsedSeconds", time.Since(start).Seconds())
			}
		}
	}()
	r, err := worker.Run(ctx, c)
	close(done)
	result := "success"
	if err != nil {
		result = "interrupted"
	} else if c.Mode == "fail" {
		result = "failure"
	}
	log.Info("finish", "elapsedSeconds", time.Since(start).Seconds(), "checksum", r.Checksum, "interrupted", r.Interrupted, "result", result, "error", err)
	if err != nil {
		os.Exit(130)
	}
	if c.Mode == "fail" {
		os.Exit(c.ExitCode)
	}
}
