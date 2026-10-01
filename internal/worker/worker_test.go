package worker

import (
	"context"
	"testing"
	"time"
)

func TestBoundsAndCancellation(t *testing.T) {
	for _, mode := range []string{"cpu", "memory", "sleep", "fail", "prepare"} {
		t.Run(mode, func(t *testing.T) {
			c := Config{Mode: mode, Duration: time.Second, Bytes: 4096, ExitCode: 7}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			r, err := Run(ctx, c)
			if err != context.Canceled || !r.Interrupted {
				t.Fatalf("%+v %v", r, err)
			}
		})
	}
	c := Config{Mode: "memory", Duration: time.Second, Bytes: 257 << 20, ExitCode: 7}
	if c.Validate() == nil {
		t.Fatal("unbounded allocation accepted")
	}
}

func TestExternalDeadlineIsInterruption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	r, err := Run(ctx, Config{Mode: "sleep", Duration: time.Second, ExitCode: 7})
	if err != context.DeadlineExceeded || !r.Interrupted {
		t.Fatalf("external deadline reported as success: %+v %v", r, err)
	}
}
func TestOwnDurationCompletes(t *testing.T) {
	for _, mode := range []string{"cpu", "memory", "sleep", "prepare", "fail"} {
		t.Run(mode, func(t *testing.T) {
			r, err := Run(context.Background(), Config{Mode: mode, Duration: 10 * time.Millisecond, Seed: 42, Bytes: 4096, ExitCode: 7})
			if err != nil || r.Interrupted {
				t.Fatalf("bounded completion: %+v %v", r, err)
			}
		})
	}
}
