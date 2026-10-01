// Package worker provides bounded, interruptible demonstration workloads.
package worker

import (
	"context"
	"fmt"
	"time"
)

type Config struct {
	Mode     string
	Duration time.Duration
	Seed     uint64
	Bytes    int
	ExitCode int
}
type Result struct {
	Checksum    uint64 `json:"checksum"`
	Interrupted bool   `json:"interrupted"`
}

func (c Config) Validate() error {
	if c.Duration <= 0 || c.Duration > 10*time.Minute {
		return fmt.Errorf("duration must be in (0,10m]")
	}
	if c.Bytes < 0 || c.Bytes > 256<<20 {
		return fmt.Errorf("memory must be between 0 and 256 MiB")
	}
	if c.ExitCode < 1 || c.ExitCode > 125 {
		return fmt.Errorf("failure exit code must be 1..125")
	}
	switch c.Mode {
	case "cpu", "memory", "sleep", "fail", "prepare":
	default:
		return fmt.Errorf("unknown mode %q", c.Mode)
	}
	return nil
}
func Run(ctx context.Context, c Config) (Result, error) {
	if err := c.Validate(); err != nil {
		return Result{}, err
	}
	parent := ctx
	if err := parent.Err(); err != nil {
		return Result{Checksum: c.Seed, Interrupted: true}, err
	}
	ctx, cancel := context.WithTimeout(parent, c.Duration)
	defer cancel()
	r := Result{Checksum: c.Seed}
	var memory []byte
	if c.Mode == "memory" {
		memory = make([]byte, c.Bytes)
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.Interrupted = parent.Err() != nil
			if r.Interrupted {
				return r, parent.Err()
			}
			return r, nil
		default:
		}
		switch c.Mode {
		case "cpu":
			for i := 0; i < 10000; i++ {
				r.Checksum = r.Checksum*6364136223846793005 + 1442695040888963407
			}
		case "memory":
			for i := 0; i < len(memory); i += 4096 {
				memory[i]++
				r.Checksum += uint64(memory[i])
			}
			select {
			case <-ctx.Done():
			case <-ticker.C:
			}
		default:
			select {
			case <-ctx.Done():
			case <-ticker.C:
			}
		}
	}
}
