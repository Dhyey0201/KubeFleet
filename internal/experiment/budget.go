package experiment

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"
)

func FullBudget() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b, err := exec.CommandContext(ctx, "docker", "info", "--format", "{{json .}}").Output()
	if err != nil {
		return err
	}
	var budget struct {
		NCPU     int
		MemTotal int64
	}
	if err := json.Unmarshal(b, &budget); err != nil {
		return err
	}
	if budget.NCPU < 8 || budget.MemTotal < 16*1024*1024*1024 {
		return fmt.Errorf("full benchmark requires >=8 CPUs and >=16 GiB Docker memory (observed CPUs=%d bytes=%d)", budget.NCPU, budget.MemTotal)
	}
	return nil
}
