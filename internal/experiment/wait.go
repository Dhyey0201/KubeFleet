package experiment

import (
	"context"
	"fmt"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

func WaitAll(ctx context.Context, c client.Client, submissions *Submissions) error {
	timer := time.NewTicker(250 * time.Millisecond)
	defer timer.Stop()
	for {
		pending := false
		for _, s := range submissions.Results {
			if !s.Created {
				continue
			}
			var run api.BatchRun
			if err := c.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.Name}, &run); err != nil {
				return err
			}
			if run.UID != s.UID {
				return fmt.Errorf("run UID changed during experiment")
			}
			if run.Status.Result == nil {
				pending = true
			}
		}
		if !pending {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}
