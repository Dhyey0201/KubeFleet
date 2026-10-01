package experiment

import (
	"context"
	"fmt"
	errors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

func Cleanup(ctx context.Context, c client.Client, submissions *Submissions) error {
	expected := map[string]map[string]types.UID{}
	for _, s := range submissions.Results {
		if !s.Created {
			continue
		}
		if s.UID == "" || s.Namespace == "" || s.Name == "" {
			return fmt.Errorf("refusing cleanup without namespace/name/UID")
		}
		if expected[s.Namespace] == nil {
			expected[s.Namespace] = map[string]types.UID{}
		}
		if uid, exists := expected[s.Namespace][s.Name]; exists && uid != s.UID {
			return fmt.Errorf("refusing ambiguous cleanup identity for %s/%s", s.Namespace, s.Name)
		}
		expected[s.Namespace][s.Name] = s.UID
	}
	// One preflight list per namespace replaces O(n) GET requests. Check all
	// identities before deletion, and retain an API UID precondition on each DELETE.
	var deleting []api.BatchRun
	for namespace, names := range expected {
		var runs api.BatchRunList
		if err := c.List(ctx, &runs, client.InNamespace(namespace)); err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return err
		}
		for _, run := range runs.Items {
			uid, exists := names[run.Name]
			if !exists {
				continue
			}
			if run.UID != uid {
				return fmt.Errorf("refusing cleanup of replaced BatchRun %s/%s", namespace, run.Name)
			}
			deleting = append(deleting, run)
		}
	}
	for i := range deleting {
		run := &deleting[i]
		uid := run.UID
		if err := c.Delete(ctx, run, client.PropagationPolicy(meta.DeletePropagationForeground), client.Preconditions{UID: &uid}); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	for {
		remaining := false
		for namespace, names := range expected {
			var runs api.BatchRunList
			if err := c.List(ctx, &runs, client.InNamespace(namespace)); err != nil {
				if errors.IsNotFound(err) {
					continue
				}
				return err
			}
			for _, run := range runs.Items {
				if uid, exists := names[run.Name]; exists && run.UID == uid {
					remaining = true
				}
			}
		}
		if !remaining {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}
