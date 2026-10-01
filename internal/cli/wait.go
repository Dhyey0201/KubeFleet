package cli

import (
	"context"
	"fmt"
	errors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"time"
)

func TerminalCode(r *api.BatchRun) (int, bool) {
	if r.Status.Result == nil {
		return 0, false
	}
	switch r.Status.Result.Outcome {
	case "Succeeded":
		return 0, true
	case "Failed":
		return 2, true
	case "Cancelled":
		return 3, true
	default:
		return 1, true
	}
}

// Wait re-lists on every disconnection, including 410 Gone; no watch closure is
// treated as success. Context bounds both watch setup and event consumption.
func Wait(ctx context.Context, resource dynamic.ResourceInterface, name string) (*api.BatchRun, int, error) {
	var expectedUID types.UID
	for {
		if err := ctx.Err(); err != nil {
			if err == context.DeadlineExceeded {
				return nil, 4, err
			}
			return nil, 1, err
		}
		list, err := resource.List(ctx, meta.ListOptions{FieldSelector: fields.OneTermEqualSelector("metadata.name", name).String()})
		if err != nil {
			if errors.IsForbidden(err) || errors.IsUnauthorized(err) {
				return nil, 1, err
			}
			if err := pause(ctx); err != nil {
				continue
			}
			continue
		}
		if len(list.Items) == 0 {
			return nil, 1, fmt.Errorf("BatchRun %q does not exist", name)
		}
		run := &api.BatchRun{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(list.Items[0].Object, run); err != nil {
			return nil, 1, err
		}
		if expectedUID == "" {
			expectedUID = run.UID
		} else if run.UID != expectedUID {
			return nil, 1, fmt.Errorf("BatchRun UID changed during reconnect")
		}
		if code, ok := TerminalCode(run); ok {
			return run, code, nil
		}
		uid := run.UID
		stream, err := resource.Watch(ctx, meta.ListOptions{FieldSelector: fields.OneTermEqualSelector("metadata.name", name).String(), ResourceVersion: list.GetResourceVersion(), AllowWatchBookmarks: true})
		if err != nil {
			if errors.IsForbidden(err) || errors.IsUnauthorized(err) {
				return nil, 1, err
			}
			_ = pause(ctx)
			continue
		}
		reconnect := false
		for !reconnect {
			select {
			case <-ctx.Done():
				stream.Stop()
				if ctx.Err() == context.DeadlineExceeded {
					return nil, 4, ctx.Err()
				}
				return nil, 1, ctx.Err()
			case event, ok := <-stream.ResultChan():
				if event.Type == watch.Error {
					err := errors.FromObject(event.Object)
					if errors.IsForbidden(err) || errors.IsUnauthorized(err) {
						stream.Stop()
						return nil, 1, err
					}
				}
				if !ok || event.Type == watch.Error {
					reconnect = true
					break
				}
				if event.Type == watch.Bookmark {
					continue
				}
				if event.Type == watch.Deleted {
					stream.Stop()
					return nil, 1, fmt.Errorf("BatchRun deleted during wait")
				}
				object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(event.Object)
				if err != nil {
					stream.Stop()
					return nil, 1, err
				}
				current := &api.BatchRun{}
				if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object, current); err != nil {
					stream.Stop()
					return nil, 1, err
				}
				if current.UID != uid {
					stream.Stop()
					return nil, 1, fmt.Errorf("BatchRun UID changed during wait")
				}
				if code, ok := TerminalCode(current); ok {
					stream.Stop()
					return current, code, nil
				}
			}
		}
		stream.Stop()
		_ = pause(ctx)
	}
}
func pause(ctx context.Context) error {
	timer := time.NewTimer(250 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
