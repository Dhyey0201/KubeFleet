package experiment

import (
	"context"
	stderrors "errors"
	"fmt"
	"k8s.io/apimachinery/pkg/api/equality"
	errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"net"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sync"
	"time"
)

type Submission struct {
	AttemptMilliseconds  []int64   `json:"attemptMilliseconds,omitempty"`
	ExistingUID          types.UID `json:"existingUID,omitempty"`
	AmbiguousCommit      bool      `json:"ambiguousCommit,omitempty"`
	Namespace            string    `json:"namespace"`
	Name                 string    `json:"name"`
	IntendedMilliseconds int64     `json:"intendedMilliseconds"`
	ActualMilliseconds   int64     `json:"actualMilliseconds"`
	Attempts             int       `json:"attempts"`
	Created              bool      `json:"created"`
	UID                  types.UID `json:"uid,omitempty"`
	Error                string    `json:"error,omitempty"`
}
type Submissions struct {
	SchemaVersion string       `json:"schemaVersion"`
	TraceSHA256   string       `json:"traceSHA256"`
	StartedAt     time.Time    `json:"startedAt"`
	Results       []Submission `json:"results"`
}

func Replay(ctx context.Context, c client.Client, t *Trace, hash string, concurrency int) (*Submissions, error) {
	if concurrency < 1 || concurrency > 8 {
		return nil, fmt.Errorf("concurrency must be 1..8")
	}
	if err := t.Validate(); err != nil {
		return nil, err
	}
	if len(hash) != 64 {
		return nil, fmt.Errorf("trace SHA256 required")
	}
	start := time.Now()
	out := &Submissions{Version, hash, start.UTC(), make([]Submission, len(t.Entries))}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i, e := range t.Entries {
		out.Results[i] = Submission{Namespace: e.Namespace, Name: e.Name, IntendedMilliseconds: e.AtMilliseconds, Error: "not attempted"}
	}
	for i, e := range t.Entries {
		wait := time.Until(start.Add(time.Duration(e.AtMilliseconds) * time.Millisecond))
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				wg.Wait()
				return out, ctx.Err()
			case <-timer.C:
			}
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return out, ctx.Err()
		}
		wg.Add(1)
		go func(i int, e Entry) {
			defer wg.Done()
			defer func() { <-sem }()
			result := Submission{Namespace: e.Namespace, Name: e.Name, IntendedMilliseconds: e.AtMilliseconds, ActualMilliseconds: time.Since(start).Milliseconds()}
			run := t.Run(e)
			run.Labels = map[string]string{"batch.kubefleet.dev/trace": hash[:16]}
			for attempt := 1; attempt <= 3; attempt++ {
				result.Attempts = attempt
				result.AttemptMilliseconds = append(result.AttemptMilliseconds, time.Since(start).Milliseconds())
				callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				err := c.Create(callCtx, run)
				cancel()
				if err == nil {
					result.Created = true
					result.UID = run.UID
					break
				}
				// A timed-out Create may have committed. Only the identical trace/run spec
				// is accepted; do not overwrite, change names, or adopt unrelated objects.
				ambiguous := errors.IsTimeout(err) || errors.IsServerTimeout(err) || transportFailure(err)
				result.AmbiguousCommit = result.AmbiguousCommit || ambiguous
				if errors.IsAlreadyExists(err) || ambiguous {
					var existing api.BatchRun
					readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					read := c.Get(readCtx, client.ObjectKeyFromObject(run), &existing)
					cancel()
					if read == nil {
						result.ExistingUID = existing.UID
					}
					if result.AmbiguousCommit && read == nil && existing.Labels["batch.kubefleet.dev/trace"] == hash[:16] && equality.Semantic.DeepEqual(existing.Spec, t.Run(e).Spec) {
						result.Created = true
						result.UID = existing.UID
						break
					}
				}
				result.Error = err.Error()
				if !errors.IsTooManyRequests(err) && !errors.IsTimeout(err) && !errors.IsServerTimeout(err) && !errors.IsServiceUnavailable(err) && !transportFailure(err) {
					break
				}
				timer := time.NewTimer(time.Duration(attempt) * 200 * time.Millisecond)
				select {
				case <-ctx.Done():
					timer.Stop()
					out.Results[i] = result
					return
				case <-timer.C:
				}
			}
			if result.Created {
				result.Error = ""
			}
			out.Results[i] = result
		}(i, e)
	}
	wg.Wait()
	return out, ctx.Err()
}

func transportFailure(err error) bool {
	var network net.Error
	return stderrors.Is(err, context.DeadlineExceeded) || stderrors.As(err, &network)
}
