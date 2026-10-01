package experiment

import (
	"context"
	"fmt"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

type ControllerLoad struct {
	Safety               []SafetySnapshot `json:"safety"`
	SchemaVersion        string           `json:"schemaVersion"`
	Requested            int              `json:"requested"`
	Created              int              `json:"created"`
	Projected            int              `json:"projected"`
	Queued               int              `json:"queued"`
	Admitted             int              `json:"admitted"`
	Running              int              `json:"running"`
	Completed            int              `json:"completed"`
	Pods                 int              `json:"pods"`
	CreateSeconds        float64          `json:"createSeconds"`
	ProjectionSeconds    float64          `json:"projectionSeconds"`
	CleanupSeconds       float64          `json:"cleanupSeconds"`
	CleanupBudgetSeconds float64          `json:"cleanupBudgetSeconds"`
	SafetyStopped        bool             `json:"safetyStopped"`
	StopReason           string           `json:"stopReason,omitempty"`
	Quality              []string         `json:"quality"`
	Submissions          *Submissions     `json:"submissions"`
}

// ControllerBurst keeps every generated Job behind a deliberately missing queue,
// bounding active Pods at zero. Its results are management load, not Job throughput.
func ControllerBurst(ctx context.Context, c client.Client, count int, image string, probe SafetyProbe) (*ControllerLoad, error) {
	if count != 100 && count != 500 && count != 1000 {
		return nil, fmt.Errorf("controller load count must be 100, 500 or 1000")
	}
	if count == 1000 {
		if err := FullBudget(); err != nil {
			return nil, err
		}
	}
	if probe == nil {
		return nil, fmt.Errorf("a live safety probe is required")
	}
	started := time.Now()
	id := fmt.Sprintf("controller-%d", started.UnixNano())
	out := &ControllerLoad{SchemaVersion: Version, Requested: count, Quality: []string{"management_load_only", "no_completed_job_throughput_claim"}, Submissions: &Submissions{SchemaVersion: Version, StartedAt: started.UTC(), Results: []Submission{}}}
	defer func() {
		// Bound API pacing plus native foreground GC, which outlives DELETE
		// acknowledgement. The 500-object kind run needs more than 155 seconds.
		cleanupBudget := time.Minute + time.Duration(count)*500*time.Millisecond
		out.CleanupBudgetSeconds = cleanupBudget.Seconds()
		cleanupStarted := time.Now()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupBudget)
		defer cancel()
		err := Cleanup(cleanupCtx, c, out.Submissions)
		out.CleanupSeconds = time.Since(cleanupStarted).Seconds()
		if err != nil {
			out.SafetyStopped = true
			out.StopReason = "cleanup incomplete"
			out.Quality = append(out.Quality, "cleanup_incomplete: "+err.Error())
		}
	}()
	checkSafety := func() error {
		snapshot, err := probe(ctx)
		out.Safety = append(out.Safety, snapshot)
		if err == nil {
			err = snapshot.Check()
		}
		if err != nil {
			out.SafetyStopped = true
			out.StopReason = err.Error()
		}
		return err
	}
	if err := checkSafety(); err != nil {
		return out, err
	}
	for i := 0; i < count; i++ {
		if i > 0 && i%10 == 0 {
			if err := checkSafety(); err != nil {
				return out, err
			}
		}
		namespace := fmt.Sprintf("team-%c", 'a'+i%3)
		r := &api.BatchRun{ObjectMeta: meta.ObjectMeta{Name: fmt.Sprintf("%s-%04d", id, i), Namespace: namespace, Labels: map[string]string{"batch.kubefleet.dev/controller-load": id}}, Spec: api.BatchRunSpec{QueueName: "controller-load-missing", Image: image, Parallelism: 1, Completions: 1, PriorityClassName: "normal", TotalTimeoutSeconds: 600, ExecutionTimeoutSeconds: 60, Args: []string{"sleep", "--duration=1s"}, Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("250m"), core.ResourceMemory: resource.MustParse("64Mi")}, Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("250m"), core.ResourceMemory: resource.MustParse("64Mi")}}}}
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := c.Create(callCtx, r)
		cancel()
		submission := Submission{Namespace: r.Namespace, Name: r.Name, IntendedMilliseconds: int64(i) * 200, ActualMilliseconds: time.Since(started).Milliseconds(), Attempts: 1, Created: err == nil, UID: r.UID}
		if err != nil {
			submission.Error = err.Error()
		}
		out.Submissions.Results = append(out.Submissions.Results, submission)
		if err != nil {
			out.SafetyStopped = true
			out.StopReason = "API create failure"
			return out, err
		}
		out.Created++
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			out.SafetyStopped = true
			out.StopReason = "experiment deadline"
			return out, ctx.Err()
		case <-timer.C:
		}
	}
	out.CreateSeconds = time.Since(started).Seconds()
	if err := checkSafety(); err != nil {
		return out, err
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		var runs api.BatchRunList
		if err := c.List(ctx, &runs, client.MatchingLabels{"batch.kubefleet.dev/controller-load": id}); err != nil {
			out.SafetyStopped = true
			out.StopReason = "API list failure"
			return out, err
		}
		out.Projected, out.Queued, out.Admitted, out.Running, out.Completed = 0, 0, 0, 0, 0
		for _, run := range runs.Items {
			if run.Status.JobRef != nil {
				out.Projected++
			}
			switch run.Status.Phase {
			case "Queued", "WaitingForPrerequisite":
				out.Queued++
			case "Admitted":
				out.Admitted++
			case "Running":
				out.Running++
			case "Succeeded", "Failed", "Cancelled":
				out.Completed++
			}
		}
		var pods core.PodList
		if err := c.List(ctx, &pods); err != nil {
			return out, err
		}
		out.Pods = 0
		runUIDs := map[string]bool{}
		for _, run := range runs.Items {
			runUIDs[string(run.UID)] = true
		}
		for _, pod := range pods.Items {
			if runUIDs[pod.Labels["batch.kubefleet.dev/run-uid"]] {
				out.Pods++
			}
		}
		if out.Pods > 0 {
			out.SafetyStopped = true
			out.StopReason = "unexpected active Pod"
			return out, fmt.Errorf("controller load escaped missing-queue barrier")
		}
		if out.Projected == count {
			out.ProjectionSeconds = time.Since(started).Seconds()
			if err := checkSafety(); err != nil {
				return out, err
			}
			return out, nil
		}
		select {
		case <-ctx.Done():
			out.SafetyStopped = true
			out.StopReason = "experiment deadline"
			return out, ctx.Err()
		case <-ticker.C:
		}
	}
}
