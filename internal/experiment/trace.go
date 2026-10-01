package experiment

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kvalidation "k8s.io/apimachinery/pkg/util/validation"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/validation"
	"math/rand"
	"os"
	"time"
)

const Version = "v1"

type Trace struct {
	SchemaVersion string  `json:"schemaVersion"`
	Seed          int64   `json:"seed"`
	Size          string  `json:"size"`
	Mix           string  `json:"mix"`
	Image         string  `json:"image"`
	Entries       []Entry `json:"entries"`
}
type Entry struct {
	AtMilliseconds       int64                      `json:"atMilliseconds"`
	Namespace            string                     `json:"namespace"`
	Name                 string                     `json:"name"`
	CPU                  string                     `json:"cpu"`
	Memory               string                     `json:"memory"`
	DurationMilliseconds int64                      `json:"durationMilliseconds"`
	Mode                 string                     `json:"mode"`
	Priority             string                     `json:"priority"`
	Parallelism          int32                      `json:"parallelism"`
	Prerequisite         *api.PrerequisiteReference `json:"prerequisite,omitempty"`
}

func Generate(seed int64, count int, size, mix, image string) (*Trace, error) {
	if seed < 0 {
		return nil, fmt.Errorf("seed must be nonnegative")
	}
	if count < 1 || count > 120 {
		return nil, fmt.Errorf("count must be 1..120")
	}
	if size != "smoke" && size != "full" {
		return nil, fmt.Errorf("size must be smoke or full")
	}
	if image == "" {
		return nil, fmt.Errorf("image is required")
	}
	switch mix {
	case "balanced", "asymmetric", "arrival", "mixed", "priority", "prerequisite":
	default:
		return nil, fmt.Errorf("unsupported mix")
	}
	rng := rand.New(rand.NewSource(seed))
	t := &Trace{Version, seed, size, mix, image, make([]Entry, count)}
	for i := range t.Entries {
		team := i % 3
		if mix == "prerequisite" {
			team = 0
		}
		at := int64(i * 200)
		duration := int64(1500)
		priority := "normal"
		if mix == "asymmetric" || mix == "arrival" || mix == "priority" {
			team = 0
			at = 0
		}
		if mix == "arrival" && i >= count*2/3 {
			team = 1
			at = 1500
		}
		if mix == "mixed" {
			duration = int64(500 + rng.Intn(6)*500)
		}
		if mix == "priority" {
			priority = "low"
			if i == count-1 {
				team = 1
				at = 2000
				priority = "high"
			}
		}
		cpu, mem := "250m", "64Mi"
		if size == "full" {
			cpu, mem = "2", "512Mi"
			// Keep the duration mix and permit bounded serial drain on the
			// isolated profile, including the maximum 120-entry burst.
			duration *= 10
			at *= 10
		}
		t.Entries[i] = Entry{AtMilliseconds: at, Namespace: fmt.Sprintf("team-%c", 'a'+team), Name: fmt.Sprintf("trace-%d-%03d", seed, i), CPU: cpu, Memory: mem, DurationMilliseconds: duration, Mode: "cpu", Priority: priority, Parallelism: 1}
	}
	return t, t.Validate()
}
func (t *Trace) Validate() error {
	if t.Seed < 0 {
		return fmt.Errorf("seed must be nonnegative")
	}
	if t.SchemaVersion != Version || len(t.Entries) < 1 || len(t.Entries) > 120 {
		return fmt.Errorf("unsupported trace version or count")
	}
	if t.Size != "smoke" && t.Size != "full" {
		return fmt.Errorf("unknown trace size")
	}
	seen := map[string]bool{}
	previousAt := int64(-1)
	for _, e := range t.Entries {
		if e.AtMilliseconds < previousAt {
			return fmt.Errorf("trace entries must be in submission-time order")
		}
		previousAt = e.AtMilliseconds
		if e.Namespace != "team-a" && e.Namespace != "team-b" && e.Namespace != "team-c" {
			return fmt.Errorf("trace namespace is not a configured tenant")
		}
		if e.Name == "" || len(kvalidation.IsDNS1123Subdomain(e.Name)) > 0 {
			return fmt.Errorf("invalid trace name")
		}
		key := e.Namespace + "/" + e.Name
		if seen[key] {
			return fmt.Errorf("duplicate trace name %s", key)
		}
		seen[key] = true
		if e.AtMilliseconds < 0 || e.AtMilliseconds > 600000 || e.DurationMilliseconds < 100 || e.DurationMilliseconds > 600000 || e.Parallelism < 1 || e.Parallelism > 3 {
			return fmt.Errorf("entry timing/parallelism exceeds safety bounds")
		}
		q, err := resource.ParseQuantity(e.Memory)
		if err != nil || q.Sign() <= 0 || q.Cmp(resource.MustParse("512Mi")) > 0 {
			return fmt.Errorf("entry memory must be positive and <=512Mi")
		}
		cpu, err := resource.ParseQuantity(e.CPU)
		if err != nil || cpu.Sign() <= 0 || cpu.Cmp(resource.MustParse("2")) > 0 {
			return fmt.Errorf("entry CPU must be positive and <=2")
		}
		if t.Size == "smoke" && (cpu.Cmp(resource.MustParse("250m")) > 0 || q.Cmp(resource.MustParse("64Mi")) > 0) {
			return fmt.Errorf("smoke resources exceed uniform scale bounds")
		}
		switch e.Mode {
		case "cpu", "memory", "sleep", "fail", "prepare":
		default:
			return fmt.Errorf("unknown worker mode")
		}
		r := t.Run(e)
		if errs := validation.ValidateSpec(r.Spec); len(errs) > 0 {
			return errs.ToAggregate()
		}
	}
	return nil
}
func (t *Trace) Run(e Entry) *api.BatchRun {
	cpu, mem := resource.MustParse(e.CPU), resource.MustParse(e.Memory)
	totalTimeout := int32(1800)
	if t.Size == "full" {
		totalTimeout = 3600
	}
	return &api.BatchRun{TypeMeta: meta.TypeMeta{APIVersion: api.GroupVersion.String(), Kind: "BatchRun"}, ObjectMeta: meta.ObjectMeta{Name: e.Name, Namespace: e.Namespace}, Spec: api.BatchRunSpec{QueueName: e.Namespace, Image: t.Image, Command: []string{"/worker"}, Args: []string{e.Mode, fmt.Sprintf("--duration=%s", time.Duration(e.DurationMilliseconds)*time.Millisecond), fmt.Sprintf("--seed=%d", t.Seed)}, Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: cpu, core.ResourceMemory: mem}, Limits: core.ResourceList{core.ResourceCPU: cpu, core.ResourceMemory: mem}}, Parallelism: e.Parallelism, Completions: e.Parallelism, TotalTimeoutSeconds: totalTimeout, ExecutionTimeoutSeconds: 660, PriorityClassName: e.Priority, Prerequisite: e.Prerequisite}}
}
func ReadTrace(path string) (*Trace, string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	if len(b) > 1024*1024 {
		return nil, "", fmt.Errorf("trace too large")
	}
	t := &Trace{}
	if err := strictJSON(b, t); err != nil {
		return nil, "", err
	}
	if err := t.Validate(); err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(b)
	return t, hex.EncodeToString(sum[:]), nil
}
func WriteJSONExclusive(path string, value any) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}
func strictJSON(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if err := d.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("trailing JSON data")
	}
	return nil
}
