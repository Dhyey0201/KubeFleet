package cli

import (
	"strings"
	"testing"
)

const manifest = `apiVersion: batch.kubefleet.dev/v1alpha1
kind: BatchRun
metadata:
  name: sample
spec:
  queueName: team-a
  image: kubefleet-worker:compat
  resources:
    requests: {cpu: 250m, memory: 64Mi}
    limits: {cpu: 250m, memory: 128Mi}
`

func TestStrictManifestAndDefaults(t *testing.T) {
	run, err := ParseBytes([]byte(manifest))
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(run); err != nil {
		t.Fatal(err)
	}
	if run.Spec.Parallelism != 1 || run.Spec.TotalTimeoutSeconds != 3600 || run.Spec.ExecutionTimeoutSeconds != 600 || run.Spec.PriorityClassName != "normal" {
		t.Fatal("defaults differ from API")
	}
	cases := []struct {
		name, input string
		parseOK     bool
	}{
		{"unknown", manifest + "  alien: true\n", false},
		{"duplicate", manifest + "  queueName: team-b\n", false},
		{"multiple", manifest + "---\n" + manifest, false},
		{"zero", manifest + "  parallelism: 0\n", true},
		{"negative", manifest + "  executionTimeoutSeconds: -1\n", true},
		{"status", manifest + "status:\n  stopReason: forged\n", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, err := ParseBytes([]byte(c.input))
			if c.parseOK {
				if err != nil {
					t.Fatal(err)
				}
				if Validate(r) == nil {
					t.Fatal("invalid explicit value defaulted or accepted")
				}
			} else if err == nil {
				t.Fatal("strict parse accepted invalid manifest")
			}
		})
	}
}
func TestManifestBounds(t *testing.T) {
	if _, err := ParseBytes([]byte(strings.Repeat(" ", 1024*1024+1))); err == nil {
		t.Fatal("unbounded input")
	}
}
