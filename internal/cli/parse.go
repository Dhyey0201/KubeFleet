package cli

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"kubefleet.local/kubefleet/internal/validation"
	"os"
	"sigs.k8s.io/yaml"
)

// Parse accepts one bounded document and distinguishes omitted fields from
// explicitly supplied zero values before applying API defaults.
func Parse(path string) (*api.BatchRun, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseBytes(data)
}
func ParseBytes(data []byte) (*api.BatchRun, error) {
	if len(data) > 1024*1024 {
		return nil, fmt.Errorf("manifest exceeds 1 MiB")
	}
	decoder := yamlutil.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
	raw, err := decoder.Read()
	if err != nil {
		return nil, err
	}
	if more, err := decoder.Read(); err != io.EOF || len(bytes.TrimSpace(more)) != 0 {
		return nil, fmt.Errorf("exactly one YAML document is required")
	}
	j, err := yaml.YAMLToJSONStrict(raw)
	if err != nil {
		return nil, err
	}
	var run api.BatchRun
	d := json.NewDecoder(bytes.NewReader(j))
	d.DisallowUnknownFields()
	if err := d.Decode(&run); err != nil {
		return nil, err
	}
	if run.APIVersion != api.GroupVersion.String() || run.Kind != "BatchRun" {
		return nil, fmt.Errorf("expected %s BatchRun", api.GroupVersion)
	}
	if run.Name == "" || run.GenerateName != "" {
		return nil, fmt.Errorf("explicit metadata.name required")
	}
	if run.UID != "" || run.ResourceVersion != "" || run.Status.Phase != "" || run.Status.Result != nil || len(run.Status.Conditions) > 0 {
		return nil, fmt.Errorf("server metadata and status are not submission input")
	}
	var fields map[string]json.RawMessage
	var root map[string]json.RawMessage
	_ = json.Unmarshal(j, &root)
	if value, ok := root["status"]; ok && string(value) != "{}" {
		return nil, fmt.Errorf("status is server-owned")
	}
	_ = json.Unmarshal(root["spec"], &fields)
	for key, dest := range map[string]*int32{"parallelism": &run.Spec.Parallelism, "completions": &run.Spec.Completions, "totalTimeoutSeconds": &run.Spec.TotalTimeoutSeconds, "executionTimeoutSeconds": &run.Spec.ExecutionTimeoutSeconds} {
		if _, exists := fields[key]; !exists {
			switch key {
			case "parallelism", "completions":
				*dest = 1
			case "totalTimeoutSeconds":
				*dest = 3600
			case "executionTimeoutSeconds":
				*dest = 600
			}
		}
	}
	if _, exists := fields["priorityClassName"]; !exists {
		run.Spec.PriorityClassName = "normal"
	}
	if run.Spec.Prerequisite != nil {
		var p map[string]json.RawMessage
		_ = json.Unmarshal(fields["prerequisite"], &p)
		if _, exists := p["waitTimeoutSeconds"]; !exists {
			run.Spec.Prerequisite.WaitTimeoutSeconds = 120
		}
	}
	return &run, nil
}
func Validate(run *api.BatchRun) error {
	if errs := validation.ValidateSpec(run.Spec); len(errs) > 0 {
		return errs.ToAggregate()
	}
	return nil
}
