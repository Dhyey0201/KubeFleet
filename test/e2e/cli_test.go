//go:build integration

package e2e

import (
	"encoding/json"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	api "kubefleet.local/kubefleet/api/v1alpha1"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func (f *suite) cli(want int, args ...string) []byte {
	f.t.Helper()
	root, err := filepath.Abs("../..")
	must(f.t, err)
	cmd := exec.CommandContext(f.ctx, filepath.Join(root, "bin/kubefleet"), args...)
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			code = exit.ExitCode()
		} else {
			f.t.Fatal(err)
		}
	}
	f.save("cli-"+args[0], map[string]any{"args": args, "exit": code, "output": string(out)})
	if code != want {
		f.t.Fatalf("CLI exit %d expected %d: %s", code, want, out)
	}
	return out
}
func TestM5CLI(t *testing.T) {
	f := newSuite(t)
	f.profile("isolated")
	template := f.run("cli-template", "sleep", nil)
	f.cancel(template)
	f.terminal(template, "Cancelled", "")
	f.clean(template)
	for _, test := range []struct {
		mode string
		code int
	}{{"sleep", 0}, {"fail", 2}} {
		r := template.DeepCopy()
		r.TypeMeta = meta.TypeMeta{APIVersion: api.GroupVersion.String(), Kind: "BatchRun"}
		r.ObjectMeta = meta.ObjectMeta{Name: f.id + "-cli-" + test.mode, Namespace: "team-a"}
		r.Status = api.BatchRunStatus{}
		r.Spec.Cancel = false
		r.Spec.Args = []string{test.mode, "--duration=500ms"}
		path := filepath.Join(t.TempDir(), "run.json")
		b, err := json.Marshal(r)
		must(t, err)
		must(t, os.WriteFile(path, b, 0600))
		f.cli(0, "validate", "-f", path)
		f.cli(0, "validate", "-f", path, "--server")
		f.cli(0, "submit", "-f", path)
		f.cli(test.code, "wait", r.Name, "-n", "team-a", "--timeout", "30s", "-o", "json")
		// AlreadyExists is an error and never overwrites the original UID/spec.
		f.cli(1, "submit", "-f", path)
		f.cli(0, "status", r.Name, "-n", "team-a", "-o", "yaml")
		f.cli(0, "cancel", r.Name, "-n", "team-a")
		must(t, f.c.Delete(f.ctx, r))
	}
	r := f.run("cli-wait", "sleep", nil)
	f.cli(4, "wait", r.Name, "-n", "team-a", "--timeout", "100ms")
	root, err := filepath.Abs("../..")
	must(t, err)
	cmd := exec.Command(filepath.Join(root, "bin/kubefleet"), "wait", r.Name, "-n", "team-a", "--timeout", "120s")
	must(t, cmd.Start())
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	must(t, cmd.Process.Signal(os.Interrupt))
	err = cmd.Wait()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 || time.Since(start) > 2*time.Second {
		t.Fatalf("SIGINT cancellation: %v elapsed %s", err, time.Since(start))
	}
	out := f.cli(0, "diagnose", r.Name, "-n", "team-a", "-o", "json")
	if !strings.Contains(string(out), `"observedAt"`) {
		t.Fatal("no observation time")
	}
	f.cli(0, "list", "-n", "team-a", "-o", "json")
	f.cli(3, "cancel", r.Name, "-n", "team-a", "--wait", "--timeout", "30s")
	f.clean(r)
}
