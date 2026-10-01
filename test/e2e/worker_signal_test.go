//go:build integration

package e2e

import (
	"bufio"
	"context"
	core "k8s.io/api/core/v1"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestM2WorkerSIGTERMLogs(t *testing.T) {
	f := newSuite(t)
	f.profile("isolated")
	r := f.run("sigterm", "memory", nil)
	uid := f.runningUID(r)
	var pod core.Pod
	for _, p := range f.pods(r) {
		if p.UID == uid {
			pod = p
		}
	}
	ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
	defer cancel()
	stream, err := f.k.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &core.PodLogOptions{Container: "worker", Follow: true}).Stream(ctx)
	must(t, err)
	defer stream.Close()
	started, done := make(chan struct{}), make(chan struct{})
	lines := []string{}
	go func() {
		defer close(done)
		scan := bufio.NewScanner(stream)
		notified := false
		for scan.Scan() {
			line := scan.Text()
			lines = append(lines, line)
			if !notified && strings.Contains(line, `"msg":"start"`) {
				close(started)
				notified = true
			}
		}
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("worker log stream did not observe startup")
	}
	f.cancel(r)
	f.terminal(r, "Cancelled", "")
	f.clean(r)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("worker log stream did not terminate")
	}
	logs := strings.Join(lines, "\n")
	must(t, os.WriteFile(filepath.Join(f.dir, f.id+"-worker-sigterm.log"), []byte(logs+"\n"), 0644))
	if !strings.Contains(logs, `"result":"interrupted"`) || !strings.Contains(logs, `"interrupted":true`) {
		t.Fatal("SIGTERM did not produce an explicit interrupted result")
	}
}
