package experiment

import "testing"

func TestSafetyStopsOnMeasuredLimits(t *testing.T) {
	baseline := SafetySnapshot{HostFreeDiskBytes: 16 * 1024 * 1024 * 1024, ManagerMemoryBytes: 100 * 1024 * 1024, ManagerCPU: .1, APIErrorsPerSecond: 0, HostLoad1: 1}
	if err := baseline.Check(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*SafetySnapshot){func(s *SafetySnapshot) { s.HostFreeDiskBytes = 1 }, func(s *SafetySnapshot) { s.ManagerMemoryBytes = 230 * 1024 * 1024 }, func(s *SafetySnapshot) { s.ManagerCPU = 2 }, func(s *SafetySnapshot) { s.APIErrorsPerSecond = 2 }, func(s *SafetySnapshot) { s.HostLoad1 = 20 }} {
		s := baseline
		mutate(&s)
		if s.Check() == nil {
			t.Fatal("safety limit accepted")
		}
	}
}
