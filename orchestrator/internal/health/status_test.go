package health

import "testing"

func TestTaskHealthUsesConfiguredThreshold(t *testing.T) {
	health := NewTaskHealth(2)
	if changed, status := health.RecordResult(true); changed || status != StatusInitializing {
		t.Fatalf("first pass = (%v, %q), want unchanged initializing", changed, status)
	}
	if changed, status := health.RecordResult(true); !changed || status != StatusHealthy {
		t.Fatalf("second pass = (%v, %q), want changed healthy", changed, status)
	}
	if changed, status := health.RecordResult(false); changed || status != StatusHealthy {
		t.Fatalf("first failure = (%v, %q), want unchanged healthy", changed, status)
	}
	if changed, status := health.RecordResult(false); !changed || status != StatusUnhealthy {
		t.Fatalf("second failure = (%v, %q), want changed unhealthy", changed, status)
	}
}

func TestTaskHealthInitializingTransitionsToUnhealthy(t *testing.T) {
	health := NewTaskHealth(2)
	if changed, status := health.RecordResult(false); changed || status != StatusInitializing {
		t.Fatalf("first failure = (%v, %q), want unchanged initializing", changed, status)
	}
	if changed, status := health.RecordResult(false); !changed || status != StatusUnhealthy {
		t.Fatalf("second failure = (%v, %q), want changed unhealthy", changed, status)
	}
	if changed, status := health.RecordResult(true); changed || status != StatusUnhealthy {
		t.Fatalf("first pass after unhealthy = (%v, %q), want unchanged unhealthy", changed, status)
	}
	if changed, status := health.RecordResult(true); !changed || status != StatusHealthy {
		t.Fatalf("second pass after unhealthy = (%v, %q), want changed healthy", changed, status)
	}
}
