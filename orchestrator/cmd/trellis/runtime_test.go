package main

import (
	"strings"
	"testing"
)

func TestNormalBuildRejectsInjectedRuntime(t *testing.T) {
	if buildTestRuntime != nil {
		t.Skip("integration build compiles in the injected runtime")
	}
	_, _, _, err := openRuntime(&config{Runtime: "injected", DataDir: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), `unsupported runtime "injected"`) {
		t.Fatalf("openRuntime(injected) error = %v, want unsupported runtime", err)
	}
}
