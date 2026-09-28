//go:build integration

package main

import (
	"testing"

	"github.com/spf13/pflag"
)

func TestIntegrationBuildOpensInjectedRuntimeWithoutCapabilities(t *testing.T) {
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("runtime", "containerd", "Workload runtime: containerd")
	buildTestRuntime.addFlags(flags)
	if flags.Lookup("runtime-faults") == nil {
		t.Fatal("runtime-faults flag is not registered in an integration build")
	}
	r, closer, capabilities, err := openRuntime(&config{Runtime: "injected", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closer.Close() }()
	if r == nil {
		t.Fatal("openRuntime returned a nil runtime")
	}
	if len(capabilities) != 0 {
		t.Fatalf("injected runtime advertised capabilities %v; it executes nothing", capabilities)
	}
	if _, _, _, err := openRuntime(&config{Runtime: "unknown", DataDir: t.TempDir()}); err == nil {
		t.Fatal("openRuntime accepted an unknown runtime")
	}
}
