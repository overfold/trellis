//go:build integration

package main

import (
	"fmt"
	"io"
	"path/filepath"

	containerruntime "github.com/clofour/trellis/internal/runtime"
	"github.com/spf13/pflag"
)

// The injected runtime reports workloads as running without executing them. It
// is compiled into the node binary only for the multi-process integration suite
// (go build -tags=integration); normal builds reject --runtime injected.
func init() {
	var faults string
	buildTestRuntime = &testRuntime{
		name: "injected",
		addFlags: func(f *pflag.FlagSet) {
			f.StringVar(&faults, "runtime-faults", "", "Injected runtime fault-control file (integration builds only)")
		},
		open: func(dataDir string) (containerruntime.ContainerRuntime, io.Closer, error) {
			r, err := containerruntime.NewInjectedRuntime(filepath.Join(dataDir, "injected-runtime.json"), faults)
			if err != nil {
				return nil, nil, fmt.Errorf("init injected runtime: %w", err)
			}
			return r, r, nil
		},
	}
}
