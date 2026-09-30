// Command generate-schemas regenerates the checked-in Trellis job schemas.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/overfold/trellis/internal/specschema"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	check := flag.Bool("check", false, "fail if the checked-in schemas differ from generated output")
	outputDir := flag.String("output-dir", filepath.Join("..", "schemas"), "directory containing generated schema files")
	flag.Parse()

	apiSchema, yamlSchema, err := specschema.Generate()
	if err != nil {
		return err
	}
	files := []struct {
		path string
		data []byte
	}{
		{path: filepath.Join(*outputDir, "trellis-job-api.schema.json"), data: apiSchema},
		{path: filepath.Join(*outputDir, "trellis-job.schema.json"), data: yamlSchema},
	}

	if *check {
		var stale []string
		for _, file := range files {
			existing, err := os.ReadFile(file.path)
			if err != nil || !bytes.Equal(existing, file.data) {
				stale = append(stale, file.path)
			}
		}
		if len(stale) > 0 {
			return fmt.Errorf("generated schemas are stale: %v; run 'go run ./cmd/generate-schemas' from orchestrator/", stale)
		}
		return nil
	}

	for _, file := range files {
		if err := os.MkdirAll(filepath.Dir(file.path), 0o755); err != nil {
			return fmt.Errorf("create schema directory for %s: %w", file.path, err)
		}
		if err := os.WriteFile(file.path, file.data, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", file.path, err)
		}
		fmt.Println(file.path)
	}
	return nil
}
