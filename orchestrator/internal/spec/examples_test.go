// Package spec defines and validates Trellis job specifications.
package spec

import (
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestExampleManifestsValidate(t *testing.T) {
	root := filepath.Join("..", "..", "..", "examples")

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".yaml" {
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		t.Run(rel, func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			job, err := ParseYAML(raw)
			if err != nil {
				t.Fatalf("parse example manifest: %v", err)
			}
			if err := Validate(job); err != nil {
				t.Fatalf("validate example manifest: %v", err)
			}
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk examples: %v", err)
	}
}

func TestStarterManifestSurfacesMatchHelloExample(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	canonicalRaw, err := os.ReadFile(filepath.Join(root, "examples", "hello", "trellis.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := ParseYAML(canonicalRaw)
	if err != nil {
		t.Fatalf("parse canonical hello example: %v", err)
	}

	docsRaw, err := os.ReadFile(filepath.Join(root, "docs", "public", "getting-started.md"))
	if err != nil {
		t.Fatal(err)
	}
	docsYAML, err := firstFencedYAML(string(docsRaw))
	if err != nil {
		t.Fatal(err)
	}
	docsSpec, err := ParseYAML([]byte(docsYAML))
	if err != nil {
		t.Fatalf("parse Getting Started starter manifest: %v", err)
	}
	if !reflect.DeepEqual(canonical, docsSpec) {
		t.Fatalf("Getting Started starter manifest drifted from examples/hello/trellis.yaml")
	}
}

func firstFencedYAML(document string) (string, error) {
	const fence = "```yaml\n"
	start := strings.Index(document, fence)
	if start < 0 {
		return "", os.ErrNotExist
	}
	start += len(fence)
	end := strings.Index(document[start:], "\n```")
	if end < 0 {
		return "", os.ErrInvalid
	}
	return document[start : start+end], nil
}
