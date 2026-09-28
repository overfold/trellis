package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestSecretInputsEnforceExactSizeBoundary(t *testing.T) {
	exact := bytes.Repeat([]byte{'x'}, 64<<10)
	if value, err := readSecret(bytes.NewReader(exact)); err != nil || !bytes.Equal(value, exact) {
		t.Fatalf("exact stdin boundary: length %d, error %v", len(value), err)
	}
	if value, err := readSecret(bytes.NewReader(append(exact, 'x'))); err == nil || value != nil {
		t.Fatalf("oversized stdin = %d bytes, error %v", len(value), err)
	}

	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, exact, 0o600); err != nil {
		t.Fatal(err)
	}
	if value, err := readSecretFile(path); err != nil || !bytes.Equal(value, exact) {
		t.Fatalf("exact file boundary: length %d, error %v", len(value), err)
	}
	if err := os.WriteFile(path, append(exact, 'x'), 0o600); err != nil {
		t.Fatal(err)
	}
	if value, err := readSecretFile(path); err == nil || value != nil {
		t.Fatalf("oversized file = %d bytes, error %v", len(value), err)
	}
}
