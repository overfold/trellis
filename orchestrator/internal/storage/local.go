// Package storage provides filesystem-backed persistent storage.
package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LocalStorage stores JSON values under a local data directory.
type LocalStorage struct {
	dataRoot string
}

// NewLocalStorage creates local storage rooted at dataRoot.
func NewLocalStorage(dataRoot string) *LocalStorage {
	return &LocalStorage{
		dataRoot: dataRoot,
	}
}

// Init creates the storage root.
func (s *LocalStorage) Init() error {
	err := os.MkdirAll(s.dataRoot, 0o750)
	if err != nil {
		return fmt.Errorf("init data dir %s: %w", s.dataRoot, err)
	}

	return nil
}

// Get decodes the JSON value stored at key into value.
func (s *LocalStorage) Get(key string, value any) error {
	path := s.formatPath(key)
	if path == "" {
		return fmt.Errorf("invalid storage key %q", key)
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("get key %s: %w", key, err)
	}

	err = json.Unmarshal(content, value)
	if err != nil {
		return fmt.Errorf("unmarshal json: %w", err)
	}

	return nil
}

// Put atomically encodes and stores a JSON value at key.
func (s *LocalStorage) Put(key string, value any) error {
	path := s.formatPath(key)
	if path == "" {
		return fmt.Errorf("invalid storage key %q", key)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create parent directory: %w", err)
	}

	content, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal json: %w", err)
	}

	tmpFile, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()
	if err := tmpFile.Chmod(0o600); err != nil {
		return fmt.Errorf("chmod tmp file: %w", err)
	}

	_, err = tmpFile.Write(content)
	if err != nil {
		_ = os.Remove(tmpFile.Name())
		return fmt.Errorf("write tmp file: %w", err)
	}
	if err = tmpFile.Sync(); err != nil {
		return fmt.Errorf("sync tmp file: %w", err)
	}
	if err = tmpFile.Close(); err != nil {
		_ = os.Remove(tmpFile.Name())
		return fmt.Errorf("close tmp file: %w", err)
	}
	err = os.Rename(tmpFile.Name(), path)
	if err != nil {
		return fmt.Errorf("rename tmp file: %w", err)
	}
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		defer func() { _ = dir.Close() }()
		if err := dir.Sync(); err != nil {
			return fmt.Errorf("sync parent directory: %w", err)
		}
	}

	return nil
}

// Delete removes key if it exists.
func (s *LocalStorage) Delete(key string) error {
	path := s.formatPath(key)
	if path == "" {
		return fmt.Errorf("invalid storage key %q", key)
	}

	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete key %s: %w", key, err)
	}

	return nil
}

// ListRaw reads independent records below a key. Errors are returned per file
// so one malformed or unreadable allocation cannot block recovery of others.
func (s *LocalStorage) ListRaw(key string) (map[string]json.RawMessage, []error) {
	root := s.formatPath(key)
	if root == "" {
		return nil, []error{fmt.Errorf("invalid storage key %q", key)}
	}
	result := make(map[string]json.RawMessage)
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return result, nil
	}
	if err != nil {
		return result, []error{err}
	}
	var errs []error
	for _, entry := range entries {
		// Put publishes only by rename; interrupted writes are not records.
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".tmp-") {
			continue
		}
		content, err := os.ReadFile(filepath.Join(root, entry.Name()))
		if err != nil {
			errs = append(errs, fmt.Errorf("read %s: %w", entry.Name(), err))
			continue
		}
		result[entry.Name()] = content
	}
	return result, errs
}

func (s *LocalStorage) formatPath(key string) string {
	if key == "" || filepath.IsAbs(key) {
		return ""
	}
	clean := filepath.Clean(key)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return ""
	}
	path := filepath.Join(s.dataRoot, clean)
	rel, err := filepath.Rel(s.dataRoot, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return ""
	}
	return path
}
