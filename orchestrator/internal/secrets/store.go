// Package secrets encrypts and persists namespace-scoped secrets.
package secrets

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/state"
)

// MaxValueSize is the largest accepted plaintext secret.
const MaxValueSize = 64 << 10

var (
	// ErrNotFound indicates that a secret does not exist.
	ErrNotFound = errors.New("secret not found")
	// ErrVersionConflict indicates a failed version precondition.
	ErrVersionConflict = errors.New("secret version conflict")
)

// Metadata describes an encrypted secret without exposing its value.
type Metadata struct {
	Namespace      string    `json:"namespace"`
	Name           string    `json:"name"`
	Version        uint64    `json:"version"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	CiphertextSize int       `json:"ciphertext_size"`
	KeyID          string    `json:"key_id"`
}

type record struct {
	Metadata
	RecordID   string `json:"record_id"`
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
	WrapNonce  string `json:"wrap_nonce"`
	WrappedDEK string `json:"wrapped_dek"`
}

// Store encrypts secret values before persisting them in state storage.
type Store struct {
	state   state.Store
	cluster string
	keyID   string
	aead    cipher.AEAD
	now     func() time.Time
}

// NewStore configures AES-256-GCM encryption. key must contain exactly 32 bytes.
func NewStore(store state.Store, cluster, keyID string, key []byte) (*Store, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("secrets key must be exactly 32 bytes")
	}
	if strings.TrimSpace(keyID) == "" {
		return nil, fmt.Errorf("secrets key ID is required")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Store{state: store, cluster: cluster, keyID: keyID, aead: aead, now: time.Now}, nil
}

func (s *Store) prefix(namespace string) string {
	return fmt.Sprintf("trellis/%s/secrets/%s/", s.cluster, url.PathEscape(namespace))
}

func (s *Store) key(namespace, name string) string { return s.prefix(namespace) + url.PathEscape(name) }

func aad(namespace, name, recordID string, version uint64) []byte {
	return fmt.Appendf(nil, "trellis-secret\x00%s\x00%s\x00%s\x00%d", namespace, name, recordID, version)
}

// Set creates or updates a secret with an optional version precondition.
func (s *Store) Set(ctx context.Context, namespace, name string, value []byte, expected *uint64) (*Metadata, error) {
	if len(value) == 0 || len(value) > MaxValueSize {
		return nil, fmt.Errorf("secret value must be between 1 and %d bytes", MaxValueSize)
	}
	current, err := s.load(ctx, namespace, name)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	currentVersion := uint64(0)
	if current != nil {
		currentVersion = current.Version
	}
	if expected != nil && *expected != currentVersion {
		return nil, ErrVersionConflict
	}
	now := s.now().UTC()
	created, recordID := now, uuid.NewString()
	if current != nil {
		created, recordID = current.CreatedAt, current.RecordID
	}
	version := currentVersion + 1
	dek := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return nil, fmt.Errorf("generate data encryption key: %w", err)
	}
	defer clear(dek)
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}
	dataAEAD, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, dataAEAD.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate secret nonce: %w", err)
	}
	ciphertext := dataAEAD.Seal(nil, nonce, value, aad(namespace, name, recordID, version))
	wrapNonce := make([]byte, s.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, wrapNonce); err != nil {
		return nil, fmt.Errorf("generate key wrap nonce: %w", err)
	}
	wrappedDEK := s.aead.Seal(nil, wrapNonce, dek, append(aad(namespace, name, recordID, version), []byte("\x00dek")...))
	rec := record{Metadata: Metadata{Namespace: namespace, Name: name, Version: version, CreatedAt: created, UpdatedAt: now, CiphertextSize: len(ciphertext), KeyID: s.keyID}, RecordID: recordID, Nonce: base64.RawStdEncoding.EncodeToString(nonce), Ciphertext: base64.RawStdEncoding.EncodeToString(ciphertext), WrapNonce: base64.RawStdEncoding.EncodeToString(wrapNonce), WrappedDEK: base64.RawStdEncoding.EncodeToString(wrappedDEK)}
	raw, err := json.Marshal(&rec)
	if err != nil {
		return nil, err
	}
	if err := s.state.Put(ctx, s.key(namespace, name), raw); err != nil {
		return nil, fmt.Errorf("persist encrypted secret: %w", err)
	}
	meta := rec.Metadata
	return &meta, nil
}

func (s *Store) load(ctx context.Context, namespace, name string) (*record, error) {
	raw, err := s.state.Get(ctx, s.key(namespace, name))
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, ErrNotFound
	}
	var rec record
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, fmt.Errorf("decode encrypted secret: %w", err)
	}
	return &rec, nil
}

// GetMetadata returns metadata for a secret.
func (s *Store) GetMetadata(ctx context.Context, namespace, name string) (*Metadata, error) {
	rec, err := s.load(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	meta := rec.Metadata
	return &meta, nil
}

// List returns secret metadata for a namespace.
func (s *Store) List(ctx context.Context, namespace string) ([]Metadata, error) {
	values, err := s.state.List(ctx, s.prefix(namespace))
	if err != nil {
		return nil, err
	}
	result := make([]Metadata, 0, len(values))
	for _, raw := range values {
		var rec record
		if err := json.Unmarshal(raw, &rec); err != nil {
			return nil, fmt.Errorf("decode encrypted secret metadata: %w", err)
		}
		result = append(result, rec.Metadata)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

// Namespaces returns the sorted unique namespaces that contain at least one
// secret. Only record keys are inspected; no secret is decrypted.
func (s *Store) Namespaces(ctx context.Context) ([]string, error) {
	root := fmt.Sprintf("trellis/%s/secrets/", s.cluster)
	values, err := s.state.List(ctx, root)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	for key := range values {
		escaped, _, ok := strings.Cut(strings.TrimPrefix(key, root), "/")
		if !ok {
			continue
		}
		namespace, err := url.PathUnescape(escaped)
		if err != nil {
			return nil, fmt.Errorf("decode secret namespace: %w", err)
		}
		seen[namespace] = struct{}{}
	}
	result := make([]string, 0, len(seen))
	for namespace := range seen {
		result = append(result, namespace)
	}
	sort.Strings(result)
	return result, nil
}

// Resolve decrypts a secret and returns its value and version.
func (s *Store) Resolve(ctx context.Context, namespace, name string) ([]byte, uint64, error) {
	rec, err := s.load(ctx, namespace, name)
	if err != nil {
		return nil, 0, err
	}
	return s.decrypt(rec, namespace, name)
}

// ValidateRecord authenticates an encrypted backup record without persisting it
// or retaining its plaintext. It uses the same key and authentication as Resolve.
func (s *Store) ValidateRecord(raw []byte) error {
	var rec record
	if err := json.Unmarshal(raw, &rec); err != nil {
		return fmt.Errorf("decode encrypted secret: %w", err)
	}
	plaintext, _, err := s.decrypt(&rec, rec.Namespace, rec.Name)
	clear(plaintext)
	return err
}

func (s *Store) decrypt(rec *record, namespace, name string) ([]byte, uint64, error) {
	if rec.KeyID != s.keyID {
		return nil, 0, fmt.Errorf("secret key %q is unavailable", rec.KeyID)
	}
	nonce, err := base64.RawStdEncoding.DecodeString(rec.Nonce)
	if err != nil {
		return nil, 0, fmt.Errorf("decode secret nonce: %w", err)
	}
	defer clear(nonce)
	ciphertext, err := base64.RawStdEncoding.DecodeString(rec.Ciphertext)
	if err != nil {
		return nil, 0, fmt.Errorf("decode secret ciphertext: %w", err)
	}
	defer clear(ciphertext)
	wrapNonce, err := base64.RawStdEncoding.DecodeString(rec.WrapNonce)
	if err != nil {
		return nil, 0, fmt.Errorf("decode key wrap nonce: %w", err)
	}
	defer clear(wrapNonce)
	wrappedDEK, err := base64.RawStdEncoding.DecodeString(rec.WrappedDEK)
	if err != nil {
		return nil, 0, fmt.Errorf("decode wrapped key: %w", err)
	}
	defer clear(wrappedDEK)
	if len(wrapNonce) != s.aead.NonceSize() {
		return nil, 0, fmt.Errorf("invalid key wrap nonce size")
	}
	dek, err := s.aead.Open(nil, wrapNonce, wrappedDEK, append(aad(namespace, name, rec.RecordID, rec.Version), []byte("\x00dek")...))
	if err != nil {
		return nil, 0, fmt.Errorf("unwrap data encryption key: %w", err)
	}
	defer clear(dek)
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, 0, err
	}
	dataAEAD, err := cipher.NewGCM(block)
	if err != nil {
		return nil, 0, err
	}
	if len(nonce) != dataAEAD.NonceSize() {
		return nil, 0, fmt.Errorf("invalid secret nonce size")
	}
	plaintext, err := dataAEAD.Open(nil, nonce, ciphertext, aad(namespace, name, rec.RecordID, rec.Version))
	if err != nil {
		return nil, 0, fmt.Errorf("decrypt secret: %w", err)
	}
	return plaintext, rec.Version, nil
}

// Delete removes a secret.
func (s *Store) Delete(ctx context.Context, namespace, name string) error {
	if _, err := s.load(ctx, namespace, name); err != nil {
		return err
	}
	return s.state.Delete(ctx, s.key(namespace, name))
}

type sealedValue struct {
	KeyID      string `json:"key_id"`
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
}

// Seal encrypts a small control-plane value with the active secrets key and
// binds it to associated. The result is safe to persist in replicated state.
func (s *Store) Seal(plaintext, associated []byte) ([]byte, error) {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate seal nonce: %w", err)
	}
	ciphertext := s.aead.Seal(nil, nonce, plaintext, sealAAD(associated))
	return json.Marshal(&sealedValue{KeyID: s.keyID, Nonce: base64.RawStdEncoding.EncodeToString(nonce), Ciphertext: base64.RawStdEncoding.EncodeToString(ciphertext)})
}

// Open decrypts a value produced by Seal with the same associated data.
func (s *Store) Open(sealed, associated []byte) ([]byte, error) {
	var value sealedValue
	if err := json.Unmarshal(sealed, &value); err != nil {
		return nil, fmt.Errorf("decode sealed value: %w", err)
	}
	if value.KeyID != s.keyID {
		return nil, fmt.Errorf("sealing key %q is unavailable", value.KeyID)
	}
	nonce, err := base64.RawStdEncoding.DecodeString(value.Nonce)
	if err != nil {
		return nil, fmt.Errorf("decode sealed nonce: %w", err)
	}
	ciphertext, err := base64.RawStdEncoding.DecodeString(value.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decode sealed ciphertext: %w", err)
	}
	plaintext, err := s.aead.Open(nil, nonce, ciphertext, sealAAD(associated))
	if err != nil {
		return nil, fmt.Errorf("open sealed value: %w", err)
	}
	return plaintext, nil
}

// sealAAD separates sealed control-plane values from secret records, which
// use their own associated-data format under the same key.
func sealAAD(associated []byte) []byte {
	return append([]byte("trellis-sealed\x00"), associated...)
}
