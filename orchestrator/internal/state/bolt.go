// Package state provides persistent and replicated orchestrator state stores.
package state

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/clofour/trellis/internal/spec"
	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"
)

var bucketName = []byte("trellis")

// BoltStore persists state in a local Bolt database.
type BoltStore struct {
	db *bolt.DB
}

// NewBoltStore opens a Bolt-backed state store.
func NewBoltStore(path string) (*BoltStore, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("open bolt database: %w", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create bucket: %w", err)
	}
	return &BoltStore{db: db}, nil
}

// Get returns a copy of the value stored at key.
func (b *BoltStore) Get(_ context.Context, key string) ([]byte, error) {
	var result []byte
	err := b.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketName).Get([]byte(key))
		if v != nil {
			result = make([]byte, len(v))
			copy(result, v)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	return result, nil
}

// List returns values whose keys start with prefix.
func (b *BoltStore) List(_ context.Context, prefix string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	err := b.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketName).Cursor()
		p := []byte(prefix)
		for k, v := c.Seek(p); k != nil && len(k) >= len(p) && string(k[:len(p)]) == prefix; k, v = c.Next() {
			cp := make([]byte, len(v))
			copy(cp, v)
			result[string(k)] = cp
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", prefix, err)
	}
	return result, nil
}

func listBucket(bucket *bolt.Bucket, prefix string) map[string][]byte {
	result := make(map[string][]byte)
	c := bucket.Cursor()
	p := []byte(prefix)
	for k, v := c.Seek(p); k != nil && strings.HasPrefix(string(k), prefix); k, v = c.Next() {
		result[string(k)] = append([]byte(nil), v...)
	}
	return result
}

// Put stores value at key.
func (b *BoltStore) Put(_ context.Context, key string, value []byte) error {
	err := b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(key), value)
	})
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

// Delete removes key.
func (b *BoltStore) Delete(_ context.Context, key string) error {
	err := b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Delete([]byte(key))
	})
	if err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

// Batch applies mutations in one Bolt write transaction.
func (b *BoltStore) Batch(_ context.Context, mutations []Mutation) error {
	if err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		for _, mutation := range mutations {
			if mutation.Key == "" {
				return fmt.Errorf("batch contains an empty key")
			}
			var err error
			if mutation.Value == nil {
				err = bucket.Delete([]byte(mutation.Key))
			} else {
				err = bucket.Put([]byte(mutation.Key), mutation.Value)
			}
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("apply batch: %w", err)
	}
	return nil
}

// DesiredSnapshot reads all desired-state prefixes from one Bolt view.
func (b *BoltStore) DesiredSnapshot(cluster string) (*DesiredSnapshot, error) {
	prefix := "trellis/" + cluster + "/"
	var result *DesiredSnapshot
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		result = &DesiredSnapshot{
			Jobs:                     relativeEntries(listBucket(bucket, prefix+"jobs/"), prefix+"jobs/"),
			JobRevisions:             relativeEntries(listBucket(bucket, prefix+"job-revisions/"), prefix+"job-revisions/"),
			Secrets:                  relativeEntries(listBucket(bucket, prefix+"secrets/"), prefix+"secrets/"),
			VolumeRegistrations:      relativeEntries(listBucket(bucket, prefix+"volume-registrations/"), prefix+"volume-registrations/"),
			NetworkPortRegistrations: relativeEntries(listBucket(bucket, prefix+"network-port-registrations/"), prefix+"network-port-registrations/"),
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("backup desired state: %w", err)
	}
	return result, nil
}

func relativeEntries(entries map[string][]byte, prefix string) map[string][]byte {
	result := make(map[string][]byte, len(entries))
	for key, value := range entries {
		result[strings.TrimPrefix(key, prefix)] = value
	}
	return result
}

// Restore atomically replaces all stored state.
func (b *BoltStore) Restore(data map[string][]byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		_ = tx.DeleteBucket(bucketName)
		bucket, err := tx.CreateBucket(bucketName)
		if err != nil {
			return err
		}
		for k, v := range data {
			if err := bucket.Put([]byte(k), v); err != nil {
				return err
			}
		}
		return nil
	})
}

// RestoreDesired atomically verifies that the target is fresh and installs
// job definitions, encrypted secret records, volume locality metadata, and
// namespace WireGuard port assignments.
func (b *BoltStore) RestoreDesired(cluster string, snapshot *DesiredSnapshot) error {
	if err := ValidateDesiredSnapshot(snapshot, nil); err != nil {
		return err
	}
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		jobsPrefix := []byte(fmt.Sprintf("trellis/%s/jobs/", cluster))
		revisionsPrefix := []byte(fmt.Sprintf("trellis/%s/job-revisions/", cluster))
		secretsPrefix := []byte(fmt.Sprintf("trellis/%s/secrets/", cluster))
		volumesPrefix := []byte(fmt.Sprintf("trellis/%s/volume-registrations/", cluster))
		networkPortsPrefix := []byte(fmt.Sprintf("trellis/%s/network-port-registrations/", cluster))
		allocationsPrefix := []byte(fmt.Sprintf("trellis/%s/allocations/", cluster))
		for _, prefix := range [][]byte{jobsPrefix, revisionsPrefix, secretsPrefix, volumesPrefix, networkPortsPrefix, allocationsPrefix} {
			key, _ := bucket.Cursor().Seek(prefix)
			if key != nil && len(key) >= len(prefix) && string(key[:len(prefix)]) == string(prefix) {
				return fmt.Errorf("restore requires a fresh cluster with no jobs, secrets, volume registrations, network port registrations, or allocations")
			}
		}
		for key, value := range snapshot.JobRevisions {
			if err := bucket.Put(append(append([]byte(nil), revisionsPrefix...), key...), value); err != nil {
				return err
			}
		}
		for key, value := range snapshot.Jobs {
			if key == "" {
				return fmt.Errorf("backup contains an empty job key")
			}
			if err := bucket.Put(append(append([]byte(nil), jobsPrefix...), key...), value); err != nil {
				return err
			}
		}
		for key, value := range snapshot.Secrets {
			if key == "" {
				return fmt.Errorf("backup contains an empty secret key")
			}
			if err := bucket.Put(append(append([]byte(nil), secretsPrefix...), key...), value); err != nil {
				return err
			}
		}
		for key, value := range snapshot.VolumeRegistrations {
			if key == "" {
				return fmt.Errorf("backup contains an empty volume registration key")
			}
			if err := bucket.Put(append(append([]byte(nil), volumesPrefix...), key...), value); err != nil {
				return err
			}
		}
		for key, value := range snapshot.NetworkPortRegistrations {
			if key == "" {
				return fmt.Errorf("backup contains an empty network port registration key")
			}
			if err := bucket.Put(append(append([]byte(nil), networkPortsPrefix...), key...), value); err != nil {
				return err
			}
		}
		return nil
	})
}

type persistedJob struct {
	Spec     *spec.JobSpec `json:"Spec"`
	Revision int           `json:"Revision"`
}

type persistedJobRevision struct {
	Revision  int           `json:"revision"`
	Spec      *spec.JobSpec `json:"spec"`
	CreatedAt time.Time     `json:"created_at"`
}

type volumeRegistration struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	NodeID    uuid.UUID `json:"node_id"`
}

type networkPortRegistration struct {
	Namespace string `json:"namespace"`
	Slot      int    `json:"slot"`
}

type secretRecord struct {
	Namespace      string `json:"namespace"`
	Name           string `json:"name"`
	Version        uint64 `json:"version"`
	CiphertextSize int    `json:"ciphertext_size"`
	KeyID          string `json:"key_id"`
	RecordID       string `json:"record_id"`
	Nonce          string `json:"nonce"`
	Ciphertext     string `json:"ciphertext"`
	WrapNonce      string `json:"wrap_nonce"`
	WrappedDEK     string `json:"wrapped_dek"`
}

// ValidateDesiredSnapshot validates state-owned encoding, key identity, and
// invariants without writing. additionalJobValidation may add server-level
// policy checks after canonical spec validation.
func ValidateDesiredSnapshot(snapshot *DesiredSnapshot, additionalJobValidation func(*spec.JobSpec) error) error {
	if snapshot == nil {
		return fmt.Errorf("restore snapshot is missing")
	}
	for key, raw := range snapshot.Jobs {
		var job persistedJob
		if key == "" || json.Unmarshal(raw, &job) != nil || job.Spec == nil || job.Revision < 1 {
			return fmt.Errorf("invalid job record %q", key)
		}
		identity := job.Spec.Name
		if job.Spec.Namespace != "" {
			identity = job.Spec.Namespace + "\x00" + job.Spec.Name
		}
		if key != url.QueryEscape(identity) {
			return fmt.Errorf("job key %q does not match job identity", key)
		}
		if err := validateRestoredJob(job.Spec, additionalJobValidation); err != nil {
			return fmt.Errorf("validate job %q: %w", key, err)
		}
	}
	for key, raw := range snapshot.JobRevisions {
		var revision persistedJobRevision
		lastSlash := strings.LastIndexByte(key, '/')
		if lastSlash < 1 || json.Unmarshal(raw, &revision) != nil || revision.Spec == nil || revision.Revision < 1 || revision.CreatedAt.IsZero() {
			return fmt.Errorf("invalid job revision record %q", key)
		}
		identity := revision.Spec.Name
		if revision.Spec.Namespace != "" {
			identity = revision.Spec.Namespace + "\x00" + revision.Spec.Name
		}
		if key[:lastSlash] != url.QueryEscape(identity) || key[lastSlash+1:] != strconv.Itoa(revision.Revision) {
			return fmt.Errorf("job revision key %q does not match record identity", key)
		}
		if err := validateRestoredJob(revision.Spec, additionalJobValidation); err != nil {
			return fmt.Errorf("validate job revision %q: %w", key, err)
		}
	}
	for key, raw := range snapshot.VolumeRegistrations {
		var record volumeRegistration
		if key == "" || json.Unmarshal(raw, &record) != nil || record.Namespace == "" || record.Name == "" || record.NodeID == uuid.Nil || key != url.QueryEscape(record.Namespace+"\x00"+record.Name) {
			return fmt.Errorf("invalid volume registration %q", key)
		}
	}
	for key, raw := range snapshot.NetworkPortRegistrations {
		var record networkPortRegistration
		if key == "" || json.Unmarshal(raw, &record) != nil || record.Namespace == "" || record.Slot < 0 || key != url.QueryEscape(record.Namespace) {
			return fmt.Errorf("invalid network port registration %q", key)
		}
	}
	for key, raw := range snapshot.Secrets {
		var record secretRecord
		if json.Unmarshal(raw, &record) != nil || record.Namespace == "" || record.Name == "" || record.Version == 0 || record.RecordID == "" || record.KeyID == "" || record.CiphertextSize < 1 || key != url.PathEscape(record.Namespace)+"/"+url.PathEscape(record.Name) {
			return fmt.Errorf("invalid secret record %q", key)
		}
		for _, encoded := range []string{record.Nonce, record.Ciphertext, record.WrapNonce, record.WrappedDEK} {
			if decoded, err := base64.RawStdEncoding.DecodeString(encoded); err != nil || len(decoded) == 0 {
				return fmt.Errorf("invalid secret record %q", key)
			}
		}
		ciphertext, _ := base64.RawStdEncoding.DecodeString(record.Ciphertext)
		if len(ciphertext) != record.CiphertextSize {
			return fmt.Errorf("invalid secret record %q", key)
		}
	}
	return nil
}

func validateRestoredJob(job *spec.JobSpec, additional func(*spec.JobSpec) error) error {
	if err := spec.Validate(job); err != nil {
		return err
	}
	if additional != nil {
		return additional(job)
	}
	return nil
}

// Close closes the Bolt database.
func (b *BoltStore) Close() error {
	return b.db.Close()
}
