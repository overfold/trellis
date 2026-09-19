// Package state provides persistent and replicated orchestrator state stores.
package state

import (
	"context"
	"fmt"

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
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		jobsPrefix := []byte(fmt.Sprintf("trellis/%s/jobs/", cluster))
		secretsPrefix := []byte(fmt.Sprintf("trellis/%s/secrets/", cluster))
		volumesPrefix := []byte(fmt.Sprintf("trellis/%s/volume-registrations/", cluster))
		networkPortsPrefix := []byte(fmt.Sprintf("trellis/%s/network-port-registrations/", cluster))
		allocationsPrefix := []byte(fmt.Sprintf("trellis/%s/allocations/", cluster))
		for _, prefix := range [][]byte{jobsPrefix, secretsPrefix, volumesPrefix, networkPortsPrefix, allocationsPrefix} {
			key, _ := bucket.Cursor().Seek(prefix)
			if key != nil && len(key) >= len(prefix) && string(key[:len(prefix)]) == string(prefix) {
				return fmt.Errorf("restore requires a fresh cluster with no jobs, secrets, volume registrations, network port registrations, or allocations")
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

// Close closes the Bolt database.
func (b *BoltStore) Close() error {
	return b.db.Close()
}
