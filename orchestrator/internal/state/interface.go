package state

import "context"

// Store defines persistent key-value state operations.
type Store interface {
	Get(ctx context.Context, key string) ([]byte, error)
	List(ctx context.Context, prefix string) (map[string][]byte, error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

// Mutation is one operation in an atomic batch. A nil Value deletes Key;
// a non-nil Value stores it (including an empty value).
type Mutation struct {
	Key   string
	Value []byte
}

// AtomicStore extends Store with all-or-nothing multi-key updates.
type AtomicStore interface {
	Store
	Batch(ctx context.Context, mutations []Mutation) error
}
