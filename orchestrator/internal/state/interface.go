package state

import "context"

// Store defines persistent key-value state operations.
type Store interface {
	Get(ctx context.Context, key string) ([]byte, error)
	List(ctx context.Context, prefix string) (map[string][]byte, error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

// PrefixIterator visits matching entries in key order without materializing
// the whole prefix. The callback must not retain value after it returns.
type PrefixIterator interface {
	IteratePrefix(ctx context.Context, prefix string, visit func(key string, value []byte) error) error
}

// Mutation is one operation in an atomic batch. A nil Value deletes Key;
// a non-nil Value stores it (including an empty value). DeletePrefix removes
// every matching key and is mutually exclusive with Key and Value.
type Mutation struct {
	Key          string `json:"Key"`
	Value        []byte `json:"Value"`
	DeletePrefix string `json:"DeletePrefix"`
}

// AtomicStore extends Store with all-or-nothing multi-key updates.
type AtomicStore interface {
	Store
	Batch(ctx context.Context, mutations []Mutation) error
}
