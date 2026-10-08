package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

func TestCommittedRestoreRestart(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(fmt.Sprint("snapshot=", snapshot), func(t *testing.T) {
			cfg := RaftConfig{DataDir: t.TempDir(), BindAddr: "127.0.0.1:0", ServerID: "restart", Bootstrap: true}
			store, err := NewRaftStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			waitLeader(t, store)
			key, raw := validSnapshotJob(t, "default", "web", 1)
			desired := &DesiredSnapshot{Jobs: map[string][]byte{key: raw}}
			if err := store.RestoreDesired("test", desired); err != nil {
				t.Fatal(err)
			}
			if err := store.RestoreDesired("test", desired); !errors.Is(err, ErrRestoreNotFresh) {
				t.Fatalf("freshness: %v", err)
			}
			if snapshot {
				if err := store.raft.Snapshot().Error(); err != nil {
					t.Fatal(err)
				}
			}
			jobPath := "trellis/test/jobs/" + key
			if err := store.Delete(t.Context(), jobPath); err != nil {
				t.Fatal(err)
			}
			tombstone := "trellis/test/node-tombstones/" + uuid.NewString()
			if err := store.Put(t.Context(), tombstone, []byte("removed")); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = NewRaftStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			waitLeader(t, store)
			if err := store.raft.Barrier(0).Error(); err != nil {
				t.Fatal(err)
			}
			if got, err := store.Get(t.Context(), jobPath); err != nil || got != nil {
				t.Fatalf("deleted job resurrected: %s, %v", got, err)
			}
			if got, err := store.Get(t.Context(), tombstone); err != nil || string(got) != "removed" {
				t.Fatalf("tombstone lost: %s, %v", got, err)
			}
		})
	}
}

func TestFSMCheckpointRollsBackWithFailedCommand(t *testing.T) {
	store := newTestRaftStore(t)
	command, _ := json.Marshal(fsmCommand{Op: "batch", Mutations: []Mutation{
		{Key: "first", Value: []byte("must roll back")},
		{Key: strings.Repeat("x", bolt.MaxKeySize+1), Value: []byte("invalid")},
	}})
	if err := store.fsm.apply(&raft.Log{Index: 100, Data: command}); err == nil {
		t.Fatal("invalid command succeeded")
	}
	command, _ = json.Marshal(fsmCommand{Op: "put", Key: "first", Value: []byte("retry")})
	if err := store.fsm.apply(&raft.Log{Index: 100, Data: command}); err != nil {
		t.Fatal(err)
	}
	got, _ := store.Get(t.Context(), "first")
	if !bytes.Equal(got, []byte("retry")) {
		t.Fatalf("failed command advanced checkpoint: %q", got)
	}
}

func TestRestorePreflightStorageKeyBoundary(t *testing.T) {
	store := newTestRaftStore(t)
	storeCluster := "long-target-cluster"
	prefix := "trellis/" + storeCluster + "/volume-registrations/"
	for _, excess := range []int{1, 0} {
		name := strings.Repeat("x", bolt.MaxKeySize-len(prefix)-len(url.QueryEscape("default/"))+excess)
		raw, err := json.Marshal(volumeRegistration{Namespace: "default", Name: name, NodeID: uuid.New()})
		if err != nil {
			t.Fatal(err)
		}
		desired := &DesiredSnapshot{VolumeRegistrations: map[string][]byte{url.QueryEscape("default/" + name): raw}}
		waitLeader(t, store)
		before := store.AppliedIndex()
		err = store.RestoreDesired(storeCluster, desired)
		if excess > 0 {
			if err == nil || !strings.Contains(err.Error(), "storage limit") {
				t.Fatalf("oversized key: %v", err)
			}
			if store.AppliedIndex() != before {
				t.Fatal("invalid restore entered log")
			}
		} else if err != nil {
			t.Fatalf("storage-sized key rejected: %v", err)
		}
	}
}
