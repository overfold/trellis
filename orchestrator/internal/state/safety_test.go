package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// Run a real follower FSM on Raft's goroutine in a subprocess: recovering a
// panic in the test goroutine would not prove the node actually fails stop.
func TestFollowerApplyFailureStopsNode(t *testing.T) {
	if mode := os.Getenv("TRELLIS_TEST_APPLY_FAILURE"); mode != "" {
		leader := newTestRaftStore(t)
		waitLeader(t, leader)
		follower, id := newTestRaftFollower(t)
		if err := leader.AddNonvoter(id, follower.LocalAddr()); err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		if err := leader.Put(ctx, "baseline", []byte("ready")); err != nil {
			t.Fatal(err)
		}
		waitReplicatedValue(t, follower, "baseline", []byte("ready"))
		switch mode {
		case "storage":
			if err := follower.fsm.store.Close(); err != nil {
				t.Fatal(err)
			}
			if err := leader.Put(ctx, "committed", []byte("must not skip")); err != nil {
				t.Fatal(err)
			}
		case "inequality":
			if err := follower.fsm.store.Put(ctx, "trellis/new/allocations/stale", []byte(`{}`)); err != nil {
				t.Fatal(err)
			}
			key, value := validSnapshotJob(t, "default", "web", 3)
			if err := leader.RestoreDesired("new", &DesiredSnapshot{Jobs: map[string][]byte{key: value}}); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("unknown test mode")
		}
		time.Sleep(5 * time.Second)
		t.Fatal("node continued running after follower apply failure")
	}
	for _, mode := range []string{"storage", "inequality"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFollowerApplyFailureStopsNode$", "-test.v")
			cmd.Env = append(os.Environ(), "TRELLIS_TEST_APPLY_FAILURE="+mode)
			output, err := cmd.CombinedOutput()
			if err == nil || ctx.Err() != nil || !bytes.Contains(output, []byte("panic: fatal Raft FSM apply at index")) {
				t.Fatalf("expected fatal process exit, got %v: %s", err, output)
			}
			want := "database not open"
			if mode == "inequality" {
				want = "restore requires a fresh cluster"
			}
			if !bytes.Contains(output, []byte(want)) {
				t.Fatalf("wrong failure: %s", output)
			}
		})
	}
}

func TestRestoreFreshnessConflictPreservesEveryResource(t *testing.T) {
	store, err := NewBoltStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, resource := range []string{"jobs", "job-revisions", "secrets", "volume-registrations", "network-port-registrations", "network-subnet-registrations", "allocations", "replacement-backoffs"} {
		t.Run(resource, func(t *testing.T) {
			key := "trellis/test/" + resource + "/retained"
			if err := store.Put(t.Context(), key, []byte("retained")); err != nil {
				t.Fatal(err)
			}
			if err := store.RestoreDesired("test", &DesiredSnapshot{Cluster: []byte(`{}`)}); !errors.Is(err, ErrRestoreNotFresh) {
				t.Fatalf("restore error = %v", err)
			}
			if got, err := store.Get(t.Context(), key); err != nil || string(got) != "retained" {
				t.Fatalf("retained state changed: %q %v", got, err)
			}
			if got, err := store.Get(t.Context(), "trellis/test/meta"); err != nil || got != nil {
				t.Fatalf("cluster partially installed: %q %v", got, err)
			}
			if err := store.Delete(t.Context(), key); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRestoreBarrierFailureIsNotFreshnessConflict(t *testing.T) {
	store := newTestRaftStore(t)
	waitLeader(t, store)
	if err := store.Raft().Shutdown().Error(); err != nil {
		t.Fatal(err)
	}
	if err := store.RestoreDesired("test", &DesiredSnapshot{}); err == nil || errors.Is(err, ErrRestoreNotFresh) {
		t.Fatalf("barrier failure = %v", err)
	}
}

func waitReplicatedValue(t *testing.T, store *RaftStore, key string, want []byte) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, err := store.Get(t.Context(), key)
		if err == nil && bytes.Equal(got, want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("follower did not replicate %s", key)
}

func TestRaftRestoreUniformRejectionIsHealthy(t *testing.T) {
	leader := newTestRaftStore(t)
	waitLeader(t, leader)
	follower, id := newTestRaftFollower(t)
	if err := leader.AddNonvoter(id, follower.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	key, value := validSnapshotJob(t, "recovered", "service", 3)
	snapshot := &DesiredSnapshot{Jobs: map[string][]byte{key: value}}
	if err := leader.RestoreDesired("new", snapshot); err != nil {
		t.Fatal(err)
	}
	waitReplicatedValue(t, follower, "trellis/new/jobs/"+key, value)
	before := leader.Raft().LastIndex()
	if err := leader.RestoreDesired("new", snapshot); !errors.Is(err, ErrRestoreNotFresh) {
		t.Fatalf("restore rejection = %v", err)
	}
	// The barrier is allowed a log entry, but the rejected restore is not.
	for index := before + 1; index <= leader.Raft().LastIndex(); index++ {
		var entry raft.Log
		if err := leader.logStore.GetLog(index, &entry); err != nil {
			t.Fatal(err)
		}
		if entry.Type == raft.LogCommand {
			t.Fatal("rejected restore was replicated")
		}
	}
	if err := leader.Put(t.Context(), "after-rejection", []byte("healthy")); err != nil {
		t.Fatal(err)
	}
	waitReplicatedValue(t, follower, "after-rejection", []byte("healthy"))
	if err := leader.RestoreDesired("new", &DesiredSnapshot{Jobs: map[string][]byte{"bad": []byte(`{}`)}}); err == nil {
		t.Fatal("invalid snapshot accepted")
	}
	if err := leader.Put(t.Context(), "after-validation", []byte("healthy")); err != nil {
		t.Fatal(err)
	}
	waitReplicatedValue(t, follower, "after-validation", []byte("healthy"))
}

func TestRestoreBackoffFreshnessAndCleanup(t *testing.T) {
	for _, cleanupFirst := range []bool{false, true} {
		for _, incarnation := range []string{"same", "different"} {
			for _, revision := range []int{3, 4} {
				t.Run(fmt.Sprintf("cleanup=%v/incarnation=%s/revision=%d", cleanupFirst, incarnation, revision), func(t *testing.T) {
					store, err := NewBoltStore(filepath.Join(t.TempDir(), "state.db"))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = store.Close() })
					key, value := validSnapshotJob(t, "default", "web", 3)
					snapshot := &DesiredSnapshot{Jobs: map[string][]byte{key: value}}
					backoffKey := "trellis/new/replacement-backoffs/default/web/group"
					backoff, _ := json.Marshal(map[string]any{"job_incarnation": incarnation, "job_revision": revision, "failures": 7, "delayed_replacements": 1})
					if err := store.Put(t.Context(), backoffKey, backoff); err != nil {
						t.Fatal(err)
					}
					// An unrelated cluster's scheduling state is never removed.
					otherKey := "trellis/other/replacement-backoffs/default/web/group"
					if err := store.Put(t.Context(), otherKey, backoff); err != nil {
						t.Fatal(err)
					}
					if !cleanupFirst {
						if err := store.RestoreDesired("new", snapshot); !errors.Is(err, ErrRestoreNotFresh) {
							t.Fatalf("restore backoff-only target: %v", err)
						}
						if got, err := store.Get(t.Context(), "trellis/new/jobs/"+key); err != nil || got != nil {
							t.Fatalf("rejection partially installed job: %q, %v", got, err)
						}
					}
					if err := store.Batch(t.Context(), []Mutation{{DeletePrefix: "trellis/new/replacement-backoffs/"}}); err != nil {
						t.Fatal(err)
					}
					if err := store.RestoreDesired("new", snapshot); err != nil {
						t.Fatal(err)
					}
					if got, err := store.Get(t.Context(), "trellis/new/jobs/"+key); err != nil || !bytes.Equal(got, value) {
						t.Fatalf("restored job = %q, %v", got, err)
					}
					if got, err := store.Get(t.Context(), backoffKey); err != nil || got != nil {
						t.Fatalf("stale scheduling state survived: %q, %v", got, err)
					}
					if got, err := store.Get(t.Context(), otherKey); err != nil || !bytes.Equal(got, backoff) {
						t.Fatalf("other cluster changed: %q, %v", got, err)
					}
				})
			}
		}
	}
}
