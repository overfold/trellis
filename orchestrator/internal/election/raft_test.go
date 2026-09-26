package election

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

type noopFSM struct{}

func (noopFSM) Apply(*raft.Log) interface{}         { return nil }
func (noopFSM) Snapshot() (raft.FSMSnapshot, error) { return noopSnap{}, nil }
func (noopFSM) Restore(rc io.ReadCloser) error      { return rc.Close() }

type noopSnap struct{}

func (noopSnap) Persist(sink raft.SnapshotSink) error { return sink.Close() }
func (noopSnap) Release()                             {}

func newTestRaft(t *testing.T) (*raft.Raft, string, uuid.UUID) {
	t.Helper()
	dir := t.TempDir()
	port := freePort(t)
	bind := fmt.Sprintf("127.0.0.1:%d", port)
	addr, _ := net.ResolveTCPAddr("tcp", bind)
	transport, err := raft.NewTCPTransport(bind, addr, 1, time.Second, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dir, "log.db"))
	if err != nil {
		t.Fatal(err)
	}
	snaps, _ := raft.NewFileSnapshotStore(dir, 1, os.Stderr)

	nodeID := uuid.New()
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(nodeID.String())
	cfg.HeartbeatTimeout = 200 * time.Millisecond
	cfg.ElectionTimeout = 200 * time.Millisecond
	cfg.LeaderLeaseTimeout = 100 * time.Millisecond

	r, err := raft.NewRaft(cfg, noopFSM{}, logStore, logStore, snaps, transport)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		r.Shutdown()
		_ = logStore.Close()
	})

	r.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{{ID: raft.ServerID(nodeID.String()), Address: transport.LocalAddr()}},
	})

	return r, bind, nodeID
}

func TestRaftElector_ElectedEvent(t *testing.T) {
	r, bind, _ := newTestRaft(t)
	nodeID := uuid.New()
	elector := NewRaftElector(r, Leader{NodeID: nodeID, Address: bind}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	events := make(chan Event, 1)
	go func() { _ = elector.Run(ctx, events) }()

	select {
	case event := <-events:
		if !event.Elected {
			t.Fatal("expected Elected=true")
		}
		if event.Leader.Address != bind {
			t.Fatalf("expected address %s, got %s", bind, event.Leader.Address)
		}
		if event.Leader.NodeID != nodeID {
			t.Fatalf("expected node ID %s, got %s", nodeID, event.Leader.NodeID)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for elected event")
	}
}

func TestRaftElector_Current(t *testing.T) {
	r, bind, nodeID := newTestRaft(t)
	elector := NewRaftElector(r, Leader{NodeID: nodeID, Address: bind}, nil)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		leader, err := elector.Current(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if leader != nil && leader.Address != "" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for leader via Current()")
}
