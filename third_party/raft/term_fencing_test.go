package raft

import (
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
)

func TestTrellisHeartbeatTermTransitionUsesConsumer(t *testing.T) {
	transport, err := NewTCPTransportWithLogger("127.0.0.1:0", nil, 1, time.Second, hclog.NewNullLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	config := DefaultConfig()
	config.LocalID = "node"
	config.Logger = hclog.NewNullLogger()
	config.skipStartup = true
	store := NewInmemStore()
	r, err := NewRaft(config, &MockFSM{}, store, store, NewInmemSnapshotStore(), transport)
	if err != nil {
		t.Fatal(err)
	}
	// No main loop is running: a higher-term heartbeat must remain queued,
	// not mutate the term through a NetworkTransport connection goroutine.
	done := make(chan error, 1)
	go func() {
		var response AppendEntriesResponse
		done <- transport.AppendEntries("node", transport.LocalAddr(), &AppendEntriesRequest{
			RPCHeader: RPCHeader{ProtocolVersion: ProtocolVersionMax, ID: []byte("node"), Addr: []byte(transport.LocalAddr())},
			Term:      10, Leader: []byte(transport.LocalAddr()),
		}, &response)
	}()
	select {
	case rpc := <-transport.Consumer():
		if r.CurrentTerm() != 0 {
			t.Fatalf("heartbeat bypassed main-loop term admission: term=%d", r.CurrentTerm())
		}
		r.processRPC(rpc)
		if r.CurrentTerm() != 10 {
			t.Fatalf("main-loop processing did not advance term: %d", r.CurrentTerm())
		}
	case err := <-done:
		t.Fatalf("heartbeat bypassed Consumer: %v, term=%d", err, r.CurrentTerm())
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat did not reach Consumer")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
