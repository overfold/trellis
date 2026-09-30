package state

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/overfold/trellis/orchestrator/internal/tlsutil"
)

var (
	testCACert, testCAKey []byte
)

func init() {
	var err error
	testCACert, testCAKey, err = tlsutil.GenerateCA()
	if err != nil {
		panic(err)
	}
}

func testTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	cert, key, err := tlsutil.GenerateNodeCert(testCACert, testCAKey, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	m := &tlsutil.Materials{CACert: testCACert, CAKey: testCAKey, Cert: cert, Key: key}
	cfg, err := tlsutil.PeerTLSConfig(m)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

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

func newTestRaftStore(t *testing.T) *RaftStore {
	t.Helper()
	dir := t.TempDir()
	port := freePort(t)
	bind := fmt.Sprintf("127.0.0.1:%d", port)
	store, err := NewRaftStore(RaftConfig{
		DataDir:       dir,
		BindAddr:      bind,
		Advertise:     bind,
		ServerID:      bind,
		Bootstrap:     true,
		TLS:           testTLSConfig(t),
		AuthorizePeer: allowAnyRaftPeer,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestRaftTLSStreamBindsPeerToAdvertisedAddress(t *testing.T) {
	clientCert, clientKey, err := tlsutil.GenerateNodeCert(testCACert, testCAKey, uuid.New(), "client.example")
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := tlsutil.PeerTLSConfig(&tlsutil.Materials{CACert: testCACert, Cert: clientCert, Key: clientKey})
	if err != nil {
		t.Fatal(err)
	}

	dial := func(t *testing.T, serverSAN string) error {
		t.Helper()
		serverCert, serverKey, err := tlsutil.GenerateNodeCert(testCACert, testCAKey, uuid.New(), serverSAN)
		if err != nil {
			t.Fatal(err)
		}
		serverTLS, err := tlsutil.ServerTLSConfig(&tlsutil.Materials{CACert: testCACert, Cert: serverCert, Key: serverKey})
		if err != nil {
			t.Fatal(err)
		}
		listener, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = listener.Close() }()
		serverDone := make(chan struct{})
		go func() {
			defer close(serverDone)
			conn, acceptErr := listener.Accept()
			if acceptErr == nil {
				if tlsConn, ok := conn.(*tls.Conn); ok {
					_ = tlsConn.Handshake()
				}
				_ = conn.Close()
			}
		}()
		_, port, err := net.SplitHostPort(listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		stream := &tlsStreamLayer{tlsCfg: clientTLS}
		conn, dialErr := stream.Dial(raft.ServerAddress(net.JoinHostPort("localhost", port)), time.Second)
		if conn != nil {
			_ = conn.Close()
		}
		<-serverDone
		return dialErr
	}

	if err := dial(t, "localhost"); err != nil {
		t.Fatalf("certificate for advertised host was rejected: %v", err)
	}
	if err := dial(t, "other.example"); err == nil {
		t.Fatal("Raft stream accepted a cluster certificate not bound to the advertised host")
	}
}

func allowAnyRaftPeer(*x509.Certificate) error { return nil }

func TestRaftListenerRequiresPeerAuthorizer(t *testing.T) {
	bind := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	if _, err := NewRaftStore(RaftConfig{DataDir: t.TempDir(), BindAddr: bind, Advertise: bind, ServerID: bind, Bootstrap: true, TLS: testTLSConfig(t)}); err == nil {
		t.Fatal("Raft TLS listener started without a peer authorizer")
	}
}

// A certificate that chains to the cluster CA is not by itself entitled to
// open Raft streams: the listener asks the authorizer about every peer.
func TestRaftListenerRejectsUnauthorizedPeers(t *testing.T) {
	allowed := uuid.New()
	var seen atomic.Int32
	bind := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	store, err := NewRaftStore(RaftConfig{
		DataDir: t.TempDir(), BindAddr: bind, Advertise: bind, ServerID: bind, Bootstrap: true, TLS: testTLSConfig(t),
		AuthorizePeer: func(certificate *x509.Certificate) error {
			id, err := tlsutil.NodeID(certificate)
			if err != nil {
				return err
			}
			seen.Add(1)
			if id != allowed {
				return errors.New("not a member")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	connect := func(id uuid.UUID) error {
		t.Helper()
		cert, key, err := tlsutil.GenerateNodeCert(testCACert, testCAKey, id)
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := tlsutil.PeerTLSConfig(&tlsutil.Materials{CACert: testCACert, Cert: cert, Key: key})
		if err != nil {
			t.Fatal(err)
		}
		conn, err := tls.Dial("tcp", bind, cfg)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		// TLS 1.3 clients finish their handshake before the server has
		// judged the client certificate; the rejection arrives on read.
		_ = conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		_, err = conn.Read(make([]byte, 1))
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return nil
		}
		return err
	}
	if err := connect(allowed); err != nil {
		t.Fatalf("authorized peer was rejected: %v", err)
	}
	if err := connect(uuid.New()); err == nil {
		t.Fatal("CA-signed peer rejected by the authorizer opened a Raft stream")
	}
	if seen.Load() < 2 {
		t.Fatalf("authorizer saw %d peers, want every inbound handshake", seen.Load())
	}
}

func waitLeader(t *testing.T, store *RaftStore) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if leader, _ := store.Raft().LeaderWithID(); leader != "" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for leader")
}

func TestRaftStore_PutGetDelete(t *testing.T) {
	store := newTestRaftStore(t)
	waitLeader(t, store)
	ctx := context.Background()

	if err := store.Put(ctx, "key1", []byte("value1")); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, "key1")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "value1" {
		t.Fatalf("got %q, want %q", got, "value1")
	}

	if err := store.Delete(ctx, "key1"); err != nil {
		t.Fatal(err)
	}
	got, err = store.Get(ctx, "key1")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil after delete, got %q", got)
	}
}

func TestRaftStore_Batch(t *testing.T) {
	store := newTestRaftStore(t)
	waitLeader(t, store)
	ctx := context.Background()

	if err := store.Batch(ctx, []Mutation{{Key: "job", Value: []byte("job")}, {Key: "revision", Value: []byte("revision")}}); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"job": "job", "revision": "revision"} {
		got, err := store.Get(ctx, key)
		if err != nil || string(got) != want {
			t.Fatalf("get %s = %q, %v; want %q", key, got, err, want)
		}
	}
	if err := store.Batch(ctx, []Mutation{{Key: "partial", Value: []byte("bad")}, {Key: "", Value: []byte("fail")}}); err == nil {
		t.Fatal("expected invalid batch to fail")
	}
	if got, err := store.Get(ctx, "partial"); err != nil || got != nil {
		t.Fatalf("failed Raft batch was not atomic: value=%q err=%v", got, err)
	}
}

func TestRaftStore_BatchReplicatesPrefixDeletion(t *testing.T) {
	store := newTestRaftStore(t)
	waitLeader(t, store)
	ctx := context.Background()
	if err := store.Batch(ctx, []Mutation{
		{Key: "revisions/job/1", Value: []byte("one")},
		{Key: "revisions/job/2", Value: []byte("two")},
		{Key: "revisions/other/1", Value: []byte("other")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Batch(ctx, []Mutation{{DeletePrefix: "revisions/job/"}, {Key: "revisions/job/3", Value: []byte("three")}}); err != nil {
		t.Fatal(err)
	}
	entries, err := store.List(ctx, "revisions/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || string(entries["revisions/job/3"]) != "three" || string(entries["revisions/other/1"]) != "other" {
		t.Fatalf("replicated prefix replacement = %#v", entries)
	}
}

func TestRaftStore_List(t *testing.T) {
	store := newTestRaftStore(t)
	waitLeader(t, store)
	ctx := context.Background()

	if err := store.Put(ctx, "prefix/a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "prefix/b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "other/c", []byte("3")); err != nil {
		t.Fatal(err)
	}

	entries, err := store.List(ctx, "prefix/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}

func TestRaftStore_Replication(t *testing.T) {
	leader := newTestRaftStore(t)
	waitLeader(t, leader)

	followerDir := t.TempDir()
	followerPort := freePort(t)
	followerBind := fmt.Sprintf("127.0.0.1:%d", followerPort)
	follower, err := NewRaftStore(RaftConfig{
		DataDir:       followerDir,
		BindAddr:      followerBind,
		Advertise:     followerBind,
		ServerID:      followerBind,
		Bootstrap:     false,
		TLS:           testTLSConfig(t),
		AuthorizePeer: allowAnyRaftPeer,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = follower.Close() })

	if err := leader.AddNonvoter(followerBind, follower.LocalAddr()); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := leader.Put(ctx, "replicated", []byte("data")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := follower.Get(ctx, "replicated")
		if string(got) == "data" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("data did not replicate to follower within timeout")
}

func TestRaftStore_Snapshot(t *testing.T) {
	store := newTestRaftStore(t)
	waitLeader(t, store)
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		if err := store.Put(ctx, fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("val-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	snap := store.Raft().Snapshot()
	if err := snap.Error(); err != nil {
		t.Fatal(err)
	}

	followerDir := t.TempDir()
	followerPort := freePort(t)
	followerBind := fmt.Sprintf("127.0.0.1:%d", followerPort)
	follower, err := NewRaftStore(RaftConfig{
		DataDir:       followerDir,
		BindAddr:      followerBind,
		Advertise:     followerBind,
		ServerID:      followerBind,
		Bootstrap:     false,
		TLS:           testTLSConfig(t),
		AuthorizePeer: allowAnyRaftPeer,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = follower.Close() })

	if err := store.AddNonvoter(followerBind, follower.LocalAddr()); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ := follower.List(ctx, "key-")
		if len(entries) == 20 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	entries, _ := follower.List(ctx, "key-")
	t.Fatalf("expected 20 entries on follower, got %d", len(entries))
}

func TestRaftStore_RejoinExistingState(t *testing.T) {
	dir := t.TempDir()
	port := freePort(t)
	bind := fmt.Sprintf("127.0.0.1:%d", port)
	tlsCfg := testTLSConfig(t)

	store1, err := NewRaftStore(RaftConfig{DataDir: dir, BindAddr: bind, Advertise: bind, ServerID: bind, Bootstrap: true, TLS: tlsCfg, AuthorizePeer: allowAnyRaftPeer})
	if err != nil {
		t.Fatal(err)
	}
	waitLeader(t, store1)
	_ = store1.Put(context.Background(), "persist", []byte("yes"))
	_ = store1.Close()

	store2, err := NewRaftStore(RaftConfig{DataDir: dir, BindAddr: bind, Advertise: bind, ServerID: bind, Bootstrap: true, TLS: tlsCfg, AuthorizePeer: allowAnyRaftPeer})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store2.Close() }()
	waitLeader(t, store2)

	if !store2.HadExistingState() {
		t.Fatal("expected HadExistingState to be true")
	}

	got, _ := store2.Get(context.Background(), "persist")
	if string(got) != "yes" {
		t.Fatalf("expected persisted data after restart, got %q", got)
	}

	raftDir := filepath.Join(dir, "raft")
	files, _ := filepath.Glob(filepath.Join(raftDir, "*.db"))
	if len(files) < 2 {
		t.Fatalf("expected raft db files in %s, found %d", raftDir, len(files))
	}
}

func newTestRaftFollower(t *testing.T) (*RaftStore, string) {
	t.Helper()
	bind := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	follower, err := NewRaftStore(RaftConfig{DataDir: t.TempDir(), BindAddr: bind, Advertise: bind, ServerID: bind, TLS: testTLSConfig(t), AuthorizePeer: allowAnyRaftPeer})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = follower.Close() })
	return follower, bind
}

func memberVoter(t *testing.T, store *RaftStore, id string) (voter, found bool) {
	t.Helper()
	members, err := store.Membership()
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range members {
		if member.ID == id {
			return member.Voter, true
		}
	}
	return false, false
}

func TestRaftStore_NonvoterPromotionAndDemotion(t *testing.T) {
	leader := newTestRaftStore(t)
	waitLeader(t, leader)
	follower, id := newTestRaftFollower(t)

	if err := leader.AddNonvoter(id, follower.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if voter, found := memberVoter(t, leader, id); !found || voter {
		t.Fatalf("joined member voter=%v found=%v, want a non-voter", voter, found)
	}
	if err := leader.PromoteVoter(id, follower.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if voter, _ := memberVoter(t, leader, id); !voter {
		t.Fatal("promoted member is not a voter")
	}
	// Rejoining must never take away an existing vote.
	if err := leader.AddNonvoter(id, follower.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if voter, _ := memberVoter(t, leader, id); !voter {
		t.Fatal("rejoin demoted a voter")
	}
	if err := leader.DemoteVoter(id); err != nil {
		t.Fatal(err)
	}
	if voter, found := memberVoter(t, leader, id); !found || voter {
		t.Fatalf("demoted member voter=%v found=%v, want a non-voter", voter, found)
	}
}

func TestRaftStore_LeadershipTransferTargetsVotersOnly(t *testing.T) {
	leader := newTestRaftStore(t)
	waitLeader(t, leader)
	nonvoter, nonvoterID := newTestRaftFollower(t)
	if err := leader.AddNonvoter(nonvoterID, nonvoter.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if err := leader.LeadershipTransfer(); err == nil {
		t.Fatal("leadership transfer succeeded with only a non-voter to receive it")
	}
	if leader.Raft().State() != raft.Leader {
		t.Fatalf("leader state = %s after refused transfer", leader.Raft().State())
	}

	voter, voterID := newTestRaftFollower(t)
	if err := leader.AddNonvoter(voterID, voter.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if err := leader.PromoteVoter(voterID, voter.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	// Let the new voter catch up so the transfer has an eligible target.
	if err := leader.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := leader.LeadershipTransfer(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, id := leader.Raft().LeaderWithID(); id != "" && string(id) != leader.LocalAddr() {
			if string(id) != voterID {
				t.Fatalf("leadership moved to %s, want voter %s", id, voterID)
			}
			if nonvoter.Raft().State() == raft.Leader {
				t.Fatal("non-voter became leader")
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("leadership did not move to the voter")
}
