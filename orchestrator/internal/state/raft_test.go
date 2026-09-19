package state

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/tlsutil"
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
	cert, key, err := tlsutil.GenerateNodeCert(testCACert, testCAKey, "test-node")
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
		DataDir:   dir,
		BindAddr:  bind,
		Advertise: bind,
		ServerID:  bind,
		Bootstrap: true,
		TLS:       testTLSConfig(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
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
		DataDir:   followerDir,
		BindAddr:  followerBind,
		Advertise: followerBind,
		ServerID:  followerBind,
		Bootstrap: false,
		TLS:       testTLSConfig(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = follower.Close() })

	if err := leader.AddVoter(followerBind, follower.LocalAddr()); err != nil {
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
		DataDir:   followerDir,
		BindAddr:  followerBind,
		Advertise: followerBind,
		ServerID:  followerBind,
		Bootstrap: false,
		TLS:       testTLSConfig(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = follower.Close() })

	if err := store.AddVoter(followerBind, follower.LocalAddr()); err != nil {
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

	store1, err := NewRaftStore(RaftConfig{DataDir: dir, BindAddr: bind, Advertise: bind, ServerID: bind, Bootstrap: true, TLS: tlsCfg})
	if err != nil {
		t.Fatal(err)
	}
	waitLeader(t, store1)
	_ = store1.Put(context.Background(), "persist", []byte("yes"))
	_ = store1.Close()

	store2, err := NewRaftStore(RaftConfig{DataDir: dir, BindAddr: bind, Advertise: bind, ServerID: bind, Bootstrap: true, TLS: tlsCfg})
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
