package state

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/overfold/trellis/orchestrator/internal/tlsutil"
	bolt "go.etcd.io/bbolt"
)

var _ Store = (*RaftStore)(nil)
var _ AtomicStore = (*RaftStore)(nil)
var _ PrefixIterator = (*RaftStore)(nil)

// RaftStore replicates state through a Raft cluster.
type RaftStore struct {
	// submitMu keeps restore preflight and commit ordered with all local writes.
	submitMu         sync.RWMutex
	raft             *raft.Raft
	fsm              *fsm
	transport        raft.Transport
	logStore         *raftboltdb.BoltStore
	hadExistingState bool
}

// DesiredSnapshot is the portable portion of control-plane state. Keys are
// relative to their state prefixes so a backup can be restored into a freshly
// bootstrapped cluster with a different name. Volume registrations preserve
// locality metadata only; volume bytes remain external to the backup. Namespace
// network port and subnet registrations preserve stable WireGuard pathway
// assignments. Cluster is the replicated cluster record, which carries the
// cluster settings. A backup reads it from the same view as the desired
// state; a restore installs it in the same transaction, so settings and the
// jobs they admit are never restored separately.
type DesiredSnapshot struct {
	Cluster                    []byte            `json:"cluster,omitempty"`
	Jobs                       map[string][]byte `json:"jobs"`
	JobRevisions               map[string][]byte `json:"job_revisions,omitempty"`
	Secrets                    map[string][]byte `json:"secrets"`
	VolumeRegistrations        map[string][]byte `json:"volume_registrations"`
	NetworkPortRegistrations   map[string][]byte `json:"network_port_registrations"`
	NetworkSubnetRegistrations map[string][]byte `json:"network_subnet_registrations,omitempty"`
}

// BackupDesired takes a linearizable view of desired state. The barrier makes
// sure the local FSM contains every write committed before the request.
func (r *RaftStore) BackupDesired(cluster string) (*DesiredSnapshot, error) {
	if err := r.raft.Barrier(10 * time.Second).Error(); err != nil {
		return nil, fmt.Errorf("raft backup barrier: %w", err)
	}
	return r.fsm.store.DesiredSnapshot(cluster)
}

// RestoreDesired installs a backup as one Raft log entry. Ordinary validation
// and freshness rejections happen before replication; a committed installation
// failing on any replica is a fatal state-machine failure.
func (r *RaftStore) RestoreDesired(cluster string, snapshot *DesiredSnapshot) error {
	if err := ValidateDesiredSnapshot(snapshot, nil); err != nil {
		return err
	}
	if err := ValidateDesiredSnapshotKeys(cluster, snapshot); err != nil {
		return err
	}
	r.submitMu.Lock()
	defer r.submitMu.Unlock()
	if err := r.raft.Barrier(10 * time.Second).Error(); err != nil {
		return err
	}
	if err := r.fsm.store.checkRestoreFresh(cluster); err != nil {
		return err
	}
	cmd := fsmCommand{Op: "restore_desired", Cluster: cluster, Snapshot: snapshot}
	data, _ := json.Marshal(cmd)
	fut := r.raft.Apply(data, 10*time.Second)
	if err := fut.Error(); err != nil {
		return err
	}
	if resp, ok := fut.Response().(error); ok && resp != nil {
		return resp
	}
	return nil
}

// Batch applies mutations as one Raft log entry and one Bolt transaction.
func (r *RaftStore) Batch(ctx context.Context, mutations []Mutation) error {
	if err := validateMutations(mutations); err != nil {
		return err
	}
	r.submitMu.RLock()
	defer r.submitMu.RUnlock()
	cmd := fsmCommand{Op: "batch", Mutations: mutations}
	data, _ := json.Marshal(cmd)
	if err := ctx.Err(); err != nil {
		return err
	}
	fut := r.raft.Apply(data, 10*time.Second)
	if err := fut.Error(); err != nil {
		return err
	}
	if resp, ok := fut.Response().(error); ok {
		return resp
	}
	return nil
}

// IteratePrefix streams matching entries from the local replicated FSM, like List.
func (r *RaftStore) IteratePrefix(ctx context.Context, prefix string, visit func(key string, value []byte) error) error {
	return r.fsm.store.IteratePrefix(ctx, prefix, visit)
}

// RaftConfig configures replicated state storage.
type RaftConfig struct {
	DataDir   string
	BindAddr  string
	Advertise string
	ServerID  string
	Bootstrap bool
	TLS       *tls.Config
	// AuthorizePeer decides whether a peer whose certificate chains to the
	// cluster CA may open an inbound Raft stream. The CA proves only that a
	// certificate was issued for some node; this check decides whether that
	// node is currently entitled to replicate or vote with this member. It is
	// required with TLS and runs during every inbound TLS handshake and before
	// each RPC frame is released for dispatch on an established stream.
	AuthorizePeer func(certificate *x509.Certificate) error
	// Logger receives Raft, transport, and snapshot-store diagnostics at Warn
	// and above, rate limited. Nil uses slog.Default.
	Logger *slog.Logger
}

type tlsStreamLayer struct {
	net.Listener
	advertise         net.Addr
	tlsCfg            *tls.Config
	targetID          func(raft.ServerAddress) (uuid.UUID, error)
	authorize         func(*x509.Certificate) error
	verifyPeerAddress func(uuid.UUID, raft.ServerAddress) error
}

func (t *tlsStreamLayer) Addr() net.Addr {
	if t.advertise != nil {
		return t.advertise
	}
	return t.Listener.Addr()
}

func (t *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	targetID, err := t.targetID(address)
	if err != nil {
		return nil, fmt.Errorf("resolve Raft peer %q: %w", address, err)
	}
	// Raft's StreamLayer contract supplies a timeout, but no caller context.
	conn, err := (&net.Dialer{Timeout: timeout}).DialContext(context.Background(), "tcp", string(address))
	if err != nil {
		return nil, err
	}
	peerTLS := t.tlsCfg.Clone()
	// Membership binds each advertised address to an immutable node UUID. Use
	// that UUID's DNS identity so the normal TLS verifier checks both the chain
	// and the exact node SAN; PeerTLSConfig's verifier additionally checks the
	// certificate's URI identity.
	peerTLS.InsecureSkipVerify = false
	peerTLS.ServerName = tlsutil.NodeServerName(targetID)
	previousVerify := peerTLS.VerifyConnection
	peerTLS.VerifyConnection = func(state tls.ConnectionState) error {
		if previousVerify != nil {
			if err := previousVerify(state); err != nil {
				return err
			}
		}
		if len(state.PeerCertificates) == 0 {
			return fmt.Errorf("Raft peer certificate is missing")
		}
		actualID, err := tlsutil.NodeID(state.PeerCertificates[0])
		if err != nil {
			return fmt.Errorf("verify Raft peer identity: %w", err)
		}
		if actualID != targetID {
			return fmt.Errorf("Raft peer certificate identifies node %s, expected %s", actualID, targetID)
		}
		return nil
	}
	tlsConn := tls.Client(conn, peerTLS)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := tlsConn.SetDeadline(time.Now().Add(timeout)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := tlsConn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return tlsConn, nil
}

// inboundPeerTLS returns the listener configuration for inbound Raft streams:
// the peer configuration with authorize applied to the verified client
// certificate of every handshake. A rejected handshake closes the stream
// before any Raft RPC is read from it.
func inboundPeerTLS(peer *tls.Config, authorize func(*x509.Certificate) error) (*tls.Config, error) {
	if authorize == nil {
		return nil, fmt.Errorf("raft TLS requires a peer authorizer")
	}
	if peer.ClientAuth != tls.RequireAndVerifyClientCert {
		return nil, fmt.Errorf("raft TLS must require and verify client certificates")
	}
	listenTLS := peer.Clone()
	listenTLS.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
			return fmt.Errorf("raft peer presented no verified certificate")
		}
		if err := authorize(state.VerifiedChains[0][0]); err != nil {
			return fmt.Errorf("raft peer rejected: %w", err)
		}
		return nil
	}
	return listenTLS, nil
}

// NewRaftStore creates a Raft-backed state store.
func NewRaftStore(cfg RaftConfig) (*RaftStore, error) {
	if cfg.TLS != nil {
		if _, err := uuid.Parse(cfg.ServerID); err != nil {
			return nil, fmt.Errorf("Raft TLS server ID must be a UUID: %w", err)
		}
	}
	raftDir := filepath.Join(cfg.DataDir, "raft")
	if err := os.MkdirAll(raftDir, 0o750); err != nil {
		return nil, fmt.Errorf("create raft dir: %w", err)
	}

	fsmStore, err := NewBoltStore(filepath.Join(raftDir, "fsm.db"))
	if err != nil {
		return nil, fmt.Errorf("create FSM store: %w", err)
	}
	f := &fsm{store: fsmStore}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "log.db"))
	if err != nil {
		return nil, fmt.Errorf("create raft log store: %w", err)
	}

	logger := newRaftLogger(cfg.Logger, "raft")

	snapshotStore, err := raft.NewFileSnapshotStoreWithLogger(raftDir, 2, logger.Named("snapshot"))
	if err != nil {
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	hadState, err := raft.HasExistingState(logStore, logStore, snapshotStore)
	if err != nil {
		return nil, fmt.Errorf("check existing raft state: %w", err)
	}

	advertise := cfg.Advertise
	if advertise == "" {
		advertise = cfg.BindAddr
	}
	advAddr, err := net.ResolveTCPAddr("tcp", advertise)
	if err != nil {
		return nil, fmt.Errorf("resolve raft advertise address: %w", err)
	}

	var raftRef atomic.Pointer[raft.Raft]
	resolveTargetID := func(address raft.ServerAddress) (uuid.UUID, error) {
		r := raftRef.Load()
		if r == nil {
			return uuid.Nil, fmt.Errorf("Raft is not initialized")
		}
		future := r.GetConfiguration()
		if err := future.Error(); err != nil {
			return uuid.Nil, fmt.Errorf("read Raft membership: %w", err)
		}
		for _, server := range future.Configuration().Servers {
			if server.Address != address {
				continue
			}
			id, err := uuid.Parse(string(server.ID))
			if err != nil {
				return uuid.Nil, fmt.Errorf("member %q has non-UUID server ID: %w", server.ID, err)
			}
			return id, nil
		}
		return uuid.Nil, fmt.Errorf("address is not in Raft membership")
	}

	var transport raft.Transport
	if cfg.TLS != nil {
		listenTLS, err := inboundPeerTLS(cfg.TLS, cfg.AuthorizePeer)
		if err != nil {
			return nil, err
		}
		ln, err := tls.Listen("tcp", cfg.BindAddr, listenTLS)
		if err != nil {
			return nil, fmt.Errorf("create TLS listener: %w", err)
		}
		verifyPeerAddress := func(id uuid.UUID, address raft.ServerAddress) error {
			r := raftRef.Load()
			if r == nil {
				return fmt.Errorf("Raft is not initialized")
			}
			future := r.GetConfiguration()
			if err := future.Error(); err != nil {
				return err
			}
			members := future.Configuration().Servers
			// A joining replica has no configuration until its bootstrap-trusted
			// leader sends the first log/snapshot. Certificate authorization still
			// runs; once configuration arrives it also owns the address binding.
			if len(members) == 0 {
				return nil
			}
			for _, member := range members {
				if string(member.ID) == id.String() && member.Address == address {
					return nil
				}
			}
			return fmt.Errorf("Raft RPC address is not bound to peer %s", id)
		}
		stream := &tlsStreamLayer{Listener: ln, advertise: advAddr, tlsCfg: cfg.TLS, targetID: resolveTargetID, authorize: cfg.AuthorizePeer, verifyPeerAddress: verifyPeerAddress}
		transport = raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
			Stream:  stream,
			MaxPool: 3,
			Timeout: 10 * time.Second,
			Logger:  logger.Named("transport"),
		})
	} else {
		t, err := raft.NewTCPTransportWithConfig(cfg.BindAddr, advAddr, &raft.NetworkTransportConfig{
			MaxPool: 3,
			Timeout: 10 * time.Second,
			Logger:  logger.Named("transport"),
		})
		if err != nil {
			return nil, fmt.Errorf("create raft transport: %w", err)
		}
		transport = t
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.ServerID)
	raftCfg.Logger = logger

	r, err := raft.NewRaft(raftCfg, f, logStore, logStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("create raft: %w", err)
	}
	raftRef.Store(r)

	if cfg.Bootstrap && !hadState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      raft.ServerID(cfg.ServerID),
				Address: transport.LocalAddr(),
			}},
		}
		if fut := r.BootstrapCluster(config); fut.Error() != nil {
			return nil, fmt.Errorf("bootstrap raft cluster: %w", fut.Error())
		}
	}

	return &RaftStore{
		raft:             r,
		fsm:              f,
		transport:        transport,
		logStore:         logStore,
		hadExistingState: hadState,
	}, nil
}

// Raft returns the underlying Raft instance.
func (r *RaftStore) Raft() *raft.Raft { return r.raft }

// LocalAddr returns the local Raft transport address.
func (r *RaftStore) LocalAddr() string { return string(r.transport.LocalAddr()) }

// HadExistingState reports whether persistent Raft state existed at startup.
func (r *RaftStore) HadExistingState() bool { return r.hadExistingState }

// Get returns a replicated value by key.
func (r *RaftStore) Get(ctx context.Context, key string) ([]byte, error) {
	return r.fsm.store.Get(ctx, key)
}

// List returns replicated values whose keys start with prefix.
func (r *RaftStore) List(ctx context.Context, prefix string) (map[string][]byte, error) {
	return r.fsm.store.List(ctx, prefix)
}

// Put applies a replicated value update.
func (r *RaftStore) Put(ctx context.Context, key string, value []byte) error {
	if err := validateMutations([]Mutation{{Key: key, Value: value}}); err != nil {
		return err
	}
	r.submitMu.RLock()
	defer r.submitMu.RUnlock()
	cmd := fsmCommand{Op: "put", Key: key, Value: value}
	data, _ := json.Marshal(cmd)
	if err := ctx.Err(); err != nil {
		return err
	}
	fut := r.raft.Apply(data, 10*time.Second)
	if err := fut.Error(); err != nil {
		return err
	}
	if resp, ok := fut.Response().(error); ok && resp != nil {
		return resp
	}
	return nil
}

// Delete applies a replicated key deletion.
func (r *RaftStore) Delete(ctx context.Context, key string) error {
	r.submitMu.RLock()
	defer r.submitMu.RUnlock()
	cmd := fsmCommand{Op: "delete", Key: key}
	data, _ := json.Marshal(cmd)
	if err := ctx.Err(); err != nil {
		return err
	}
	fut := r.raft.Apply(data, 10*time.Second)
	if err := fut.Error(); err != nil {
		return err
	}
	if resp, ok := fut.Response().(error); ok && resp != nil {
		return resp
	}
	return nil
}

// RaftMember is one server in the latest, possibly uncommitted, Raft
// configuration.
type RaftMember struct {
	ID      string
	Address string
	Voter   bool
}

// Membership returns the latest Raft configuration known to this server.
// hashicorp/raft does not expose that configuration's log index, so callers
// that plan changes from it must serialize their own reads and changes.
func (r *RaftStore) Membership() ([]RaftMember, error) {
	fut := r.raft.GetConfiguration()
	if err := fut.Error(); err != nil {
		return nil, err
	}
	servers := fut.Configuration().Servers
	members := make([]RaftMember, 0, len(servers))
	for _, server := range servers {
		members = append(members, RaftMember{ID: string(server.ID), Address: string(server.Address), Voter: server.Suffrage == raft.Voter})
	}
	return members, nil
}

// AddNonvoter adds a server that replicates the log without voting. An
// existing voter with the same ID keeps its vote and only has its address
// updated, so a rejoin never demotes a member.
func (r *RaftStore) AddNonvoter(id, address string) error {
	return r.raft.AddNonvoter(raft.ServerID(id), raft.ServerAddress(address), 0, 10*time.Second).Error()
}

// PromoteVoter gives a member a vote.
func (r *RaftStore) PromoteVoter(id, address string) error {
	return r.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(address), 0, 10*time.Second).Error()
}

// DemoteVoter removes a member's vote, keeping it as a non-voter.
func (r *RaftStore) DemoteVoter(id string) error {
	return r.raft.DemoteVoter(raft.ServerID(id), 0, 10*time.Second).Error()
}

// AppliedIndex returns the last log index applied to the local FSM.
func (r *RaftStore) AppliedIndex() uint64 { return r.raft.AppliedIndex() }

// RemoveServer removes a member from Raft.
func (r *RaftStore) RemoveServer(id string) error {
	fut := r.raft.RemoveServer(raft.ServerID(id), 0, 10*time.Second)
	return fut.Error()
}

// LeadershipTransfer asks Raft to hand leadership to the most up-to-date
// voter. Non-voters are never chosen; with no other voter it fails.
func (r *RaftStore) LeadershipTransfer() error {
	return r.raft.LeadershipTransfer().Error()
}

// Close shuts down Raft and closes its stores.
func (r *RaftStore) Close() error {
	if fut := r.raft.Shutdown(); fut.Error() != nil {
		return fut.Error()
	}
	if err := r.logStore.Close(); err != nil {
		return err
	}
	return r.fsm.store.Close()
}

type fsm struct {
	store *BoltStore
}

type fsmCommand struct {
	Op        string           `json:"op"`
	Key       string           `json:"key,omitempty"`
	Value     []byte           `json:"value,omitempty"`
	Cluster   string           `json:"cluster,omitempty"`
	Snapshot  *DesiredSnapshot `json:"snapshot,omitempty"`
	Mutations []Mutation       `json:"mutations,omitempty"`
}

func (f *fsm) Apply(log *raft.Log) any {
	if err := f.apply(log); err != nil {
		// Raft discards follower responses and advances its applied index even
		// on error. Never let a replica serve, vote, or snapshot after skipping
		// a committed mutation. A panic on the Raft FSM goroutine terminates
		// the process; include identity, not command contents (possibly secrets).
		panic(fmt.Sprintf("fatal Raft FSM apply at index %d term %d: %v", log.Index, log.Term, err))
	}
	return nil
}

func (f *fsm) apply(log *raft.Log) error {
	var cmd fsmCommand
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return err
	}
	return f.store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		// The checkpoint travels with FSM snapshots and is committed atomically
		// with each command. Raft replays logs after restart; applying them twice
		// can resurrect deleted state or reject a previously committed restore.
		checkpoint := []byte("\x00raft-applied-index")
		if saved := bucket.Get(checkpoint); len(saved) == 8 && log.Index <= binary.BigEndian.Uint64(saved) {
			return nil
		}
		var err error
		switch cmd.Op {
		case "put":
			err = bucket.Put([]byte(cmd.Key), cmd.Value)
		case "delete":
			err = bucket.Delete([]byte(cmd.Key))
		case "batch":
			err = validateMutations(cmd.Mutations)
			if err == nil {
				err = applyMutations(tx, cmd.Mutations)
			}
		case "restore_desired":
			err = ValidateDesiredSnapshot(cmd.Snapshot, nil)
			if err == nil {
				err = restoreDesired(tx, cmd.Cluster, cmd.Snapshot)
			}
		default:
			return fmt.Errorf("unknown FSM command: %s", cmd.Op)
		}
		if err != nil {
			return err
		}
		return bucket.Put(checkpoint, binary.BigEndian.AppendUint64(nil, log.Index))
	})
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	snapshot, err := f.store.snapshot()
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{snapshot: snapshot}, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()
	return f.store.RestoreReader(rc)
}

type fsmSnapshot struct {
	snapshot *boltSnapshot
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	defer s.Release()
	if err := s.snapshot.persistTo(sink); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {
	if s.snapshot != nil {
		_ = s.snapshot.Close()
		s.snapshot = nil
	}
}
