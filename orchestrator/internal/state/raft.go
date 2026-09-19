package state

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

var _ Store = (*RaftStore)(nil)

// RaftStore replicates state through a Raft cluster.
type RaftStore struct {
	raft             *raft.Raft
	fsm              *fsm
	transport        raft.Transport
	logStore         *raftboltdb.BoltStore
	hadExistingState bool
}

// DesiredSnapshot is the portable portion of control-plane state. Keys are
// relative to their state prefixes so a backup can be restored into a freshly
// bootstrapped cluster with a different name. Volume registrations preserve
// locality metadata only; volume bytes remain external to the backup.
type DesiredSnapshot struct {
	Jobs                     map[string][]byte `json:"jobs"`
	Secrets                  map[string][]byte `json:"secrets"`
	VolumeRegistrations      map[string][]byte `json:"volume_registrations"`
	NetworkPortRegistrations map[string][]byte `json:"network_port_registrations"`
}

// BackupDesired takes a linearizable view of desired state. The barrier makes
// sure the local FSM contains every write committed before the request.
func (r *RaftStore) BackupDesired(cluster string) (*DesiredSnapshot, error) {
	if err := r.raft.Barrier(10 * time.Second).Error(); err != nil {
		return nil, fmt.Errorf("raft backup barrier: %w", err)
	}
	return r.fsm.desiredSnapshot(cluster)
}

// RestoreDesired installs a backup as one Raft log entry. The FSM rejects the
// operation unless both desired-state prefixes are empty.
func (r *RaftStore) RestoreDesired(cluster string, snapshot *DesiredSnapshot) error {
	cmd := fsmCommand{Op: "restore_desired", Cluster: cluster, Snapshot: snapshot}
	data, err := json.Marshal(cmd)
	if err != nil {
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

// RaftConfig configures replicated state storage.
type RaftConfig struct {
	DataDir   string
	BindAddr  string
	Advertise string
	ServerID  string
	Bootstrap bool
	TLS       *tls.Config
}

type tlsStreamLayer struct {
	net.Listener
	advertise net.Addr
	tlsCfg    *tls.Config
}

func (t *tlsStreamLayer) Addr() net.Addr {
	if t.advertise != nil {
		return t.advertise
	}
	return t.Listener.Addr()
}

func (t *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", string(address), timeout)
	if err != nil {
		return nil, err
	}
	tlsConn := tls.Client(conn, t.tlsCfg)
	if err := tlsConn.SetDeadline(time.Now().Add(timeout)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := tlsConn.Handshake(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := tlsConn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return tlsConn, nil
}

// NewRaftStore creates a Raft-backed state store.
func NewRaftStore(cfg RaftConfig) (*RaftStore, error) {
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

	snapshotStore, err := raft.NewFileSnapshotStore(raftDir, 2, io.Discard)
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

	var transport raft.Transport
	if cfg.TLS != nil {
		ln, err := tls.Listen("tcp", cfg.BindAddr, cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("create TLS listener: %w", err)
		}
		stream := &tlsStreamLayer{Listener: ln, advertise: advAddr, tlsCfg: cfg.TLS}
		transport = raft.NewNetworkTransport(stream, 3, 10*time.Second, io.Discard)
	} else {
		t, err := raft.NewTCPTransport(cfg.BindAddr, advAddr, 3, 10*time.Second, io.Discard)
		if err != nil {
			return nil, fmt.Errorf("create raft transport: %w", err)
		}
		transport = t
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.ServerID)
	raftCfg.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   "raft",
		Level:  hclog.Warn,
		Output: os.Stderr,
	})

	r, err := raft.NewRaft(raftCfg, f, logStore, logStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("create raft: %w", err)
	}

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
func (r *RaftStore) Put(_ context.Context, key string, value []byte) error {
	cmd := fsmCommand{Op: "put", Key: key, Value: value}
	data, err := json.Marshal(cmd)
	if err != nil {
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
func (r *RaftStore) Delete(_ context.Context, key string) error {
	cmd := fsmCommand{Op: "delete", Key: key}
	data, err := json.Marshal(cmd)
	if err != nil {
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

// AddVoter adds a voting server to Raft.
func (r *RaftStore) AddVoter(id, address string) error {
	fut := r.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(address), 0, 30*time.Second)
	return fut.Error()
}

// RemoveServer removes a server from Raft.
func (r *RaftStore) RemoveServer(id string) error {
	fut := r.raft.RemoveServer(raft.ServerID(id), 0, 10*time.Second)
	return fut.Error()
}

// LeadershipTransfer asks Raft to hand leadership to another voter.
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
	Op       string           `json:"op"`
	Key      string           `json:"key,omitempty"`
	Value    []byte           `json:"value,omitempty"`
	Cluster  string           `json:"cluster,omitempty"`
	Snapshot *DesiredSnapshot `json:"snapshot,omitempty"`
}

func (f *fsm) Apply(log *raft.Log) interface{} {
	var cmd fsmCommand
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return err
	}
	ctx := context.Background()
	switch cmd.Op {
	case "put":
		return f.store.Put(ctx, cmd.Key, cmd.Value)
	case "delete":
		return f.store.Delete(ctx, cmd.Key)
	case "restore_desired":
		if cmd.Snapshot == nil {
			return fmt.Errorf("restore snapshot is missing")
		}
		return f.store.RestoreDesired(cmd.Cluster, cmd.Snapshot)
	default:
		return fmt.Errorf("unknown FSM command: %s", cmd.Op)
	}
}

func (f *fsm) desiredSnapshot(cluster string) (*DesiredSnapshot, error) {
	jobsPrefix := fmt.Sprintf("trellis/%s/jobs/", cluster)
	secretsPrefix := fmt.Sprintf("trellis/%s/secrets/", cluster)
	volumesPrefix := fmt.Sprintf("trellis/%s/volume-registrations/", cluster)
	networkPortsPrefix := fmt.Sprintf("trellis/%s/network-port-registrations/", cluster)
	jobs, err := f.store.List(context.Background(), jobsPrefix)
	if err != nil {
		return nil, err
	}
	secrets, err := f.store.List(context.Background(), secretsPrefix)
	if err != nil {
		return nil, err
	}
	volumes, err := f.store.List(context.Background(), volumesPrefix)
	if err != nil {
		return nil, err
	}
	networkPorts, err := f.store.List(context.Background(), networkPortsPrefix)
	if err != nil {
		return nil, err
	}
	result := &DesiredSnapshot{
		Jobs:                     make(map[string][]byte, len(jobs)),
		Secrets:                  make(map[string][]byte, len(secrets)),
		VolumeRegistrations:      make(map[string][]byte, len(volumes)),
		NetworkPortRegistrations: make(map[string][]byte, len(networkPorts)),
	}
	for key, value := range jobs {
		result.Jobs[key[len(jobsPrefix):]] = value
	}
	for key, value := range secrets {
		result.Secrets[key[len(secretsPrefix):]] = value
	}
	for key, value := range volumes {
		result.VolumeRegistrations[key[len(volumesPrefix):]] = value
	}
	for key, value := range networkPorts {
		result.NetworkPortRegistrations[key[len(networkPortsPrefix):]] = value
	}
	return result, nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	data, err := f.store.List(context.Background(), "")
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: data}, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()
	var data map[string][]byte
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return err
	}
	return f.store.Restore(data)
}

type fsmSnapshot struct {
	data map[string][]byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.data)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	if _, err := sink.Write(data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
