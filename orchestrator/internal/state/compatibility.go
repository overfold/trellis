package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/hashicorp/raft"
	"github.com/overfold/trellis/orchestrator/internal/compatibility"
	bolt "go.etcd.io/bbolt"
)

const compatibilityKey = "\x00compatibility-release"

// PrepareReleaseJoin checkpoints and compacts older command history before a
// successor is admitted. With no trailing logs, an empty node receives current
// state rather than commands whose original semantics it no longer implements.
func (r *RaftStore) PrepareReleaseJoin(ctx context.Context, release uint64) error {
	active, err := r.ActiveRelease()
	if err != nil {
		return err
	}
	if err := compatibility.Check(release, active); err != nil {
		return err
	}
	if release == active {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.raft.Snapshot().Error(); err != nil && !errors.Is(err, raft.ErrNothingNewToSnapshot) {
		return fmt.Errorf("checkpoint before successor admission: %w", err)
	}
	return nil
}

func bucketRelease(bucket *bolt.Bucket) (uint64, error) {
	raw := bucket.Get([]byte(compatibilityKey))
	if raw == nil {
		return compatibility.Current(), nil // A new, not yet initialized FSM.
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("invalid persisted compatibility level")
	}
	return binary.BigEndian.Uint64(raw), nil
}

// ActiveRelease reads the replicated execution and storage semantics. It is
// not inferred from whichever binary happens to be the leader.
func (r *RaftStore) ActiveRelease() (uint64, error) {
	var active uint64
	err := r.fsm.store.db.View(func(tx *bolt.Tx) error {
		var err error
		active, err = bucketRelease(tx.Bucket(bucketName))
		return err
	})
	return active, err
}

func (r *RaftStore) encodeCommand(cmd fsmCommand) ([]byte, error) {
	active, err := r.ActiveRelease()
	if err != nil {
		return nil, err
	}
	if err := compatibility.Check(compatibility.Current(), active); err != nil {
		return nil, err
	}
	cmd.Format, cmd.Release = 1, active
	return json.Marshal(cmd)
}

// ActivateRelease advances the compatibility boundary in one fenced Raft entry.
// The server must verify all admitted nodes before calling this method.
func (r *RaftStore) ActivateRelease(ctx context.Context, target uint64) error {
	ctx = r.LeaderContext(ctx)
	r.submitMu.Lock()
	defer r.submitMu.Unlock()
	if err := r.raft.Barrier(10 * time.Second).Error(); err != nil {
		return err
	}
	active, err := r.ActiveRelease()
	if err != nil {
		return err
	}
	if target == active {
		return nil
	}
	if active == 0 || target != active+1 || target != compatibility.Current() {
		return fmt.Errorf("cannot activate release %d from level %d with binary %d", target, active, compatibility.Current())
	}
	data, err := r.encodeCommand(fsmCommand{Op: "activate_release", TargetRelease: target})
	if err != nil {
		return err
	}
	return r.raft.ApplyInTerm(ctx, raftTerm(ctx), data, 10*time.Second).Error()
}

// exchangeRelease runs before any Raft RPC. The fixed-size preface cannot
// consume bytes from the first RPC or buffer unbounded peer-controlled data.
func exchangeRelease(conn net.Conn, active func() (uint64, error), inbound bool) (uint64, uint64, error) {
	level, err := active()
	if err != nil {
		return 0, 0, err
	}
	var local, peer [24]byte
	copy(local[:8], "TRELLIS1")
	binary.BigEndian.PutUint64(local[8:16], compatibility.Current())
	binary.BigEndian.PutUint64(local[16:], level)
	read := func() error {
		_, err := io.ReadFull(conn, peer[:])
		return err
	}
	write := func() error {
		_, err := io.Copy(conn, bytes.NewReader(local[:]))
		return err
	}
	if inbound {
		err = read()
		if err == nil {
			err = write()
		}
	} else {
		err = write()
		if err == nil {
			err = read()
		}
	}
	if err != nil {
		return 0, 0, err
	}
	if string(peer[:8]) != "TRELLIS1" {
		return 0, 0, fmt.Errorf("Raft peer does not speak the compatibility protocol")
	}
	remote, remoteActive := binary.BigEndian.Uint64(peer[8:16]), binary.BigEndian.Uint64(peer[16:])
	if err := compatibility.Check(remote, remoteActive); err != nil {
		return 0, 0, err
	}
	// A lagging, upgraded replica may need a snapshot at the peer's newer
	// active level. Both binaries must implement the higher boundary; using
	// the lagging level alone would prevent that replica from catching up.
	boundary := max(level, remoteActive)
	if err := compatibility.Check(remote, boundary); err != nil {
		return 0, 0, err
	}
	if err := compatibility.Check(compatibility.Current(), boundary); err != nil {
		return 0, 0, err
	}
	return remote, remoteActive, nil
}
