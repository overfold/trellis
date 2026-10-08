package state

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	"github.com/overfold/trellis/orchestrator/internal/tlsutil"
)

// authorizedRaftConn gates complete RPC frames, not individual socket reads:
// NetworkTransport buffers reads and dispatches heartbeats outside Consumer.
// Keep the original wire bytes and stream snapshot payloads without buffering.
type authorizedRaftConn struct {
	net.Conn
	reader            *bufio.Reader
	pending           *bytes.Reader
	snapshot          int64
	certificate       *x509.Certificate
	authorize         func(*x509.Certificate) error
	verifyPeerAddress func(uuid.UUID, raft.ServerAddress) error
}

func (t *tlsStreamLayer) Accept() (net.Conn, error) {
	conn, err := t.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &authorizedRaftConn{Conn: conn, reader: bufio.NewReader(conn), authorize: t.authorize, verifyPeerAddress: t.verifyPeerAddress}, nil
}

func (c *authorizedRaftConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if c.pending != nil && c.pending.Len() > 0 {
		return c.pending.Read(p)
	}
	if c.snapshot > 0 {
		n, err := c.reader.Read(p[:min(int64(len(p)), c.snapshot)])
		c.snapshot -= int64(n)
		return n, err
	}
	if c.certificate == nil {
		conn, ok := c.Conn.(*tls.Conn)
		if !ok {
			return 0, fmt.Errorf("Raft stream requires TLS")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := conn.HandshakeContext(ctx)
		cancel()
		if err != nil {
			return 0, err
		}
		c.certificate = conn.ConnectionState().PeerCertificates[0]
	}
	kind, err := c.reader.ReadByte()
	if err != nil {
		return 0, err
	}
	// Hashicorp Raft's five RPC types use a MessagePack request following
	// the type byte (0 append, 1 vote, 2 snapshot, 3 timeout, 4 pre-vote).
	if kind > 4 {
		return 0, fmt.Errorf("unknown Raft RPC type %d", kind)
	}
	var raw codec.Raw
	if err := codec.NewDecoder(c.reader, &codec.MsgpackHandle{}).Decode(&raw); err != nil {
		return 0, err
	}
	var request struct {
		raft.RPCHeader
		Size      int64
		Leader    []byte
		Candidate []byte
	}
	if err := codec.NewDecoderBytes(raw, &codec.MsgpackHandle{}).Decode(&request); err != nil {
		return 0, err
	}
	id, err := tlsutil.NodeID(c.certificate)
	if err != nil {
		return 0, err
	}
	if string(request.ID) != id.String() {
		return 0, fmt.Errorf("Raft RPC identity does not match TLS peer")
	}
	if len(request.Addr) == 0 || (len(request.Leader) > 0 && !bytes.Equal(request.Leader, request.Addr)) || (len(request.Candidate) > 0 && !bytes.Equal(request.Candidate, request.Addr)) {
		return 0, fmt.Errorf("Raft RPC has conflicting peer addresses")
	}
	if err := c.authorize(c.certificate); err != nil {
		return 0, fmt.Errorf("Raft RPC peer rejected: %w", err)
	}
	if c.verifyPeerAddress != nil {
		if err := c.verifyPeerAddress(id, raft.ServerAddress(request.Addr)); err != nil {
			return 0, err
		}
	}
	if kind == 2 {
		if request.Size < 0 {
			return 0, fmt.Errorf("negative Raft snapshot size")
		}
		c.snapshot = request.Size
	}
	c.pending = bytes.NewReader(append([]byte{kind}, raw...))
	return c.pending.Read(p)
}
