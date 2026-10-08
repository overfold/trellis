package state

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
)

func TestRaftEstablishedTLSStreamReauthorizesBeforeDispatch(t *testing.T) {
	for _, attack := range []string{"removed", "spoofed identity", "spoofed address"} {
		t.Run(attack, func(t *testing.T) {
			peerID, serverID := uuid.New(), uuid.New()
			var removed atomic.Bool
			authorize := func(*x509.Certificate) error {
				if removed.Load() {
					return errors.New("tombstoned")
				}
				return nil
			}
			cfg, err := inboundPeerTLS(testTLSConfig(t, serverID), authorize)
			if err != nil {
				t.Fatal(err)
			}
			listener, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
			if err != nil {
				t.Fatal(err)
			}
			transport := raft.NewNetworkTransport(&tlsStreamLayer{Listener: listener, authorize: authorize, verifyPeerAddress: func(id uuid.UUID, address raft.ServerAddress) error {
				if id != peerID || address != "peer" {
					return errors.New("address binding mismatch")
				}
				return nil
			}}, 1, time.Second, io.Discard)
			t.Cleanup(func() { _ = transport.Close() })
			var dispatched atomic.Int32
			transport.SetHeartbeatHandler(func(rpc raft.RPC) {
				dispatched.Add(1)
				rpc.Respond(&raft.AppendEntriesResponse{Term: 1, Success: true}, nil)
			})
			conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", listener.Addr().String(), testTLSConfig(t, peerID))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
			encoder := codec.NewEncoder(conn, &codec.MsgpackHandle{})
			decoder := codec.NewDecoder(bufio.NewReader(conn), &codec.MsgpackHandle{})
			send := func(id uuid.UUID, address string) {
				t.Helper()
				if _, err := conn.Write([]byte{0}); err != nil {
					t.Fatal(err)
				}
				if err := encoder.Encode(&raft.AppendEntriesRequest{RPCHeader: raft.RPCHeader{ID: []byte(id.String()), Addr: []byte(address)}, Term: 999}); err != nil {
					t.Fatal(err)
				}
			}
			send(peerID, "peer")
			var responseError string
			var response raft.AppendEntriesResponse
			if err := decoder.Decode(&responseError); err != nil {
				t.Fatal(err)
			}
			if err := decoder.Decode(&response); err != nil {
				t.Fatal(err)
			}
			if responseError != "" || !response.Success || dispatched.Load() != 1 {
				t.Fatal("authorized heartbeat did not dispatch")
			}
			claimedID := peerID
			address := "peer"
			switch attack {
			case "spoofed identity":
				claimedID = uuid.New()
			case "spoofed address":
				address = "other-member"
			case "removed":
				removed.Store(true)
			}
			send(claimedID, address)
			if err := decoder.Decode(&responseError); err == nil {
				t.Fatal("rejected RPC received a response")
			}
			if dispatched.Load() != 1 {
				t.Fatal("rejected high-term heartbeat reached fast-path dispatch")
			}
		})
	}
}

func TestRaftFrameGatePreservesBufferedRPCsAndSnapshotPayload(t *testing.T) {
	id := uuid.New()
	certificate, err := x509.ParseCertificate(testTLSConfig(t, id).Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	var wire bytes.Buffer
	for kind := byte(0); kind <= 4; kind++ {
		wire.WriteByte(kind)
		request := struct {
			raft.RPCHeader
			Size int64
		}{raft.RPCHeader{ID: []byte(id.String()), Addr: []byte("peer")}, 7}
		if err := codec.NewEncoder(&wire, &codec.MsgpackHandle{}).Encode(request); err != nil {
			t.Fatal(err)
		}
		if kind == 2 {
			wire.WriteString("payload")
		}
	}
	input := bytes.Clone(wire.Bytes())
	calls := 0
	conn := &authorizedRaftConn{reader: bufio.NewReader(&wire), certificate: certificate, authorize: func(*x509.Certificate) error { calls++; return nil }}
	got, err := io.ReadAll(conn)
	if err != nil || !bytes.Equal(got, input) || calls != 5 {
		t.Fatalf("wire preserved=%t checks=%d error=%v", bytes.Equal(got, input), calls, err)
	}
	// Both frames are already available to the inner buffered reader. A
	// socket-read authorizer would erroneously admit the second one too.
	reader := bytes.NewReader(input)
	calls = 0
	conn = &authorizedRaftConn{reader: bufio.NewReader(reader), certificate: certificate, authorize: func(*x509.Certificate) error {
		calls++
		if calls == 2 {
			return errors.New("removed")
		}
		return nil
	}}
	if _, err := conn.Read(make([]byte, len(input))); err != nil {
		t.Fatal(err)
	}
	if n, err := conn.Read(make([]byte, len(input))); n != 0 || err == nil {
		t.Fatal("buffered second RPC bypassed reauthorization")
	}
}
