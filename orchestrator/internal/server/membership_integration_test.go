//go:build integration

package server

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/overfold/trellis/orchestrator/internal/state"
	"github.com/overfold/trellis/orchestrator/internal/tlsutil"
)

// The hook lives only in the test's Joiner: production removal, persistence,
// Hashicorp Raft, TCP, and certificate-bound TLS authorization are real.
type promotionFaultJoiner struct {
	*state.RaftStore
	beforePromotion func()
	afterPromotion  func()
}

func (j *promotionFaultJoiner) PromoteVoter(id, address string) error {
	j.beforePromotion()
	if err := j.RaftStore.PromoteVoter(id, address); err != nil {
		return err
	}
	if j.afterPromotion != nil {
		j.afterPromotion()
	}
	return nil
}

func TestRemovalPromotionLossAndRecovery(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		lost      int
		committed bool
	}{
		{name: "old-voter", lost: 1},
		{name: "promoted-node", lost: 3},
		{name: "old-voter-after-promotion-commit", lost: 1, committed: true},
	} {
		name, lost := scenario.name, scenario.lost
		t.Run(name, func(t *testing.T) {
			ca, key, err := tlsutil.GenerateCA()
			if err != nil {
				t.Fatal(err)
			}
			var stores [4]*state.RaftStore
			var configs [4]state.RaftConfig
			var controllers [4]*StateController
			var authorizers [4]*RaftPeerAuthorizer
			var certificates [4]*x509.Certificate
			var ids [4]uuid.UUID
			for i := range ids {
				ids[i] = uuid.New()
			}
			wait := func(what string, ready func() bool) {
				t.Helper()
				deadline := time.Now().Add(20 * time.Second)
				for time.Now().Before(deadline) {
					if ready() {
						return
					}
					time.Sleep(20 * time.Millisecond)
				}
				t.Fatalf("timed out: %s", what)
			}
			start := func(i int) {
				t.Helper()
				stores[i], err = state.NewRaftStore(configs[i])
				if err != nil {
					t.Fatal(err)
				}
				controllers[i] = NewStateController(stores[i], "test")
				authorizers[i].Bind(controllers[i], stores[i].Membership)
			}
			stop := func(i int) {
				t.Helper()
				if stores[i] != nil {
					if err := stores[i].Close(); err != nil {
						t.Fatal(err)
					}
					stores[i] = nil
				}
			}
			t.Cleanup(func() {
				for i := range stores {
					stop(i)
				}
			})
			for i, id := range ids {
				cert, privateKey, err := tlsutil.GenerateNodeCert(ca, key, id)
				if err != nil {
					t.Fatal(err)
				}
				pair, err := tls.X509KeyPair(cert, privateKey)
				if err != nil {
					t.Fatal(err)
				}
				certificates[i], err = x509.ParseCertificate(pair.Certificate[0])
				if err != nil {
					t.Fatal(err)
				}
				peerTLS, err := tlsutil.PeerTLSConfig(&tlsutil.Materials{CACert: ca, Cert: cert, Key: privateKey})
				if err != nil {
					t.Fatal(err)
				}
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				address := listener.Addr().String()
				_ = listener.Close()
				authorizers[i] = NewRaftPeerAuthorizer()
				authorizers[i].TrustJoinMembers(sortedIDs(ids[:]...))
				configs[i] = state.RaftConfig{DataDir: t.TempDir(), BindAddr: address, Advertise: address, ServerID: id.String(), Bootstrap: i == 0, TLS: peerTLS, AuthorizePeer: authorizers[i].Authorize}
				start(i)
			}
			wait("initial leader", func() bool { return stores[0].Raft().State() == raft.Leader })
			server := func(i int) *Server {
				// Fresh term-local heartbeats/progress, with c still unavailable.
				s := &Server{joiner: stores[i], nodeID: ids[i], now: time.Now, leaderSince: time.Now().Add(-time.Hour), nodes: map[uuid.UUID]*Node{}, state: controllers[i]}
				for n, id := range ids {
					if stores[n] != nil {
						addTestNode(s, &Node{ID: id, Status: NodeStatusHealthy}, time.Now())
						s.RecordRaftProgress(t.Context(), id, stores[i].AppliedIndex())
					}
				}
				return s
			}
			s := server(0)
			for i, id := range ids {
				if err := s.BindNodeCertificate(t.Context(), id, certificates[i]); err != nil {
					t.Fatal(err)
				}
				if i > 0 {
					if err := stores[0].AddNonvoter(id.String(), configs[i].Advertise); err != nil {
						t.Fatal(err)
					}
					if i < 3 {
						if err := stores[0].PromoteVoter(id.String(), configs[i].Advertise); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			index := stores[0].AppliedIndex()
			wait("caught-up replacement", func() bool { return stores[3].AppliedIndex() >= index })
			stop(2) // A/B still have the original 2/3 quorum.
			s = server(0)
			s.joiner = &promotionFaultJoiner{RaftStore: stores[0], beforePromotion: func() {
				if removed, err := controllers[0].NodeRemoved(t.Context(), ids[1].String()); err != nil || !removed {
					t.Fatalf("promotion preceded tombstone: %v, %v", removed, err)
				}
				if !scenario.committed {
					stop(lost) // No lost peer can acknowledge the promotion entry.
				}
			}, afterPromotion: func() {
				if scenario.committed {
					stop(lost) // A/D alone can commit the final A/C/D configuration.
				}
			}}
			err = s.RemoveMember(t.Context(), ids[1].String())
			if scenario.committed && err != nil {
				t.Fatalf("removal after committed promotion needs only two final voters: %v", err)
			}
			if !scenario.committed && !errors.Is(err, raft.ErrLeadershipLost) {
				t.Fatalf("promotion with only two of four voters: %v", err)
			}
			// Failure cannot undo committed revocation, regardless of whether
			// GetConfiguration exposes the uncommitted promotion as a voter.
			if s.AuthorizeNodeCertificate(t.Context(), ids[1], certificates[1]) {
				t.Fatal("failed promotion undid API certificate revocation")
			}
			if err := authorizers[0].Authorize(certificates[1]); !errors.Is(err, ErrNodeRemoved) {
				t.Fatalf("failed promotion undid Raft TLS revocation: %v", err)
			}
			if _, err := s.JoinMember(t.Context(), ids[1], configs[1].Advertise); !errors.Is(err, ErrNodeRemoved) {
				t.Fatalf("tombstoned member rejoined: %v", err)
			}
			if lost != 1 {
				stop(1) // Do not use the revoked identity to recover quorum.
			}
			start(2) // Recover a non-revoked old voter, not the removed identity.
			if lost == 3 {
				start(3)
			}
			leader := -1
			wait("recovery election", func() bool {
				for i, store := range stores {
					if store != nil && store.Raft().State() == raft.Leader {
						leader = i
						return true
					}
				}
				return false
			})
			wait("recovery barrier", func() bool { return stores[leader].Raft().Barrier(time.Second).Error() == nil })
			if err := server(leader).RemoveMember(t.Context(), ids[1].String()); err != nil {
				t.Fatalf("retry removal: %v", err)
			}
			wait("final replicated membership and tombstone", func() bool {
				for i, store := range stores {
					if store == nil {
						continue
					}
					members, err := store.Membership()
					if err != nil || len(members) != 3 {
						return false
					}
					for _, member := range members {
						if !member.Voter || member.ID == ids[1].String() {
							return false
						}
					}
					if err := authorizers[i].Authorize(certificates[1]); !errors.Is(err, ErrNodeRemoved) {
						return false
					}
				}
				return true
			})
			// Exercise the actual listener as well as its authorization callback.
			// TLS 1.3 may return from the client handshake before the server's
			// rejection arrives, so read the server alert if Dial succeeds.
			peerTLS := configs[1].TLS.Clone()
			peerTLS.ServerName = tlsutil.NodeServerName(ids[leader])
			conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", configs[leader].Advertise, peerTLS)
			if err == nil {
				if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				var alert [1]byte
				_, err = conn.Read(alert[:])
				_ = conn.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "bad certificate") {
				t.Fatalf("Raft TLS listener did not return a certificate rejection: %v", err)
			}
			t.Logf("%s: removal/retry converged to A/C/D voters with B revoked", name)
		})
	}
}
