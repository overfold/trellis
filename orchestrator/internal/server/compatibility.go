package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/compatibility"
	"github.com/overfold/trellis/orchestrator/internal/state"
)

func (s *Server) activeRelease() (uint64, error) {
	if store, ok := s.state.store.(*state.RaftStore); ok {
		return store.ActiveRelease()
	}
	return compatibility.Current(), nil
}

func (s *Server) checkRelease(release uint64) error {
	active, err := s.activeRelease()
	if err != nil {
		return err
	}
	return compatibility.Check(release, active)
}

func (h *Handler) checkExecutionRelease(c *echo.Context) error {
	var release uint64
	if raw := c.QueryParam("release"); raw != "" {
		var err error
		release, err = strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "invalid binary compatibility release")
		}
	}
	if err := h.server.checkRelease(release); err != nil {
		return echo.NewHTTPError(http.StatusConflict, err.Error())
	}
	return nil
}

// Compatibility probes the running binary on every registered node. Unreachable
// nodes remain visible and block activation; durable registration is not proof
// that a binary is still running. Callers serialize membership for activation.
func (s *Server) Compatibility(ctx context.Context) (*api.ClusterCompatibility, error) {
	active, err := s.activeRelease()
	if err != nil {
		return nil, err
	}
	status := &api.ClusterCompatibility{ActiveRelease: active, BinaryRelease: compatibility.Current(), Nodes: []api.NodeCompatibility{}}
	s.mu.RLock()
	nodes := make(map[uuid.UUID]string, len(s.nodes))
	for id, node := range s.nodes {
		nodes[id] = net.JoinHostPort(node.Host, strconv.Itoa(node.Port))
	}
	s.mu.RUnlock()
	if s.joiner != nil {
		members, err := s.joiner.Membership()
		if err != nil {
			return nil, err
		}
		for _, member := range members {
			id, err := uuid.Parse(member.ID)
			if err != nil {
				return nil, err
			}
			if _, registered := nodes[id]; !registered {
				nodes[id] = ""
			}
		}
	}
	ids := make([]uuid.UUID, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	for _, id := range ids {
		node := api.NodeCompatibility{ID: id}
		switch {
		case id == s.nodeID:
			node.Release = new(compatibility.Current())
		case nodes[id] == "":
			node.Error = "Raft member has not registered its agent"
		default:
			probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			release, err := s.client.Compatibility(probeCtx, id, nodes[id])
			cancel()
			if err != nil {
				node.Error = "running binary could not be verified"
			} else {
				node.Release = &release
			}
		}
		status.Nodes = append(status.Nodes, node)
	}
	return status, nil
}

// ActivateRelease closes the rollback window only after every worker and Raft
// member is running the target binary. Membership and registration cannot race
// the probes and replicated activation. Removed nodes do not block activation.
func (s *Server) ActivateRelease(ctx context.Context, target uint64) (*api.ClusterCompatibility, error) {
	ctx, release := s.bindTerm(ctx)
	defer release()
	s.membershipMu.Lock()
	defer s.membershipMu.Unlock()
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if err := s.checkTerm(ctx); err != nil {
		return nil, err
	}
	status, err := s.Compatibility(ctx)
	if err != nil {
		return nil, err
	}
	if target == status.ActiveRelease {
		return status, nil
	}
	if status.ActiveRelease == 0 || target != status.ActiveRelease+1 || target != compatibility.Current() {
		return nil, fmt.Errorf("activation requires the immediate successor of a stable active release, served by the target binary")
	}
	for _, node := range status.Nodes {
		if node.Release == nil || *node.Release != target {
			return nil, fmt.Errorf("node %s is not verified at release %d", node.ID, target)
		}
	}
	store, ok := s.state.store.(*state.RaftStore)
	if !ok {
		return nil, fmt.Errorf("compatibility activation requires Raft")
	}
	if err := store.ActivateRelease(ctx, target); err != nil {
		return nil, err
	}
	status.ActiveRelease = target
	return status, nil
}
