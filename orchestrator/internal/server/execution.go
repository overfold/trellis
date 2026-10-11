package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/network"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/state"
)

func (s *StateController) executionCleanupKey(node uuid.UUID, allocation string, generation uint64) string {
	return fmt.Sprintf("%s/%s/execution-cleanup/%s/%s/%d", trellisNamespace, s.cluster, node, url.PathEscape(allocation), generation)
}

// Cleanup authority is committed with the plan, including pruning. A leader
// that fails before projecting node documents must not forget that decision.
type executionCleanupDecision struct {
	Node       uuid.UUID `json:"node"`
	ID         string    `json:"id"`
	Generation uint64    `json:"generation"`
	RetainLogs bool      `json:"retain_logs"`
}

func (s *StateController) executionDecisionKey(node uuid.UUID, allocation string, generation uint64) string {
	return fmt.Sprintf("%s/%s/execution-decisions/%s/%s/%d", trellisNamespace, s.cluster, node, url.PathEscape(allocation), generation)
}

// executionCleanupMutations preserves irreversible log removal in canonical
// authority, including duplicate decisions in one commit. Callers hold mutation
// ordering until the returned mutations commit with their allocation changes.
func (s *StateController) executionCleanupMutations(ctx context.Context, decisions []executionCleanupDecision) ([]state.Mutation, error) {
	byKey := make(map[string]executionCleanupDecision, len(decisions))
	keys := make([]string, 0, len(decisions))
	for _, decision := range decisions {
		key := s.executionDecisionKey(decision.Node, decision.ID, decision.Generation)
		if prior, ok := byKey[key]; ok {
			decision.RetainLogs = decision.RetainLogs && prior.RetainLogs
		} else {
			keys = append(keys, key)
		}
		byKey[key] = decision
	}
	sort.Strings(keys)
	mutations := make([]state.Mutation, 0, len(keys))
	for _, key := range keys {
		decision := byKey[key]
		var prior executionCleanupDecision
		found, err := s.get(ctx, key, &prior)
		if err != nil {
			return nil, err
		}
		if found {
			decision.RetainLogs = decision.RetainLogs && prior.RetainLogs
		}
		raw, err := json.Marshal(decision)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, state.Mutation{Key: key, Value: raw})
	}
	return mutations, nil
}

func (s *StateController) executionDecisions(ctx context.Context, node uuid.UUID) ([]executionCleanupDecision, error) {
	values, err := s.store.List(ctx, fmt.Sprintf("%s/%s/execution-decisions/%s/", trellisNamespace, s.cluster, node))
	if err != nil {
		return nil, err
	}
	decisions := make([]executionCleanupDecision, 0, len(values))
	for _, raw := range values {
		var decision executionCleanupDecision
		if err := json.Unmarshal(raw, &decision); err != nil {
			return nil, err
		}
		decisions = append(decisions, decision)
	}
	return decisions, nil
}

func (s *StateController) executionCleaned(ctx context.Context, node uuid.UUID, allocation string, generation uint64) (bool, error) {
	var complete bool
	_, err := s.get(ctx, s.executionCleanupKey(node, allocation, generation), &complete)
	return complete, err
}

// executionIntentLocked returns only a view belonging to the current epoch.
// Missing projections fail closed until reconciliation rebuilds them.
func (s *Server) executionIntentLocked(id uuid.UUID) *nodeapi.ExecutionIntent {
	intent := s.executionIntents[id]
	if intent == nil || intent.Epoch != s.currentControlEpoch() {
		return nil
	}
	return intent
}

// publishExecution caches a projection of already committed authority. Versions
// are local to an epoch; acquiring a new durable epoch permits rebuilding them.
func (s *Server) publishExecution(ctx context.Context, next *nodeapi.ExecutionIntent) error {
	s.executionMu.Lock()
	defer s.executionMu.Unlock()
	if err := s.checkTerm(ctx); err != nil {
		return err
	}
	// Detach mutable slices/policies and compare canonical wire content, so
	// omitted empty maps and slices do not spuriously advance the version.
	raw, err := json.Marshal(next)
	if err != nil {
		return err
	}
	var cloned nodeapi.ExecutionIntent
	if err := json.Unmarshal(raw, &cloned); err != nil {
		return err
	}
	prior := s.executionIntentLocked(cloned.NodeID)
	if prior != nil {
		cloned.Version = prior.Version
		if reflect.DeepEqual(prior, &cloned) {
			return nil
		}
	}
	if cloned.Version == math.MaxUint64 {
		return fmt.Errorf("node execution version exhausted")
	}
	cloned.Version++
	if s.executionIntents == nil {
		s.executionIntents = make(map[uuid.UUID]*nodeapi.ExecutionIntent)
	}
	s.executionIntents[next.NodeID] = &cloned
	if s.executionWatch == nil {
		s.executionWatch = make(map[uuid.UUID]chan struct{})
	}
	if watch := s.executionWatch[next.NodeID]; watch != nil {
		close(watch)
	}
	s.executionWatch[next.NodeID] = make(chan struct{})
	return nil
}

// ExecutionIntent blocks only while the caller's version is current. Timeout
// returns the same complete committed document; cancellation never returns an
// empty document that could accidentally authorize deletion.
func (s *Server) ExecutionIntent(ctx context.Context, id uuid.UUID, version uint64) (*nodeapi.ExecutionIntent, error) {
	timer := time.NewTimer(25 * time.Second)
	defer timer.Stop()
	for {
		s.executionMu.Lock()
		intent := s.executionIntentLocked(id)
		if intent == nil || intent.Version != version {
			s.executionMu.Unlock()
			return intent, nil
		}
		if s.executionWatch == nil {
			s.executionWatch = make(map[uuid.UUID]chan struct{})
		}
		watch := s.executionWatch[id]
		if watch == nil {
			watch = make(chan struct{})
			s.executionWatch[id] = watch
		}
		s.executionMu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			s.executionMu.Lock()
			intent := s.executionIntentLocked(id)
			s.executionMu.Unlock()
			return intent, nil
		case <-watch:
		}
	}
}

// publishReconcileIntent projects canonical authority without persisting the
// response documents. All admission and cleanup decisions are already committed.
func (s *Server) publishReconcileIntent(ctx context.Context) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	// Durable cleanup decisions reserve a node even before fresh inventory.
	s.mu.RLock()
	ids := make([]uuid.UUID, 0, len(s.nodes))
	for id := range s.nodes {
		ids = append(ids, id)
	}
	s.mu.RUnlock()
	cleanupPending := make(map[uuid.UUID]bool)
	cleaned := make(map[uuid.UUID]map[allocationGeneration]bool)
	decisions := make(map[uuid.UUID][]executionCleanupDecision, len(ids))
	for _, id := range ids {
		entries, err := s.state.executionDecisions(ctx, id)
		if err != nil {
			return err
		}
		decisions[id] = entries
		cleaned[id] = make(map[allocationGeneration]bool)
		for _, entry := range entries {
			complete, err := s.state.executionCleaned(ctx, id, entry.ID, entry.Generation)
			if err != nil {
				return err
			}
			cleaned[id][allocationGeneration{id: entry.ID, generation: entry.Generation}] = complete
			cleanupPending[id] = cleanupPending[id] || !complete
		}
	}
	s.mu.RLock()
	epoch, leader := s.controlEpoch, s.nodeID
	cleanupAbsent := !s.leaderSince.IsZero() && s.now().Sub(s.leaderSince) >= leaderRecoveryGrace
	documents := make(map[uuid.UUID]*nodeapi.ExecutionIntent, len(s.nodes))
	blocked := make(map[uuid.UUID]bool)
	running := make(map[uuid.UUID]map[allocationGeneration]bool)
	for id, node := range s.nodes {
		running[id] = make(map[allocationGeneration]bool)
		blocked[id] = cleanupPending[id] || s.nodeHasUnownedExecutionLocked(node)
		documents[id] = &nodeapi.ExecutionIntent{NodeID: id, LeaderID: leader, Epoch: epoch, Complete: true, CleanupAbsent: cleanupAbsent, Allocations: []nodeapi.AllocationIntent{}, NetworkPlans: map[string]network.Plan{}}
	}
	for _, allocation := range s.allocations {
		allocation.mu.Lock()
		if allocation.Node == nil || allocation.Phase == lifecycle.PhasePending {
			allocation.mu.Unlock()
			continue
		}
		doc := documents[allocation.Node.ID]
		if doc == nil {
			allocation.mu.Unlock()
			continue
		}
		request := nodeapi.AllocationRequest{AllocationID: allocation.ID, Generation: allocation.Generation, JobRevision: allocation.JobRevision, Epoch: epoch, Namespace: allocation.Namespace, JobName: allocation.JobName, GroupName: allocation.TaskGroupName, Tasks: allocation.Tasks, Restart: allocation.Restart, Attempt: allocation.Attempt, Draining: allocation.Draining, DrainSequence: allocation.DrainSequence}
		if allocation.CleanupComplete {
			cleaned[allocation.Node.ID][allocationGeneration{id: allocation.ID, generation: allocation.Generation}] = true
		}
		networkUnavailable := false
		if tasksUseWireGuard(allocation.Tasks) && !allocation.CleanupComplete {
			plan, err := s.networkPlan(allocation.Namespace, allocation.Node)
			if err != nil {
				// Withhold admission, not destruction or preservation of other
				// executions, when a namespace cannot yet be addressed.
				networkUnavailable = true
			} else {
				request.NetworkPlan = plan
				for _, task := range request.Tasks {
					if task.APIAccess != nil {
						_, port, err := net.SplitHostPort(s.serverAddr)
						if err == nil {
							plan.APIPort, _ = strconv.Atoi(port)
						}
					}
				}
				doc.NetworkPlans[allocation.Namespace] = *plan
			}
		}
		mode := nodeapi.ExecutionRetain
		if allocation.Phase == lifecycle.PhaseStarting || allocation.Phase == lifecycle.PhaseRunning {
			mode = nodeapi.ExecutionRun
		}
		job := s.jobs[jobKey(allocation.Namespace, allocation.JobName)]
		eligible := job != nil && job.Incarnation == allocation.JobIncarnation && job.Revision == allocation.JobRevision && jobHasGroup(job, allocation.TaskGroupName)
		if allocation.Phase != lifecycle.PhaseRunning && !eligible {
			mode = nodeapi.ExecutionRetain
		}
		if allocation.Phase == lifecycle.PhaseStarting && allocation.NextRetryAt != nil && s.now().Before(*allocation.NextRetryAt) {
			mode = nodeapi.ExecutionRetain
		}
		if allocation.CleanupComplete || allocation.Phase == lifecycle.PhaseStopping || allocation.Phase == lifecycle.PhaseStopped {
			mode = nodeapi.ExecutionCleanup
		}
		if mode == nodeapi.ExecutionRun && (networkUnavailable || blocked[allocation.Node.ID] && allocation.Phase != lifecycle.PhaseRunning) {
			mode = nodeapi.ExecutionRetain
		}
		if allocation.Phase == lifecycle.PhaseRunning {
			running[allocation.Node.ID][allocationGeneration{id: allocation.ID, generation: allocation.Generation}] = true
		}
		hashInput := request
		hashInput.Epoch, hashInput.Attempt = 0, 0
		hashInput.Draining, hashInput.DrainSequence = false, 0
		if request.NetworkPlan != nil {
			hashInput.NetworkPlan = &network.Plan{CIDR: request.NetworkPlan.CIDR, Gateway: request.NetworkPlan.Gateway}
		}
		raw, _ := json.Marshal(hashInput)
		hash := sha256.Sum256(raw)
		request.ExecutionHash = hex.EncodeToString(hash[:])
		doc.Allocations = append(doc.Allocations, nodeapi.AllocationIntent{Mode: mode, Request: request, RetainLogs: true})
		allocation.mu.Unlock()
	}
	s.mu.RUnlock()
	for id, doc := range documents {
		for _, decision := range decisions[id] {
			found := false
			for i := range doc.Allocations {
				entry := &doc.Allocations[i]
				if entry.Request.AllocationID == decision.ID && entry.Request.Generation == decision.Generation {
					entry.Mode, entry.RetainLogs = nodeapi.ExecutionCleanup, decision.RetainLogs
					found = true
				}
			}
			if !found {
				complete := cleaned[id][allocationGeneration{id: decision.ID, generation: decision.Generation}]
				if complete && cleanupAbsent {
					continue
				}
				doc.Allocations = append(doc.Allocations, nodeapi.AllocationIntent{Mode: nodeapi.ExecutionCleanup, Request: nodeapi.AllocationRequest{AllocationID: decision.ID, Generation: decision.Generation, Epoch: epoch}, RetainLogs: decision.RetainLogs})
			}
		}
		pendingCleanup := false
		for _, entry := range doc.Allocations {
			if entry.Mode == nodeapi.ExecutionCleanup && !cleaned[id][allocationGeneration{id: entry.Request.AllocationID, generation: entry.Request.Generation}] {
				pendingCleanup = true
			}
		}
		if pendingCleanup {
			for i := range doc.Allocations {
				entry := &doc.Allocations[i]
				if entry.Mode == nodeapi.ExecutionRun && !running[id][allocationGeneration{id: entry.Request.AllocationID, generation: entry.Request.Generation}] {
					// Publication of cleanup is not acknowledgement of cleanup.
					// Preserve existing work, but do not admit starts into held
					// node resources until a later observation proves release.
					entry.Mode = nodeapi.ExecutionRetain
				}
			}
		}
		sort.Slice(doc.Allocations, func(i, j int) bool {
			a, b := doc.Allocations[i].Request, doc.Allocations[j].Request
			if a.AllocationID == b.AllocationID {
				return a.Generation < b.Generation
			}
			return a.AllocationID < b.AllocationID
		})
		if err := s.publishExecution(ctx, doc); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) handleExecution(c *echo.Context) error {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid node ID")
	}
	if err := requireExactNode(c, id, "execution identity does not match certificate"); err != nil {
		return err
	}
	version, err := strconv.ParseUint(c.QueryParam("version"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid execution version")
	}
	epoch, err := strconv.ParseUint(c.QueryParam("epoch"), 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid execution epoch")
	}
	if epoch != h.server.currentControlEpoch() {
		version = 0
	}
	intent, err := h.server.ExecutionIntent(c.Request().Context(), id, version)
	if err != nil || intent == nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "execution intent unavailable")
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusOK, intent)
}

func (h *Handler) handleExecutionCredentials(c *echo.Context) error {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid node ID")
	}
	if err := requireExactNode(c, id, "execution identity does not match certificate"); err != nil {
		return err
	}
	values := make(map[string]uint64, 4)
	for _, name := range []string{"version", "generation", "epoch", "attempt"} {
		value, err := strconv.ParseUint(c.QueryParam(name), 10, 64)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "invalid execution "+name)
		}
		values[name] = value
	}
	s := h.server
	// Read authority under publication ordering, resolve outside the watcher
	// lock, then revalidate before delivery. Slow token storage must not hold
	// up blocked pulls or unrelated node publication.
	s.executionMu.Lock()
	locked := true
	defer func() {
		if locked {
			s.executionMu.Unlock()
		}
	}()
	ctx := c.Request().Context()
	if err := s.checkTerm(ctx); err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "control-plane leadership changed")
	}
	doc := s.executionIntentLocked(id)
	if doc == nil || doc.Version != values["version"] || doc.Epoch != values["epoch"] || doc.Epoch != s.currentControlEpoch() {
		return echo.NewHTTPError(http.StatusConflict, "execution authorization changed")
	}
	var request *nodeapi.AllocationRequest
	for i := range doc.Allocations {
		entry := &doc.Allocations[i]
		attempt := entry.Request.Attempt
		if attempt < 0 {
			continue
		}
		if entry.Mode == nodeapi.ExecutionRun && entry.Request.AllocationID == c.Param("allocation") && entry.Request.Generation == values["generation"] && uint64(attempt) == values["attempt"] && entry.Request.Epoch == values["epoch"] {
			request = &entry.Request
			break
		}
	}
	if request == nil {
		return echo.NewHTTPError(http.StatusForbidden, "allocation is not authorized to run")
	}
	s.executionMu.Unlock()
	locked = false
	response := nodeapi.ExecutionCredentials{}
	defer func() {
		for i := range response.Secrets {
			clear(response.Secrets[i].Value)
		}
	}()
	for _, task := range request.Tasks {
		for _, ref := range task.Secrets {
			if s.secrets == nil {
				return echo.NewHTTPError(http.StatusServiceUnavailable, "execution secrets unavailable")
			}
			value, version, err := s.secrets.Resolve(ctx, request.Namespace, ref.Name)
			if err != nil {
				return echo.NewHTTPError(http.StatusServiceUnavailable, "execution secret unavailable")
			}
			response.Secrets = append(response.Secrets, nodeapi.DeliveredSecret{Task: task.Name, Name: ref.Name, Version: version, Target: ref.Target, Env: ref.Env, Path: ref.Path, Mode: ref.Mode, Value: value})
		}
		if task.APIAccess != nil {
			token, err := s.apiAccessToken(ctx, task.APIAccess, request, task.Name)
			if err != nil {
				return echo.NewHTTPError(http.StatusServiceUnavailable, "workload credential unavailable")
			}
			address, err := workloadAPIAddress(s.serverAddr)
			if err != nil {
				return echo.NewHTTPError(http.StatusServiceUnavailable, "workload API address unavailable")
			}
			if response.EnvOverrides == nil {
				response.EnvOverrides = make(map[string]map[string]string)
			}
			response.EnvOverrides[task.Name] = map[string]string{"TRELLIS_TOKEN": token, "TRELLIS_ADDR": address, "TRELLIS_NAMESPACE": request.Namespace}
			if ca, _, err := s.ClusterCA(); err == nil && ca != "" {
				response.EnvOverrides[task.Name]["TRELLIS_CA_CERT"] = ca
			}
		}
	}
	s.executionMu.Lock()
	locked = true
	current := s.executionIntentLocked(id)
	if s.checkTerm(ctx) != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "execution authorization unavailable")
	}
	if current == nil || current.Version != doc.Version || current.Epoch != doc.Epoch {
		return echo.NewHTTPError(http.StatusConflict, "execution authorization changed")
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusOK, response)
}
