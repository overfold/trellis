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
)

func (s *StateController) executionKey(id uuid.UUID) string {
	return fmt.Sprintf("%s/%s/node-execution/%s", trellisNamespace, s.cluster, id)
}

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

// GetExecutionIntent reads committed execution authority, not observations.
func (s *StateController) GetExecutionIntent(ctx context.Context, id uuid.UUID) (*nodeapi.ExecutionIntent, error) {
	var intent nodeapi.ExecutionIntent
	found, err := s.get(ctx, s.executionKey(id), &intent)
	if err != nil || !found {
		return nil, err
	}
	return &intent, nil
}

// publishExecution serializes read/commit/notification. The document is never
// visible to a waiter before the replicated write succeeds.
func (s *Server) publishExecution(ctx context.Context, next *nodeapi.ExecutionIntent) error {
	s.executionMu.Lock()
	defer s.executionMu.Unlock()
	if err := s.checkTerm(ctx); err != nil {
		return err
	}
	prior, err := s.state.GetExecutionIntent(ctx, next.NodeID)
	if err != nil {
		return err
	}
	if prior != nil {
		next.Version = prior.Version
		if reflect.DeepEqual(prior, next) {
			return nil
		}
	}
	if next.Version == math.MaxUint64 {
		return fmt.Errorf("node execution version exhausted")
	}
	next.Version++
	if err := s.state.put(ctx, s.state.executionKey(next.NodeID), next); err != nil {
		return err
	}
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
		intent, err := s.state.GetExecutionIntent(ctx, id)
		if err != nil || intent == nil || intent.Version != version {
			s.executionMu.Unlock()
			return intent, err
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
			return s.state.GetExecutionIntent(ctx, id)
		case <-watch:
		}
	}
}

// publishReconcileIntent turns planner decisions into durable authority. Lost
// generations remain retain-only unless the planner explicitly chose cleanup;
// an earlier cleanup choice is irreversible even across leadership changes.
func (s *Server) publishReconcileIntent(ctx context.Context, actions []Action) error {
	for _, action := range actions {
		if action.Allocation == nil || (action.Type != ActionStart && action.Type != ActionStop) {
			continue
		}
		if err := s.persistAllocationUpdate(ctx, action.Allocation, func(next *Allocation) error {
			if action.controlPlanned && next.DrainSequence != action.drainSequence {
				return nil
			}
			if action.Type == ActionStop && activeAllocationPhase(next.Phase) {
				return next.Transition(lifecycle.PhaseStopping, s.now().UTC(), "", "")
			}
			if action.Type == ActionStart && next.Phase == lifecycle.PhasePlaced {
				s.mu.RLock()
				job := s.jobs[jobKey(next.Namespace, next.JobName)]
				eligible := job != nil && job.Incarnation == next.JobIncarnation && job.Revision == next.JobRevision && jobHasGroup(job, next.TaskGroupName)
				s.mu.RUnlock()
				if !eligible {
					return context.Canceled
				}
				return next.Transition(lifecycle.PhaseStarting, s.now().UTC(), "", "")
			}
			return nil
		}); err != nil {
			return err
		}
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	// Previous cleanup entries reserve a node even when the current leader has
	// not received its inventory yet. Missing observations never prove cleanup.
	s.mu.RLock()
	ids := make([]uuid.UUID, 0, len(s.nodes))
	for id := range s.nodes {
		ids = append(ids, id)
	}
	s.mu.RUnlock()
	previous := make(map[uuid.UUID]*nodeapi.ExecutionIntent, len(ids))
	cleanupPending := make(map[uuid.UUID]bool)
	cleaned := make(map[uuid.UUID]map[allocationGeneration]bool)
	for _, id := range ids {
		prior, err := s.state.GetExecutionIntent(ctx, id)
		if err != nil {
			return err
		}
		previous[id] = prior
		cleaned[id] = make(map[allocationGeneration]bool)
		if prior != nil {
			for _, entry := range prior.Allocations {
				complete, err := s.state.executionCleaned(ctx, id, entry.Request.AllocationID, entry.Request.Generation)
				if err != nil {
					return err
				}
				cleaned[id][allocationGeneration{id: entry.Request.AllocationID, generation: entry.Request.Generation}] = complete
				cleanupPending[id] = cleanupPending[id] || entry.Mode == nodeapi.ExecutionCleanup && !complete
			}
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
		request := nodeapi.AllocationRequest{AllocationID: allocation.ID, Generation: allocation.Generation, JobRevision: allocation.JobRevision, Epoch: epoch, Namespace: allocation.Namespace, JobName: allocation.JobName, GroupName: allocation.TaskGroupName, Tasks: allocation.Tasks, Attempt: allocation.Attempt, Draining: allocation.Draining, DrainSequence: allocation.DrainSequence}
		if allocation.CleanupComplete {
			cleaned[allocation.Node.ID][allocationGeneration{id: allocation.ID, generation: allocation.Generation}] = true
		}
		if job := s.jobs[jobKey(allocation.Namespace, allocation.JobName)]; job != nil {
			for _, group := range job.Spec.TaskGroups {
				if group.Name == allocation.TaskGroupName {
					request.Restart = group.Restart
				}
			}
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
		for _, action := range actions {
			if action.Allocation == allocation {
				if action.controlPlanned && allocation.DrainSequence != action.drainSequence {
					continue
				}
				if action.Type == ActionStart && eligible && activeAllocationPhase(allocation.Phase) && allocation.Phase != lifecycle.PhaseStopping && (allocation.NextRetryAt == nil || !s.now().Before(*allocation.NextRetryAt)) {
					mode = nodeapi.ExecutionRun
				} else if action.Type == ActionStop {
					mode = nodeapi.ExecutionCleanup
				}
			}
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
	for _, action := range actions {
		if action.Type != ActionStopObserved {
			continue
		}
		if doc := documents[action.Node.ID]; doc != nil {
			found := false
			for i := range doc.Allocations {
				entry := &doc.Allocations[i]
				if entry.Request.AllocationID == action.ID && entry.Request.Generation == action.Generation {
					entry.Mode, entry.RetainLogs = nodeapi.ExecutionCleanup, action.RetainLogs
					found = true
				}
			}
			if !found {
				doc.Allocations = append(doc.Allocations, nodeapi.AllocationIntent{Mode: nodeapi.ExecutionCleanup, Request: nodeapi.AllocationRequest{AllocationID: action.ID, Generation: action.Generation, Epoch: epoch}, RetainLogs: action.RetainLogs})
			}
		}
	}
	for id, doc := range documents {
		decisions, err := s.state.executionDecisions(ctx, id)
		if err != nil {
			return err
		}
		for _, decision := range decisions {
			complete, err := s.state.executionCleaned(ctx, id, decision.ID, decision.Generation)
			if err != nil {
				return err
			}
			cleaned[id][allocationGeneration{id: decision.ID, generation: decision.Generation}] = complete
			found := false
			for i := range doc.Allocations {
				entry := &doc.Allocations[i]
				if entry.Request.AllocationID == decision.ID && entry.Request.Generation == decision.Generation {
					entry.Mode, entry.RetainLogs = nodeapi.ExecutionCleanup, decision.RetainLogs
					found = true
				}
			}
			if !found {
				complete, err := s.state.executionCleaned(ctx, id, decision.ID, decision.Generation)
				if err != nil {
					return err
				}
				if complete && cleanupAbsent {
					continue
				}
				doc.Allocations = append(doc.Allocations, nodeapi.AllocationIntent{Mode: nodeapi.ExecutionCleanup, Request: nodeapi.AllocationRequest{AllocationID: decision.ID, Generation: decision.Generation, Epoch: epoch}, RetainLogs: decision.RetainLogs})
			}
		}
		prior := previous[id]
		if prior != nil {
			for _, old := range prior.Allocations {
				found := false
				for i := range doc.Allocations {
					entry := &doc.Allocations[i]
					if entry.Request.AllocationID == old.Request.AllocationID && entry.Request.Generation == old.Request.Generation {
						found = true
						// Execution identity is frozen at first authorization; the
						// current job may now be deleted or have a different policy.
						epoch, attempt := entry.Request.Epoch, entry.Request.Attempt
						draining, sequence := entry.Request.Draining, entry.Request.DrainSequence
						plan := entry.Request.NetworkPlan
						if plan == nil && !cleaned[id][allocationGeneration{id: old.Request.AllocationID, generation: old.Request.Generation}] {
							plan = old.Request.NetworkPlan
						}
						entry.Request = old.Request
						entry.Request.Epoch, entry.Request.Attempt = epoch, attempt
						entry.Request.Draining, entry.Request.DrainSequence = draining, sequence
						entry.Request.NetworkPlan = plan
						if old.Mode == nodeapi.ExecutionCleanup {
							entry.Mode = old.Mode
							entry.RetainLogs = entry.RetainLogs && old.RetainLogs
						}
					}
				}
				if !found {
					if cleaned[id][allocationGeneration{id: old.Request.AllocationID, generation: old.Request.Generation}] {
						continue
					}
					old.Request.Epoch = epoch
					if cleanupAbsent {
						old.Mode, old.RetainLogs = nodeapi.ExecutionCleanup, false
					} else if old.Mode == nodeapi.ExecutionRun {
						old.Mode = nodeapi.ExecutionRetain
					}
					doc.Allocations = append(doc.Allocations, old)
				}
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
	doc, err := s.state.GetExecutionIntent(ctx, id)
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "execution intent unavailable")
	}
	if doc == nil || doc.Version != values["version"] || doc.Epoch != values["epoch"] || doc.Epoch != s.currentControlEpoch() {
		return echo.NewHTTPError(http.StatusConflict, "execution authorization changed")
	}
	var request *nodeapi.AllocationRequest
	for i := range doc.Allocations {
		entry := &doc.Allocations[i]
		attempt := entry.Request.Attempt
		if attempt < 0 { continue }
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
	current, err := s.state.GetExecutionIntent(ctx, id)
	if err != nil || s.checkTerm(ctx) != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "execution authorization unavailable")
	}
	if current == nil || current.Version != doc.Version || current.Epoch != doc.Epoch {
		return echo.NewHTTPError(http.StatusConflict, "execution authorization changed")
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusOK, response)
}
