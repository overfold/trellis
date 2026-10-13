package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/compatibility"
	"github.com/overfold/trellis/orchestrator/internal/network"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// AcceptExecution validates a complete document before persistence or cleanup.
func (a *Agent) AcceptExecution(doc *nodeapi.ExecutionIntent) error {
	return a.acceptExecution(doc, false)
}

func (a *Agent) acceptExecution(doc *nodeapi.ExecutionIntent, cached bool) error {
	a.intentMu.Lock()
	defer a.intentMu.Unlock()
	if doc == nil || doc.NodeID != a.nodeID || doc.LeaderID == uuid.Nil || !doc.Complete || doc.Version == 0 || doc.Epoch == 0 || doc.Allocations == nil || doc.NetworkPlans == nil {
		return fmt.Errorf("invalid complete execution intent")
	}
	if err := compatibility.Check(compatibility.Current(), doc.Release); err != nil {
		return err
	}
	if !cached && (a.server == nil || a.server.ControlPlane().LeaderID != doc.LeaderID) {
		return fmt.Errorf("untrusted execution leader")
	}
	seen := map[string]bool{}
	active := map[string]uint64{}
	identities := map[string]nodeapi.AllocationRequest{}
	modes := map[string]nodeapi.ExecutionMode{}
	for _, entry := range doc.Allocations {
		r := entry.Request
		key := fmt.Sprintf("%s/%d", r.AllocationID, r.Generation)
		if seen[key] || !spec.ValidIdentifier(r.AllocationID) || r.Generation == 0 || r.Epoch != doc.Epoch || len(r.Secrets) > 0 || len(r.EnvOverrides) > 0 {
			return fmt.Errorf("invalid or conflicting allocation intent")
		}
		seen[key] = true
		identities[key] = r
		modes[key] = entry.Mode
		if r.NetworkPlan != nil {
			if err := validateExecutionPlan(*r.NetworkPlan); err != nil {
				return err
			}
		}
		if entry.Mode != nodeapi.ExecutionRun && entry.Mode != nodeapi.ExecutionRetain && entry.Mode != nodeapi.ExecutionCleanup {
			return fmt.Errorf("invalid execution mode")
		}
		if entry.Mode == nodeapi.ExecutionCleanup {
			continue
		}
		if active[r.AllocationID] != 0 {
			return fmt.Errorf("multiple live generations for %s", r.AllocationID)
		}
		active[r.AllocationID] = r.Generation
		if r.JobRevision <= 0 || r.ExecutionHash == "" {
			return fmt.Errorf("invalid execution identity")
		}
		if !spec.ValidDiscoveryIdentifier(r.Namespace) || !spec.ValidDiscoveryIdentifier(r.JobName) || !spec.ValidDiscoveryIdentifier(r.GroupName) {
			return fmt.Errorf("invalid execution namespace, job or group")
		}
		if err := validateCanonicalRequest(&r); err != nil {
			return err
		}
		if len(r.Tasks) == 0 || r.Attempt < 0 {
			return fmt.Errorf("invalid execution tasks or attempt")
		}
		tasks := map[string]bool{}
		for _, task := range r.Tasks {
			if !spec.ValidIdentifier(task.Name) || tasks[task.Name] {
				return fmt.Errorf("duplicate execution task")
			}
			tasks[task.Name] = true
		}
	}
	for _, entry := range doc.Allocations {
		if generation := active[entry.Request.AllocationID]; entry.Mode == nodeapi.ExecutionCleanup && generation != 0 && entry.Request.Generation >= generation {
			return fmt.Errorf("cleanup conflicts with live generation for %s", entry.Request.AllocationID)
		}
	}
	for namespace, plan := range doc.NetworkPlans {
		if !spec.ValidDiscoveryIdentifier(namespace) {
			return fmt.Errorf("empty network namespace")
		}
		if err := validateExecutionPlan(plan); err != nil {
			return err
		}
	}
	if a.intent != nil {
		for _, old := range a.intent.Allocations {
			r := old.Request
			key := fmt.Sprintf("%s/%d", r.AllocationID, r.Generation)
			if next, ok := identities[key]; ok && modes[key] != nodeapi.ExecutionCleanup && next.ExecutionHash != "" && r.ExecutionHash != "" && (r.ExecutionHash != next.ExecutionHash || r.JobRevision != next.JobRevision || r.Namespace != next.Namespace || r.JobName != next.JobName || r.GroupName != next.GroupName || next.Attempt < r.Attempt) {
				return fmt.Errorf("mixed execution identity for %s", r.AllocationID)
			}
		}
		if doc.Epoch < a.intent.Epoch || doc.Epoch == a.intent.Epoch && doc.Version < a.intent.Version {
			return fmt.Errorf("stale execution intent")
		}
		if doc.Epoch == a.intent.Epoch && doc.Version == a.intent.Version && !reflect.DeepEqual(doc, a.intent) {
			return fmt.Errorf("conflicting execution version")
		}
	}
	for _, record := range a.GetAllocations() {
		key := fmt.Sprintf("%s/%d", record.AllocationID, record.Generation)
		if r, ok := identities[key]; ok && modes[key] != nodeapi.ExecutionCleanup && (r.ExecutionHash != record.ExecutionHash || r.JobRevision != record.JobRevision || r.Namespace != record.Namespace || r.JobName != record.JobName || r.GroupName != record.GroupName) {
			return fmt.Errorf("mixed local execution identity for %s", record.AllocationID)
		}
	}
	a.mu.RLock()
	epoch := a.epoch
	a.mu.RUnlock()
	if doc.Epoch < epoch {
		return ErrStaleEpoch
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	var cloned nodeapi.ExecutionIntent
	if err = json.Unmarshal(data, &cloned); err != nil {
		return err
	}
	if a.local != nil && !cached {
		if err = a.local.Put("agent/execution-intent", &cloned); err != nil {
			return err
		}
	}
	if err = a.AcceptEpoch(doc.Epoch); err != nil {
		return err
	}
	if a.intentContext == nil || a.intent == nil || a.intent.Epoch != cloned.Epoch || a.intent.Version != cloned.Version {
		if a.intentCancel != nil {
			a.intentCancel()
		}
		a.intentContext, a.intentCancel = context.WithCancel(a.lifetimeContext())
	}
	a.intent = &cloned
	a.intentFresh = !cached
	if a.intentWake == nil {
		a.intentWake = make(chan struct{}, 1)
	}
	select {
	case a.intentWake <- struct{}{}:
	default:
	}
	a.mu.Lock()
	for id, start := range a.starts {
		authorized := false
		for _, entry := range cloned.Allocations {
			r := entry.Request
			if r.AllocationID == id && entry.Mode == nodeapi.ExecutionRun && r.Generation == start.generation && r.Attempt == start.attempt {
				authorized = true
			}
		}
		if !authorized && !start.finished() {
			start.cancel()
		}
	}
	a.mu.Unlock()
	return nil
}

func (a *Agent) recoverExecutionIntent() error {
	if a.local != nil {
		var cached nodeapi.ExecutionIntent
		if err := a.local.Get("agent/execution-intent", &cached); err == nil {
			if err := a.acceptExecution(&cached, true); !errors.Is(err, ErrStaleEpoch) {
				return err
			}
			// An authenticated exec may have advanced the epoch before the
			// next pull. Preserve local work and fetch fresh authority instead
			// of replaying the older document or refusing recovery.
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (a *Agent) runExecutionLoop(ctx context.Context) {
	go a.runIntentReconcile(ctx)
	for ctx.Err() == nil {
		a.intentMu.Lock()
		version := uint64(0)
		epoch := uint64(0)
		if a.intent != nil {
			version = a.intent.Version
			epoch = a.intent.Epoch
		}
		a.intentMu.Unlock()
		doc, err := a.server.PullExecution(ctx, a.nodeID, a.server.ControlPlane().LeaderID, epoch, version)
		if err == nil {
			err = a.AcceptExecution(doc)
		}
		if err != nil {
			a.intentMu.Lock()
			a.intentFresh = false
			a.intentMu.Unlock()
			a.log.Debug("pull execution intent", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (a *Agent) runIntentReconcile(ctx context.Context) {
	a.intentMu.Lock()
	if a.intentWake == nil {
		a.intentWake = make(chan struct{}, 1)
	}
	wake := a.intentWake
	a.intentMu.Unlock()
	for ctx.Err() == nil {
		a.intentMu.Lock()
		doc, workCtx, fresh := a.intent, a.intentContext, a.intentFresh
		a.intentMu.Unlock()
		if doc != nil && workCtx != nil {
			a.dispatchExecution(workCtx, doc, fresh)
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-time.After(time.Second):
		}
	}
}

// One worker per allocation preserves cleanup-before-start backpressure without
// allowing a stuck runtime or credential request to block unrelated work.
const maxIntentWorkers = 32

func (a *Agent) dispatchExecution(ctx context.Context, doc *nodeapi.ExecutionIntent, fresh bool) {
	groups := map[string][]nodeapi.AllocationIntent{}
	for namespace := range doc.NetworkPlans {
		groups["network/"+namespace] = nil
	}
	present := map[string]bool{}
	for _, entry := range doc.Allocations {
		id := entry.Request.AllocationID
		groups[id] = append(groups[id], entry)
		present[completionKey(id, entry.Request.Generation)] = true
	}
	if doc.CleanupAbsent {
		a.mu.RLock()
		for id, start := range a.starts {
			key := completionKey(id, start.generation)
			if !present[key] {
				present[key] = true
				groups[id] = append(groups[id], nodeapi.AllocationIntent{Mode: nodeapi.ExecutionCleanup, Request: nodeapi.AllocationRequest{AllocationID: id, Generation: start.generation, Epoch: doc.Epoch}})
			}
		}
		for _, record := range a.retainedLogs {
			key := completionKey(record.AllocationID, record.Generation)
			if !present[key] {
				present[key] = true
				groups[record.AllocationID] = append(groups[record.AllocationID], nodeapi.AllocationIntent{Mode: nodeapi.ExecutionCleanup, Request: nodeapi.AllocationRequest{AllocationID: record.AllocationID, Generation: record.Generation, Epoch: doc.Epoch}})
			}
		}
		a.mu.RUnlock()
		for _, record := range a.GetAllocations() {
			key := completionKey(record.AllocationID, record.Generation)
			if !present[key] {
				present[key] = true
				groups[record.AllocationID] = append(groups[record.AllocationID], nodeapi.AllocationIntent{Mode: nodeapi.ExecutionCleanup, Request: nodeapi.AllocationRequest{AllocationID: record.AllocationID, Generation: record.Generation, Epoch: doc.Epoch}})
			}
		}
	}
	for id, entries := range groups {
		a.intentMu.Lock()
		if a.intentWorkers == nil {
			a.intentWorkers = map[string]bool{}
		}
		if a.intentWorkers[id] || len(a.intentWorkers) >= maxIntentWorkers {
			a.intentMu.Unlock()
			continue
		}
		a.intentWorkers[id] = true
		a.intentMu.Unlock()
		go func() {
			defer func() {
				a.intentMu.Lock()
				delete(a.intentWorkers, id)
				if a.intent != nil && (a.intent.Epoch != doc.Epoch || a.intent.Version != doc.Version) && a.intentWake != nil {
					select {
					case a.intentWake <- struct{}{}:
					default:
					}
				}
				a.intentMu.Unlock()
			}()
			ctx, cancel := context.WithTimeout(ctx, allocationControlWait)
			defer cancel()
			part := *doc
			part.Allocations = entries
			part.CleanupAbsent = false
			part.NetworkPlans = nil
			if entries == nil {
				namespace := id[len("network/"):]
				part.NetworkPlans = map[string]network.Plan{namespace: doc.NetworkPlans[namespace]}
			}
			if err := a.reconcileExecutionAllowed(context.WithValue(ctx, executionFenceKey{}, [2]uint64{doc.Epoch, doc.Version}), &part, fresh); err != nil {
				a.log.Debug("reconcile allocation intent", "allocation", id, "error", err)
			}
		}()
	}
}

type executionFenceKey struct{}

// fenceExecution serializes admission with document persistence, not runtime I/O.
func (a *Agent) fenceExecution(ctx context.Context) (func(), error) {
	a.intentMu.Lock()
	if revision, ok := ctx.Value(executionFenceKey{}).([2]uint64); ok && (a.intent == nil || [2]uint64{a.intent.Epoch, a.intent.Version} != revision) {
		a.intentMu.Unlock()
		return nil, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		a.intentMu.Unlock()
		return nil, err
	}
	var once sync.Once
	return func() { once.Do(a.intentMu.Unlock) }, nil
}

func (a *Agent) reconcileExecution(ctx context.Context, doc *nodeapi.ExecutionIntent) error {
	return a.reconcileExecutionAllowed(context.WithValue(ctx, executionFenceKey{}, [2]uint64{doc.Epoch, doc.Version}), doc, true)
}

func (a *Agent) reconcileExecutionAllowed(ctx context.Context, doc *nodeapi.ExecutionIntent, allowStarts bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for namespace, plan := range doc.NetworkPlans {
		if err := a.UpdateNetworkPlan(ctx, &nodeapi.NetworkPlanRequest{Epoch: doc.Epoch, Namespace: namespace, Plan: plan}); err != nil {
			return err
		}
	}
	present := map[string]bool{}
	for _, entry := range doc.Allocations {
		r := entry.Request
		present[completionKey(r.AllocationID, r.Generation)] = true
		if entry.Mode == nodeapi.ExecutionCleanup {
			a.mu.RLock()
			_, completed := a.cleanupComplete[completionKey(r.AllocationID, r.Generation)]
			logsRemain := false
			for _, record := range a.retainedLogs {
				if record.AllocationID == r.AllocationID && record.Generation <= r.Generation {
					logsRemain = true
				}
			}
			a.mu.RUnlock()
			if completed && (entry.RetainLogs || !logsRemain) {
				continue
			}
			if completed {
				unlock, err := a.waitAllocationOperation(ctx, r.AllocationID)
				if err != nil {
					return err
				}
				err = a.removeRetainedLogs(r.AllocationID, r.Generation)
				unlock()
				if err != nil {
					return err
				}
				continue
			}
			if err := a.StopGroup(ctx, &nodeapi.StopAllocationRequest{AllocationID: r.AllocationID, Generation: r.Generation, Epoch: doc.Epoch, RetainLogs: entry.RetainLogs}); err != nil {
				return err
			}
		}
	}
	if doc.CleanupAbsent {
		absent := map[string]cleanupCompletion{}
		a.mu.RLock()
		for id, start := range a.starts {
			key := completionKey(id, start.generation)
			if !present[key] {
				absent[key] = cleanupCompletion{ID: id, Generation: start.generation}
			}
		}
		for _, record := range a.retainedLogs {
			key := completionKey(record.AllocationID, record.Generation)
			if !present[key] {
				absent[key] = cleanupCompletion{ID: record.AllocationID, Generation: record.Generation}
			}
		}
		a.mu.RUnlock()
		for _, record := range a.GetAllocations() {
			key := completionKey(record.AllocationID, record.Generation)
			if !present[key] {
				absent[key] = cleanupCompletion{ID: record.AllocationID, Generation: record.Generation}
			}
		}
		for _, record := range absent {
			if err := a.StopGroup(ctx, &nodeapi.StopAllocationRequest{AllocationID: record.ID, Generation: record.Generation, Epoch: doc.Epoch}); err != nil {
				return err
			}
		}
	}
	for _, entry := range doc.Allocations {
		r := entry.Request
		if entry.Mode == nodeapi.ExecutionCleanup {
			continue
		}
		drain := &nodeapi.DrainAllocationRequest{AllocationID: r.AllocationID, Generation: r.Generation, Epoch: r.Epoch, Sequence: r.DrainSequence}
		if r.DrainSequence > 0 {
			var err error
			if r.Draining {
				err = a.DrainGroup(ctx, drain)
			} else {
				err = a.ResumeGroup(ctx, drain)
			}
			if err != nil {
				return err
			}
		}
		if entry.Mode != nodeapi.ExecutionRun || !allowStarts {
			continue
		}
		established := true
		for _, task := range r.Tasks {
			record, ok := a.snapshotAllocation(taskRecordID(r.AllocationID, r.Generation, task.Name))
			established = established && ok && record.ExecutionHash == r.ExecutionHash && record.Status != "starting" && record.Status != "stopping"
		}
		if established {
			continue
		}
		a.mu.RLock()
		start := a.starts[r.AllocationID]
		same := start != nil && start.generation == r.Generation && start.attempt == r.Attempt
		a.mu.RUnlock()
		if same {
			continue
		}
		credentials, err := a.server.ExecutionCredentials(ctx, a.nodeID, doc.LeaderID, doc.Version, r)
		if err != nil {
			return err
		}
		r.Secrets, r.EnvOverrides = credentials.Secrets, credentials.EnvOverrides
		err = a.StartGroup(ctx, &r)
		for i := range r.Secrets {
			clear(r.Secrets[i].Value)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func validateExecutionPlan(plan network.Plan) error {
	if plan.ListenPort < 0 || plan.ListenPort > 65535 || plan.APIPort < 0 || plan.APIPort > 65535 {
		return fmt.Errorf("invalid execution network port")
	}
	if plan.CIDR != "" {
		if _, err := netip.ParsePrefix(plan.CIDR); err != nil {
			return fmt.Errorf("invalid execution CIDR")
		}
	}
	if plan.Gateway != "" {
		if _, err := netip.ParseAddr(plan.Gateway); err != nil {
			return fmt.Errorf("invalid execution gateway")
		}
	}
	if plan.WireGuardAddress != "" {
		if _, err := netip.ParsePrefix(plan.WireGuardAddress); err != nil {
			return fmt.Errorf("invalid execution WireGuard address")
		}
	}
	for _, peer := range plan.Peers {
		if peer.Endpoint != "" {
			host, port, err := net.SplitHostPort(peer.Endpoint)
			number, portErr := strconv.Atoi(port)
			if err != nil || portErr != nil || host == "" || number < 1 || number > 65535 {
				return fmt.Errorf("invalid execution peer endpoint")
			}
		}
		if err := network.ValidateWireGuardPublicKey(peer.PublicKey); err != nil {
			return err
		}
		for _, prefix := range peer.AllowedIPs {
			if _, err := netip.ParsePrefix(prefix); err != nil {
				return fmt.Errorf("invalid execution peer route")
			}
		}
	}
	return nil
}
