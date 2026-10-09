package agent

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/network"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

// detachAllocationNetwork removes an allocation's network attachment. A
// record that holds only the intent saved before Attach, because the agent
// failed or stopped before recording the result, is detached by allocation
// ID. Detaching is idempotent, so an intent whose Attach never ran is fine.
func (a *Agent) detachAllocationNetwork(ctx context.Context, allocation *Allocation) error {
	if allocation.Network == nil && allocation.NetworkIntent != nil {
		if recovery, ok := a.network.(network.AttachmentRecovery); ok {
			return recovery.DetachAllocation(ctx, allocation.NetworkIntent.AllocationID)
		}
	}
	// A manager without recovery support has nothing to remove for an
	// allocation without an attachment.
	return a.network.Detach(ctx, allocation.Network)
}

// removeOrphanedResources removes node-local resources that no allocation
// owns. It runs only once recovery has accounted for every container.
func (a *Agent) removeOrphanedResources(ctx context.Context) {
	a.removeOrphanedSecretDirs()
	a.removeOrphanedNetworkAttachments(ctx)
}

// removeOrphanedNetworkAttachments detaches network attachments that no
// allocation record or in-memory allocation owns. They are left behind when
// the agent stops during Attach and its allocation record is later removed
// without the attachment being known, for example after cleanup of a
// container recovered only from runtime labels. It is gated like the secret
// sweep: it needs durable records, and every record must be readable.
func (a *Agent) removeOrphanedNetworkAttachments(ctx context.Context) {
	if a.local == nil {
		return
	}
	recovery, ok := a.network.(network.AttachmentRecovery)
	if !ok {
		return
	}
	// List attachments before reading ownership. A start registers its
	// allocation before Attach records the attachment, so a listed
	// attachment of a concurrent start is always owned below.
	ids, err := recovery.Attachments(ctx)
	if err != nil {
		// Unreadable attachment records are left alone; readable ones are
		// still swept.
		a.log.Error("orphaned network sweep: some attachment records are unreadable", "error", err)
	}
	if len(ids) == 0 {
		return
	}
	// Record file names encode allocation IDs. A record recovery could not
	// parse still claims its attachment.
	records, recordErrs := a.local.ListRaw("agent/allocations")
	if len(recordErrs) > 0 {
		a.log.Error("skip orphaned network sweep: allocation records are unreadable", "error", errors.Join(recordErrs...))
		return
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		if _, owned := records[allocationFileName(id)]; owned {
			continue
		}
		a.detachOrphanedNetwork(ctx, recovery, id)
	}
}

// detachOrphanedNetwork reserves a task ID atomically with checking ownership.
// Neither storage nor external cleanup holds Agent.mu. Cancellation bounds
// the caller's wait, not the reservation: a noncooperative manager must return
// before a start may reuse this ID. Other tasks and agent writers remain free.
func (a *Agent) detachOrphanedNetwork(ctx context.Context, recovery network.AttachmentRecovery, id string) {
	if ctx.Err() != nil {
		return
	}
	a.mu.Lock()
	if a.allocations[id] != nil || a.orphanDetaches[id] {
		a.mu.Unlock()
		return
	}
	for allocationID, start := range a.starts {
		for _, task := range start.tasks {
			if taskRecordID(allocationID, start.generation, task) == id {
				a.mu.Unlock()
				return
			}
		}
	}
	if a.orphanDetaches == nil {
		a.orphanDetaches = make(map[string]bool)
	}
	a.orphanDetaches[id] = true
	a.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			a.mu.Lock()
			delete(a.orphanDetaches, id)
			a.mu.Unlock()
		}()
		// Recheck durable ownership after reservation: the sweep's snapshot
		// may predate a start which has since finished or failed.
		if a.local != nil {
			records, errs := a.local.ListRaw("agent/allocations")
			if len(errs) != 0 {
				a.log.Error("skip orphaned network detach: allocation records are unreadable", "error", errors.Join(errs...))
				return
			}
			if _, owned := records[allocationFileName(id)]; owned {
				return
			}
		}
		if ctx.Err() != nil {
			return
		}
		if err := recovery.DetachAllocation(ctx, id); err != nil {
			a.log.Error("remove orphaned network attachment", "allocation", id, "error", err)
			return
		}
		a.log.Info("removed orphaned network attachment", "allocation", id)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func allocationNetworkAddress(allocation *Allocation) string {
	if allocation == nil || allocation.Network == nil {
		return ""
	}
	address := allocation.Network.Address
	if host, _, ok := strings.Cut(address, "/"); ok {
		return host
	}
	return address
}

func (a *Agent) lockNetworkPlan(ctx context.Context) (func(), error) {
	a.operationMu.Lock()
	if a.planOperation == nil {
		a.planOperation = make(chan struct{}, 1)
	}
	operation := a.planOperation
	a.operationMu.Unlock()
	select {
	case operation <- struct{}{}:
		return func() { <-operation }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// UpdateNetworkPlan refreshes the network shared by running allocations.
func (a *Agent) UpdateNetworkPlan(ctx context.Context, request *nodeapi.NetworkPlanRequest) error {
	unlock, err := a.lockNetworkPlan(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := a.AcceptEpoch(request.Epoch); err != nil {
		return err
	}
	active := false
	if err := func() error {
		a.mu.RLock()
		defer a.mu.RUnlock()
		if request.Epoch < a.epoch {
			return fmt.Errorf("%w: received %d, highest accepted %d", ErrStaleEpoch, request.Epoch, a.epoch)
		}
		var desiredCIDR netip.Prefix
		if request.Plan.CIDR != "" {
			var err error
			desiredCIDR, err = netip.ParsePrefix(request.Plan.CIDR)
			if err != nil {
				return fmt.Errorf("invalid network plan CIDR %q: %w", request.Plan.CIDR, err)
			}
			desiredCIDR = desiredCIDR.Masked()
		}
		for _, allocation := range a.allocations {
			if allocation.Namespace != request.Namespace || allocation.Network == nil {
				continue
			}
			active = true
			if request.Plan.Gateway != "" && allocation.Network.Gateway != "" && request.Plan.Gateway != allocation.Network.Gateway {
				return fmt.Errorf("network plan would change active namespace gateway from %s to %s", allocation.Network.Gateway, request.Plan.Gateway)
			}
			if desiredCIDR.IsValid() && allocation.Network.Address != "" {
				current, err := netip.ParsePrefix(allocation.Network.Address)
				if err != nil {
					return fmt.Errorf("invalid active network address %q: %w", allocation.Network.Address, err)
				}
				if current.Masked() != desiredCIDR {
					return fmt.Errorf("network plan would change active namespace CIDR from %s to %s", current.Masked(), desiredCIDR)
				}
			}
		}
		return nil
	}(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	if a.networkPlans == nil {
		a.networkPlans = make(map[string]network.Plan)
	}
	a.networkPlans[request.Namespace] = request.Plan
	a.mu.Unlock()
	// Retain the accepted desired topology even after a partial apply failure;
	// a delayed start must not roll it back while reconciliation retries.
	if active {
		if err := a.network.UpdatePlan(ctx, request.Namespace, request.Plan); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// The caller holds mu. Pending pulls and retained failed-cleanup records own
// the plan too; a recreated idle namespace must bootstrap from its new start.
func (a *Agent) forgetIdleNetworkPlanLocked(namespace string) {
	for _, allocation := range a.allocations {
		if allocation.Namespace == namespace && (allocation.Network != nil || allocation.NetworkIntent != nil) {
			return
		}
	}
	for _, start := range a.starts {
		if start.namespace == namespace && !start.finished() {
			return
		}
	}
	delete(a.networkPlans, namespace)
}
