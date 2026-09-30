package agent

import (
	"context"
	"errors"

	"github.com/overfold/trellis/internal/network"
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
		if _, owned := records[allocationFileName(id)]; owned {
			continue
		}
		a.detachOrphanedNetwork(ctx, recovery, id)
	}
}

// detachOrphanedNetwork detaches an unowned attachment. It holds the
// allocation lock so a start cannot register the same allocation between the
// ownership check and the detach.
func (a *Agent) detachOrphanedNetwork(ctx context.Context, recovery network.AttachmentRecovery, id string) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.allocations[id] != nil {
		return
	}
	if err := recovery.DetachAllocation(ctx, id); err != nil {
		a.log.Error("remove orphaned network attachment", "allocation", id, "error", err)
		return
	}
	a.log.Info("removed orphaned network attachment", "allocation", id)
}
