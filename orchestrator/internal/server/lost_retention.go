package server

import (
	"slices"
	"sort"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/lifecycle"
)

// retainedOriginal is a lost allocation whose returning node still reports its
// container running. Lost stays terminal: the allocation never counts toward
// its group again. Its container is only left running as a bridge until the
// group has enough running replacements, and it never blocks a replacement:
// reconciliation releases (stops) it as soon as it would.
type retainedOriginal struct {
	allocation *Allocation
	node       *Node
	kept       bool
	released   bool
}

// sortRetainedOriginals orders retained originals by allocation ID so every
// decision about them is independent of node map iteration order.
func sortRetainedOriginals(candidates []*retainedOriginal) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].allocation.ID == candidates[j].allocation.ID {
			return candidates[i].allocation.Generation < candidates[j].allocation.Generation
		}
		return candidates[i].allocation.ID < candidates[j].allocation.ID
	})
}

// release marks the original as no longer retained and returns the action
// that stops its container.
func (r *retainedOriginal) release() Action {
	r.released = true
	return r.stopAction()
}

func (r *retainedOriginal) stopAction() Action {
	return Action{Type: ActionStopObserved, Node: r.node, ID: r.allocation.ID, Generation: r.allocation.Generation}
}

func (r *retainedOriginal) inGroup(namespace, jobName, groupName string) bool {
	return r.allocation.Namespace == namespace && r.allocation.JobName == jobName && r.allocation.TaskGroupName == groupName
}

// allocationHostPorts returns the host ports an allocation's containers hold
// on its node, using the same sources as Schedule.
func allocationHostPorts(allocation *Allocation) map[int]bool {
	ports := make(map[int]bool)
	for _, port := range allocation.Ports {
		if port.HostPort > 0 {
			ports[port.HostPort] = true
		}
	}
	for _, task := range allocation.Tasks {
		if task.Networking == nil {
			continue
		}
		for _, port := range task.Networking.Ports {
			if nodePort := port.NodePort(); nodePort > 0 {
				ports[nodePort] = true
			}
		}
	}
	return ports
}

// blocksPlacedAllocation reports whether the retained container holds a host
// port that an allocation already placed on the same node needs. Such an
// allocation cannot start while the original runs, so keeping the original
// would deadlock the group.
func (r *retainedOriginal) blocksPlacedAllocation(placed []*Allocation) bool {
	held := allocationHostPorts(r.allocation)
	if len(held) == 0 {
		return false
	}
	for _, allocation := range placed {
		if allocation.Node == nil || allocation.Node.ID != r.node.ID || allocation.Phase == lifecycle.PhasePending {
			continue
		}
		for port := range allocationHostPorts(allocation) {
			if held[port] {
				return true
			}
		}
	}
	return false
}

func unreleasedAllocations(retained []*retainedOriginal) []*Allocation {
	var result []*Allocation
	for _, original := range retained {
		if !original.released {
			result = append(result, original.allocation)
		}
	}
	return result
}

// scheduleAroundRetained places a group's deficit while treating retained
// originals as occupying their node's ports, CPU, and memory, so replacements
// prefer nodes where they can start alongside them. Retained originals never
// count as replicas for spreading. When the retained originals are the only
// reason fewer replacements fit — for example the group is pinned to the
// original's node by a host volume and needs the same host port — the
// blocking originals are released, same-group originals first, so the
// replacement can take their place.
//
// Neither the intent nor any allocation is mutated. It returns the
// placements and the originals it released.
func scheduleAroundRetained(intent PlacementIntent, retained []*retainedOriginal) ([]Placement, []*retainedOriginal) {
	schedule := func(held []*Allocation) []Placement {
		candidate := intent
		candidate.Allocations = slices.Concat(intent.Allocations, held)
		return Schedule(&candidate)
	}
	var released []*retainedOriginal
	if held := unreleasedAllocations(retained); len(held) > 0 && intent.Count > 0 {
		withHeld := len(schedule(held))
		if withHeld < intent.Count {
			unblocked := schedule(nil)
			if len(unblocked) > withHeld {
				targets := make(map[uuid.UUID]bool, len(unblocked))
				for _, placement := range unblocked {
					targets[placement.NodeID] = true
				}
				var candidates []*retainedOriginal
				for _, original := range retained {
					if !original.released && targets[original.node.ID] {
						candidates = append(candidates, original)
					}
				}
				sort.SliceStable(candidates, func(i, j int) bool {
					return candidates[i].inGroup(intent.Namespace, intent.JobName, intent.TaskGroupName) &&
						!candidates[j].inGroup(intent.Namespace, intent.JobName, intent.TaskGroupName)
				})
				for _, original := range candidates {
					original.released = true
					released = append(released, original)
					if len(schedule(unreleasedAllocations(retained))) >= len(unblocked) {
						break
					}
				}
			}
		}
	}
	return schedule(unreleasedAllocations(retained)), released
}
