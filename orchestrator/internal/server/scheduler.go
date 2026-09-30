package server

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/spec"
)

// PlacementIntent describes an allocation placement request.
// Placement associates a task group index with a node.
type PlacementIntent struct {
	Namespace     string
	JobName       string
	TaskGroupName string
	Count         int
	Nodes         []*Node
	// Allocations contains every allocation occupying node resources or ports.
	Allocations []*Allocation
	// DesiredAllocations contains allocations counted for replica spreading.
	// Only placed, non-draining allocations of the intent's task group count.
	DesiredAllocations   []*Allocation
	Tasks                []spec.TaskSpec
	Constraints          []spec.ConstraintSpec
	RequiredCapabilities []spec.NodeCapability
	// VolumeOwners maps namespace/name volume registrations to their owning node.
	VolumeOwners map[string]uuid.UUID
}

// Placement associates a task group index with a selected node.
type Placement struct {
	TaskGroupName string
	NodeID        uuid.UUID
	VolumeClaims  []VolumeClaim
}

// VolumeClaim binds a previously unowned namespace-scoped volume to the node
// selected for its first placement.
type VolumeClaim struct {
	Namespace string
	Name      string
	NodeID    uuid.UUID
}

// placementDiagnostic explains why Schedule could not satisfy an intent.
// It describes the deepest placement filter reached by at least one healthy
// node, so the reason remains stable while the same eligibility facts hold.
type placementDiagnostic struct {
	Reason  string
	Message string
}

// Schedule selects placements for an intent without mutating it.
func Schedule(intent *PlacementIntent) []Placement {
	placements, _ := schedule(intent)
	return placements
}

// schedule selects placements and explains any unsatisfied count without
// changing the placement decision itself.
func schedule(intent *PlacementIntent) ([]Placement, *placementDiagnostic) {
	result := make([]Placement, 0, intent.Count)

	nodes := slices.Clone(intent.Nodes)
	slices.SortFunc(nodes, func(a, b *Node) int {
		return bytes.Compare(a.ID[:], b.ID[:])
	})
	volumeOwners := make(map[string]uuid.UUID, len(intent.VolumeOwners))
	for key, owner := range intent.VolumeOwners {
		volumeOwners[key] = owner
	}

	replicaCounts := make(map[uuid.UUID]int)
	usedCPU := make(map[uuid.UUID]int)
	usedCPUOverflow := make(map[uuid.UUID]bool)
	usedMemory := make(map[uuid.UUID]int64)
	usedMemoryOverflow := make(map[uuid.UUID]bool)
	usedPorts := make(map[uuid.UUID]map[int]bool)
	for _, alloc := range intent.DesiredAllocations {
		// A draining replica is being replaced, so it does not hold a spread
		// slot; its resources and ports still count through Allocations.
		if alloc.Node != nil && !alloc.Draining && alloc.Namespace == intent.Namespace && alloc.JobName == intent.JobName && alloc.TaskGroupName == intent.TaskGroupName {
			replicaCounts[alloc.Node.ID]++
		}
	}
	for _, alloc := range intent.Allocations {
		if alloc.Node != nil {
			if usedPorts[alloc.Node.ID] == nil {
				usedPorts[alloc.Node.ID] = make(map[int]bool)
			}
			for _, port := range alloc.Ports {
				if port.HostPort > 0 {
					usedPorts[alloc.Node.ID][port.HostPort] = true
				}
			}
			for _, task := range alloc.Tasks {
				if task.Networking != nil {
					for _, port := range task.Networking.Ports {
						if port.Port > 0 {
							usedPorts[alloc.Node.ID][port.Port] = true
						}
					}
				}
				if task.Resources != nil {
					if !usedCPUOverflow[alloc.Node.ID] {
						usedCPU[alloc.Node.ID], usedCPUOverflow[alloc.Node.ID] = checkedAddInt(usedCPU[alloc.Node.ID], task.Resources.CPU)
					}
					if !usedMemoryOverflow[alloc.Node.ID] {
						usedMemory[alloc.Node.ID], usedMemoryOverflow[alloc.Node.ID] = checkedAddInt64(usedMemory[alloc.Node.ID], int64(task.Resources.Memory))
					}
				}
			}
		}
	}
	requestedPorts := make(map[int]bool)
	for _, task := range intent.Tasks {
		if task.Networking != nil {
			for _, port := range task.Networking.Ports {
				if port.Port > 0 {
					requestedPorts[port.Port] = true
				}
			}
		}
	}

	var reqCPU int
	var reqCPUOverflow bool
	var reqMemory int64
	var reqMemoryOverflow bool
	for _, task := range intent.Tasks {
		if task.Resources != nil {
			if !reqCPUOverflow {
				reqCPU, reqCPUOverflow = checkedAddInt(reqCPU, task.Resources.CPU)
			}
			if !reqMemoryOverflow {
				reqMemory, reqMemoryOverflow = checkedAddInt64(reqMemory, int64(task.Resources.Memory))
			}
		}
	}

	for i := 0; i < intent.Count; i++ {
		var target *Node
		for _, node := range nodes {
			if node.Status != NodeStatusHealthy || !nodeMatchesConstraints(node, intent.Constraints) || !nodeHasTaskVolumes(node.ID, intent.Namespace, intent.Tasks, volumeOwners) || !nodeHasCapabilities(node, intent.RequiredCapabilities) {
				continue
			}
			portsAvailable := true
			for port := range requestedPorts {
				if usedPorts[node.ID][port] {
					portsAvailable = false
					break
				}
			}
			if !portsAvailable {
				continue
			}
			if !fitsIntCapacity(node.CPUAllocatable, usedCPU[node.ID], usedCPUOverflow[node.ID], reqCPU, reqCPUOverflow) || !fitsInt64Capacity(node.MemoryAllocatable, usedMemory[node.ID], usedMemoryOverflow[node.ID], reqMemory, reqMemoryOverflow) {
				continue
			}
			better := target == nil
			if target != nil {
				// Spreading replicas of the group across eligible nodes comes
				// first. Among nodes with equally few replicas, best fit
				// compares normalized utilization rather than adding
				// incomparable CPU and byte units. Nodes are visited in ID
				// order and only a strictly better node replaces the target,
				// so the lowest node ID wins any remaining tie.
				if replicaCounts[node.ID] != replicaCounts[target.ID] {
					better = replicaCounts[node.ID] < replicaCounts[target.ID]
				} else {
					utilization := placementUtilization(node, usedCPU[node.ID], reqCPU, usedMemory[node.ID], reqMemory)
					targetUtilization := placementUtilization(target, usedCPU[target.ID], reqCPU, usedMemory[target.ID], reqMemory)
					better = utilization > targetUtilization
				}
			}
			if better {
				target = node
			}
		}
		if target == nil {
			return result, diagnosePlacement(nodes, intent, volumeOwners, usedPorts, usedCPU, usedCPUOverflow, usedMemory, usedMemoryOverflow, requestedPorts, reqCPU, reqCPUOverflow, reqMemory, reqMemoryOverflow)
		}

		volumeClaims := claimTaskVolumes(target.ID, intent.Namespace, intent.Tasks, volumeOwners)
		result = append(result, Placement{
			TaskGroupName: intent.TaskGroupName,
			NodeID:        target.ID,
			VolumeClaims:  volumeClaims,
		})
		replicaCounts[target.ID]++
		if !usedCPUOverflow[target.ID] {
			usedCPU[target.ID], usedCPUOverflow[target.ID] = checkedAddInt(usedCPU[target.ID], reqCPU)
			usedCPUOverflow[target.ID] = usedCPUOverflow[target.ID] || reqCPUOverflow
		}
		if !usedMemoryOverflow[target.ID] {
			usedMemory[target.ID], usedMemoryOverflow[target.ID] = checkedAddInt64(usedMemory[target.ID], reqMemory)
			usedMemoryOverflow[target.ID] = usedMemoryOverflow[target.ID] || reqMemoryOverflow
		}
		if usedPorts[target.ID] == nil {
			usedPorts[target.ID] = make(map[int]bool)
		}
		for port := range requestedPorts {
			usedPorts[target.ID][port] = true
		}
	}

	return result, nil
}

func diagnosePlacement(nodes []*Node, intent *PlacementIntent, volumeOwners map[string]uuid.UUID, usedPorts map[uuid.UUID]map[int]bool, usedCPU map[uuid.UUID]int, usedCPUOverflow map[uuid.UUID]bool, usedMemory map[uuid.UUID]int64, usedMemoryOverflow map[uuid.UUID]bool, requestedPorts map[int]bool, reqCPU int, reqCPUOverflow bool, reqMemory int64, reqMemoryOverflow bool) *placementDiagnostic {
	candidates := make([]*Node, 0, len(nodes))
	for _, node := range nodes {
		if node.Status == NodeStatusHealthy {
			candidates = append(candidates, node)
		}
	}
	if len(candidates) == 0 {
		return &placementDiagnostic{Reason: "no_healthy_nodes", Message: "no healthy nodes are available for placement"}
	}
	candidates = filterNodes(candidates, func(node *Node) bool { return nodeMatchesConstraints(node, intent.Constraints) })
	if len(candidates) == 0 {
		return &placementDiagnostic{Reason: "constraint_mismatch", Message: "no healthy node satisfies the task group constraints"}
	}
	candidates = filterNodes(candidates, func(node *Node) bool {
		return nodeHasTaskVolumes(node.ID, intent.Namespace, intent.Tasks, volumeOwners)
	})
	if len(candidates) == 0 {
		return &placementDiagnostic{Reason: "volume_owner_unavailable", Message: "no eligible node owns all required volumes"}
	}
	candidates = filterNodes(candidates, func(node *Node) bool { return nodeHasCapabilities(node, intent.RequiredCapabilities) })
	if len(candidates) == 0 {
		return &placementDiagnostic{Reason: "missing_capability", Message: fmt.Sprintf("no eligible node supports required capabilities: %s", strings.Join(capabilityNames(intent.RequiredCapabilities), ", "))}
	}
	candidates = filterNodes(candidates, func(node *Node) bool {
		for port := range requestedPorts {
			if usedPorts[node.ID][port] {
				return false
			}
		}
		return true
	})
	if len(candidates) == 0 {
		ports := make([]int, 0, len(requestedPorts))
		for port := range requestedPorts {
			ports = append(ports, port)
		}
		sort.Ints(ports)
		values := make([]string, len(ports))
		for i, port := range ports {
			values[i] = fmt.Sprint(port)
		}
		return &placementDiagnostic{Reason: "host_port_conflict", Message: fmt.Sprintf("requested host ports are unavailable on eligible nodes: %s", strings.Join(values, ", "))}
	}
	candidates = filterNodes(candidates, func(node *Node) bool {
		return fitsIntCapacity(node.CPUAllocatable, usedCPU[node.ID], usedCPUOverflow[node.ID], reqCPU, reqCPUOverflow) &&
			fitsInt64Capacity(node.MemoryAllocatable, usedMemory[node.ID], usedMemoryOverflow[node.ID], reqMemory, reqMemoryOverflow)
	})
	if len(candidates) == 0 {
		return &placementDiagnostic{Reason: "insufficient_capacity", Message: "eligible nodes lack the requested CPU or memory capacity"}
	}
	panic("placement diagnosis found an eligible node")
}

func filterNodes(nodes []*Node, keep func(*Node) bool) []*Node {
	result := make([]*Node, 0, len(nodes))
	for _, node := range nodes {
		if keep(node) {
			result = append(result, node)
		}
	}
	return result
}

func checkedAddInt(a, b int) (int, bool) {
	if b > 0 && a > math.MaxInt-b || b < 0 && a < math.MinInt-b {
		return 0, true
	}
	return a + b, false
}

func checkedAddInt64(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b || b < 0 && a < math.MinInt64-b {
		return 0, true
	}
	return a + b, false
}

func fitsIntCapacity(capacity, used int, usedOverflow bool, requested int, requestedOverflow bool) bool {
	return capacity <= 0 || !usedOverflow && !requestedOverflow && used <= capacity && requested <= capacity-used
}

func fitsInt64Capacity(capacity, used int64, usedOverflow bool, requested int64, requestedOverflow bool) bool {
	return capacity <= 0 || !usedOverflow && !requestedOverflow && used <= capacity && requested <= capacity-used
}

func nodeHasCapabilities(node *Node, required []spec.NodeCapability) bool {
	for _, capability := range required {
		if !slices.Contains(node.Capabilities, capability) {
			return false
		}
	}
	return true
}

func volumeRegistrationKey(namespace, name string) string { return namespace + "/" + name }

func nodeHasTaskVolumes(nodeID uuid.UUID, namespace string, tasks []spec.TaskSpec, owners map[string]uuid.UUID) bool {
	for _, task := range tasks {
		for _, volume := range task.Volumes {
			if owner, ok := owners[volumeRegistrationKey(namespace, volume.Name)]; ok && owner != nodeID {
				return false
			}
		}
	}
	return true
}

func claimTaskVolumes(nodeID uuid.UUID, namespace string, tasks []spec.TaskSpec, owners map[string]uuid.UUID) []VolumeClaim {
	var claims []VolumeClaim
	for _, task := range tasks {
		for _, volume := range task.Volumes {
			key := volumeRegistrationKey(namespace, volume.Name)
			if _, ok := owners[key]; !ok {
				owners[key] = nodeID
				claims = append(claims, VolumeClaim{Namespace: namespace, Name: volume.Name, NodeID: nodeID})
			}
		}
	}
	return claims
}

func nodeMatchesConstraints(node *Node, constraints []spec.ConstraintSpec) bool {
	for _, constraint := range constraints {
		var value string
		switch constraint.Attribute {
		case "os":
			value = node.OS
		case "arch":
			value = node.Arch
		default:
			value = node.Labels[constraint.Attribute]
		}
		if value != constraint.Value {
			return false
		}
	}
	return true
}

func placementUtilization(node *Node, usedCPU, requestedCPU int, usedMemory, requestedMemory int64) float64 {
	var cpuRatio, memoryRatio float64
	if node.CPUAllocatable > 0 {
		cpu, overflow := checkedAddInt(usedCPU, requestedCPU)
		if overflow {
			cpuRatio = math.Inf(1)
		} else {
			cpuRatio = float64(cpu) / float64(node.CPUAllocatable)
		}
	}
	if node.MemoryAllocatable > 0 {
		memory, overflow := checkedAddInt64(usedMemory, requestedMemory)
		if overflow {
			memoryRatio = math.Inf(1)
		} else {
			memoryRatio = float64(memory) / float64(node.MemoryAllocatable)
		}
	}
	return max(cpuRatio, memoryRatio)
}
