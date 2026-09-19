package server

import (
	"bytes"
	"math"
	"slices"

	"github.com/clofour/trellis/internal/spec"
	"github.com/google/uuid"
)

// PlacementIntent describes an allocation placement request.
// Placement associates a task group index with a node.
type PlacementIntent struct {
	Namespace            string
	JobName              string
	TaskGroupName        string
	Count                int
	Nodes                []*Node
	Allocations          []*Allocation
	Tasks                []spec.TaskSpec
	Constraints          []spec.ConstraintSpec
	RequiredCapabilities []spec.NodeCapability
	// VolumeOwners maps namespace/name volume registrations to their owning node.
	// Schedule mutates the map when it places the first allocation for a volume.
	VolumeOwners map[string]uuid.UUID
}

// Placement associates a task group index with a selected node.
type Placement struct {
	TaskGroupName string
	NodeID        uuid.UUID
}

// Schedule selects placements for an intent.
func Schedule(intent *PlacementIntent) []Placement {
	result := make([]Placement, 0, intent.Count)

	nodes := slices.Clone(intent.Nodes)
	slices.SortFunc(nodes, func(a, b *Node) int {
		return bytes.Compare(a.ID[:], b.ID[:])
	})
	if intent.VolumeOwners == nil {
		intent.VolumeOwners = make(map[string]uuid.UUID)
	}

	replicaCounts := make(map[uuid.UUID]int)
	usedCPU := make(map[uuid.UUID]int)
	usedMemory := make(map[uuid.UUID]int64)
	for _, alloc := range intent.Allocations {
		if alloc.Node != nil {
			if alloc.Namespace == intent.Namespace && alloc.JobName == intent.JobName && alloc.TaskGroupName == intent.TaskGroupName {
				replicaCounts[alloc.Node.ID]++
			}
			for _, task := range alloc.Tasks {
				if task.Resources != nil {
					usedCPU[alloc.Node.ID] = saturatingAddInt(usedCPU[alloc.Node.ID], task.Resources.CPU)
					usedMemory[alloc.Node.ID] = saturatingAddInt64(usedMemory[alloc.Node.ID], int64(task.Resources.Memory))
				}
			}
		}
	}

	for i := 0; i < intent.Count; i++ {
		var target *Node
		var reqCPU int
		var reqMemory int64
		for _, task := range intent.Tasks {
			if task.Resources != nil {
				reqCPU = saturatingAddInt(reqCPU, task.Resources.CPU)
				reqMemory = saturatingAddInt64(reqMemory, int64(task.Resources.Memory))
			}
		}
		for _, node := range nodes {
			if node.Status != NodeStatusHealthy || !nodeMatchesConstraints(node, intent.Constraints) || !nodeHasTaskVolumes(node.ID, intent.Namespace, intent.Tasks, intent.VolumeOwners) || !nodeHasCapabilities(node, intent.RequiredCapabilities) {
				continue
			}
			if (node.CPU > 0 && saturatingAddInt(usedCPU[node.ID], reqCPU) > node.CPU) || (node.Memory > 0 && saturatingAddInt64(usedMemory[node.ID], reqMemory) > node.Memory) {
				continue
			}
			better := target == nil
			if target != nil {
				// Best fit compares normalized utilization rather than adding
				// incomparable CPU and byte units. Replica count is a soft
				// anti-affinity tiebreaker after the best-fit score.
				utilization := placementUtilization(node, usedCPU[node.ID]+reqCPU, usedMemory[node.ID]+reqMemory)
				targetUtilization := placementUtilization(target, usedCPU[target.ID]+reqCPU, usedMemory[target.ID]+reqMemory)
				better = utilization > targetUtilization ||
					utilization == targetUtilization && replicaCounts[node.ID] < replicaCounts[target.ID]
			}
			if better {
				target = node
			}
		}
		if target == nil {
			break
		}

		claimTaskVolumes(target.ID, intent.Namespace, intent.Tasks, intent.VolumeOwners)
		result = append(result, Placement{
			TaskGroupName: intent.TaskGroupName,
			NodeID:        target.ID,
		})
		replicaCounts[target.ID]++
		usedCPU[target.ID] = saturatingAddInt(usedCPU[target.ID], reqCPU)
		usedMemory[target.ID] = saturatingAddInt64(usedMemory[target.ID], reqMemory)
	}

	return result
}

func saturatingAddInt(a, b int) int {
	if b > 0 && a > math.MaxInt-b {
		return math.MaxInt
	}
	return a + b
}

func saturatingAddInt64(a, b int64) int64 {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
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

func claimTaskVolumes(nodeID uuid.UUID, namespace string, tasks []spec.TaskSpec, owners map[string]uuid.UUID) {
	for _, task := range tasks {
		for _, volume := range task.Volumes {
			key := volumeRegistrationKey(namespace, volume.Name)
			if _, ok := owners[key]; !ok {
				owners[key] = nodeID
			}
		}
	}
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

func placementUtilization(node *Node, cpu int, memory int64) float64 {
	var cpuRatio, memoryRatio float64
	if node.CPU > 0 {
		cpuRatio = float64(cpu) / float64(node.CPU)
	}
	if node.Memory > 0 {
		memoryRatio = float64(memory) / float64(node.Memory)
	}
	return max(cpuRatio, memoryRatio)
}
