package spec

// NodeCapability identifies an optional feature a node can execute.
// Capabilities are discovered by the node; workloads derive their requirements
// from their existing runtime configuration.
type NodeCapability string

const (
	// CapabilityRunsc indicates support for the gVisor OCI runtime.
	CapabilityRunsc NodeCapability = "runtime.runsc"
)

// GroupUsesWireGuard reports whether any task in a group requests the namespace WireGuard network.
func GroupUsesWireGuard(group *TaskGroupSpec) bool {
	if group == nil {
		return false
	}
	for i := range group.Tasks {
		if group.Tasks[i].Networking != nil && group.Tasks[i].Networking.Mode == TaskNetworkWireGuard {
			return true
		}
	}
	return false
}

// GroupRequiredCapabilities derives the node features needed by a task group.
// Every node provides namespace networking, so only the runtime can require
// a capability.
func GroupRequiredCapabilities(group *TaskGroupSpec) []NodeCapability {
	if group == nil {
		return nil
	}
	var capabilities []NodeCapability
	if group.Runtime == RuntimeRunsc {
		capabilities = append(capabilities, CapabilityRunsc)
	}
	return capabilities
}
