package spec

import "testing"

func TestGroupRequiredCapabilities(t *testing.T) {
	group := &TaskGroupSpec{Runtime: RuntimeRunsc, Tasks: []TaskSpec{{Networking: &TaskNetworkingSpec{Mode: TaskNetworkWireGuard}}}}
	got := GroupRequiredCapabilities(group)
	want := []NodeCapability{CapabilityRunsc, CapabilityNamespaceNetworking}
	if len(got) != len(want) {
		t.Fatalf("capabilities = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("capabilities = %v, want %v", got, want)
		}
	}
}
