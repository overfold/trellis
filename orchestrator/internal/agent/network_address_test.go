package agent

import (
	"testing"

	"github.com/clofour/trellis/internal/network"
)

func TestAllocationNetworkAddress(t *testing.T) {
	if got := allocationNetworkAddress(nil); got != "" {
		t.Fatalf("nil allocation address = %q, want empty", got)
	}
	if got := allocationNetworkAddress(&Allocation{}); got != "" {
		t.Fatalf("host allocation address = %q, want empty", got)
	}
	allocation := &Allocation{Network: &network.Attachment{Address: "10.86.213.2/24"}}
	if got := allocationNetworkAddress(allocation); got != "10.86.213.2" {
		t.Fatalf("namespace allocation address = %q, want 10.86.213.2", got)
	}
}
