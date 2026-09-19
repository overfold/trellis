package agent

import (
	"testing"

	"github.com/clofour/trellis/internal/network"
)

func TestAllocationObservedAddressUsesNamespaceAttachment(t *testing.T) {
	allocation := &Allocation{Network: &network.Attachment{Address: "10.86.213.2/24"}}
	if got := allocationObservedAddress(allocation); got != "10.86.213.2" {
		t.Fatalf("allocationObservedAddress() = %q, want 10.86.213.2", got)
	}
}

func TestAllocationObservedAddressRejectsMissingOrInvalidAttachment(t *testing.T) {
	for _, allocation := range []*Allocation{
		nil,
		{},
		{Network: &network.Attachment{}},
		{Network: &network.Attachment{Address: "not-a-prefix"}},
	} {
		if got := allocationObservedAddress(allocation); got != "" {
			t.Fatalf("allocationObservedAddress(%#v) = %q, want empty", allocation, got)
		}
	}
}
