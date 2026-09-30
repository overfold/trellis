// Package network manages allocation namespace networking.
package network

import "context"

// PeerPlan describes a WireGuard peer in a network plan.
type PeerPlan struct {
	PublicKey, Endpoint string
	AllowedIPs          []string
}

// Plan describes the network configuration for an allocation.
type Plan struct {
	CIDR, Gateway, WireGuardAddress string
	ListenPort                      int
	APIPort                         int
	Peers                           []PeerPlan
}

// PortMapping publishes HostPort on the node to ContainerPort at the
// allocation's namespace address.
type PortMapping struct {
	HostPort      int `json:"host_port"`
	ContainerPort int `json:"container_port"`
}

// AttachRequest contains the information needed to attach an allocation.
type AttachRequest struct {
	AllocationID, Namespace, Network string
	Plan                             Plan
	Ports                            []PortMapping
}

// Attachment records resources created for an allocation network.
type Attachment struct {
	AllocationID       string
	Namespace          string
	Network            string
	NetworkNamespace   string
	HostVeth           string
	Bridge             string
	WireGuardInterface string
	Gateway            string
	APIPort            int
	Address            string
	LeasePath          string
	Ports              []PortMapping
}

// Manager attaches and detaches allocation networks and reconciles peers on
// an already attached namespace network. Every node provides one.
type Manager interface {
	Attach(context.Context, AttachRequest) (*Attachment, error)
	Detach(context.Context, *Attachment) error
	UpdatePlan(context.Context, string, Plan) error
}

// AttachmentIntent is what an allocation records before Attach, so an
// attachment whose result was never recorded can still be found and removed.
type AttachmentIntent struct {
	AllocationID string
	Namespace    string
	Network      string
}

// AttachmentRecovery finds and removes attachments by allocation ID alone,
// such as those left behind when the agent stopped during Attach.
type AttachmentRecovery interface {
	// DetachAllocation removes everything Attach created for the
	// allocation. It succeeds when some or all of it is already gone.
	DetachAllocation(context.Context, string) error
	// Attachments lists the allocation IDs whose attachments may still
	// exist on this node. It may return IDs alongside an error that
	// describes records it could not read.
	Attachments(context.Context) ([]string, error)
}
