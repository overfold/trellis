package api

import (
	"time"

	"github.com/overfold/trellis/internal/spec"
)

// ClusterSettings are the replicated cluster-wide semantics every leader
// applies. Memory values are canonical byte counts and durations are
// nanoseconds.
type ClusterSettings struct {
	JobLimits      spec.Limits            `json:"job_limits"`
	Reconciliation ReconciliationSettings `json:"reconciliation"`
	Network        ClusterNetworkSettings `json:"network"`
}

// ReconciliationSettings tune how the leader reacts to lost and failed
// allocations. Durations are nanoseconds.
type ReconciliationSettings struct {
	AllocationLossTimeout       time.Duration `json:"allocation_loss_timeout"`
	ReplacementBackoffBase      time.Duration `json:"replacement_backoff_base"`
	ReplacementBackoffMax       time.Duration `json:"replacement_backoff_max"`
	ReplacementStableAfter      time.Duration `json:"replacement_stable_after"`
	TerminalAllocationRetention int           `json:"terminal_allocation_retention"`
}

// ClusterNetworkSettings describe namespace networking. They are fixed when
// the cluster is created.
type ClusterNetworkSettings struct {
	WireGuardPool      string `json:"wireguard_pool"`
	WireGuardPortCount int    `json:"wireguard_port_count"`
}
