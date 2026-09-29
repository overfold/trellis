package api

import "github.com/overfold/trellis/internal/spec"

// ClusterSettings are the replicated cluster-wide semantics every leader
// applies. Memory values are canonical byte counts.
type ClusterSettings struct {
	JobLimits spec.Limits            `json:"job_limits"`
	Network   ClusterNetworkSettings `json:"network"`
}

// ClusterNetworkSettings describe namespace networking. They are fixed when
// the cluster is created.
type ClusterNetworkSettings struct {
	WireGuardPool      string `json:"wireguard_pool"`
	WireGuardPortCount int    `json:"wireguard_port_count"`
}
