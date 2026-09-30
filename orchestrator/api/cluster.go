package api

import "time"

// ClusterSettings are the replicated cluster-wide semantics every leader
// applies. Memory values are canonical byte counts and durations are
// nanoseconds.
type ClusterSettings struct {
	JobLimits      JobLimits              `json:"job_limits"`
	Reconciliation ReconciliationSettings `json:"reconciliation"`
	Network        ClusterNetworkSettings `json:"network"`
}

// JobLimits bound job admission and set task resource defaults. CPU values
// are millicores and memory values are byte counts.
type JobLimits struct {
	MaxReplicasPerTaskGroup           int   `json:"max_replicas_per_task_group"`
	MaxTaskGroupsPerJob               int   `json:"max_task_groups_per_job"`
	MaxTasksPerTaskGroup              int   `json:"max_tasks_per_task_group"`
	MaxDesiredAllocations             int   `json:"max_desired_allocations"`
	MaxDesiredAllocationsPerNamespace int   `json:"max_desired_allocations_per_namespace"`
	DefaultTaskCPU                    int   `json:"default_task_cpu"`
	DefaultTaskMemory                 int64 `json:"default_task_memory"`
	MaxTaskCPU                        int   `json:"max_task_cpu"`
	MaxTaskMemory                     int64 `json:"max_task_memory"`
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
