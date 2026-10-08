package server

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"time"

	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// DefaultWireGuardPool is the namespace address pool of a cluster created
// without an explicit pool.
const DefaultWireGuardPool = "10.64.0.0/10"

// DefaultWireGuardPortCount is the number of namespace WireGuard ports of a
// cluster created without an explicit count.
const DefaultWireGuardPortCount = 256

var (
	// ErrInvalidClusterSettings reports settings that fail validation.
	ErrInvalidClusterSettings = errors.New("invalid cluster settings")
	// ErrClusterSettingsConflict reports a settings change that the current
	// replicated state cannot accept.
	ErrClusterSettingsConflict = errors.New("cluster settings conflict")
)

// ClusterSettings are cluster-wide semantics that must not depend on which
// node is leader. They live in the replicated cluster record: the node that
// creates the cluster supplies them once, and job limits and reconciliation
// settings change afterwards only through administrator-authorized updates.
// Network settings are fixed at creation because every namespace subnet and
// WireGuard port slot is assigned from them.
type ClusterSettings struct {
	JobLimits          spec.Limits            `json:"job_limits"`
	Reconciliation     ReconciliationSettings `json:"reconciliation"`
	WireGuardPool      netip.Prefix           `json:"wireguard_pool"`
	WireGuardPortCount int                    `json:"wireguard_port_count"`
}

// DefaultClusterSettings returns the settings of a cluster created without
// explicit values.
func DefaultClusterSettings() ClusterSettings {
	return ClusterSettings{
		JobLimits:          spec.DefaultLimits(),
		Reconciliation:     DefaultReconciliationSettings(),
		WireGuardPool:      netip.MustParsePrefix(DefaultWireGuardPool),
		WireGuardPortCount: DefaultWireGuardPortCount,
	}
}

// ReconciliationSettings tune how the leader reacts to lost and failed
// allocations. Every leader must apply the same values, so they are
// replicated cluster settings rather than node configuration.
type ReconciliationSettings struct {
	// AllocationLossTimeout is how long a node may go without a heartbeat
	// before the leader marks its allocations lost.
	AllocationLossTimeout time.Duration `json:"allocation_loss_timeout"`
	// ReplacementBackoffBase is the replacement delay after the first
	// consecutive failure of a task group.
	ReplacementBackoffBase time.Duration `json:"replacement_backoff_base"`
	// ReplacementBackoffMax caps the exponential replacement delay.
	ReplacementBackoffMax time.Duration `json:"replacement_backoff_max"`
	// ReplacementStableAfter is how long a replacement placed after the latest
	// failure must run, and not be unhealthy, before the failure count resets.
	ReplacementStableAfter time.Duration `json:"replacement_stable_after"`
	// TerminalAllocationRetention is how many stopped, failed, or lost
	// allocation records are kept per job task group.
	TerminalAllocationRetention int `json:"terminal_allocation_retention"`
}

// Bounds of the reconciliation settings.
const (
	// DefaultAllocationLossTimeout is the loss timeout of a new cluster.
	DefaultAllocationLossTimeout = 45 * time.Second
	// MinAllocationLossTimeout keeps the loss timeout at or above the point
	// where a silent node is marked unhealthy (three heartbeat intervals).
	MinAllocationLossTimeout = 3 * heartbeatInterval
	// MaxAllocationLossTimeout bounds the loss timeout.
	MaxAllocationLossTimeout = 24 * time.Hour
	// MinReplacementBackoff bounds the replacement backoff base from below
	// so a crash-looping group cannot be replaced in a tight loop.
	MinReplacementBackoff = time.Second
	// MaxReplacementBackoff bounds the replacement backoff base and maximum.
	MaxReplacementBackoff = 24 * time.Hour
	// MinReplacementStableAfter bounds the stability period from below.
	MinReplacementStableAfter = 10 * time.Second
	// MaxReplacementStableAfter bounds the stability period from above.
	MaxReplacementStableAfter = 24 * time.Hour
	// MaxTerminalAllocationRetention bounds retained terminal records per
	// task group so history cannot grow the replicated state without bound.
	MaxTerminalAllocationRetention = 100
)

// DefaultReconciliationSettings returns the reconciliation settings of a new
// cluster.
func DefaultReconciliationSettings() ReconciliationSettings {
	return ReconciliationSettings{
		AllocationLossTimeout:       DefaultAllocationLossTimeout,
		ReplacementBackoffBase:      10 * time.Second,
		ReplacementBackoffMax:       5 * time.Minute,
		ReplacementStableAfter:      10 * time.Minute,
		TerminalAllocationRetention: 5,
	}
}

// Validate checks that every reconciliation setting is within its bounds.
func (r ReconciliationSettings) Validate() error {
	if r.AllocationLossTimeout < MinAllocationLossTimeout || r.AllocationLossTimeout > MaxAllocationLossTimeout {
		return fmt.Errorf("allocation_loss_timeout %s must be between %s and %s", r.AllocationLossTimeout, MinAllocationLossTimeout, MaxAllocationLossTimeout)
	}
	if r.ReplacementBackoffBase < MinReplacementBackoff || r.ReplacementBackoffBase > MaxReplacementBackoff {
		return fmt.Errorf("replacement_backoff_base %s must be between %s and %s", r.ReplacementBackoffBase, MinReplacementBackoff, MaxReplacementBackoff)
	}
	if r.ReplacementBackoffMax < r.ReplacementBackoffBase || r.ReplacementBackoffMax > MaxReplacementBackoff {
		return fmt.Errorf("replacement_backoff_max %s must be between replacement_backoff_base (%s) and %s", r.ReplacementBackoffMax, r.ReplacementBackoffBase, MaxReplacementBackoff)
	}
	if r.ReplacementStableAfter < MinReplacementStableAfter || r.ReplacementStableAfter > MaxReplacementStableAfter {
		return fmt.Errorf("replacement_stable_after %s must be between %s and %s", r.ReplacementStableAfter, MinReplacementStableAfter, MaxReplacementStableAfter)
	}
	if r.TerminalAllocationRetention < 0 || r.TerminalAllocationRetention > MaxTerminalAllocationRetention {
		return fmt.Errorf("terminal_allocation_retention %d must be between 0 and %d", r.TerminalAllocationRetention, MaxTerminalAllocationRetention)
	}
	return nil
}

// replacementPolicy returns the replacement parameters the planner applies.
func (r ReconciliationSettings) replacementPolicy() ReplacementPolicy {
	return ReplacementPolicy{
		BackoffBase:    r.ReplacementBackoffBase,
		BackoffMax:     r.ReplacementBackoffMax,
		StableAfter:    r.ReplacementStableAfter,
		RetainTerminal: r.TerminalAllocationRetention,
	}
}

// ParseWireGuardPool parses a namespace address pool.
func ParseWireGuardPool(pool string) (netip.Prefix, error) {
	prefix, err := netip.ParsePrefix(pool)
	if err != nil || !validWireGuardPool(prefix) {
		return netip.Prefix{}, fmt.Errorf("WireGuard pool must be an IPv4 prefix of /16 or larger")
	}
	return prefix.Masked(), nil
}

func validWireGuardPool(prefix netip.Prefix) bool {
	return prefix.IsValid() && prefix.Addr().Is4() && prefix.Bits() <= 16
}

// ValidateWireGuardPortCount checks the number of namespace WireGuard ports.
func ValidateWireGuardPortCount(count int) error {
	if count < 1 || count > 65535 {
		return fmt.Errorf("WireGuard port count must be between 1 and 65535")
	}
	return nil
}

// Validate checks that the settings are complete and usable.
func (c ClusterSettings) Validate() error {
	if err := spec.ValidateLimits(c.JobLimits); err != nil {
		return fmt.Errorf("job limits: %w", err)
	}
	if err := c.Reconciliation.Validate(); err != nil {
		return fmt.Errorf("reconciliation: %w", err)
	}
	if !validWireGuardPool(c.WireGuardPool) || c.WireGuardPool != c.WireGuardPool.Masked() {
		return fmt.Errorf("WireGuard pool must be a masked IPv4 prefix of /16 or larger")
	}
	return ValidateWireGuardPortCount(c.WireGuardPortCount)
}

// API returns the wire representation of the settings.
func (c ClusterSettings) API() api.ClusterSettings {
	return api.ClusterSettings{
		JobLimits:      JobLimitsAPI(c.JobLimits),
		Reconciliation: c.Reconciliation.API(),
		Network: api.ClusterNetworkSettings{
			WireGuardPool:      c.WireGuardPool.String(),
			WireGuardPortCount: c.WireGuardPortCount,
		},
	}
}

// JobLimitsAPI returns the wire representation of job limits.
func JobLimitsAPI(limits spec.Limits) api.JobLimits {
	return api.JobLimits{
		MaxReplicasPerTaskGroup:           limits.MaxReplicasPerTaskGroup,
		MaxTaskGroupsPerJob:               limits.MaxTaskGroupsPerJob,
		MaxTasksPerTaskGroup:              limits.MaxTasksPerTaskGroup,
		MaxDesiredAllocations:             limits.MaxDesiredAllocations,
		MaxDesiredAllocationsPerNamespace: limits.MaxDesiredAllocationsPerNamespace,
		DefaultTaskCPU:                    limits.DefaultTaskCPU,
		DefaultTaskMemory:                 int64(limits.DefaultTaskMemory),
		MaxTaskCPU:                        limits.MaxTaskCPU,
		MaxTaskMemory:                     int64(limits.MaxTaskMemory),
	}
}

// JobLimitsFromAPI converts wire job limits.
func JobLimitsFromAPI(limits api.JobLimits) spec.Limits {
	return spec.Limits{
		MaxReplicasPerTaskGroup:           limits.MaxReplicasPerTaskGroup,
		MaxTaskGroupsPerJob:               limits.MaxTaskGroupsPerJob,
		MaxTasksPerTaskGroup:              limits.MaxTasksPerTaskGroup,
		MaxDesiredAllocations:             limits.MaxDesiredAllocations,
		MaxDesiredAllocationsPerNamespace: limits.MaxDesiredAllocationsPerNamespace,
		DefaultTaskCPU:                    limits.DefaultTaskCPU,
		DefaultTaskMemory:                 spec.ByteSize(limits.DefaultTaskMemory),
		MaxTaskCPU:                        limits.MaxTaskCPU,
		MaxTaskMemory:                     spec.ByteSize(limits.MaxTaskMemory),
	}
}

// API returns the wire representation of the reconciliation settings.
func (r ReconciliationSettings) API() api.ReconciliationSettings {
	return api.ReconciliationSettings{
		AllocationLossTimeout:       r.AllocationLossTimeout,
		ReplacementBackoffBase:      r.ReplacementBackoffBase,
		ReplacementBackoffMax:       r.ReplacementBackoffMax,
		ReplacementStableAfter:      r.ReplacementStableAfter,
		TerminalAllocationRetention: r.TerminalAllocationRetention,
	}
}

// ReconciliationSettingsFromAPI converts wire reconciliation settings.
func ReconciliationSettingsFromAPI(r api.ReconciliationSettings) ReconciliationSettings {
	return ReconciliationSettings{
		AllocationLossTimeout:       r.AllocationLossTimeout,
		ReplacementBackoffBase:      r.ReplacementBackoffBase,
		ReplacementBackoffMax:       r.ReplacementBackoffMax,
		ReplacementStableAfter:      r.ReplacementStableAfter,
		TerminalAllocationRetention: r.TerminalAllocationRetention,
	}
}

// ClusterSettingsFromAPI converts and validates wire cluster settings.
func ClusterSettingsFromAPI(settings api.ClusterSettings) (ClusterSettings, error) {
	pool, err := netip.ParsePrefix(settings.Network.WireGuardPool)
	if err != nil {
		return ClusterSettings{}, fmt.Errorf("WireGuard pool: %w", err)
	}
	result := ClusterSettings{
		JobLimits:          JobLimitsFromAPI(settings.JobLimits),
		Reconciliation:     ReconciliationSettingsFromAPI(settings.Reconciliation),
		WireGuardPool:      pool,
		WireGuardPortCount: settings.Network.WireGuardPortCount,
	}
	if err := result.Validate(); err != nil {
		return ClusterSettings{}, err
	}
	return result, nil
}

// ClusterSettings returns the replicated settings this member last loaded.
func (s *Server) ClusterSettings() ClusterSettings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clusterSettingsLocked()
}

// clusterSettingsLocked returns the in-memory settings. The caller holds s.mu.
func (s *Server) clusterSettingsLocked() ClusterSettings {
	limits := s.jobLimits
	if limits == (spec.Limits{}) {
		limits = spec.DefaultLimits()
	}
	return ClusterSettings{JobLimits: limits, Reconciliation: s.reconciliation, WireGuardPool: s.networkPool, WireGuardPortCount: s.wireGuardPortCount}
}

// reconciliationSettingsLocked returns the reconciliation settings, using
// the defaults for a server that has not loaded a cluster record. The caller
// holds s.mu.
func (s *Server) reconciliationSettingsLocked() ReconciliationSettings {
	if s.reconciliation == (ReconciliationSettings{}) {
		return DefaultReconciliationSettings()
	}
	return s.reconciliation
}

// UpdateJobLimits replaces the replicated job limits. It refuses limits that
// would stop admitting a job that is currently desired, so changing policy
// never silently stops running allocations; delete or shrink those jobs first.
func (s *Server) UpdateJobLimits(ctx context.Context, limits spec.Limits) (ClusterSettings, error) {
	if err := spec.ValidateLimits(limits); err != nil {
		return ClusterSettings{}, fmt.Errorf("%w: job limits: %w", ErrInvalidClusterSettings, err)
	}
	settings, err := s.updateClusterSettings(ctx, func(settings *ClusterSettings) error {
		s.mu.RLock()
		violations := jobLimitViolations(s.jobs, limits)
		s.mu.RUnlock()
		if len(violations) > 0 {
			return fmt.Errorf("%w: desired jobs would no longer be admitted: %s", ErrClusterSettingsConflict, joinViolations(violations))
		}
		settings.JobLimits = limits
		return nil
	})
	if err == nil {
		s.log.Info("updated cluster job limits")
	}
	return settings, err
}

// UpdateReconciliationSettings replaces the replicated reconciliation
// settings. Every later reconciliation pass on any leader applies them.
func (s *Server) UpdateReconciliationSettings(ctx context.Context, reconciliation ReconciliationSettings) (ClusterSettings, error) {
	if err := reconciliation.Validate(); err != nil {
		return ClusterSettings{}, fmt.Errorf("%w: reconciliation: %w", ErrInvalidClusterSettings, err)
	}
	settings, err := s.updateClusterSettings(ctx, func(settings *ClusterSettings) error {
		settings.Reconciliation = reconciliation
		return nil
	})
	if err == nil {
		s.log.Info("updated cluster reconciliation settings")
	}
	return settings, err
}

// updateClusterSettings applies change to the replicated cluster record and
// loads the result. Changes are serialized with every other durable mutation.
func (s *Server) updateClusterSettings(ctx context.Context, change func(*ClusterSettings) error) (ClusterSettings, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	cluster, err := s.state.GetCluster(ctx)
	if err != nil {
		return ClusterSettings{}, fmt.Errorf("load cluster settings: %w", err)
	}
	if cluster == nil {
		return ClusterSettings{}, fmt.Errorf("load cluster settings: cluster is not initialized")
	}
	if err := change(&cluster.Settings); err != nil {
		return ClusterSettings{}, err
	}
	if err := s.state.PutCluster(ctx, cluster); err != nil {
		return ClusterSettings{}, fmt.Errorf("persist cluster settings: %w", err)
	}
	s.mu.Lock()
	s.loadClusterLocked(cluster)
	settings := s.clusterSettingsLocked()
	s.mu.Unlock()
	return settings, nil
}

// loadClusterLocked installs a replicated cluster record's mutable settings.
// Network settings are fixed when the cluster is created and are loaded by
// Init. The caller holds s.mu.
func (s *Server) loadClusterLocked(cluster *Cluster) {
	s.cluster = cluster
	s.jobLimits = cluster.Settings.JobLimits
	s.reconciliation = cluster.Settings.Reconciliation
}

// jobLimitViolations applies the same admission rules as reconciliation to
// every desired job, in job key order, and describes each job the limits
// would refuse.
func jobLimitViolations(jobs map[string]*Job, limits spec.Limits) []string {
	keys := make([]string, 0, len(jobs))
	for key, job := range jobs {
		if job != nil && job.Spec != nil {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	var violations []string
	namespaceDesired := make(map[string]int64)
	for _, key := range keys {
		job := jobs[key].Spec
		if err := spec.ValidateWithLimits(job, limits); err != nil {
			violations = append(violations, fmt.Sprintf("%s (%v)", key, err))
			continue
		}
		desired := desiredAllocations(job)
		if namespaceDesired[job.Namespace]+desired > int64(limits.MaxDesiredAllocationsPerNamespace) {
			violations = append(violations, fmt.Sprintf("%s (namespace %q desired allocations exceed %d)", key, job.Namespace, limits.MaxDesiredAllocationsPerNamespace))
			continue
		}
		namespaceDesired[job.Namespace] += desired
	}
	return violations
}

func joinViolations(violations []string) string {
	const shown = 5
	result := ""
	for i, violation := range violations {
		if i == shown {
			return fmt.Sprintf("%s; and %d more", result, len(violations)-shown)
		}
		if i > 0 {
			result += "; "
		}
		result += violation
	}
	return result
}
