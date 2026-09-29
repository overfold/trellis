package server

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sort"

	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/spec"
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
// creates the cluster supplies them once, and job limits change afterwards
// only through UpdateJobLimits. Network settings are fixed at creation
// because every namespace subnet and WireGuard port slot is derived from them.
type ClusterSettings struct {
	JobLimits          spec.Limits  `json:"job_limits"`
	WireGuardPool      netip.Prefix `json:"wireguard_pool"`
	WireGuardPortCount int          `json:"wireguard_port_count"`
}

// DefaultClusterSettings returns the settings of a cluster created without
// explicit values.
func DefaultClusterSettings() ClusterSettings {
	return ClusterSettings{
		JobLimits:          spec.DefaultLimits(),
		WireGuardPool:      netip.MustParsePrefix(DefaultWireGuardPool),
		WireGuardPortCount: DefaultWireGuardPortCount,
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
	if !validWireGuardPool(c.WireGuardPool) || c.WireGuardPool != c.WireGuardPool.Masked() {
		return fmt.Errorf("WireGuard pool must be a masked IPv4 prefix of /16 or larger")
	}
	return ValidateWireGuardPortCount(c.WireGuardPortCount)
}

// API returns the wire representation of the settings.
func (c ClusterSettings) API() api.ClusterSettings {
	return api.ClusterSettings{
		JobLimits: c.JobLimits,
		Network: api.ClusterNetworkSettings{
			WireGuardPool:      c.WireGuardPool.String(),
			WireGuardPortCount: c.WireGuardPortCount,
		},
	}
}

// ClusterSettings returns the replicated settings this member last loaded.
func (s *Server) ClusterSettings() ClusterSettings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clusterSettingsLocked()
}

// clusterSettingsLocked returns the in-memory settings. The caller holds s.mu.
func (s *Server) clusterSettingsLocked() ClusterSettings {
	return ClusterSettings{JobLimits: s.jobLimits, WireGuardPool: s.networkPool, WireGuardPortCount: s.wireGuardPortCount}
}

// UpdateJobLimits replaces the replicated job limits. It refuses limits that
// would stop admitting a job that is currently desired, so changing policy
// never silently stops running allocations; delete or shrink those jobs first.
func (s *Server) UpdateJobLimits(ctx context.Context, limits spec.Limits) (ClusterSettings, error) {
	if err := spec.ValidateLimits(limits); err != nil {
		return ClusterSettings{}, fmt.Errorf("%w: job limits: %v", ErrInvalidClusterSettings, err)
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	cluster, err := s.state.GetCluster(ctx)
	if err != nil {
		return ClusterSettings{}, fmt.Errorf("load cluster settings: %w", err)
	}
	if cluster == nil {
		return ClusterSettings{}, fmt.Errorf("load cluster settings: cluster is not initialized")
	}
	s.mu.RLock()
	violations := jobLimitViolations(s.jobs, limits)
	s.mu.RUnlock()
	if len(violations) > 0 {
		return ClusterSettings{}, fmt.Errorf("%w: desired jobs would no longer be admitted: %s", ErrClusterSettingsConflict, joinViolations(violations))
	}
	cluster.Settings.JobLimits = limits
	if err := s.state.PutCluster(ctx, cluster); err != nil {
		return ClusterSettings{}, fmt.Errorf("persist cluster settings: %w", err)
	}
	s.mu.Lock()
	s.cluster = cluster
	s.jobLimits = limits
	settings := s.clusterSettingsLocked()
	s.mu.Unlock()
	s.log.Info("updated cluster job limits")
	return settings, nil
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
