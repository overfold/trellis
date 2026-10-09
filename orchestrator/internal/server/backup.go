package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"sort"

	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/state"
	"github.com/overfold/trellis/orchestrator/internal/version"
)

type desiredStore interface {
	BackupDesired(cluster string) (*state.DesiredSnapshot, error)
	RestoreDesiredContext(ctx context.Context, cluster string, snapshot *state.DesiredSnapshot) error
}

// Backup captures desired cluster state and the replicated cluster settings
// from one consistent view.
func (s *Server) Backup(_ context.Context) (*api.BackupSnapshot, error) {
	if s.backupStore == nil {
		return nil, fmt.Errorf("backup is unavailable")
	}
	snapshot, err := s.backupStore.BackupDesired(s.clusterName)
	if err != nil {
		return nil, err
	}
	if len(snapshot.Cluster) == 0 {
		return nil, fmt.Errorf("backup: cluster is not initialized")
	}
	var cluster Cluster
	if err := json.Unmarshal(snapshot.Cluster, &cluster); err != nil {
		return nil, fmt.Errorf("backup: decode cluster record: %w", err)
	}
	if err := cluster.Settings.Validate(); err != nil {
		return nil, fmt.Errorf("backup: replicated cluster settings: %w", err)
	}
	result := &api.BackupSnapshot{
		FormatVersion:              api.BackupFormatVersion,
		TrellisVersion:             version.Current(),
		CreatedAt:                  s.now().UTC(),
		ClusterSettings:            cluster.Settings.API(),
		Jobs:                       make(map[string]json.RawMessage, len(snapshot.Jobs)),
		JobRevisions:               make(map[string]json.RawMessage, len(snapshot.JobRevisions)),
		Secrets:                    make(map[string]json.RawMessage, len(snapshot.Secrets)),
		VolumeRegistrations:        make(map[string]json.RawMessage, len(snapshot.VolumeRegistrations)),
		NetworkPortRegistrations:   make(map[string]json.RawMessage, len(snapshot.NetworkPortRegistrations)),
		NetworkSubnetRegistrations: make(map[string]json.RawMessage, len(snapshot.NetworkSubnetRegistrations)),
	}
	for key, value := range snapshot.Jobs {
		result.Jobs[key] = json.RawMessage(value)
	}
	for key, value := range snapshot.JobRevisions {
		result.JobRevisions[key] = json.RawMessage(value)
	}
	for key, value := range snapshot.Secrets {
		result.Secrets[key] = json.RawMessage(value)
	}
	for key, value := range snapshot.VolumeRegistrations {
		result.VolumeRegistrations[key] = json.RawMessage(value)
	}
	for key, value := range snapshot.NetworkPortRegistrations {
		result.NetworkPortRegistrations[key] = json.RawMessage(value)
	}
	for key, value := range snapshot.NetworkSubnetRegistrations {
		result.NetworkSubnetRegistrations[key] = json.RawMessage(value)
	}
	return result, nil
}

// checkBackupFormat refuses a backup whose format differs from this
// release's. Backups are not migrated between formats.
func checkBackupFormat(backup *api.BackupSnapshot) error {
	if backup.FormatVersion == api.BackupFormatVersion {
		return nil
	}
	creator := "an unrecorded Trellis release"
	if backup.TrellisVersion != "" {
		creator = "Trellis " + backup.TrellisVersion
	}
	return fmt.Errorf("backup format version %d, created by %s, cannot be restored by Trellis %s, which reads backup format version %d only; restore it into a new cluster running a Trellis release with backup format version %d, such as the release that created it", backup.FormatVersion, creator, version.Current(), api.BackupFormatVersion, backup.FormatVersion)
}

// Restore replaces desired state and the mutable cluster settings from a
// backup. The target must be a fresh cluster created with the backup's
// network settings, because namespace subnets and WireGuard port slots are
// derived from them.
func (s *Server) Restore(ctx context.Context, backup *api.BackupSnapshot) error {
	ctx, release := s.bindTerm(ctx)
	defer release()
	if err := s.checkTerm(ctx); err != nil {
		return stateUnavailable(err)
	}
	if err := checkBackupFormat(backup); err != nil {
		return err
	}
	if s.backupStore == nil {
		return stateUnavailable(fmt.Errorf("restore is unavailable"))
	}
	settings, err := ClusterSettingsFromAPI(backup.ClusterSettings)
	if err != nil {
		return fmt.Errorf("backup cluster settings: %w", err)
	}
	s.mu.RLock()
	pool, portCount := s.networkPool, s.wireGuardPortCount
	s.mu.RUnlock()
	if settings.WireGuardPool != pool || settings.WireGuardPortCount != portCount {
		return fmt.Errorf("backup network settings (wireguard_pool %s, wireguard_port_count %d) differ from this cluster's (wireguard_pool %s, wireguard_port_count %d); restore into a new cluster created with the backup's values", settings.WireGuardPool, settings.WireGuardPortCount, pool, portCount)
	}
	snapshot := &state.DesiredSnapshot{
		Jobs:                       make(map[string][]byte, len(backup.Jobs)),
		JobRevisions:               make(map[string][]byte, len(backup.JobRevisions)),
		Secrets:                    make(map[string][]byte, len(backup.Secrets)),
		VolumeRegistrations:        make(map[string][]byte, len(backup.VolumeRegistrations)),
		NetworkPortRegistrations:   make(map[string][]byte, len(backup.NetworkPortRegistrations)),
		NetworkSubnetRegistrations: make(map[string][]byte, len(backup.NetworkSubnetRegistrations)),
	}
	restored := make(map[string]*Job, len(backup.Jobs))
	for key, value := range backup.Jobs {
		var record Job
		if err := json.Unmarshal(value, &record); err != nil {
			return fmt.Errorf("job %q contains invalid JSON", key)
		}
		if record.Spec == nil {
			return fmt.Errorf("validate job %q: job spec is missing", key)
		}
		if record.Incarnation == "" {
			return fmt.Errorf("validate job %q: job incarnation is missing", key)
		}
		if record.ResolvedImages == nil {
			return fmt.Errorf("validate job %q: resolved images are missing", key)
		}
		if _, err := s.resolveJobImages(ctx, record.Spec, record.ResolvedImages); err != nil {
			return fmt.Errorf("validate job %q: %w", key, err)
		}
		snapshot.Jobs[key] = value
		restored[jobKey(record.Spec.Namespace, record.Spec.Name)] = &record
	}
	// Restored jobs are canonical and must be admitted by the restored
	// limits, exactly as they were when the backup was taken.
	if violations := jobLimitViolations(restored, settings.JobLimits); len(violations) > 0 {
		return fmt.Errorf("restored jobs are not admitted by the backup's job limits: %s", joinViolations(violations))
	}
	for key, value := range backup.JobRevisions {
		if !json.Valid(value) {
			return fmt.Errorf("job revision %q contains invalid JSON", key)
		}
		var record JobRevisionRecord
		if err := json.Unmarshal(value, &record); err != nil {
			return fmt.Errorf("validate job revision %q: %w", key, err)
		}
		if record.Spec == nil || record.Version < 1 || record.Revision < 1 || record.CreatedAt.IsZero() {
			return fmt.Errorf("validate job revision %q: invalid revision record", key)
		}
		if record.ResolvedImages == nil {
			return fmt.Errorf("validate job revision %q: resolved images are missing", key)
		}
		if _, err := s.resolveJobImages(ctx, record.Spec, record.ResolvedImages); err != nil {
			return fmt.Errorf("validate job revision %q: %w", key, err)
		}
		snapshot.JobRevisions[key] = value
	}
	for key, value := range backup.Secrets {
		if !json.Valid(value) {
			return fmt.Errorf("secret %q contains invalid JSON", key)
		}
		snapshot.Secrets[key] = value
	}
	for key, value := range backup.VolumeRegistrations {
		if !json.Valid(value) {
			return fmt.Errorf("volume registration %q contains invalid JSON", key)
		}
		snapshot.VolumeRegistrations[key] = value
	}
	for key, value := range backup.NetworkPortRegistrations {
		var record NetworkPortRegistration
		if json.Unmarshal(value, &record) != nil || record.Slot < 0 || record.Slot >= portCount {
			return fmt.Errorf("network port registration %q is invalid or outside configured port range", key)
		}
		snapshot.NetworkPortRegistrations[key] = value
	}
	subnetCapacity := networkSubnetCapacity(pool)
	for key, value := range backup.NetworkSubnetRegistrations {
		var record NetworkSubnetRegistration
		if json.Unmarshal(value, &record) != nil || record.Index < 0 || record.Index >= subnetCapacity {
			return fmt.Errorf("network subnet registration %q is invalid or outside configured subnet capacity", key)
		}
		snapshot.NetworkSubnetRegistrations[key] = value
	}
	// Validate the complete backup before dropping excess or orphaned
	// revisions so malformed records cannot hide outside the retained window.
	if err := state.ValidateDesiredSnapshot(snapshot, nil); err != nil {
		return err
	}
	if err := state.ValidateDesiredSnapshotKeys(s.clusterName, snapshot); err != nil {
		return err
	}
	// Authenticate all secrets locally before any replicated mutation. Keys and
	// plaintext must never enter the snapshot or the deterministic FSM.
	for _, key := range slices.Sorted(maps.Keys(snapshot.Secrets)) {
		if s.secrets == nil {
			return fmt.Errorf("validate secret %q: secrets store is unavailable; configure the original secrets_key and secrets_key_id before restoring", key)
		}
		if err := s.secrets.ValidateRecord(snapshot.Secrets[key]); err != nil {
			return fmt.Errorf("validate secret %q: %w; configure the original secrets_key and secrets_key_id before restoring", key, err)
		}
	}
	retainedRevisions, err := retainedJobRevisionEntries(snapshot.Jobs, snapshot.JobRevisions)
	if err != nil {
		return err
	}
	snapshot.JobRevisions = retainedRevisions
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if err := s.checkTerm(ctx); err != nil {
		return stateUnavailable(err)
	}
	snapshot.ExpectedCluster, err = s.state.store.Get(ctx, "trellis/"+s.clusterName+"/meta")
	if err != nil {
		return fmt.Errorf("load cluster settings: %w", stateUnavailable(err))
	}
	if len(snapshot.ExpectedCluster) == 0 {
		return fmt.Errorf("load cluster settings: cluster is not initialized")
	}
	var cluster Cluster
	if err := json.Unmarshal(snapshot.ExpectedCluster, &cluster); err != nil {
		return fmt.Errorf("decode cluster settings: %w", stateUnavailable(err))
	}
	// The restored record keeps this cluster's identity, administrator key,
	// fencing epoch, and fixed network settings, and takes the backup's job
	// limits and reconciliation settings.
	cluster.Settings.JobLimits = settings.JobLimits
	cluster.Settings.Reconciliation = settings.Reconciliation
	snapshot.Cluster, err = json.Marshal(cluster)
	if err != nil {
		return fmt.Errorf("encode restored cluster record: %w", err)
	}
	if err := s.backupStore.RestoreDesiredContext(ctx, s.clusterName, snapshot); err != nil {
		if errors.Is(err, state.ErrRestoreNotFresh) {
			return err
		}
		return stateUnavailable(err)
	}
	if err := s.checkTerm(ctx); err != nil {
		return stateUnavailable(err)
	}
	s.mu.Lock()
	s.loadClusterLocked(&cluster)
	s.mu.Unlock()
	return stateUnavailable(s.Reload(ctx))
}

func retainedJobRevisionEntries(jobs, revisions map[string][]byte) (map[string][]byte, error) {
	type entry struct {
		key     string
		version int
		raw     []byte
	}
	byJob := make(map[string][]entry)
	for key, raw := range revisions {
		var record JobRevisionRecord
		if err := json.Unmarshal(raw, &record); err != nil || record.Spec == nil {
			return nil, fmt.Errorf("invalid job revision record %q", key)
		}
		identity := jobKey(record.Spec.Namespace, record.Spec.Name)
		if jobs[url.QueryEscape(identity)] == nil {
			continue
		}
		entries := append(byJob[identity], entry{key: key, version: record.Version, raw: raw})
		sort.Slice(entries, func(i, j int) bool { return entries[i].version < entries[j].version })
		if len(entries) > jobRevisionRetention {
			entries = entries[1:]
		}
		byJob[identity] = entries
	}
	result := make(map[string][]byte)
	for _, entries := range byJob {
		for _, entry := range entries {
			result[entry.key] = entry.raw
		}
	}
	return result, nil
}
