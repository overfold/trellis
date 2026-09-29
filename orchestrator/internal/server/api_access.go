package server

import (
	"context"
	"fmt"

	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/auth"
	"github.com/overfold/trellis/internal/spec"
)

// apiAccessToken returns the workload credential for one allocation
// generation. The token is bound to the allocation's job and task group and is
// persisted only as a hash plus a copy sealed with the secrets key, so start
// retries and leader changes re-deliver the same token.
func (s *Server) apiAccessToken(ctx context.Context, access *spec.APIAccessSpec, request *api.AllocationRequest) (string, error) {
	if access == nil {
		return "", nil
	}
	if s.tokenManager == nil {
		return "", fmt.Errorf("scoped API access is unavailable")
	}
	if s.secrets == nil {
		return "", fmt.Errorf("scoped API access requires the secrets encryption key")
	}

	principal := auth.Principal{
		Kind:      auth.CredentialWorkload,
		Scope:     auth.AccessScope(access.Scope),
		Access:    auth.AccessLevel(access.Access),
		Namespace: request.Namespace,
		Subject:   &auth.CredentialSubject{Namespace: request.Namespace, Job: request.JobName, TaskGroup: request.GroupName},
	}
	token, err := s.tokenManager.WorkloadToken(ctx, s.secrets, request.AllocationID, request.Generation, principal)
	if err != nil {
		return "", fmt.Errorf("create %s/%s workload API token: %w", access.Scope, access.Access, err)
	}
	return token, nil
}

type workloadGrant struct {
	subject auth.CredentialSubject
	scope   auth.AccessScope
	access  auth.AccessLevel
}

// revokeStaleWorkloadCredentials revokes workload credentials whose
// allocation record is gone, whose job or task group was deleted (including a
// job deleted and recreated under the same name), or whose
// task group no longer grants the same api_access.
func (s *Server) revokeStaleWorkloadCredentials(ctx context.Context) {
	if s.tokenManager == nil {
		return
	}
	s.mu.RLock()
	grants := make(map[string]workloadGrant)
	for _, allocation := range s.allocations {
		allocation.mu.Lock()
		id, namespace, jobName, groupName, incarnation := allocation.ID, allocation.Namespace, allocation.JobName, allocation.TaskGroupName, allocation.JobIncarnation
		allocation.mu.Unlock()
		job := s.jobs[jobKey(namespace, jobName)]
		if job == nil || job.Incarnation != incarnation {
			continue
		}
		for i := range job.Spec.TaskGroups {
			group := &job.Spec.TaskGroups[i]
			if group.Name == groupName && group.APIAccess != nil {
				grants[id] = workloadGrant{
					subject: auth.CredentialSubject{Namespace: namespace, Job: jobName, TaskGroup: groupName},
					scope:   auth.AccessScope(group.APIAccess.Scope),
					access:  auth.AccessLevel(group.APIAccess.Access),
				}
				break
			}
		}
	}
	s.mu.RUnlock()

	revoked, err := s.tokenManager.RevokeWorkloadCredentials(ctx, func(credential auth.WorkloadCredential) bool {
		grant, ok := grants[credential.AllocationID]
		principal := credential.Principal
		return ok && principal.Subject != nil && *principal.Subject == grant.subject && principal.Scope == grant.scope && principal.Access == grant.access
	})
	if err != nil {
		s.log.Error("revoke stale workload credentials", "error", err)
		return
	}
	if revoked > 0 {
		s.log.Info("revoked stale workload credentials", "count", revoked)
	}
}
