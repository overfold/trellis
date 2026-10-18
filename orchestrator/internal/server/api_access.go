package server

import (
	"context"
	"fmt"

	"github.com/overfold/trellis/orchestrator/internal/auth"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// apiAccessToken returns the workload credential for one task in an allocation
// generation. The token is bound to the allocation's job, task group and task and is
// persisted only as a hash plus a copy sealed with the secrets key, so start
// retries and leader changes re-deliver the same token.
func (s *Server) apiAccessToken(ctx context.Context, access *spec.APIAccessSpec, request *nodeapi.AllocationRequest, task string) (string, error) {
	if access == nil {
		return "", nil
	}
	if s.tokenManager == nil {
		return "", fmt.Errorf("workload API access is unavailable")
	}
	if s.secrets == nil {
		return "", fmt.Errorf("workload API access requires the secrets encryption key")
	}

	principal := auth.Principal{
		Role:    auth.Role(access.Role),
		Subject: &auth.CredentialSubject{Namespace: request.Namespace, Job: request.JobName, TaskGroup: request.GroupName, Task: task},
	}
	token, err := s.tokenManager.WorkloadToken(ctx, s.secrets, request.AllocationID, request.Generation, principal)
	if err != nil {
		return "", fmt.Errorf("create %s workload API token for task %s: %w", access.Role, task, err)
	}
	return token, nil
}

type workloadGrant struct {
	subject auth.CredentialSubject
	access  *spec.APIAccessSpec
	nodeID  string
}

// authorizeWorkloadCredential checks authority at use, independently of history
// pruning and successful Raft deletion. Silence denies API use at the configured
// loss deadline; it does not stop a partitioned container or mutate history.
func (s *Server) authorizeWorkloadCredential(ctx context.Context, credential auth.WorkloadCredential) bool {
	s.mu.RLock()
	var nodeID string
	valid := false
	for _, allocation := range s.allocations {
		if allocation.ID != credential.AllocationID {
			continue
		}
		allocation.mu.Lock()
		if allocation.Generation != credential.Generation || !activeAllocationPhase(allocation.Phase) || allocation.Node == nil {
			allocation.mu.Unlock()
			break
		}
		node := s.nodes[allocation.Node.ID]
		if node == nil {
			allocation.mu.Unlock()
			break
		}
		nodeID = node.ID.String()
		settings := s.reconciliation
		if settings.AllocationLossTimeout == 0 {
			settings = DefaultReconciliationSettings()
		}
		now := s.now()
		if now.Sub(s.leaderSince) >= leaderRecoveryGrace && now.Sub(silentSince(s.liveness.lastHeartbeat(node.ID), s.leaderSince)) >= settings.AllocationLossTimeout {
			allocation.mu.Unlock()
			break
		}
		job := s.jobs[jobKey(allocation.Namespace, allocation.JobName)]
		if job != nil && job.Incarnation == allocation.JobIncarnation && credential.Principal.Subject != nil && *credential.Principal.Subject == (auth.CredentialSubject{Namespace: allocation.Namespace, Job: allocation.JobName, TaskGroup: allocation.TaskGroupName, Task: credential.Principal.Subject.Task}) {
			for _, group := range job.Spec.TaskGroups {
				if group.Name == allocation.TaskGroupName {
					for _, task := range group.Tasks {
						if task.Name == credential.Principal.Subject.Task && task.APIAccess != nil && grantWithin(credential.Principal, task.APIAccess) {
							valid = true
							break
						}
					}
				}
			}
		}
		allocation.mu.Unlock()
		break
	}
	s.mu.RUnlock()
	if !valid {
		return false
	}
	removed, err := s.state.NodeRemoved(ctx, nodeID)
	return err == nil && !removed
}

// grantWithin reports whether a credential's role is no broader than the
// task's current api_access role.
func grantWithin(principal auth.Principal, access *spec.APIAccessSpec) bool {
	return principal.Role.Generated() && auth.Role(access.Role).Includes(principal.Role)
}

// revokeStaleWorkloadCredentials revokes workload credentials whose
// allocation is terminal or its node removed, whose record is gone, whose job
// or task was deleted (including a job recreated under the same name), or whose task's
// api_access was removed or narrowed below the credential's grant. Widening
// api_access leaves existing credentials in place until their allocations are
// replaced.
func (s *Server) revokeStaleWorkloadCredentials(ctx context.Context) {
	if s.tokenManager == nil {
		return
	}
	var trustErr error
	revoked, err := s.tokenManager.RevokeWorkloadCredentials(ctx, func() func(auth.WorkloadCredential) bool {
		s.mu.RLock()
		grants := make(map[string]map[string]workloadGrant)
		for _, allocation := range s.allocations {
			allocation.mu.Lock()
			id, namespace, jobName, groupName, incarnation := allocation.ID, allocation.Namespace, allocation.JobName, allocation.TaskGroupName, allocation.JobIncarnation
			phase, node := allocation.Phase, allocation.Node
			allocation.mu.Unlock()
			if phase == lifecycle.PhaseStopped || phase == lifecycle.PhaseFailed || phase == lifecycle.PhaseLost || (node != nil && s.nodes[node.ID] == nil) {
				continue
			}
			job := s.jobs[jobKey(namespace, jobName)]
			if job == nil || job.Incarnation != incarnation {
				continue
			}
			for i := range job.Spec.TaskGroups {
				group := &job.Spec.TaskGroups[i]
				if group.Name == groupName {
					grants[id] = make(map[string]workloadGrant)
					for _, task := range group.Tasks {
						if task.APIAccess == nil {
							continue
						}
						access := *task.APIAccess
						grant := workloadGrant{subject: auth.CredentialSubject{Namespace: namespace, Job: jobName, TaskGroup: groupName, Task: task.Name}, access: &access}
						if node != nil {
							grant.nodeID = node.ID.String()
						}
						grants[id][task.Name] = grant
					}
					break
				}
			}
		}
		s.mu.RUnlock()
		removedNodes := make(map[string]bool)
		if s.state != nil {
			for _, tasks := range grants {
				for _, grant := range tasks {
					if grant.nodeID == "" {
						continue
					}
					if _, checked := removedNodes[grant.nodeID]; checked {
						continue
					}
					removed, err := s.state.NodeRemoved(ctx, grant.nodeID)
					if err != nil {
						trustErr = err
						// Do not persist revocations based on an incomplete view.
						return func(auth.WorkloadCredential) bool { return true }
					}
					removedNodes[grant.nodeID] = removed
				}
			}
		}
		return func(credential auth.WorkloadCredential) bool {
			principal := credential.Principal
			if principal.Subject == nil {
				return false
			}
			grant, ok := grants[credential.AllocationID][principal.Subject.Task]
			return ok && !removedNodes[grant.nodeID] && principal.Subject != nil && *principal.Subject == grant.subject && grantWithin(principal, grant.access)
		}
	})
	if trustErr != nil {
		err = trustErr
	}
	if err != nil {
		s.log.Error("revoke stale workload credentials", "error", err)
		return
	}
	if revoked > 0 {
		s.log.Info("revoked stale workload credentials", "count", revoked)
	}
}
