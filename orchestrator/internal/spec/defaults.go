package spec

import (
	"errors"
	"fmt"
	"time"
)

// Job defaults resolved by Canonicalize. Every omitted optional behavior is
// written into the canonical spec before it is persisted, so the stored job
// shows its effective behavior and no other component applies defaults.
const (
	// DefaultRuntime is the OCI runtime of a task group that omits runtime.
	DefaultRuntime = RuntimeRunc
	// DefaultMaxRestarts is the restart budget of a task group that omits restart.
	DefaultMaxRestarts = 3
	// DefaultRestartWindow is the restart window of a task group that omits restart.
	DefaultRestartWindow = 10 * time.Minute
	// DefaultUpdateStrategy is the update strategy of a task group that omits it.
	DefaultUpdateStrategy = UpdateRecreate
	// DefaultMaxParallel is the rolling-update parallelism when omitted or zero.
	DefaultMaxParallel = 1
	// DefaultTaskNetworkMode is the network mode of a task that omits it.
	DefaultTaskNetworkMode = TaskNetworkWireGuard
	// DefaultHealthCheckInterval is the delay between health checks when omitted.
	DefaultHealthCheckInterval = 10 * time.Second
	// DefaultHealthCheckTimeout bounds one health check when omitted.
	DefaultHealthCheckTimeout = 5 * time.Second
	// DefaultHealthCheckThreshold is the consecutive-result threshold when omitted.
	DefaultHealthCheckThreshold = 3
	// DefaultHealthCheckPath is the HTTP health-check request target when omitted.
	DefaultHealthCheckPath = "/"
	// DefaultSecretFileMode is the mode of a file secret that omits mode.
	DefaultSecretFileMode uint32 = 0o400
)

// applyDefaults resolves every omitted optional field of job in place.
// Explicit values are kept, including invalid ones, so validation reports them.
func applyDefaults(job *JobSpec, limits Limits) {
	for groupIndex := range job.TaskGroups {
		group := &job.TaskGroups[groupIndex]
		if group.Runtime == "" {
			group.Runtime = DefaultRuntime
		}
		if group.Restart == nil {
			group.Restart = &RestartPolicySpec{MaxRestarts: DefaultMaxRestarts, Window: DefaultRestartWindow}
		}
		if group.Update == nil {
			group.Update = &UpdateSpec{}
		}
		if group.Update.Strategy == "" {
			group.Update.Strategy = DefaultUpdateStrategy
		}
		if group.Update.MaxParallel == 0 {
			group.Update.MaxParallel = DefaultMaxParallel
		}
		for taskIndex := range group.Tasks {
			task := &group.Tasks[taskIndex]
			if task.Resources == nil {
				task.Resources = &ResourcesSpec{CPU: limits.DefaultTaskCPU, Memory: limits.DefaultTaskMemory}
			}
			if task.Networking == nil {
				task.Networking = &TaskNetworkingSpec{}
			}
			if task.Networking.Mode == TaskNetworkDefault {
				task.Networking.Mode = DefaultTaskNetworkMode
			}
			if task.Networking.Mode == TaskNetworkWireGuard {
				for portIndex := range task.Networking.Ports {
					port := &task.Networking.Ports[portIndex]
					if port.HostPort == 0 {
						port.HostPort = port.Port
					}
				}
			}
			if check := task.HealthCheck; check != nil {
				if check.Interval == 0 {
					check.Interval = DefaultHealthCheckInterval
				}
				if check.Timeout == 0 {
					check.Timeout = DefaultHealthCheckTimeout
				}
				if check.Threshold == 0 {
					check.Threshold = DefaultHealthCheckThreshold
				}
				if check.Type == HealthCheckHTTP && check.Path == "" {
					check.Path = DefaultHealthCheckPath
				}
			}
			for secretIndex := range task.Secrets {
				secret := &task.Secrets[secretIndex]
				if secret.Target == SecretTargetFile && secret.Mode == 0 {
					secret.Mode = DefaultSecretFileMode
				}
			}
		}
	}
}

// ValidateCanonical checks that job is valid and that every optional behavior
// is explicit, as Canonicalize leaves it. Servers, agents, and restores use it
// to refuse specs that did not pass through canonicalization.
func ValidateCanonical(job *JobSpec) error {
	if err := Validate(job); err != nil {
		return err
	}
	issues := ValidationErrors{}
	missing := func(path string) {
		issues = append(issues, ValidationIssue{Path: path, Code: "not_canonical", Message: "must be explicit in a canonical job"})
	}
	for groupIndex := range job.TaskGroups {
		group := &job.TaskGroups[groupIndex]
		groupPath := fmt.Sprintf("task_groups[%s]", group.Name)
		if group.Runtime == RuntimeDefault {
			missing(groupPath + ".runtime")
		}
		if group.Restart == nil {
			missing(groupPath + ".restart")
		}
		if group.Update == nil {
			missing(groupPath + ".update")
		} else {
			if group.Update.Strategy == "" {
				missing(groupPath + ".update.strategy")
			}
			if group.Update.MaxParallel < 1 {
				missing(groupPath + ".update.max_parallel")
			}
		}
		for taskIndex := range group.Tasks {
			if err := ValidateCanonicalTask(&group.Tasks[taskIndex]); err != nil {
				var taskIssues ValidationErrors
				errors.As(err, &taskIssues)
				for _, issue := range taskIssues {
					issue.Path = fmt.Sprintf("%s.tasks[%s].%s", groupPath, group.Tasks[taskIndex].Name, issue.Path)
					issues = append(issues, issue)
				}
			}
		}
	}
	if len(issues) > 0 {
		return issues
	}
	return nil
}

// ValidateCanonicalTask reports the task fields that canonicalization should
// have made explicit. Paths are relative to the task. It returns nil or
// ValidationErrors.
func ValidateCanonicalTask(task *TaskSpec) error {
	issues := ValidationErrors{}
	missing := func(path string) {
		issues = append(issues, ValidationIssue{Path: path, Code: "not_canonical", Message: "must be explicit in a canonical job"})
	}
	if task.Resources == nil {
		missing("resources")
	}
	if task.Networking == nil || task.Networking.Mode == TaskNetworkDefault {
		missing("networking.mode")
	} else if task.Networking.Mode == TaskNetworkWireGuard {
		for i, port := range task.Networking.Ports {
			if port.HostPort == 0 {
				missing(fmt.Sprintf("networking.ports[%d].host_port", i))
			}
		}
	}
	if check := task.HealthCheck; check != nil {
		if check.Interval <= 0 {
			missing("health_check.interval")
		}
		if check.Timeout <= 0 {
			missing("health_check.timeout")
		}
		if check.Threshold <= 0 {
			missing("health_check.threshold")
		}
		if check.Type == HealthCheckHTTP && check.Path == "" {
			missing("health_check.path")
		}
	}
	for i, secret := range task.Secrets {
		if secret.Target == SecretTargetFile && secret.Mode == 0 {
			missing(fmt.Sprintf("secrets[%d].mode", i))
		}
	}
	if len(issues) > 0 {
		return issues
	}
	return nil
}
