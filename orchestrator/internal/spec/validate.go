package spec

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

var identifierPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$`)
var labelKeyPattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9._/-]{0,62}$`)
var envPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ValidationIssue describes one independently actionable manifest error.
type ValidationIssue struct {
	Path    string `json:"path"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ValidationErrors contains every validation issue found in a manifest.
type ValidationErrors []ValidationIssue

// Error implements error while preserving useful output for CLI callers.
func (v ValidationErrors) Error() string {
	if len(v) == 0 {
		return ""
	}
	if len(v) == 1 {
		if v[0].Path == "" {
			return v[0].Message
		}
		return fmt.Sprintf("%s: %s", v[0].Path, v[0].Message)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d validation errors", len(v))
	for _, issue := range v {
		b.WriteString("\n- ")
		if issue.Path != "" {
			b.WriteString(issue.Path)
			b.WriteString(": ")
		}
		b.WriteString(issue.Message)
	}
	return b.String()
}

// ValidIdentifier reports whether value is safe for use as an identifier.
func ValidIdentifier(value string) bool { return identifierPattern.MatchString(value) }

// Validate checks that a job specification is complete and internally consistent.
func Validate(job *JobSpec) error {
	if job == nil {
		return errors.New("job is required")
	}

	issues := ValidationErrors{}
	add := func(path, code, message string) {
		issues = append(issues, ValidationIssue{Path: path, Code: code, Message: message})
	}

	if strings.TrimSpace(job.Name) == "" {
		add("name", "required", "job name is required")
	} else if !identifierPattern.MatchString(job.Name) {
		add("name", "invalid_identifier", "job name must be a safe identifier")
	}
	if strings.TrimSpace(job.Namespace) == "" {
		add("namespace", "required", "job namespace is required")
	} else if !identifierPattern.MatchString(job.Namespace) {
		add("namespace", "invalid_identifier", "job namespace must be a safe identifier")
	}
	if len(job.TaskGroups) == 0 {
		add("task_groups", "required", "at least one task group is required")
	}

	groups := make(map[string]struct{})
	for i, group := range job.TaskGroups {
		groupPath := fmt.Sprintf("task_groups[%d]", i)
		if group.Name != "" {
			groupPath = fmt.Sprintf("task_groups[%s]", group.Name)
		}
		if strings.TrimSpace(group.Name) == "" {
			add(groupPath+".name", "required", "name is required")
		} else {
			if !identifierPattern.MatchString(group.Name) {
				add(groupPath+".name", "invalid_identifier", "name must be a safe identifier")
			}
			if _, exists := groups[group.Name]; exists {
				add(groupPath+".name", "duplicate", fmt.Sprintf("duplicate task group %q", group.Name))
			} else {
				groups[group.Name] = struct{}{}
			}
		}
		if !group.Runtime.Valid() {
			add(groupPath+".runtime", "unsupported", fmt.Sprintf("unsupported runtime %q", group.Runtime))
		}
		if group.APIAccess != nil {
			if !group.APIAccess.Scope.Valid() {
				add(groupPath+".api_access.scope", "unsupported", "must be namespace or cluster")
			}
			if !group.APIAccess.Access.Valid() {
				add(groupPath+".api_access.access", "unsupported", "must be read or write")
			}
		}
		if group.Restart != nil {
			if group.Restart.MaxRestarts < 0 {
				add(groupPath+".restart.max_restarts", "out_of_range", "must be at least 0")
			}
			if group.Restart.Window <= 0 {
				add(groupPath+".restart.window", "out_of_range", "must be positive")
			}
		}
		if group.Update != nil {
			if !group.Update.Strategy.Valid() {
				add(groupPath+".update.strategy", "unsupported", fmt.Sprintf("unsupported update strategy %q", group.Update.Strategy))
			}
			if group.Update.Strategy == UpdateRolling && group.Count < 2 {
				add(groupPath+".update.strategy", "incompatible", "rolling updates require count >= 2; use recreate for single-instance groups")
			}
			if group.Update.MaxParallel < 0 {
				add(groupPath+".update.max_parallel", "out_of_range", "must be at least 0")
			}
		}

		constraints := make(map[string]struct{}, len(group.Constraints))
		for j, constraint := range group.Constraints {
			path := fmt.Sprintf("%s.constraints[%d]", groupPath, j)
			if !labelKeyPattern.MatchString(constraint.Attribute) {
				add(path+".attribute", "invalid", fmt.Sprintf("invalid constraint attribute %q", constraint.Attribute))
			}
			if strings.TrimSpace(constraint.Value) == "" {
				add(path+".value", "required", "constraint value is required")
			}
			if constraint.Attribute != "" {
				if _, exists := constraints[constraint.Attribute]; exists {
					add(path+".attribute", "duplicate", fmt.Sprintf("duplicate constraint attribute %q", constraint.Attribute))
				} else {
					constraints[constraint.Attribute] = struct{}{}
				}
			}
		}
		for key, value := range group.Labels {
			if !labelKeyPattern.MatchString(key) {
				add(groupPath+".labels", "invalid", fmt.Sprintf("invalid label key %q", key))
			}
			if len(value) > 256 {
				add(groupPath+".labels."+key, "too_long", "label value exceeds 256 characters")
			}
		}
		if group.Count < 1 {
			add(groupPath+".count", "out_of_range", "must be at least 1")
		}
		if len(group.Tasks) == 0 {
			add(groupPath+".tasks", "required", "at least one task is required")
		}

		tasks := make(map[string]struct{})
		for j, task := range group.Tasks {
			taskPath := fmt.Sprintf("%s.tasks[%d]", groupPath, j)
			if task.Name != "" {
				taskPath = fmt.Sprintf("%s.tasks[%s]", groupPath, task.Name)
			}
			if strings.TrimSpace(task.Name) == "" {
				add(taskPath+".name", "required", "name is required")
			} else {
				if !identifierPattern.MatchString(task.Name) {
					add(taskPath+".name", "invalid_identifier", "name must be a safe identifier")
				}
				if _, exists := tasks[task.Name]; exists {
					add(taskPath+".name", "duplicate", fmt.Sprintf("duplicate task %q", task.Name))
				} else {
					tasks[task.Name] = struct{}{}
				}
			}
			if strings.TrimSpace(task.Image) == "" {
				add(taskPath+".image", "required", "image is required")
			}
			if task.Resources != nil {
				if task.Resources.CPU <= 0 {
					add(taskPath+".resources.cpu", "out_of_range", "must be positive")
				}
				if task.Resources.Memory <= 0 {
					add(taskPath+".resources.memory", "out_of_range", "must be positive")
				}
			}

			secretNames, secretEnvs, secretPaths := map[string]struct{}{}, map[string]struct{}{}, map[string]struct{}{}
			for k, secret := range task.Secrets {
				path := fmt.Sprintf("%s.secrets[%d]", taskPath, k)
				if !ValidIdentifier(secret.Name) {
					add(path+".name", "invalid_identifier", fmt.Sprintf("invalid secret name %q", secret.Name))
				}
				if secret.Name != "" {
					if _, ok := secretNames[secret.Name]; ok {
						add(path+".name", "duplicate", fmt.Sprintf("duplicate secret %q", secret.Name))
					} else {
						secretNames[secret.Name] = struct{}{}
					}
				}
				switch secret.Target {
				case SecretTargetEnv:
					if !envPattern.MatchString(secret.Env) || secret.Path != "" || secret.Mode != 0 {
						add(path, "invalid_target", "env secret requires only a valid env name")
					}
					if secret.Env != "" {
						if _, ok := secretEnvs[secret.Env]; ok {
							add(path+".env", "duplicate", fmt.Sprintf("duplicate secret env %q", secret.Env))
						} else {
							secretEnvs[secret.Env] = struct{}{}
						}
						if _, ok := task.Env[secret.Env]; ok {
							add(path+".env", "conflict", fmt.Sprintf("secret env %q conflicts with env", secret.Env))
						}
					}
				case SecretTargetFile:
					clean := filepath.Clean(secret.Path)
					if secret.Env != "" || clean != secret.Path || !strings.HasPrefix(clean, "/run/trellis-secrets/") {
						add(path, "invalid_target", "file secret requires a clean path below /run/trellis-secrets")
					}
					if secret.Mode != 0 && secret.Mode != 0o400 && secret.Mode != 0o600 {
						add(path+".mode", "unsupported", "must be 0400 or 0600")
					}
					if secret.Path != "" {
						if _, ok := secretPaths[clean]; ok {
							add(path+".path", "duplicate", fmt.Sprintf("duplicate secret path %q", clean))
						} else {
							secretPaths[clean] = struct{}{}
						}
					}
				default:
					add(path+".target", "unsupported", "must be env or file")
				}
			}

			volumes := make(map[string]struct{})
			for k, volume := range task.Volumes {
				path := fmt.Sprintf("%s.volumes[%d]", taskPath, k)
				if !identifierPattern.MatchString(volume.Name) {
					add(path+".name", "invalid_identifier", "volume name must be a safe identifier")
				}
				if strings.TrimSpace(volume.ContainerPath) == "" || !filepath.IsAbs(volume.ContainerPath) || filepath.Clean(volume.ContainerPath) != volume.ContainerPath {
					add(path+".container_path", "invalid", "clean absolute container path is required")
				}
				if strings.HasPrefix(volume.HostPath, "@/") {
					rel := strings.TrimPrefix(volume.HostPath, "@/")
					if rel == "" || filepath.IsAbs(rel) || filepath.Clean(rel) != rel || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
						add(path+".host_path", "invalid", "@/ host path must contain a clean relative path")
					}
				} else if strings.TrimSpace(volume.HostPath) == "" || !filepath.IsAbs(volume.HostPath) || filepath.Clean(volume.HostPath) != volume.HostPath {
					add(path+".host_path", "invalid", "host path must be a clean absolute path or begin with @/")
				}
				if volume.Name != "" {
					if _, exists := volumes[volume.Name]; exists {
						add(path+".name", "duplicate", fmt.Sprintf("duplicate volume %q", volume.Name))
					} else {
						volumes[volume.Name] = struct{}{}
					}
				}
			}

			if task.Networking != nil {
				if !task.Networking.Mode.Valid() {
					add(taskPath+".networking.mode", "unsupported", fmt.Sprintf("unsupported networking mode %q", task.Networking.Mode))
				}
				if task.Networking.Mode != TaskNetworkHost && len(task.Networking.Ports) > 0 {
					add(taskPath+".networking.ports", "invalid", "ports require networking mode host")
				}
				for k, port := range task.Networking.Ports {
					if port.Port < 1 || port.Port > 65535 {
						add(fmt.Sprintf("%s.networking.ports[%d].port", taskPath, k), "out_of_range", "must be between 1 and 65535")
					}
				}
			}
			if group.APIAccess != nil {
				mode := TaskNetworkDefault
				if task.Networking != nil {
					mode = task.Networking.Mode
				}
				if mode != TaskNetworkHost && mode != TaskNetworkWireGuard {
					add(taskPath+".networking.mode", "incompatible", "api_access requires host or namespace networking")
				}
			}

			if task.HealthCheck != nil {
				checkPath := taskPath + ".health_check"
				if task.HealthCheck.Interval < 0 {
					add(checkPath+".interval", "out_of_range", "must be positive")
				}
				if task.HealthCheck.Timeout < 0 {
					add(checkPath+".timeout", "out_of_range", "must be positive")
				}
				if task.HealthCheck.Threshold < 0 {
					add(checkPath+".threshold", "out_of_range", "must be at least 1 when set")
				}
				switch task.HealthCheck.Type {
				case HealthCheckHTTP, HealthCheckTCP:
					if task.HealthCheck.Port < 1 || task.HealthCheck.Port > 65535 {
						add(checkPath+".port", "out_of_range", "port is required and must be between 1 and 65535")
					}
				case HealthCheckScript:
					if len(task.HealthCheck.Command) == 0 {
						add(checkPath+".command", "required", "command is required for a script health check")
					}
				default:
					add(checkPath+".type", "unsupported", fmt.Sprintf("unsupported health check type %q", task.HealthCheck.Type))
				}
			}
		}
	}

	if len(issues) > 0 {
		return issues
	}
	return nil
}
