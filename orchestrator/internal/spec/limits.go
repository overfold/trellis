package spec

import "fmt"

// Limits is the operator-controlled policy applied to every job before it is
// persisted or reconciled. It is deliberately absent from JobSpec so authors
// cannot relax cluster safety bounds.
type Limits struct {
	MaxReplicasPerTaskGroup           int
	MaxTaskGroupsPerJob               int
	MaxTasksPerTaskGroup              int
	MaxDesiredAllocations             int
	MaxDesiredAllocationsPerNamespace int
	DefaultTaskCPU                    int
	DefaultTaskMemory                 ByteSize
	MaxTaskCPU                        int
	MaxTaskMemory                     ByteSize
}

// DefaultLimits provides bounded, useful defaults for small clusters.
func DefaultLimits() Limits {
	return Limits{
		MaxReplicasPerTaskGroup:           500,
		MaxTaskGroupsPerJob:               64,
		MaxTasksPerTaskGroup:              32,
		MaxDesiredAllocations:             1000,
		MaxDesiredAllocationsPerNamespace: 10000,
		DefaultTaskCPU:                    100,
		DefaultTaskMemory:                 128 << 20,
		MaxTaskCPU:                        1_000_000,
		MaxTaskMemory:                     1 << 40,
	}
}

// ValidateLimits checks operator policy before it is used to admit jobs.
func ValidateLimits(limits Limits) error {
	if limits.MaxReplicasPerTaskGroup < 1 || limits.MaxTaskGroupsPerJob < 1 || limits.MaxTasksPerTaskGroup < 1 || limits.MaxDesiredAllocations < 1 || limits.MaxDesiredAllocationsPerNamespace < 1 {
		return fmt.Errorf("job limits must be positive")
	}
	if limits.DefaultTaskCPU <= 0 || limits.DefaultTaskMemory <= 0 || limits.MaxTaskCPU <= 0 || limits.MaxTaskMemory <= 0 {
		return fmt.Errorf("default task CPU and memory must be positive")
	}
	if limits.DefaultTaskCPU > limits.MaxTaskCPU || limits.DefaultTaskMemory > limits.MaxTaskMemory {
		return fmt.Errorf("default task resources exceed their maximums")
	}
	return nil
}

// Canonicalize resolves operator-owned task resource defaults, then validates
// the complete canonical job. Explicit zero is invalid and never means default.
func Canonicalize(job *JobSpec, limits Limits) error {
	if err := ValidateLimits(limits); err != nil {
		return err
	}
	if job == nil {
		return Validate(job)
	}
	for groupIndex := range job.TaskGroups {
		for taskIndex := range job.TaskGroups[groupIndex].Tasks {
			task := &job.TaskGroups[groupIndex].Tasks[taskIndex]
			if task.Resources == nil {
				task.Resources = &ResourcesSpec{CPU: limits.DefaultTaskCPU, Memory: limits.DefaultTaskMemory}
			}
		}
	}
	return ValidateWithLimits(job, limits)
}

// ValidateWithLimits validates a resolved canonical job against its operator
// policy. Call Canonicalize for untrusted author input.
func ValidateWithLimits(job *JobSpec, limits Limits) error {
	if err := ValidateLimits(limits); err != nil {
		return err
	}
	if err := Validate(job); err != nil {
		return err
	}
	issues := ValidationErrors{}
	add := func(path, message string) {
		issues = append(issues, ValidationIssue{Path: path, Code: "limit_exceeded", Message: message})
	}
	if len(job.TaskGroups) > limits.MaxTaskGroupsPerJob {
		add("task_groups", fmt.Sprintf("exceeds operator limit of %d task groups", limits.MaxTaskGroupsPerJob))
	}
	total := int64(0)
	totalExceeded := false
	for i, group := range job.TaskGroups {
		path := fmt.Sprintf("task_groups[%d]", i)
		if group.Name != "" {
			path = fmt.Sprintf("task_groups[%s]", group.Name)
		}
		if group.Count > limits.MaxReplicasPerTaskGroup {
			add(path+".count", fmt.Sprintf("exceeds operator limit of %d replicas", limits.MaxReplicasPerTaskGroup))
		}
		if len(group.Tasks) > limits.MaxTasksPerTaskGroup {
			add(path+".tasks", fmt.Sprintf("exceeds operator limit of %d tasks", limits.MaxTasksPerTaskGroup))
		}
		for taskIndex, task := range group.Tasks {
			if task.Resources.CPU > limits.MaxTaskCPU {
				add(fmt.Sprintf("%s.tasks[%d].resources.cpu", path, taskIndex), fmt.Sprintf("exceeds operator limit of %d millicores", limits.MaxTaskCPU))
			}
			if task.Resources.Memory > limits.MaxTaskMemory {
				add(fmt.Sprintf("%s.tasks[%d].resources.memory", path, taskIndex), fmt.Sprintf("exceeds operator limit of %d bytes", limits.MaxTaskMemory))
			}
		}
		if int64(group.Count) > int64(limits.MaxDesiredAllocations)-total {
			totalExceeded = true
		} else {
			total += int64(group.Count)
		}
	}
	if totalExceeded || total > int64(limits.MaxDesiredAllocations) {
		add("task_groups", fmt.Sprintf("desired allocations exceed operator limit of %d", limits.MaxDesiredAllocations))
	}
	if len(issues) > 0 {
		return issues
	}
	return nil
}
