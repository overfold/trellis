package spec

import "testing"

func TestCanonicalizeAppliesResourceDefaultsAndBounds(t *testing.T) {
	limits := Limits{MaxReplicasPerTaskGroup: 2, MaxTaskGroupsPerJob: 1, MaxTasksPerTaskGroup: 1, MaxDesiredAllocations: 2, DefaultTaskCPU: 250, DefaultTaskMemory: 512 << 20}
	job := validJob()
	if err := Canonicalize(job, limits); err != nil {
		t.Fatalf("canonicalize simple job: %v", err)
	}
	resources := job.TaskGroups[0].Tasks[0].Resources
	if resources == nil || resources.CPU != 250 || resources.Memory != 512<<20 {
		t.Fatalf("resources = %#v, want configured defaults", resources)
	}

	for name, mutate := range map[string]func(*JobSpec){
		"replicas": func(job *JobSpec) { job.TaskGroups[0].Count = 3 },
		"task groups": func(job *JobSpec) {
			job.TaskGroups = append(job.TaskGroups, TaskGroupSpec{Name: "worker", Count: 1, Tasks: []TaskSpec{{Name: "worker", Image: "worker"}}})
		},
		"tasks": func(job *JobSpec) {
			job.TaskGroups[0].Tasks = append(job.TaskGroups[0].Tasks, TaskSpec{Name: "sidecar", Image: "sidecar"})
		},
		"total allocations": func(job *JobSpec) { job.TaskGroups[0].Count = 3 },
	} {
		t.Run(name, func(t *testing.T) {
			job := validJob()
			mutate(job)
			if err := Canonicalize(job, limits); err == nil {
				t.Fatal("expected operator limit rejection")
			}
		})
	}
}

func TestCanonicalizeRejectsExplicitZeroResources(t *testing.T) {
	for _, resources := range []*ResourcesSpec{{CPU: 0, Memory: 1}, {CPU: 1, Memory: 0}, {CPU: -1, Memory: 1}, {CPU: 1, Memory: -1}} {
		job := validJob()
		job.TaskGroups[0].Tasks[0].Resources = resources
		if err := Canonicalize(job, DefaultLimits()); err == nil {
			t.Fatalf("resources %#v accepted", resources)
		}
	}
}
