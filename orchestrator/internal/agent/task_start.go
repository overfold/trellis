package agent

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"time"

	"github.com/containerd/errdefs"
	"github.com/overfold/trellis/orchestrator/internal/health"
	"github.com/overfold/trellis/orchestrator/internal/network"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// taskStart describes one task of an allocation start.
type taskStart struct {
	// ID identifies the task record and its container.
	ID            string
	AllocationID  string
	Generation    uint64
	JobRevision   int
	ExecutionHash string
	Namespace     string
	JobName       string
	GroupName     string
	Spec          *spec.TaskSpec
	Runtime       string
	NetworkPlan   *network.Plan
	EnvOverrides  map[string]string
	Secrets       []nodeapi.DeliveredSecret
	Restart       *spec.RestartPolicySpec
	// Draining and DrainSequence are the generation's drain state; a draining
	// task starts restart-suppressed.
	Draining      bool
	DrainSequence uint64
}

// taskLaunch records what a task start has acquired, so a failed start
// releases exactly that. alloc is the durable record under construction;
// readers see copies published to Agent.allocations.
type taskLaunch struct {
	task             *taskStart
	alloc            *Allocation
	ports            []*runtime.Port
	secretDir        string
	netAttachment    *network.Attachment
	containerCreated bool
	startAttempted   bool
	tracked          bool
	healthRegistered bool
	committed        bool
}

// startTask creates and starts one allocation task. The caller holds the
// allocation operation lock and has pulled the task image; runGroupTasks
// applies the generation's drain state to existing records first.
func (a *Agent) startTask(ctx context.Context, task *taskStart) (runErr error) {
	if task.Spec == nil {
		return fmt.Errorf("task spec is required")
	}
	if task.ID == "" {
		return fmt.Errorf("allocation ID is required")
	}
	restartAttempts, restartWindow, running, err := a.prepareTaskRecord(ctx, task)
	if err != nil || running {
		return err
	}
	alloc := &Allocation{ID: task.ID, ContainerID: task.ID, AllocationID: task.AllocationID, Generation: task.Generation, JobRevision: task.JobRevision, ExecutionHash: task.ExecutionHash, Restart: task.Restart, RestartAttempts: restartAttempts, RestartWindow: restartWindow, Namespace: task.Namespace, JobName: task.JobName, GroupName: task.GroupName, TaskName: task.Spec.Name, Spec: task.Spec, Status: "starting", Health: "unknown", Draining: task.Draining, DrainSequence: task.DrainSequence}
	starting := *alloc
	a.mu.Lock()
	if a.orphanDetaches[task.ID] {
		a.mu.Unlock()
		return fmt.Errorf("orphaned network cleanup for task %s is still in progress; retry the start", task.ID)
	}
	a.allocations[task.ID] = &starting
	a.mu.Unlock()
	if err := a.persistAllocation(alloc); err != nil {
		a.mu.Lock()
		delete(a.allocations, task.ID)
		a.mu.Unlock()
		return fmt.Errorf("persist starting allocation: %w", err)
	}
	launch := &taskLaunch{task: task, alloc: alloc}
	defer func() {
		if !launch.committed {
			runErr = errors.Join(runErr, a.abortTaskStart(ctx, launch))
		}
	}()
	return a.launchTask(ctx, launch)
}

// prepareTaskRecord checks an existing record of the task. It reports running
// when a start retry finds the task running, and otherwise returns the
// generation's restart accounting after cleaning up an incomplete earlier
// start.
func (a *Agent) prepareTaskRecord(ctx context.Context, task *taskStart) (int, time.Time, bool, error) {
	a.mu.RLock()
	existing := a.allocations[task.ID]
	var snapshot Allocation
	if existing != nil {
		snapshot = *existing
	}
	a.mu.RUnlock()
	if existing == nil {
		// Checked before any start state exists, so a refused start never
		// reaches cleanup that would release staging it does not own. A
		// tracked allocation's staging is released by its own stop below.
		return 0, time.Time{}, false, a.releaseOrphanedStaging(ctx, task.ID)
	}
	// Restart accounting belongs to the allocation generation. A start retry
	// for the same generation keeps it, so repeated retries or agent restarts
	// cannot hide a crash loop behind a fresh budget.
	restartAttempts, restartWindow := snapshot.RestartAttempts, snapshot.RestartWindow
	if snapshot.AllocationID != task.AllocationID || snapshot.Generation != task.Generation || snapshot.JobRevision != task.JobRevision || snapshot.ExecutionHash != task.ExecutionHash {
		return 0, time.Time{}, false, fmt.Errorf("%w: %s", ErrAllocationExists, task.ID)
	}
	// An exhausted restart budget is terminal for this generation; a start
	// retry must not recreate the task with a fresh budget.
	if snapshot.RestartExhausted {
		return 0, time.Time{}, false, fmt.Errorf("%w: allocation %s", ErrRestartBudgetExhausted, task.ID)
	}
	status := snapshot.Status
	if snapshot.unobserved {
		observed, err := a.observeRecovered(ctx, task.ID)
		if err != nil {
			return 0, time.Time{}, false, err
		}
		status = observed
	}
	if status == "running" {
		return 0, time.Time{}, true, nil
	}
	// A previous start may have reached the runtime but failed before it
	// could be committed. Preserve its resources while Stop is uncertain,
	// then finish that cleanup on a later retry instead of converting the
	// retry into a terminal execution conflict. An empty status means
	// recovery already confirmed the container missing and cleaned it up.
	if status != "" {
		if err := a.stopAllocation(context.WithoutCancel(ctx), task.ID, false); err != nil {
			return 0, time.Time{}, false, fmt.Errorf("clean up incomplete allocation %s before retry: %w", task.ID, err)
		}
	}
	return restartAttempts, restartWindow, false, nil
}

// launchTask acquires the task's resources, then creates, starts, and commits
// its container. startTask releases whatever it acquired when it fails.
func (a *Agent) launchTask(ctx context.Context, launch *taskLaunch) error {
	task, alloc, ts := launch.task, launch.alloc, launch.task.Spec
	var taskPorts []spec.PortSpec
	if ts.Networking != nil {
		taskPorts = ts.Networking.Ports
	}
	for _, p := range taskPorts {
		if base, count := a.nodeInfo.WireGuardPortBase, a.nodeInfo.WireGuardPortCount; count > 0 && p.NodePort() >= base && p.NodePort() < base+count {
			return fmt.Errorf("claim node port %d: reserved for namespace WireGuard networks (%d-%d)", p.NodePort(), base, base+count-1)
		}
		port, err := a.ports.Claim(p)
		if err != nil {
			return fmt.Errorf("claim node port %d: %w", p.NodePort(), err)
		}

		launch.ports = append(launch.ports, port)
		alloc.Ports = append([]*runtime.Port(nil), launch.ports...)
		if err := a.persistAllocation(alloc); err != nil {
			return fmt.Errorf("persist port claim: %w", err)
		}
	}

	var mounts []*runtime.Mount
	for _, v := range ts.Volumes {
		mount, err := a.volumes.Create(task.Namespace, task.JobName, task.ID, v)
		if err != nil {
			return fmt.Errorf("create volume %s: %w", v.Name, err)
		}

		mounts = append(mounts, mount)
		alloc.Mounts = append([]*runtime.Mount(nil), mounts...)
	}
	if err := a.persistAllocation(alloc); err != nil {
		return fmt.Errorf("persist volume metadata: %w", err)
	}

	var taskNetMode spec.TaskNetworkMode
	if ts.Networking != nil {
		taskNetMode = ts.Networking.Mode
	}
	hostMode := taskNetMode == spec.TaskNetworkHost
	wireGuard := taskNetMode == spec.TaskNetworkWireGuard
	if wireGuard {
		if err := a.attachTaskNetwork(ctx, launch); err != nil {
			return err
		}
	}
	env := make(map[string]string, len(ts.Env)+len(task.EnvOverrides))
	maps.Copy(env, ts.Env)
	taskSecrets := task.Secrets
	for k, v := range task.EnvOverrides {
		if k == "TRELLIS_TOKEN" {
			// Canonical admission reserves this name for API-enabled groups.
			value := []byte(v)
			defer clear(value)
			taskSecrets = append(taskSecrets, nodeapi.DeliveredSecret{Task: ts.Name, Name: "api-access-token", Target: spec.SecretTargetEnv, Env: k, Value: value})
			continue
		}
		env[k] = v
	}
	if taskHasSecrets(ts.Name, taskSecrets) {
		secretDir, err := a.secretDirFor(task.ID)
		if err != nil {
			return err
		}
		// Record the location before any plaintext is written so a restarted
		// agent can always find and remove it.
		launch.secretDir, alloc.SecretDir = secretDir, secretDir
		if err := a.persistAllocation(alloc); err != nil {
			return fmt.Errorf("persist secret metadata: %w", err)
		}
		if err := createSecretDir(secretDir); err != nil {
			// Never clean up a directory this start did not create.
			launch.secretDir, alloc.SecretDir = "", ""
			return err
		}
	}
	secretMounts, err := materializeSecrets(launch.secretDir, ts.Name, taskSecrets)
	if err != nil {
		return err
	}
	mounts = append(mounts, secretMounts...)
	alloc.Mounts = append([]*runtime.Mount(nil), mounts...)
	extraHosts := map[string]string{}
	if _, apiAccess := task.EnvOverrides["TRELLIS_ADDR"]; apiAccess {
		switch {
		case hostMode:
			extraHosts["trellis"] = "127.0.0.1"
		case wireGuard && task.NetworkPlan != nil:
			extraHosts["trellis"] = task.NetworkPlan.Gateway
		}
	}
	networkNamespace := ""
	if launch.netAttachment != nil {
		networkNamespace = launch.netAttachment.NetworkNamespace
	} else if hostMode {
		networkNamespace = "/proc/1/ns/net"
	}
	if err := a.createTaskContainer(ctx, launch, env, mounts, networkNamespace, extraHosts); err != nil {
		return err
	}

	launch.startAttempted = true
	if err := a.runtime.Start(ctx, alloc.ContainerID); err != nil {
		observed, inspectErr := a.runtime.Inspect(context.WithoutCancel(ctx), alloc.ContainerID)
		if inspectErr != nil || observed.Status != runtime.StatusRunning {
			return fmt.Errorf("start container %s: %w", alloc.ContainerID, err)
		}
	}
	return a.commitTaskStart(launch, mounts)
}

// attachTaskNetwork attaches the task to its namespace WireGuard network.
func (a *Agent) attachTaskNetwork(ctx context.Context, launch *taskLaunch) error {
	task, alloc := launch.task, launch.alloc
	if task.NetworkPlan == nil {
		return fmt.Errorf("automatic WireGuard network plan is required")
	}
	unlock, err := a.lockNetworkPlan(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	plan := *task.NetworkPlan
	a.mu.RLock()
	current, ok := a.networkPlans[task.Namespace]
	a.mu.RUnlock()
	if ok {
		// API access belongs to this task's execution, not the peer topology.
		current.APIPort = plan.APIPort
		plan = current
	}
	task.NetworkPlan = &plan
	// Record the intent before Attach so a restarted agent can find and
	// detach an attachment whose result was never recorded.
	alloc.NetworkIntent = &network.AttachmentIntent{AllocationID: task.ID, Namespace: task.Namespace, Network: task.Namespace}
	if err := a.persistAllocation(alloc); err != nil {
		return fmt.Errorf("persist network intent: %w", err)
	}
	ports := make([]network.PortMapping, 0, len(launch.ports))
	for _, port := range launch.ports {
		ports = append(ports, network.PortMapping{HostPort: port.HostPort, ContainerPort: port.ContainerPort})
	}
	attachment, err := a.network.Attach(ctx, network.AttachRequest{AllocationID: task.ID, Namespace: task.Namespace, Network: task.Namespace, Plan: plan, Ports: ports})
	if err != nil {
		return fmt.Errorf("attach WireGuard network: %w", err)
	}
	launch.netAttachment = attachment
	alloc.Network = attachment
	if err := a.persistAllocation(alloc); err != nil {
		return fmt.Errorf("persist network attachment: %w", err)
	}
	return nil
}

// createTaskContainer creates the task container. The record is marked
// unverified first, so an ambiguous create is resolved by inspecting the
// container's execution labels rather than by assuming it is absent.
func (a *Agent) createTaskContainer(ctx context.Context, launch *taskLaunch, env map[string]string, mounts []*runtime.Mount, networkNamespace string, extraHosts map[string]string) error {
	task, alloc, ts := launch.task, launch.alloc, launch.task.Spec
	labels := map[string]string{
		"trellis.cluster":               a.cluster,
		"trellis.allocation-id":         task.AllocationID,
		"trellis.allocation-generation": strconv.FormatUint(task.Generation, 10),
		"trellis.job-revision":          strconv.Itoa(task.JobRevision),
		"trellis.execution-hash":        task.ExecutionHash,
		"trellis.namespace":             task.Namespace,
		"trellis.job":                   task.JobName,
		"trellis.task-group":            task.GroupName,
		"trellis.task":                  ts.Name,
	}
	runtimeMounts := append([]*runtime.Mount(nil), mounts...)
	runtimeMounts = append(runtimeMounts, &runtime.Mount{
		HostPath:      a.healthProbe,
		ContainerPath: health.ProbeContainerPath,
		ReadOnly:      true,
	})
	var cpu int
	var memory int64
	if ts.Resources != nil {
		cpu, memory = ts.Resources.CPU, int64(ts.Resources.Memory)
	}
	alloc.ContainerOwnershipUnverified = true
	if err := a.persistAllocation(alloc); err != nil {
		alloc.ContainerOwnershipUnverified = false
		return fmt.Errorf("persist pending container creation: %w", err)
	}
	_, err := a.runtime.Create(ctx, runtime.CreateOptions{
		ID:               alloc.ContainerID,
		Image:            ts.Image,
		Env:              env,
		Mounts:           runtimeMounts,
		CPU:              cpu,
		Memory:           memory,
		PidsLimit:        a.taskPidsLimit(),
		Runtime:          task.Runtime,
		NetworkNamespace: networkNamespace,
		DNSServers:       a.dnsServers,
		ExtraHosts:       extraHosts,
		Labels:           labels,
	})
	if err != nil {
		observed, inspectErr := a.runtime.Inspect(context.WithoutCancel(ctx), alloc.ContainerID)
		if inspectErr != nil {
			return errors.Join(fmt.Errorf("create container %s: %w", alloc.ContainerID, err), fmt.Errorf("inspect container ownership: %w", inspectErr))
		}
		if !a.containerMatchesAllocation(*observed, alloc) {
			return fmt.Errorf("create container %s: %w", alloc.ContainerID, err)
		}
	}
	launch.containerCreated = true
	alloc.ContainerOwnershipUnverified = false
	if err := a.persistAllocation(alloc); err != nil {
		return fmt.Errorf("persist verified container creation: %w", err)
	}
	return nil
}

// commitTaskStart stores the running record, then starts restart tracking and
// health checks for it.
func (a *Agent) commitTaskStart(launch *taskLaunch, mounts []*runtime.Mount) error {
	task, alloc, ts := launch.task, launch.alloc, launch.task.Spec
	ready := &Allocation{
		ID:            task.ID,
		AllocationID:  task.AllocationID,
		Generation:    task.Generation,
		JobRevision:   task.JobRevision,
		ExecutionHash: task.ExecutionHash,
		Restart:       task.Restart,
		Namespace:     task.Namespace,

		RestartAttempts: alloc.RestartAttempts,
		RestartWindow:   alloc.RestartWindow,

		JobName:   task.JobName,
		GroupName: task.GroupName,
		TaskName:  ts.Name,
		Spec:      ts,

		ContainerID: alloc.ContainerID,
		Ports:       launch.ports,
		Mounts:      mounts,
		SecretDir:   launch.secretDir,
		Network:     launch.netAttachment,
		Status:      "running",
		Health:      "unknown",

		Draining:      task.Draining,
		DrainSequence: task.DrainSequence,
	}
	ready.NetworkIntent = alloc.NetworkIntent
	if ts.HealthCheck == nil {
		ready.Health = "healthy"
	}
	if err := a.persistAllocation(ready); err != nil {
		return fmt.Errorf("persist allocation: %w", err)
	}
	a.mu.Lock()
	a.allocations[task.ID] = ready
	a.mu.Unlock()
	// Track only after the running record is stored, so a restart decision
	// (including terminal exhaustion) is never overwritten by startup.
	if task.Draining {
		a.reconciler.TrackStopping(task.ID, ts.HealthCheck != nil, task.Restart, alloc.RestartAttempts, alloc.RestartWindow, false)
	} else {
		a.reconciler.TrackRecovered(task.ID, ts.HealthCheck != nil, task.Restart, alloc.RestartAttempts, alloc.RestartWindow, false)
	}
	launch.tracked = true
	if ts.HealthCheck != nil {
		check := *ts.HealthCheck
		a.health.RegisterTask(task.ID, alloc.ContainerID, &check, "namespace", alloc.Namespace, "job", alloc.JobName, "allocation", alloc.AllocationID, "task", alloc.TaskName)
		launch.healthRegistered = true
	}
	if ts.HealthCheck == nil {
		if err := a.reconciler.ObserveHealth(task.ID, true); err != nil {
			return fmt.Errorf("mark allocation healthy: %w", err)
		}
	}
	// Keep volume staging mounts until the container is removed: they
	// are its OCI mount sources, which every restarted task resolves again.
	launch.committed = true
	return nil
}

// abortTaskStart releases what a failed task start acquired. A container that
// may exist, or whose ownership is unverified, keeps its record as stopping
// together with its resources, so a later start, stop, or recovery finishes
// the cleanup.
func (a *Agent) abortTaskStart(ctx context.Context, launch *taskLaunch) error {
	task, alloc := launch.task, launch.alloc
	if launch.startAttempted {
		if launch.tracked {
			if err := a.reconciler.BeginStop(ctx, task.ID); err != nil {
				return fmt.Errorf("suppress restarts before abort: %w", err)
			}
		} else {
			// Start may have succeeded even if its response was lost. Track
			// the retained allocation as stopping from the outset so an
			// observed stopped task can never be restarted.
			a.reconciler.TrackStopping(task.ID, false, task.Restart, alloc.RestartAttempts, alloc.RestartWindow, false)
			launch.tracked = true
		}
	}
	// Publish the latest durable startup state before marking it stopping.
	// Startup builds alloc privately so readers never observe it changing.
	failed := *alloc
	a.mu.Lock()
	a.allocations[task.ID] = &failed
	a.mu.Unlock()
	errs := []error{a.markAllocationStopping(task.ID)}
	if alloc.ContainerOwnershipUnverified {
		return errors.Join(append(errs, fmt.Errorf("container ownership for %s remains unverified", task.ID))...)
	}
	if launch.startAttempted {
		if err := a.runtime.Stop(context.WithoutCancel(ctx), task.ID); err != nil {
			return errors.Join(append(errs, fmt.Errorf("stop container %s during failed start: %w", task.ID, err))...)
		}
	}
	if launch.healthRegistered {
		a.health.DeregisterTask(task.ID)
	}
	var cleanupErrs []error
	if launch.tracked {
		if err := a.reconciler.Untrack(task.ID); err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("untrack allocation %s: %w", task.ID, err))
		}
	}
	containerRemoved := true
	if launch.containerCreated {
		if err := a.runtime.Remove(context.WithoutCancel(ctx), task.ID); err != nil {
			containerRemoved = false
			cleanupErrs = append(cleanupErrs, fmt.Errorf("remove container %s: %w", task.ID, err))
		}
	}
	if err := a.detachAllocationNetwork(context.WithoutCancel(ctx), alloc); err != nil {
		cleanupErrs = append(cleanupErrs, fmt.Errorf("detach allocation network: %w", err))
	}
	if launch.secretDir != "" {
		if err := removeSecretDir(launch.secretDir); err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("remove secret files: %w", err))
		}
	}
	// A container that still exists keeps its staging mounts as OCI mount
	// sources; a cleanup retry releases them after removal succeeds.
	if containerRemoved {
		if err := a.volumes.ReleaseStaging(task.ID); err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("release volume staging: %w", err))
		}
	}
	if err := errors.Join(cleanupErrs...); err != nil {
		return errors.Join(append(errs, err)...)
	}
	if err := a.deleteAllocationRecord(task.ID); err != nil {
		return errors.Join(append(errs, fmt.Errorf("delete allocation record: %w", err))...)
	}
	// Port release is infallible. Keep claims until no cleanup retry can
	// release a port that has since been assigned to another allocation.
	for _, p := range launch.ports {
		_ = a.ports.Release(p)
	}
	a.mu.Lock()
	delete(a.allocations, task.ID)
	a.forgetIdleNetworkPlanLocked(task.Namespace)
	a.mu.Unlock()
	return errors.Join(errs...)
}

// releaseOrphanedStaging ensures a start never stages volumes over staging
// kept for an existing container. Staging whose container is gone was left
// behind, for example when recovery could not list containers, and is released.
func (a *Agent) releaseOrphanedStaging(ctx context.Context, allocID string) error {
	inUse, err := a.volumes.StagingInUse(allocID)
	if err != nil {
		return fmt.Errorf("check volume staging: %w", err)
	}
	if !inUse {
		return nil
	}
	if _, err := a.runtime.Inspect(ctx, allocID); !errdefs.IsNotFound(err) {
		if err != nil {
			return fmt.Errorf("verify container %s before releasing volume staging: %w", allocID, err)
		}
		return fmt.Errorf("%w: container %s still exists", errStagingInUse, allocID)
	}
	if err := a.volumes.ReleaseStaging(allocID); err != nil {
		return fmt.Errorf("release orphaned volume staging: %w", err)
	}
	return nil
}
