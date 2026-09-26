package main

import (
	"fmt"
	"os"

	"github.com/clofour/trellis/internal/nodecapacity"
	"github.com/clofour/trellis/internal/spec"
	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"
)

type reservedResourcesConfig struct {
	CPU    *int    `yaml:"cpu"`
	Memory *string `yaml:"memory"`
}

type nodeResourcesConfig struct {
	Reserved *reservedResourcesConfig `yaml:"reserved"`
}

type jobLimitsConfig struct {
	MaxReplicasPerTaskGroup           *int    `yaml:"max_replicas_per_task_group"`
	MaxTaskGroupsPerJob               *int    `yaml:"max_task_groups_per_job"`
	MaxTasksPerTaskGroup              *int    `yaml:"max_tasks_per_task_group"`
	MaxDesiredAllocations             *int    `yaml:"max_desired_allocations"`
	MaxDesiredAllocationsPerNamespace *int    `yaml:"max_desired_allocations_per_namespace"`
	DefaultTaskCPU                    *int    `yaml:"default_task_cpu"`
	DefaultTaskMemory                 *string `yaml:"default_task_memory"`
	MaxTaskCPU                        *int    `yaml:"max_task_cpu"`
	MaxTaskMemory                     *string `yaml:"max_task_memory"`
}

type nodeConfigFile struct {
	AgentListen        *string              `yaml:"agent_listen"`
	AgentAdvertise     *string              `yaml:"agent_advertise"`
	ServerListen       *string              `yaml:"server_listen"`
	ServerAdvertise    *string              `yaml:"server_advertise"`
	RaftListen         *string              `yaml:"raft_listen"`
	RaftAdvertise      *string              `yaml:"raft_advertise"`
	Join               *string              `yaml:"join"`
	DataDir            *string              `yaml:"data_dir"`
	Cluster            *string              `yaml:"cluster"`
	AdminTokenHash     *string              `yaml:"admin_token_hash"`
	EnrollmentToken    *string              `yaml:"enrollment_token"`
	NodeSigningMode    *string              `yaml:"node_signing_mode"`
	ContainerdSock     *string              `yaml:"containerd_socket"`
	Runtime            *string              `yaml:"runtime"`
	RuntimeFaults      *string              `yaml:"runtime_faults"`
	WireGuardPool      *string              `yaml:"wireguard_pool"`
	WireGuardEndpoint  *string              `yaml:"wireguard_endpoint"`
	WireGuardPort      *int                 `yaml:"wireguard_port"`
	WireGuardPortCount *int                 `yaml:"wireguard_port_count"`
	DNSListen          *string              `yaml:"dns_listen"`
	CACert             *string              `yaml:"ca_cert"`
	CAKey              *string              `yaml:"ca_key"`
	Cert               *string              `yaml:"cert"`
	Key                *string              `yaml:"key"`
	SecretsKey         *string              `yaml:"secrets_key"`
	SecretsKeyID       *string              `yaml:"secrets_key_id"`
	Labels             *[]string            `yaml:"labels"`
	Resources          *nodeResourcesConfig `yaml:"resources"`
	JobLimits          *jobLimitsConfig     `yaml:"job_limits"`
}

func loadNodeConfig(path string, cfg *config, flags *pflag.FlagSet) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open config %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	var parsed nodeConfigFile
	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(&parsed); err != nil {
		return fmt.Errorf("parse config %s: %w", path, err)
	}

	setString := func(flag string, value *string, target *string) {
		if value != nil && !flags.Changed(flag) {
			*target = *value
		}
	}
	setString("agent-listen", parsed.AgentListen, &cfg.AgentListen)
	setString("agent-advertise", parsed.AgentAdvertise, &cfg.AgentAdvertise)
	setString("server-listen", parsed.ServerListen, &cfg.ServerListen)
	setString("server-advertise", parsed.ServerAdvertise, &cfg.ServerAdvertise)
	setString("raft-listen", parsed.RaftListen, &cfg.RaftListen)
	setString("raft-advertise", parsed.RaftAdvertise, &cfg.RaftAdvertise)
	setString("join", parsed.Join, &cfg.Join)
	setString("data-dir", parsed.DataDir, &cfg.DataDir)
	setString("cluster", parsed.Cluster, &cfg.Cluster)
	setString("admin-token-hash", parsed.AdminTokenHash, &cfg.AdminTokenHash)
	setString("enrollment-token", parsed.EnrollmentToken, &cfg.EnrollmentToken)
	setString("node-signing-mode", parsed.NodeSigningMode, &cfg.SigningMode)
	setString("containerd-sock", parsed.ContainerdSock, &cfg.ContainerdSock)
	setString("runtime", parsed.Runtime, &cfg.Runtime)
	setString("runtime-faults", parsed.RuntimeFaults, &cfg.RuntimeFaults)
	setString("wireguard-pool", parsed.WireGuardPool, &cfg.WireGuardPool)
	setString("wireguard-endpoint", parsed.WireGuardEndpoint, &cfg.WireGuardEndpoint)
	setString("dns-listen", parsed.DNSListen, &cfg.DNSListen)
	setString("ca-cert", parsed.CACert, &cfg.CACert)
	setString("ca-key", parsed.CAKey, &cfg.CAKey)
	setString("cert", parsed.Cert, &cfg.Cert)
	setString("key", parsed.Key, &cfg.Key)
	setString("secrets-key", parsed.SecretsKey, &cfg.SecretsKey)
	setString("secrets-key-id", parsed.SecretsKeyID, &cfg.SecretsKeyID)
	if parsed.WireGuardPort != nil && !flags.Changed("wireguard-port") {
		cfg.WireGuardPort = *parsed.WireGuardPort
	}
	if parsed.WireGuardPortCount != nil && !flags.Changed("wireguard-port-count") {
		cfg.WireGuardPortCount = *parsed.WireGuardPortCount
	}
	if parsed.Labels != nil && !flags.Changed("label") {
		cfg.Labels = append([]string(nil), (*parsed.Labels)...)
	}
	if parsed.JobLimits != nil {
		limits := parsed.JobLimits
		setInt := func(flag string, value *int, target *int) {
			if value != nil && !flags.Changed(flag) {
				*target = *value
			}
		}
		setInt("max-replicas-per-task-group", limits.MaxReplicasPerTaskGroup, &cfg.MaxReplicasPerTaskGroup)
		setInt("max-task-groups-per-job", limits.MaxTaskGroupsPerJob, &cfg.MaxTaskGroupsPerJob)
		setInt("max-tasks-per-task-group", limits.MaxTasksPerTaskGroup, &cfg.MaxTasksPerTaskGroup)
		setInt("max-desired-allocations", limits.MaxDesiredAllocations, &cfg.MaxDesiredAllocations)
		setInt("max-desired-allocations-per-namespace", limits.MaxDesiredAllocationsPerNamespace, &cfg.MaxDesiredAllocationsPerNamespace)
		setInt("default-task-cpu", limits.DefaultTaskCPU, &cfg.DefaultTaskCPU)
		setString("default-task-memory", limits.DefaultTaskMemory, &cfg.DefaultTaskMemory)
		setInt("max-task-cpu", limits.MaxTaskCPU, &cfg.MaxTaskCPU)
		setString("max-task-memory", limits.MaxTaskMemory, &cfg.MaxTaskMemory)
	}

	// Resource reservation policy belongs to the Trellis node. Omitted values
	// retain Trellis's built-in defaults; the installer does not materialize
	// those defaults into configuration files.
	if err := nodecapacity.ConfigureReserve(nil, nil); err != nil {
		return err
	}
	if parsed.Resources != nil && parsed.Resources.Reserved != nil {
		reserved := parsed.Resources.Reserved
		var memory *int64
		if reserved.Memory != nil {
			value, err := spec.ParseByteSize(*reserved.Memory)
			if err != nil {
				return fmt.Errorf("resources.reserved.memory: %w", err)
			}
			bytes := int64(value)
			memory = &bytes
		}
		if err := nodecapacity.ConfigureReserve(reserved.CPU, memory); err != nil {
			return fmt.Errorf("resources.reserved: %w", err)
		}
	}
	return nil
}
