export type NodeStatus = "healthy" | "unhealthy" | "draining";

export interface Node {
  id: string;
  host: string;
  port: number;
  status: NodeStatus;
  last_heartbeat: string;
  cpu: number;
  memory: number;
  os?: string;
  arch?: string;
  labels?: Record<string, string>;
  volumes?: string[];
  capabilities?: string[];
  version?: string;
}

export interface PortMapping {
  host_port: number;
  container_port: number;
}

export interface AllocationEndpoint {
  task: string;
  address?: string;
  ports?: PortMapping[];
}

export type AllocationPhase =
  | "placed"
  | "pending"
  | "starting"
  | "running"
  | "stopping"
  | "stopped"
  | "failed"
  | "lost";
export type AllocationHealth = "unknown" | "healthy" | "unhealthy";

export interface Allocation {
  id: string;
  job?: string;
  group: string;
  namespace?: string;
  node_id: string;
  labels?: Record<string, string>;
  address?: string;
  ports?: PortMapping[];
  endpoints?: AllocationEndpoint[];
  phase: AllocationPhase;
  health: AllocationHealth;
  draining?: boolean;
  generation: number;
  job_revision: number;
  created_at?: string;
  last_transition_at?: string;
  reason?: string;
  message?: string;
  attempt?: number;
  next_retry_at?: string | null;
}

export interface AllocationEvent {
  phase: AllocationPhase;
  reason?: string;
  message?: string;
  at: string;
}

export interface Job {
  namespace?: string;
  name: string;
  revision: number;
  desired: number;
  running: number;
  healthy: number;
  allocations: Allocation[] | null;
  spec?: JobSpec;
}

export interface SecretMetadata {
  namespace: string;
  name: string;
  version: number;
  created_at: string;
  updated_at: string;
  ciphertext_size: number;
  key_id: string;
}

// time.Duration is encoded as nanoseconds and memory as bytes in the JSON API.
// The YAML editor accepts human forms such as "10s" and "64MiB" and converts
// them before sending canonical JSON to Trellis.
export type DurationValue = number | string;

export interface JobSpec {
  name: string;
  namespace: string;
  task_groups: TaskGroupSpec[];
}

export type Runtime = "" | "runc" | "runsc";
export type TaskNetworkMode = "" | "isolated" | "host" | "namespace";
export type UpdateStrategy = "" | "recreate" | "rolling";
export type APIAccessScope = "namespace" | "cluster";
export type APIAccessLevel = "read" | "write";

export interface APIAccessSpec {
  scope: APIAccessScope;
  access: APIAccessLevel;
}

export interface TaskGroupSpec {
  name: string;
  count: number;
  runtime?: Runtime;
  tasks: TaskSpec[];
  labels?: Record<string, string>;
  api_access?: APIAccessSpec;
  restart?: RestartPolicySpec;
  constraints?: ConstraintSpec[];
  update?: UpdateSpec;
}

export interface ConstraintSpec {
  attribute: string;
  value: string;
}

export interface RestartPolicySpec {
  max_restarts: number;
  window: DurationValue;
}

export interface UpdateSpec {
  strategy: UpdateStrategy;
  max_parallel?: number;
}

export interface TaskSpec {
  name: string;
  image: string;
  env?: Record<string, string>;
  networking?: TaskNetworkingSpec;
  volumes?: VolumeSpec[];
  resources?: ResourcesSpec;
  health_check?: HealthCheckSpec;
  secrets?: SecretRefSpec[];
}

export interface TaskNetworkingSpec {
  mode?: TaskNetworkMode;
  ports?: PortSpec[];
}

export interface PortSpec {
  port: number;
}

export interface VolumeSpec {
  name: string;
  host_path: string;
  container_path: string;
  read_only?: boolean;
}

export type SecretTarget = "env" | "file";

export interface SecretRefSpec {
  name: string;
  target: SecretTarget;
  env?: string;
  path?: string;
  mode?: number;
}

export interface ResourcesSpec {
  cpu: number;
  memory: number;
}

export interface HealthCheckSpec {
  type: "http" | "tcp" | "script";
  port: number;
  path?: string;
  command?: string[];
  interval?: DurationValue;
  timeout?: DurationValue;
  threshold?: number;
}
