# Trellis

Trellis is a container scheduler built on containerd. It sits in the space between rolling your own deployment scripts and adopting Kubernetes — a real orchestrator for container workloads, without the operational complexity.

Every project that ships software ends up rebuilding the same infrastructure: workload placement, health checks, rolling updates, port reservation. Tools like Coolify improve the developer experience, but at their core they are not orchestrators. Kubernetes is a full orchestrator, but it brings significant complexity that many workloads simply do not need. Trellis is closer to Nomad in spirit: a lightweight, focused scheduler you can understand and operate yourself.

Every machine runs the same `trellis` daemon. Raft consensus elects one node to serve the control-plane API and reconcile jobs, while every node continues to run allocations. Up to five nodes vote in elections; any other node replicates state and can be promoted to replace a voter.

## Get started

Follow [Getting Started](docs/public/getting-started.md) for the single authoritative installation and first-workload walkthrough. It covers the interactive installer, automation flags, CLI connection, and the complete apply/inspect/update/log/delete lifecycle. Grow an installed node into a cluster with [Multi-node clusters](docs/public/multi-node.md), and use [Operations](docs/public/operations.md) for upgrades and removal.

## User model

All first-party interfaces use the same hierarchy and terminology:

```text
cluster
├── nodes
└── namespaces
    └── jobs
        └── task groups
            ├── tasks
            └── allocations (runtime instances)
```

The first-party CLI, `trellisctl`, lets humans author **YAML job manifests**. It converts that representation into Trellis's canonical JSON job model before contacting the control plane. Applying desired state creates or advances a job revision; Trellis then creates runtime **allocations** to satisfy the desired task-group replicas. Allocation **lifecycle** and **health** are separate concepts.

See the [Trellis user model](docs/public/user-model.md) for the canonical vocabulary shared by `trellisctl`, docs, examples, and API.

## Design principles

**Modular and extensible, but focused.** Trellis exposes clean primitives you can build on. The core repository stays lean — only the essentials live here. If you need something specific to your environment, you can extend Trellis yourself rather than waiting on a plugin ecosystem. No Kubernetes complexity, and no surface area you did not ask for.

**Non-opinionated and flexible.** Trellis provides the necessary building blocks without prescribing application architecture. Reverse proxies, for instance, are ordinary jobs rather than a special first-class service or ingress resource. Namespaces separate resources, discovery, and workload networks; API credentials grant cluster-wide access, and namespaces are not admission policy for arbitrary job manifests. Trellis does not add separate team or project abstractions on top. Operators can run Trellis as-is, or build their own frontends and abstractions on top for their specific use case. See [Multitenancy and trust boundaries](docs/public/multitenancy.md) when building for untrusted tenants.

**Consumers own representation; Trellis owns meaning.** The control-plane API consumes canonical JSON, not YAML, HCL, Python, or another authoring language. A consumer may expose any representation it wants, but it must convert that representation into the canonical JSON model before calling Trellis. Human conveniences such as `64MiB` or `10s` therefore belong to the consumer; canonical validation, defaults, planning, revision semantics, and reconciliation belong to Trellis. This keeps custom frontends and abstractions open-ended without allowing each interface to invent different Trellis semantics.

**Declarative, with open-ended delivery.** Trellis accepts declarative desired state. The first-party human-authored representation is YAML, but it is only one consumer of the canonical JSON model. You can use `trellisctl`, drive it from CI/CD, build a custom UI or HCL/Python abstraction, or integrate with any tooling that can produce the API model. The workflow that generates and submits desired state is entirely yours.

**Easy to use.** The tension between "flexible building blocks" and "easy to use" is addressed through thorough documentation and first-party examples. Trellis favors clear documentation over opinionated defaults that hide what is actually happening.

**Open-source.** Trellis is fully open-source. Read it, modify it, and run it wherever you like.

## Capabilities

- YAML job authoring with canonical JSON validation, semantic planning, revisioned apply, and rollout watching
- Named `trellisctl` cluster contexts with explicit flag/environment overrides for automation
- Node registration, heartbeats, draining, and balanced placement
- Allocation lifecycle management, health checks, restart handling, diagnostics, and filterable runtime queries
- Rolling and recreate update strategies
- Container resource limits, per-node port publishing and reservation, and persistent local volumes
- Per-namespace WireGuard networking with built-in DNS discovery, NAT egress, and published ports
- Namespace-scoped, write-only secrets with encrypted persistence and memory-backed delivery

## Documentation

There is one authoritative [documentation index](docs/README.md). New users should follow this order:

1. [Getting Started](docs/public/getting-started.md) — install, connect, deploy, inspect, update, read logs, and remove one trivial workload.
2. [Learning path](docs/public/learning-path.md) — add health checks, networking, secrets, volumes, rolling updates, sidecars, API access, and advanced architectures progressively.
3. [User model](docs/public/user-model.md) and [job manifest reference](docs/public/job-specification.md) — use the canonical vocabulary and complete schema.

The [examples index](examples/README.md) separates beginner, intermediate, and advanced patterns. Contributor and internals guides are linked from the documentation index rather than duplicated here.
