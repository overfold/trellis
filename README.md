# Trellis

Trellis is a container scheduler built on containerd. It sits in the space between rolling your own deployment scripts and adopting Kubernetes — a real orchestrator for container workloads, without the operational complexity.

Every project that ships software ends up rebuilding the same infrastructure: workload placement, health checks, rolling updates, port reservation. Tools like Coolify improve the developer experience, but at their core they are not orchestrators. Kubernetes is a full orchestrator, but it brings significant complexity that many workloads simply do not need. Trellis is closer to Nomad in spirit: a lightweight, focused scheduler you can understand and operate yourself.

Every machine runs the same `trellis` daemon. Raft consensus elects one node to serve the control-plane API and reconcile jobs, while every node continues to run allocations and participate in the next election.

## Quick start

The setup script downloads the latest release binaries, configures a systemd service, and creates a single-node cluster. It supports Linux x64 and requires root access. The normal plan auto-detects the node address, installs containerd when it is missing, and makes namespace networking and gVisor available out of the box. The dashboard remains disabled by default because it exposes an additional service and credential.

```sh
curl -fsSL https://raw.githubusercontent.com/clofour/trellis/main/scripts/setup.sh | sudo bash
```

The installer shows the complete plan before changing the machine. Press Enter to use it, or choose **Customize** to change cluster mode, node address, namespace networking, gVisor, or dashboard access without hunting for command-line flags.

Or clone the repository and run the script directly:

```sh
git clone https://github.com/clofour/trellis.git
sudo ./trellis/scripts/setup.sh
```

The same choices remain available as flags for automation. For example, a deliberately minimal host can opt out of the default networking and sandboxing dependencies:

```sh
curl -fsSL https://raw.githubusercontent.com/clofour/trellis/main/scripts/setup.sh | \
  sudo bash -s -- --without-networking --without-gvisor
```

Use `--join HOST:8128` to add a machine to an existing cluster. Managed enrollment uses a pinned copy of the cluster node CA plus a dedicated enrollment credential; administrator access and the shared secrets-encryption key remain separate. See [Operations](docs/public/operations.md#add-a-node) for managed and external-signing workflows.

Upgrade and removal use the same lifecycle tooling without requiring a repository clone:

```sh
curl -fsSL https://raw.githubusercontent.com/clofour/trellis/main/scripts/upgrade.sh | sudo bash
curl -fsSL https://raw.githubusercontent.com/clofour/trellis/main/scripts/uninstall.sh | sudo bash
```

See the [getting-started guide](docs/public/getting-started.md) for a walkthrough of deploying your first workload.

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

The first-party CLI and dashboard let humans author **YAML job manifests**. They convert that representation into Trellis's canonical JSON job model before contacting the control plane. Applying desired state creates or advances a job revision; Trellis then creates runtime **allocations** to satisfy the desired task-group replicas. Allocation **lifecycle** and **health** are separate concepts.

See the [Trellis user model](docs/public/user-model.md) for the canonical vocabulary shared by `trellisctl`, the dashboard, docs, examples, and API.

## Design principles

**Modular and extensible, but focused.** Trellis exposes clean primitives you can build on. The core repository stays lean — only the essentials live here. If you need something specific to your environment, you can extend Trellis yourself rather than waiting on a plugin ecosystem. No Kubernetes complexity, and no surface area you did not ask for.

**Non-opinionated and flexible.** Trellis provides the necessary building blocks without prescribing application architecture. Reverse proxies, for instance, are ordinary jobs rather than a special first-class service or ingress resource. Namespaces provide the tenant, authorization, discovery, and workload-isolation boundary; Trellis does not add separate team or project abstractions on top. Operators can run Trellis as-is, or build their own frontends and abstractions on top for their specific use case.

**Consumers own representation; Trellis owns meaning.** The control-plane API consumes canonical JSON, not YAML, HCL, Python, or another authoring language. A consumer may expose any representation it wants, but it must convert that representation into the canonical JSON model before calling Trellis. Human conveniences such as `64MiB` or `10s` therefore belong to the consumer; canonical validation, defaults, planning, revision semantics, and reconciliation belong to Trellis. This keeps custom frontends and abstractions open-ended without allowing each interface to invent different Trellis semantics.

**Declarative, with open-ended delivery.** Trellis accepts declarative desired state. The first-party human-authored representation is YAML, but it is only one consumer of the canonical JSON model. You can use `trellisctl`, drive it from CI/CD, use the first-party dashboard, build a custom UI or HCL/Python abstraction, or integrate with any tooling that can produce the API model. The workflow that generates and submits desired state is entirely yours.

**Easy to use.** The tension between "flexible building blocks" and "easy to use" is addressed through thorough documentation and first-party examples. Trellis favors clear documentation over opinionated defaults that hide what is actually happening.

**Open-source.** Trellis is fully open-source. Read it, modify it, and run it wherever you like.

## Capabilities

- YAML job authoring with canonical JSON validation, semantic planning, revisioned apply, and rollout watching
- Named `trellisctl` cluster contexts with explicit flag/environment overrides for automation
- Node registration, heartbeats, draining, and balanced placement
- Allocation lifecycle management, health checks, restart handling, diagnostics, and filterable runtime queries
- Rolling and recreate update strategies
- Container resource limits, explicit host-port reservations, and persistent local volumes
- Built-in DNS discovery for healthy job allocations and optional WireGuard namespace networking
- Namespace-scoped, write-only secrets with encrypted persistence and memory-backed delivery
- A Next.js operations dashboard for cluster health, job/allocation diagnostics, node draining, secret management, and opt-in job writes

## Documentation

There is one authoritative [documentation index](docs/README.md). New users should follow this order:

1. [Getting Started](docs/public/getting-started.md) — install, connect, deploy, inspect, update, read logs, and remove one trivial workload.
2. [Learning path](docs/public/learning-path.md) — add health checks, networking, secrets, volumes, rolling updates, sidecars, API access, and advanced architectures progressively.
3. [User model](docs/public/user-model.md) and [job manifest reference](docs/public/job-specification.md) — use the canonical vocabulary and complete schema.

The [examples index](examples/README.md) separates beginner, intermediate, and advanced patterns. Contributor and internals guides are linked from the documentation index rather than duplicated here.
