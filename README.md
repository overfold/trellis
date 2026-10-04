# Trellis

A lightweight container orchestrator, built on containerd, for the space between deploy scripts and Kubernetes.

Every project that ships software ends up rebuilding the same infrastructure: workload placement, health checks, rolling updates, and port reservation. Deployment tools such as Coolify improve the developer experience, but they are not orchestrators. Kubernetes is, but it brings complexity that many workloads do not need. Trellis is closer to [Nomad](https://github.com/hashicorp/nomad) in spirit: a focused scheduler that you can read, understand, and operate yourself.

You describe the jobs you want to run, and Trellis places them across your machines, keeps them healthy, and rolls out changes. It gives you primitives rather than a platform, so you can build your own workflows on top. [Bower](https://github.com/overfold/bower), a deployment dashboard, is one example.

> [!NOTE]
> Trellis is experimental and pre-1.0. Expect breaking changes between releases, and do not rely on it for production workloads yet.

## Features

- **Declarative jobs.** Write YAML job manifests, validate them locally, preview a semantic plan, and apply them as revisions you can watch roll out.
- **Scheduling and placement.** Nodes register, heartbeat, and drain. Trellis balances allocations across them and reserves published ports per node.
- **Health and recovery.** HTTP and TCP health checks, restart policies, bounded replacement backoff, and diagnostics that explain why a job is not ready.
- **Updates.** Rolling and recreate strategies, with every change previewed as a plan before you apply it.
- **Networking.** Each namespace gets its own WireGuard network with built-in DNS discovery, NAT egress, and published ports.
- **Storage and secrets.** Persistent local volumes, and write-only, namespace-scoped secrets that are encrypted at rest and delivered to tasks in memory.
- **Highly available control plane.** Every machine runs the same `trellis` daemon. Raft elects a leader to serve the API, while every node keeps running workloads.
- **CLI and API.** `trellisctl` with named cluster contexts for humans and automation, and a JSON HTTP API with a public Go client for everything else.

## Quick start

You need a Debian or Ubuntu x86-64 machine with `sudo`.

1. Install a single-node cluster. The installer shows its plan before it changes anything, installs containerd if it is missing, and saves a `trellisctl` context for your user:

   ```sh
   curl -fsSL https://raw.githubusercontent.com/overfold/trellis/main/scripts/install.sh | sudo bash
   ```

2. Download the `hello` example manifest and deploy it:

   ```sh
   curl -fsSL https://raw.githubusercontent.com/overfold/trellis/main/examples/hello/trellis.yaml -o trellis.yaml
   trellisctl jobs apply --file trellis.yaml --wait
   ```

3. Check that it is running and read its logs:

   ```sh
   trellisctl jobs status hello
   trellisctl jobs logs hello --tail 100
   ```

[Getting Started](docs/public/getting-started.md) explains each step and continues the walkthrough with an update and a clean removal. To grow the node into a cluster, see [Multi-node clusters](docs/public/multi-node.md).

## How it works

Every interface, including the CLI, API, docs, and examples, uses the same hierarchy:

```text
cluster
├── nodes
└── namespaces
    └── jobs
        └── task groups
            ├── tasks
            └── allocations (runtime instances)
```

A **job** describes the desired state of a workload. Applying it creates a new job revision, and Trellis then creates **allocations** to run the requested number of copies of each task group. An allocation's **lifecycle**, such as running or failed, is tracked separately from its **health**.

Each cluster elects a leader through Raft. The leader serves the API and reconciles jobs, while every node, the leader included, runs allocations. Up to five nodes vote in elections. Any additional node replicates state and can be promoted to replace a voter. The [user model](docs/public/user-model.md) defines this vocabulary precisely.

## Design principles

These principles decide what belongs in Trellis and what does not. Contributors use them to judge scope and tradeoffs.

**Modular and extensible, but focused.** Trellis exposes clean primitives that you can build on, and the core keeps only the essentials. If your environment needs something specific, extend Trellis yourself instead of waiting for a plugin ecosystem. You get no Kubernetes-style complexity, and no surface area you did not ask for.

**Non-opinionated and flexible.** Trellis provides building blocks without prescribing an application architecture. A reverse proxy, for example, is an ordinary job rather than a special ingress resource, and Trellis adds no team or project abstractions. Run Trellis as it is, or build your own frontend and abstractions on top of it for your use case.

**Consumers own representation; Trellis owns meaning.** The API accepts only canonical JSON, not YAML, HCL, Python, or another authoring language. Each consumer can offer any representation it likes, but converts it to canonical JSON before calling Trellis. Human conveniences such as `64MiB` or `10s` therefore belong to the consumer, while validation, defaults, planning, revisions, and reconciliation belong to Trellis. Frontends stay open-ended, and no interface can invent its own Trellis semantics.

**Declarative, with open-ended delivery.** Trellis accepts declarative desired state and does not care how it is produced. YAML through `trellisctl` is the first-party option, but you can equally drive Trellis from CI/CD, a custom UI, an HCL or Python abstraction, or any tool that can produce the API model.

**Easy to use through clarity.** Trellis resolves the tension between flexible building blocks and ease of use with thorough documentation and first-party examples, not with opinionated defaults that hide what is actually happening.

**Open source and inspectable.** You can read Trellis, modify it, and run it wherever you like.

Namespaces separate resources, service discovery, and workload networks, but they are not a security boundary. API credentials are cluster-wide, so read [Multitenancy and trust boundaries](docs/public/multitenancy.md) before exposing Trellis to untrusted tenants.

## Documentation

The [documentation index](docs/README.md) lists every guide in learning order.

- [Getting Started](docs/public/getting-started.md): install a node and run the full job lifecycle
- [Learning path](docs/public/learning-path.md): add health checks, networking, rolling updates, secrets, volumes, sidecars, and API access, one at a time
- [Examples](examples/README.md): beginner, intermediate, and advanced manifests
- [CLI workflows](docs/public/cli.md): contexts, and how to check, preview, apply, inspect, and delete jobs
- [Job manifest reference](docs/public/job-specification.md): the complete YAML schema and validation rules
- [Operations](docs/public/operations.md): upgrades, backups, TLS, failure diagnosis, and removal
- [Multi-node clusters](docs/public/multi-node.md): adding nodes, choosing a cluster size, and handling node failure
- [Architecture](docs/developer/architecture.md): how Trellis is built, for contributors and integrators

## Contributing

Bug reports, feature requests, and questions go in [GitHub issues](https://github.com/overfold/trellis/issues).

To build and test Trellis locally, see [Development and testing](docs/developer/development.md). Read the [architecture guide](docs/developer/architecture.md) before changing subsystem boundaries.
