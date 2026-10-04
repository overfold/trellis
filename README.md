# Trellis

A lightweight container orchestrator, built on containerd, for the space between deploy scripts and Kubernetes.

Trellis places containers across your machines, keeps them healthy, and rolls out changes. Unlike deployment tools such as Coolify, it is a real orchestrator. Unlike Kubernetes, it is one you can read, understand, and operate yourself, much like [Nomad](https://github.com/hashicorp/nomad). It gives you primitives rather than a platform: [Bower](https://github.com/overfold/bower), a deployment dashboard, is one example of what you can build on top.

![Trellis CLI: preview a job plan, apply the manifest, inspect a healthy allocation, and delete the job.](docs/images/demo.gif)

> [!NOTE]
> Trellis is experimental and pre-1.0. Expect breaking changes between releases, and do not rely on it for production workloads yet.

## Features

- **Declarative jobs.** YAML manifests through `trellisctl`, or JSON through the API and Go client, previewed as a plan before you apply them.
- **Scheduling and recovery.** Balanced placement, health checks, and automatic replacement of failed work.
- **Safe updates.** Rolling or recreate strategies, watched until the new version is healthy.
- **Networking.** A private WireGuard network per namespace, with DNS discovery and published ports.
- **Storage and secrets.** Persistent local volumes, and encrypted, write-only secrets.
- **High availability.** The same `trellis` daemon on every machine, with a leader elected through Raft.

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

Every machine runs the same `trellis` daemon. Raft elects one as the leader, which serves the API and schedules work, while every node, the leader included, runs workloads.

Jobs live in **namespaces** and contain **task groups**: sets of containers that Trellis places and scales together. Each running copy of a task group is an **allocation**. When you apply a job, the leader plans the change, places allocations on nodes with capacity, and replaces any that fail or whose node stops responding. The [user model](docs/public/user-model.md) defines every term.

## Design principles

These principles decide what belongs in Trellis. [Design principles](docs/public/design-principles.md) explains each one in full.

- **Modular and extensible, but focused.** Clean primitives to build on, and no surface area you did not ask for.
- **Non-opinionated and flexible.** Building blocks, not an application architecture: a reverse proxy is just another job.
- **Consumers own representation; Trellis owns meaning.** The API accepts canonical JSON, so frontends choose their own format but cannot redefine how Trellis behaves.
- **Declarative, with open-ended delivery.** Submit desired state from the CLI, CI/CD, a custom UI, or anything that speaks the API.
- **Easy to use.** Thorough documentation and examples instead of defaults that hide what is happening.
- **Open-source.** Read it, modify it, and run it wherever you like.

Namespaces are not a security boundary, because API credentials are cluster-wide. Read [Multitenancy and trust boundaries](docs/public/multitenancy.md) before exposing Trellis to untrusted tenants.

## Documentation

The [documentation index](docs/README.md) lists every guide in learning order.

- [Getting Started](docs/public/getting-started.md): install a node and run the full job lifecycle
- [Learning path](docs/public/learning-path.md): add features one at a time, from health checks to API access
- [Examples](examples/README.md): beginner, intermediate, and advanced manifests
- [Job manifest reference](docs/public/job-specification.md): the complete YAML schema
- [CLI workflows](docs/public/cli.md): contexts and every job command
- [Operations](docs/public/operations.md): upgrades, backups, TLS, and troubleshooting

## Contributing

Bug reports, feature requests, and questions go in [GitHub issues](https://github.com/overfold/trellis/issues).

To build and test Trellis locally, see [Development and testing](docs/developer/development.md). Read the [architecture guide](docs/developer/architecture.md) before changing subsystem boundaries.
