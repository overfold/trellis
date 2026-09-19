# Trellis documentation

This is the authoritative documentation index for Trellis. The root README is the project front door; this page owns the learning order and documentation structure. Public guides describe user and operator workflows. Developer guides explain implementation internals.

Trellis is experimental. These pages describe the current repository and do not promise production readiness.

## Start here

Read these in order if this is your first time using Trellis:

1. **[Getting Started](public/getting-started.md)** — install a single-node cluster, connect the CLI, and complete the apply/inspect/update/log/delete lifecycle.
2. **[Learning path](public/learning-path.md)** — add health checks, networking, rolling updates, secrets, volumes, sidecars, API access, and advanced patterns in a deliberate sequence.
3. **[User model](public/user-model.md)** — learn the precise vocabulary shared by manifests, the CLI, Console, examples, and API.
4. **[Core concepts](public/core-concepts.md)** — understand scheduling, reconciliation, networking, persistence, and updates.

Getting Started is the only installation walkthrough and [`examples/hello`](../examples/hello/) is the only first-workload example. Other pages link back to them rather than maintaining competing quick starts.

## Workload guides and reference

| Guide | Use it to |
|---|---|
| [CLI workflows](public/cli.md) | Manage contexts; check, preview, apply, inspect, watch, log, and delete jobs |
| [Job manifest reference](public/job-specification.md) | Look up the complete current YAML schema and validation rules |
| [Examples](../examples/README.md) | Run beginner, intermediate, and advanced manifests in learning order |
| [Cookbook](public/cookbook.md) | Adapt Trellis primitives to deployment outcomes and architecture patterns |

## Operating Trellis

| Guide | Use it to |
|---|---|
| [Operations](public/operations.md) | Maintain jobs and nodes, manage backups/secrets, observe failures, and configure TLS |
| [Trellis Console](public/dashboard.md) | Configure the first-party low-level operator console and use its operational views |

## Developer documentation

Read these after the public model unless you are working on Trellis itself:

- [Architecture and major boundaries](developer/architecture.md)
- [Control plane, reconciliation, and lifecycle](developer/control-plane.md)
- [Runtime, networking, storage, and secrets](developer/node-internals.md)
- [JSON HTTP API](developer/api.md)
- [Development and testing](developer/development.md)

## Documentation contract

- YAML is the only human-authored job-manifest format.
- The manifest schema in [Job manifest reference](public/job-specification.md) must match `orchestrator/internal/spec/types.go`.
- Commands use the canonical `apply`, `status`, `logs`, and `delete` workflow from the CLI guide; validation/planning and watch/history are modes of those commands rather than separate verbs.
- Example READMEs state their level and prerequisites; advanced patterns must not masquerade as turnkey beginner workloads.
- Internal Raft, RPC, and storage mechanics belong in developer documentation or explicitly advanced operator sections.

> Security note: use TLS, protect cluster and namespace tokens, bind administrative endpoints to a trusted network, and keep the secrets encryption key outside the data directory.
