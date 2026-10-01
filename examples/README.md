# Trellis examples

The examples form a learning path, not a flat catalog. Start with one conceptually small workload and add operational concerns only after the preceding behavior is familiar. Every `trellis.yaml` uses the same YAML job-manifest schema accepted by the CLI, and every manifest is parsed and validated by the test suite.

Complete [Getting Started](../docs/public/getting-started.md) first, or follow the expanded [learning path](../docs/public/learning-path.md). Commands below assume `trellisctl` is connected to the manifest's namespace.

## Beginner

| Order | Example | Adds |
| --- | --- | --- |
| 1 | [`hello/`](hello/) | One job, task group, allocation, and task; the complete check → preview → apply → inspect → update → logs → delete lifecycle. |

The first workload deliberately omits ports, health-check settings, secrets, volumes, and rollout configuration. Those are important, but none is required to see Trellis reconcile desired state.

## Intermediate building blocks

| Order | Example | Adds | Prerequisites |
| --- | --- | --- | --- |
| 2 | [`web-service/`](web-service/) | One reachable service, one published node port, and HTTP health. | One healthy node. |
| 3 | [`replicated-service/`](replicated-service/) | A second replica and the placement consequences of a shared fixed node port. | At least two schedulable nodes. |
| 4 | [`rolling-update/`](rolling-update/) | Rolling replacement and explicit overlap/capacity requirements. | Two running replicas plus a third compatible node for old/new overlap. |
| 5 | [`secrets/`](secrets/) | Namespace-scoped secrets delivered as an environment variable and a file. | Shared node secrets-encryption key. |
| 6 | [`volumes/`](volumes/) | Trellis-managed and absolute host paths, durable volume registrations, and placement constraints. | Prepared node path and label. |
| 7 | [`sidecar/`](sidecar/) | Two tasks deliberately coupled in one placement/scaling/lifecycle unit. | Two schedulable nodes for two fixed host ports; a custom nginx image for useful metrics. |
| 8 | [`namespace-networking/`](namespace-networking/) | Private namespace networking and DNS discovery between ordinary workloads. | Namespace networking enabled on every participating node; two nodes recommended for a cross-node path. |

These examples teach reusable primitives. Apply one only after reading its README, because host networking, namespace networking, secret key management, and node-local storage all carry operator responsibilities outside the manifest.

## Advanced patterns

| Example | Composes | Why it is advanced |
| --- | --- | --- |
| [`api-access/`](api-access/) | Cluster-scoped read API access and a controller loop. | The whole task group receives cluster-wide visibility and must be trusted. |
| [`deployment-strategies/`](deployment-strategies/) | Rolling, blue/green, and weighted canary releases. | Blue/green and canary routing require an external proxy/controller, enough nodes for fixed listeners, and release observability. |
| [`wordpress/`](wordpress/) | Multiple tasks, host networking, secrets, host volumes, health checks, and restart policy. | It is a coupled development stack, not a production topology. |
| [`patroni/`](patroni/) | Namespace networking, optional runsc sandboxing, discovery, secrets, local volumes, and recreate replacement. | It is an architecture skeleton that still requires a real DCS, routing, fencing, backups, and failure testing. |

These are compositions of ordinary Trellis primitives, not new resource types. Treat them as design references and risk checklists rather than turnkey production deployments.

## Apply the first example

From the repository root:

```sh
trellisctl jobs apply --check --file examples/hello/trellis.yaml
trellisctl jobs apply --dry-run --file examples/hello/trellis.yaml
trellisctl jobs apply --file examples/hello/trellis.yaml --wait
trellisctl jobs status hello
trellisctl jobs logs hello --tail 100
trellisctl jobs delete hello --wait
```

For a manifest in another namespace, select or create a context for that namespace, or pass `--namespace`. The CLI rejects a mismatch between the effective namespace and the manifest instead of silently applying to a different scope.

[Documentation index](../docs/README.md) · [Job manifest reference](../docs/public/job-specification.md)
