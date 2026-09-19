# Namespace networking and discovery

**Level:** intermediate  
**Prerequisites:** complete the sidecar stage; enable namespace networking on every node that may run this job; use at least two schedulable nodes if you want to observe real cross-node traffic.

This example introduces the private network attached to a Trellis namespace without introducing a proxy, ingress abstraction, or application platform. Both task groups request `networking.mode: namespace` to join the private namespace network.

The `web` task group runs two tutorial allocations. Once they are healthy, Trellis publishes them through DNS as:

```text
web.namespace-networking.default.trellis
```

The `observer` group runs the same small tutorial image with an opt-in peer probe. Every ten seconds it requests the `web` group's `/health` endpoint through that DNS name. This makes namespace networking and discovery visible in ordinary task logs instead of requiring a special debugging image.

## Prepare the nodes

Fresh Trellis installs include the namespace-networking dependencies and gVisor/runsc by default, so the normal setup path is enough:

```sh
curl -fsSL https://raw.githubusercontent.com/clofour/trellis/main/scripts/setup.sh | sudo bash
```

Keep **Namespace networking** enabled in the plan. **Customize** can opt out on deliberately minimal hosts, but every node that may run this example needs namespace networking available. For an additional cluster member, use the [documented join workflow](../../docs/public/operations.md#add-a-node). Ensure the configured WireGuard UDP range can pass between participating nodes (`51820-52075` by default); each namespace is assigned one stable port from that range.

If these nodes were installed before namespace networking was enabled, use the node configuration and setup guidance in the [learning path](../../docs/public/learning-path.md#8-namespace-networking-and-discovery) before applying this manifest.

## Run the example

From the repository root:

```sh
trellisctl jobs apply --check --file examples/namespace-networking/trellis.yaml
trellisctl jobs apply --dry-run --file examples/namespace-networking/trellis.yaml
trellisctl jobs apply --file examples/namespace-networking/trellis.yaml --wait
trellisctl jobs status namespace-networking
```

Check which nodes received the allocations. The scheduler prefers to spread replicas when capacity permits, but namespace networking does not require one allocation per node.

Then inspect the observer output:

```sh
trellisctl jobs logs namespace-networking --group observer --tail 100
```

A working namespace network and discovery path produces lines like:

```text
peer reachable: http://web.namespace-networking.default.trellis:8080/health (200 OK)
```

A temporary lookup or network failure is printed as `peer check failed`. If an allocation itself did not start, inspect current diagnostics and lifecycle history through `status`:

```sh
trellisctl jobs status namespace-networking
trellisctl jobs status namespace-networking --history
```

`status --history` is allocation lifecycle history; `jobs logs` is stdout/stderr from the tutorial process. Use `--history --allocation SHORT_ID` with the short ID from `jobs status` to narrow lifecycle history, or `jobs logs --allocation SHORT_ID` to narrow logs.

## What this demonstrates

- `namespace` is the manifest-level networking semantic; WireGuard is a current node implementation detail. `runsc` is installed by default but remains an explicitly selected task runtime.
- Healthy task-group allocations are discoverable at `group.job.namespace.trellis`.
- Discovery returns runtime endpoints; it is not leader election, locking, or application consensus.
- No host port is declared or exposed. Communication stays on the namespace network.
- The namespace remains the isolation and discovery boundary. Jobs in another namespace do not join this network merely because they know the DNS name.

Remove the example when finished:

```sh
trellisctl jobs delete namespace-networking --wait
```

[Examples index](../README.md) · [Learning path](../../docs/public/learning-path.md#8-namespace-networking-and-discovery) · [Next: API access](../api-access/)
