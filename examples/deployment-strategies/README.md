# Deployment strategy examples

**Level:** Advanced · **Prerequisites:** run the `web-service` rolling update and operate an external proxy/controller for traffic-switching patterns

This directory compares three release patterns. Trellis implements `recreate` and `rolling` as task-group update strategies. Blue/green and canary releases are compositions of independent jobs plus an external, label-driven proxy; `blue_green` and `canary` are not valid strategy values.

Every manifest in this directory uses host networking and reserves node port 80, so every simultaneously running allocation needs a distinct node where port 80 is free. Publishing port 80 from namespace networking has the same one-per-node constraint. These examples are therefore intentionally capacity-expensive: they make the overlap required by each release pattern visible instead of hiding it behind a networking abstraction.

The fixtures use explicit nginx version tags so the release changes remain readable. Tags can still be repointed; replace them with qualified image digests before adapting these patterns to production.

## Rolling update

`rolling.yaml` runs three replicas with a readiness check and `max_parallel: 1`.

```sh
trellisctl jobs apply --file examples/deployment-strategies/rolling.yaml
# Change the image tag or digest, then apply the same job again.
trellisctl jobs apply --file examples/deployment-strategies/rolling.yaml
trellisctl --namespace default jobs status shop-rolling
```

Old allocations become draining. Trellis starts at most one not-yet-healthy replacement at a time and removes old capacity after replacements become healthy. With the fixed port in this example, three steady-state replicas require three nodes and `max_parallel: 1` requires at least one additional compatible node during the update. If the readiness check never succeeds, progress intentionally stalls; inspect allocation events and task logs rather than repeatedly applying the same manifest.

Rollback is another revision: restore the earlier image digest/configuration and apply it. Reusing an earlier tag resolves its current content, which may no longer be the original artifact. API consumers can submit a retained version's `spec` and `resolved_images` together for an exact rollback. Trellis does not erase revision history by calling the new revision a rollback.

## Blue/green switch

`blue.yaml` and `green.yaml` use different job names and route labels, so they can coexist when enough nodes are available:

```sh
trellisctl jobs apply --file examples/deployment-strategies/blue.yaml
trellisctl jobs apply --file examples/deployment-strategies/green.yaml
trellisctl --namespace default jobs status shop-green
```

Configure the external proxy/controller for `route:shop-blue`, validate green directly, then change the filter to `route:shop-green` and reload the proxy. Keep blue during an observation window for a fast routing rollback:

```sh
# After the release is accepted:
trellisctl --namespace default jobs delete shop-blue
```

This pattern requires roughly double workload capacity during overlap; with three port-80 replicas in each release, that means six suitable nodes for the manifests as written. Schema and data migrations must remain compatible with both releases until blue is retired. The traffic switch is external state and should be reviewed, versioned, and observable.

## Weighted canary

`stable.yaml` and `canary.yaml` share `route:shop-weighted` but use `track` and `trellis/weight` labels:

```sh
trellisctl jobs apply --file examples/deployment-strategies/stable.yaml
trellisctl jobs apply --file examples/deployment-strategies/canary.yaml
```

`trellis/weight` is a convention of the separate [`trellis-proxy-sync`](https://github.com/overfold/trellis-proxy-sync) project, not something Trellis interprets. Run `trellis-proxy-sync -label route:shop-weighted -container-port 80 ...` with a template that consumes each upstream's weight. Observe errors, latency, saturation, and application-specific success metrics by release track. Increase canary exposure by changing its weight or replica count; remove it immediately with `trellisctl jobs delete shop-canary`.

Weights apply to individual discovered allocations. Four stable replicas at weight 100 plus one canary at weight 5 produce an aggregate stable weight of 400 and canary weight of 5. The manifests therefore require five nodes while both tracks run. Confirm the resulting percentage and load-balancer semantics, especially with sticky sessions or long-lived connections.

## Requirements common to all strategies

- Pin production images to immutable digests; mutable tags make rollback ambiguous.
- Use a readiness check that proves the process can serve real requests, not merely that its port opened.
- Budget node/port capacity for every allocation that must coexist during a release.
- Keep the proxy's cluster-scoped discovery token private; enforce any namespace selection in trusted controller configuration, not through token authorization.
- Monitor desired, running, and healthy counts throughout a release.
- Make database and message-format changes compatible across every version that may run simultaneously.

The public [Cookbook](../../docs/public/cookbook.md) explains the reasoning and tradeoffs in more detail; these files provide concrete manifest shapes.

[Examples index](../README.md) · [Learning path](../../docs/public/learning-path.md)
