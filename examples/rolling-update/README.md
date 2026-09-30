# Rolling update

**Level:** Intermediate · **Prerequisites:** complete [`replicated-service`](../replicated-service/) and have a third compatible node available while exercising the update

This example keeps the same two-replica healthy service and adds only rollout policy:

```yaml
update:
  strategy: rolling
  max_parallel: 1
```

A rolling update starts healthy replacement capacity before removing all old capacity. Because every replica publishes node port 8080, an old and new allocation cannot overlap on the same node. With two existing replicas, the first replacement therefore needs a third compatible node with port 8080 free.

## Deploy the first revision

```sh
trellisctl jobs apply --check --file examples/rolling-update/trellis.yaml
trellisctl jobs apply --dry-run --file examples/rolling-update/trellis.yaml
trellisctl jobs apply --file examples/rolling-update/trellis.yaml --wait
trellisctl jobs status rolling-update
```

## Change one execution-affecting field

Change only the tutorial image:

```diff
-        image: ghcr.io/overfold/trellis-tutorial:v1
+        image: ghcr.io/overfold/trellis-tutorial:v2
```

Preview and apply the new revision:

```sh
trellisctl jobs apply --dry-run --file examples/rolling-update/trellis.yaml
trellisctl jobs apply --file examples/rolling-update/trellis.yaml --wait
trellisctl jobs status rolling-update
trellisctl jobs logs rolling-update --tail 100
```

The plan should show the image change. During convergence, old and new allocations overlap; Trellis waits for healthy replacement capacity before completing the rollout. If overlap capacity or health blocks progress, `trellisctl jobs status rolling-update` includes workload diagnostics; pair it with `trellisctl nodes status NODE` for node capacity/configuration.

Remove it when finished:

```sh
trellisctl jobs delete rolling-update --wait
```

Continue through the [example learning path](../README.md) for secrets, volumes, sidecars, namespace networking, API access, and advanced release patterns.
