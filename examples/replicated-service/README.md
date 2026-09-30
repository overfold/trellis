# Replicated web service

**Level:** Intermediate · **Prerequisites:** complete [`web-service`](../web-service/) and have at least two schedulable nodes

This example changes one idea from the single-service example: `count` becomes `2`.

Each replica publishes node port 8080. Because one node cannot publish the same node port twice, the two allocations must land on different nodes. This makes the placement consequence of scaling explicit before adding rolling-update overlap.

## Deploy and inspect placement

```sh
trellisctl jobs apply --check --file examples/replicated-service/trellis.yaml
trellisctl jobs apply --dry-run --file examples/replicated-service/trellis.yaml
trellisctl jobs apply --file examples/replicated-service/trellis.yaml --wait
trellisctl jobs status replicated-service
```

The status output should show two healthy allocations on different nodes. Query either node on port 8080 to reach a replica.

If only one compatible node is available, `jobs status` shows the placement failure. Pair it with the node views when needed:

```sh
trellisctl jobs status replicated-service
trellisctl nodes list
trellisctl nodes status NODE
```

to inspect the placement failure and the relevant node capacity/configuration.

Remove it when finished:

```sh
trellisctl jobs delete replicated-service --wait
```

Next, continue to [`rolling-update`](../rolling-update/) to keep these two replicas and introduce overlapping replacement as a separate concept.
