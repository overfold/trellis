# Healthy web service

**Level:** Intermediate · **Prerequisites:** complete [`hello`](../hello/) on one healthy node

This example adds the first service-specific concerns to the minimal workload without introducing scaling or rollout strategy yet:

- `networking.ports` publishes port 8080, where the tutorial application listens, on the node that runs it;
- an HTTP health check keeps the allocation unready until `/health` succeeds.

The task omits `networking.mode`, so it joins the default `namespace` network: it gets a private address, and Trellis forwards node port 8080 to it. Add `host_port` to publish on a different node port, for example `host_port: 80`. See [Networking and ports](../../docs/public/job-specification.md#networking-and-ports) for the other modes.

## Deploy and inspect

```sh
trellisctl jobs apply --check --file examples/web-service/trellis.yaml
trellisctl jobs apply --dry-run --file examples/web-service/trellis.yaml
trellisctl jobs apply --file examples/web-service/trellis.yaml --wait
trellisctl jobs status web-service
```

The allocations listed by `jobs status` show the selected node and the published node port. From a trusted test network, open `http://NODE_ADDRESS:8080` or query `/health` to verify that the service is reachable. This example deliberately serves plaintext HTTP; do not expose it to an untrusted network without placing it behind TLS.

If the health check does not succeed, `jobs status` includes the failure details automatically; pair it with logs when needed:

```sh
trellisctl jobs status web-service
trellisctl jobs logs web-service
```

Remove it when finished:

```sh
trellisctl jobs delete web-service --wait
```

Next, continue to [`replicated-service`](../replicated-service/) to add a second replica and learn the placement consequences of a fixed node port before introducing rolling replacement.
