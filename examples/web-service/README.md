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
trellisctl nodes list
```

Match the allocation's **Node** ID from `jobs status` to the **ID** in `nodes list`. The **Node** column in `nodes list` gives the node address with its agent port (`8127` by default); use that host with the workload port **8080**, declared in the manifest. `jobs status` does not list published ports.

From a trusted test network, open `http://NODE_ADDRESS:8080` or query `/health`, replacing `NODE_ADDRESS` with that host (not the agent port):

```sh
curl --fail http://NODE_ADDRESS:8080/health
curl --fail http://NODE_ADDRESS:8080/
```

Expect `ok` from `/health` and the tutorial v1 greeting from `/`. This example deliberately serves plaintext HTTP; do not expose it to an untrusted network without placing it behind TLS.

If the health check does not succeed, `jobs status` includes the failure details automatically; pair it with logs when needed:

```sh
trellisctl jobs status web-service
trellisctl jobs logs web-service
```

Remove it when finished:

```sh
trellisctl jobs delete web-service --wait
```

## Try it without cloning

To try just this example after Getting Started, download the manifest matching your installed release into your working directory:

```sh
TRELLIS_VERSION=$(trellisctl version)
curl -fsSL "https://raw.githubusercontent.com/overfold/trellis/${TRELLIS_VERSION}/examples/web-service/trellis.yaml" -o web-service.yaml
trellisctl jobs apply --check --file web-service.yaml
trellisctl jobs apply --dry-run --file web-service.yaml
trellisctl jobs apply --file web-service.yaml --wait
trellisctl jobs status web-service
trellisctl nodes list
```

Use the address lookup, reachability checks, and removal command above. For further lessons, [clone the release's examples and guides](../../docs/public/learning-path.md#get-the-examples); no build is needed.

Next, [choose your continuation](../../docs/public/learning-path.md#choose-your-next-step): stay on one node with your own image, secrets, or volumes, or add nodes and continue to [`replicated-service`](../replicated-service/) to learn placement before rolling replacement.
