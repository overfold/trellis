# Sidecar pattern

**Level:** Intermediate · **Prerequisites:** complete `web-service`; use two schedulable nodes for two fixed host ports

This example runs nginx and an nginx Prometheus exporter as one scheduling unit. It demonstrates when two processes should share placement and lifecycle without being built into one image.

## What the manifest demonstrates

- A task group is the scaling and placement unit. With `count: 2`, Trellis creates two allocations and starts both `app` and `metrics-sidecar` in each allocation.
- The application owns the public port and readiness check; the exporter has a separate resource reservation.
- Both tasks explicitly use `networking.mode: host`, so the exporter can scrape nginx over the node loopback address `127.0.0.1`. Tasks do not share loopback merely because they belong to one group.
- A rolling update limits replacement concurrency to one allocation.

## Prerequisites

Use at least two schedulable nodes if both replicas must run: host-networked nginx reserves port 80 on each selected node. Each node needs containerd and the two images must be pullable. A rolling replacement also needs another compatible node with port 80 available while old and new allocations overlap.

The stock nginx image does not enable `/stub_status`. Build an nginx image with a server/location such as:

```nginx
location = /stub_status {
    stub_status;
    allow 127.0.0.1;
    deny all;
}
```

Change the `app` image in `trellis.yaml` to that image before expecting exporter metrics.

## Deploy and inspect

```sh
trellisctl jobs apply --file examples/sidecar/trellis.yaml
trellisctl --namespace default jobs status sidecar-demo
trellisctl --namespace default jobs list
```

Read each task's logs directly at the job level; no allocation UUID is required:

```sh
trellisctl jobs logs sidecar-demo --task app
trellisctl jobs logs sidecar-demo --task metrics-sidecar
```

## Adapt the pattern

Sidecars work well for local proxies, metrics exporters, and tightly coupled agents. Give each task explicit resources and pin image versions. Do not use a sidecar merely to colocate unrelated services: a group scales, updates, drains, and fails as one unit. If the helper must discover many replicas rather than only its local application, use the API-access/controller pattern instead.

[Examples index](../README.md) · [Next: Networking and discovery](../../docs/public/learning-path.md#8-namespace-networking-and-discovery)
