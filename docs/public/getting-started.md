# Getting Started

This is the shortest complete Trellis journey: install one node, use the CLI as your normal user, deploy a deliberately small workload, inspect and update it, read its logs, and remove it again. You do not need to clone or build the repository.

## 1. Install one node

You need a Debian or Ubuntu x86-64 machine with `sudo`. The installer can install containerd when it is missing.

```sh
curl -fsSL https://raw.githubusercontent.com/overfold/trellis/main/scripts/setup.sh | sudo bash
```

The default plan is the feature-complete beginner path: create a new single-node cluster, auto-detect a reachable node address, and install the namespace-networking dependencies (WireGuard, iproute2, and iptables, which every node needs) and gVisor/runsc. The plan is shown before anything changes. Press Enter to install it, or choose **Customize** to change the cluster mode, address, or gVisor. You do not need to discover command-line flags just to make a different first-install choice.

For automation, the same choices are available as flags. `--without-gvisor` opts out of gVisor.

The installer uses the administrator key transiently to mint a normal `cluster/write` operator credential and saves a `local` context for the user who invoked `sudo`. It displays the base64 PKCS#8 Ed25519 private key once so you can move it to an operator password manager; the daemon receives and replicates only the public key. Routine `trellisctl` commands therefore do **not** need `sudo` and do not receive the administrator key.

Verify the service and saved context:

```sh
sudo systemctl status trellis --no-pager
trellisctl context current
trellisctl nodes list
```

`trellis`, `trellisctl`, and the internal `trellis-health-probe` helper are installed in `/usr/local/bin`. The daemon mounts the helper read-only into managed tasks for HTTP and TCP health checks; it is not an operator CLI. The daemon keeps its configuration and secrets key root-readable under `/etc/trellis`, but never the administrator private key; your user context contains the scoped operator token plus the cluster CA. Adding nodes later uses short-lived join tokens that the administrator mints on demand; see [Multi-node clusters](multi-node.md#add-a-node).

## 2. Create the first manifest

Create an empty working directory and save this as `trellis.yaml`:

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/overfold/trellis/main/schemas/trellis-job.schema.json
name: hello
namespace: default
task_groups:
  - name: web
    count: 1
    tasks:
      - name: hello
        image: ghcr.io/overfold/trellis-tutorial:v1
        resources:
          cpu: 100
          memory: 64MiB
```

This is one job containing one task group, one desired allocation, and one task. It intentionally declares no networking, ports, explicit health check, volume, secret, API access, or update policy yet; the task gets the default private namespace network without being reachable from outside it. A running task without an explicit health check is considered healthy.

The image is a tiny first-party tutorial workload. It stays running and emits a recognizable `Trellis tutorial v1` log line, so the first deployment has something concrete to inspect. The same file is maintained at [`examples/hello/trellis.yaml`](../../examples/hello/trellis.yaml).

## 3. Check, preview, and deploy

```sh
trellisctl jobs apply --check --file trellis.yaml
trellisctl jobs apply --dry-run --file trellis.yaml
trellisctl jobs apply --file trellis.yaml --wait
```

`--check` parses and validates the human YAML locally without contacting the cluster. `--dry-run` and normal apply use canonical JSON and Trellis's server-owned planning semantics.

## 4. Inspect the job

```sh
trellisctl jobs list
trellisctl jobs status hello
trellisctl jobs logs hello --tail 100
```

You should see the tutorial v1 startup/log message. If the job is not ready, `jobs status` includes the relevant placement, lifecycle, retry, and health diagnostics automatically. Use `trellisctl jobs status hello --history` when you need the recorded lifecycle transitions.

## 5. Update it

Change only the image tag:

```yaml
image: ghcr.io/overfold/trellis-tutorial:v2
```

Then preview and apply the new revision:

```sh
trellisctl jobs apply --dry-run --file trellis.yaml
trellisctl jobs apply --file trellis.yaml --wait
trellisctl jobs status hello
trellisctl jobs logs hello --tail 100
```

The semantic plan shows the image change, Trellis replaces the allocation, and the new logs identify tutorial v2. This makes the first update visible both before and after it happens.

## 6. Remove it

```sh
trellisctl jobs delete hello --wait
trellisctl jobs list
```

You have now completed the full workload lifecycle: install → connect → deploy → inspect → update → logs → remove.

## Troubleshooting

- `sudo journalctl -u trellis -n 200` shows daemon/control-plane logs.
- `trellisctl jobs status hello` summarizes placement, start, retry, and health failures when the job is not ready.
- `trellisctl jobs status hello --history` shows allocation lifecycle transitions.
- Image-pull failures usually mean the node cannot reach GHCR or the image/tag is unavailable.
- `trellisctl context current` and `trellisctl nodes list` verify the saved operator connection.

Continue with the [learning path](learning-path.md). It reuses the tutorial application and adds concepts one at a time: first a published port and `/health`, then replicas and placement, then rolling-update overlap before moving on to secrets, volumes, sidecars, namespace networking, API access, release patterns, and stateful architectures.

[Documentation index](../README.md) · [Next: Learning path](learning-path.md)
