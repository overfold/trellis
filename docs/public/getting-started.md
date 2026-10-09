# Getting Started

This is the shortest complete Trellis journey: install one node, use the CLI as your normal user, deploy a deliberately small workload, inspect and update it, read its logs, and remove it again. You do not need to clone or build the repository.

## 1. Install one node

You need a systemd-based Debian or Ubuntu x86-64 host with `sudo`, `curl`, `jq`, OpenSSL, `tar`, `bzip2`, and `sha256sum`/`sha512sum` (GNU coreutils), and outbound access to GitHub, Google Cloud Storage, and the package repositories. If needed, install the download/verification prerequisites with `sudo apt-get update && sudo apt-get install -y curl ca-certificates jq openssl tar bzip2 coreutils`. The installer can install containerd when it is missing. Run it on the host, not inside a container; Trellis and containerd need the same host mount namespace.

Published Linux x86-64 binaries are static (`CGO_ENABLED=0`, baseline `GOAMD64=v1`), so they do not require the release builder's glibc. CI executes the release archive on Ubuntu 20.04, Ubuntu 24.04, and Debian 12 userspace. This checks binary compatibility, not an entire systemd/containerd host installation. Kernel and runtime prerequisites still apply; in particular, recursive read-only host volumes require `mount_setattr` support (Linux 5.12 or newer), and fail closed on Ubuntu 20.04's original 5.4 kernel. Use a supported newer kernel for that feature, and maintain OS security updates independently of Trellis.

Have a password manager ready before installing: the installer displays the administrator private key once, and Trellis does not retain it.

```sh
curl -fsSL https://raw.githubusercontent.com/overfold/trellis/main/scripts/install.sh | sudo bash
```

The default plan is the feature-complete beginner path: create a new single-node cluster, auto-detect a reachable node address, and install the namespace-networking dependencies (WireGuard, iproute2, and iptables, which every node needs) and gVisor/runsc. The plan is shown before anything changes. Press Enter to install it, or choose **Customize** to change the cluster mode, address, or gVisor. You do not need to discover command-line flags just to make a different first-install choice.

For automation, the same choices are available as flags. `--without-gvisor` opts out of gVisor.

gVisor uses containerd's direct Runtime v2 shim. Fresh installation verifies the
upstream bundle's SHA-512 digest before installing `runsc`,
`containerd-shim-runsc-v1`, and their sidecars under
`/usr/local/bin/trellis-gvisor`, with links beside the Trellis binaries. It does
not install the Docker-configuring Debian `runsc` package, run Docker registration
commands, or rewrite containerd/Docker configuration. Complete pre-existing
runtime installations are kept; an incomplete unrelated installation requires
manual repair rather than being overwritten. Both service PATHs must find the
runtime and shim; this is normally true for `/usr/local/bin`.

Before changing host packages, services, or node state, the installer stages the Linux x64 release and verifies its SHA-256 digest from GitHub's HTTPS release API before extraction or execution. Missing, malformed, or mismatching digests stop installation. This detects corruption or artifact substitution relative to the release metadata, not compromise of the release account; see the [download trust model](operations.md#release-download-trust-model).

The release tag, asset URL, and digest shown in the wrapper's plan are pinned through engine execution; a moving `latest` cannot change the approved release. An incomplete installation with an active daemon is explicitly restarted after binary publication, and its running executable must report the selected version before the installer records completion. If resuming changes the running release, the installer first uses the [upgrade maintenance flow](operations.md#upgrade-a-node), including operator credentials, multi-node evacuation, and rollback on failed verification. A failed resume remains incomplete; correct the reported failure and rerun. This is not a substitute for the coordinated control-plane procedure required at incompatible snapshot boundaries.

The installer uses the administrator key transiently to mint a normal `cluster/write` operator credential and saves a `local` context for the user who invoked `sudo`. It displays the base64 PKCS#8 Ed25519 private key once so you can move it to an operator password manager; the daemon receives and replicates only the public key. Routine `trellisctl` commands therefore do **not** need `sudo` and do not receive the administrator key.

On resume or join, a retained `local` credential is reused only after an
authenticated node-list request against the running cluster using its current
CA. A credential from a replaced cluster is not silently kept: the installer
recreates it when it has the new administrator key, or stops with operator
configuration instructions when it does not. Other contexts remain unchanged.

Before reporting a new single-node cluster ready, the installer waits for the local worker to register and become healthy. If the worker does not become ready within the bounded retry window, installation stops with a diagnostic rather than reporting success; check the daemon logs and rerun the installer to resume.

Verify the service and saved context:

```sh
sudo systemctl status trellis --no-pager
trellisctl context current
trellisctl nodes list
```

`trellis`, `trellisctl`, and the internal `trellis-health-probe` helper are installed in `/usr/local/bin`. The daemon mounts the helper read-only into managed tasks for HTTP and TCP health checks; it is not an operator CLI. The daemon keeps its configuration and secrets key root-readable under `/etc/trellis`, but never the administrator private key; your user context contains the scoped operator token and a path to `/run/trellis/ca.crt`, the running daemon's publicly readable CA. Adding nodes later uses short-lived join tokens that the administrator mints on demand; see [Multi-node clusters](multi-node.md#add-a-node).

## 2. Create the first manifest

Create an empty working directory and download the example matching your installed release:

```sh
TRELLIS_VERSION=$(trellisctl version)
curl -fsSL "https://raw.githubusercontent.com/overfold/trellis/${TRELLIS_VERSION}/examples/hello/trellis.yaml" -o trellis.yaml
```

For the release described by this guide, the manifest is:

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

## Next: explore at your own pace

Getting Started required no repository checkout. For the extended [learning path](learning-path.md#get-the-examples), clone the repository at your installed release to get the examples and their guides together; you do not need to build Trellis. If you only want to try a reachable web service, its guide also offers a [direct manifest download](../../examples/web-service/README.md#try-it-without-cloning).

After that service, choose whether to keep exploring on one node with your own image, secrets, and volumes, or add nodes to learn replica placement and rolling-update overlap. You do not need a multi-node cluster to continue learning useful workload features.

[Documentation index](../README.md) · [Next: Learning path](learning-path.md)
