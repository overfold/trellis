# Development and testing

## Toolchains

The orchestrator module targets Go 1.26.4. The dashboard uses Next.js 16, React 19, TypeScript, Tailwind CSS, ESLint, and npm's lockfile.

```sh
cd orchestrator
go test ./...
go vet ./...
golangci-lint run

CGO_ENABLED=0 go build ./cmd/trellis ./cmd/trellisctl ./cmd/trellis-proxy-sync

cd ../ui
npm ci
npm run lint
npm run build
```

Build the `trellis` node with `CGO_ENABLED=0` when creating a binary for use on a node. The agent mounts that executable into isolated runsc workloads for HTTP and TCP health checks, where the workload image may not contain a dynamic loader.

Containerd end-to-end tests need a Linux host, containerd, permissions on its socket, and `CONTAINERD_ADDRESS`. Multi-node integration uses the test/injected runtime and is separated in CI. Tests beside each package document state-machine invariants, Raft persistence, scheduler behavior, network planning, durability, update regressions, and security validation.

## Three-node Vagrant demo

[`orchestrator/Vagrantfile`](../../orchestrator/Vagrantfile) provides a real three-node local demo cluster for development and for the multi-node public learning-path examples. It currently targets Hyper-V and uses Vagrant hostmanager integration. With those host prerequisites configured:

```sh
cd orchestrator
vagrant up
```

The Vagrant environment provisions `control`, `worker-1`, and `worker-2` Debian 12 VMs, installs containerd and Trellis, joins the nodes, and applies the workloads in `demo/workloads.sh`. It is a disposable development/demo environment rather than a production deployment method.

## Design rules

- Put wire-compatible JSON structures in `internal/api`; keep job YAML/JSON schema in `internal/spec`.
- Validate user-controlled identifiers, paths, ports, resources, and enum values before persistence.
- Treat start/stop as retriable and idempotent; preserve epoch and generation checks.
- Never log or return secret plaintext. Clear temporary byte slices where feasible.
- Keep desired-state mutations Raft-backed and deterministic. Raft FSM application must not depend on wall-clock or unordered map traversal.
- Do not make the scheduler mutate inputs; deterministic order makes failures reproducible.
- Add a focused unit test and, for manifests, place examples under `examples/` so `TestExampleManifestsValidate` covers them.

## Entry points

- `cmd/trellis`: production node composition and flags.
- `cmd/trellisctl`: CLI, precedence-aware config, TLS setup.
- `cmd/trellis-proxy-sync`: polling service-catalog consumer for external proxies.
- `ui/src/app/api`: dashboard's authenticated server-side forwarding layer.
