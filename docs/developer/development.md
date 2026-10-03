# Development and testing

## Toolchains

The Go module `github.com/overfold/trellis` is rooted at the repository (`go.mod` in the repository root) and targets Go 1.26.4. All of its code lives under `orchestrator/`, so its packages are `github.com/overfold/trellis/orchestrator/...` and release tags (`vX.Y.Z`) are module versions. Run commands from `orchestrator/`, as CI does; `go test ./...` from the repository root is equivalent. `tutorial/` is a separate module.

```sh
cd orchestrator
go test ./...
go vet ./...
golangci-lint run

go build ./cmd/trellis ./cmd/trellisctl
CGO_ENABLED=0 go build ./cmd/trellis-health-probe
```

Containerd end-to-end tests need a Linux host, containerd, permissions on its socket, and `CONTAINERD_ADDRESS`. Build the task-local probe first and pass its host path to the suite:

```sh
CGO_ENABLED=0 go build -o /tmp/trellis-health-probe ./cmd/trellis-health-probe
sudo env TRELLIS_HEALTH_PROBE=/tmp/trellis-health-probe "$(command -v go)" test -tags=containerd_e2e ./internal/runtime -run 'TestContainerd(AllocationAdoption|HealthProbe|StopsCreatedTask|RestartsTaskWithManagedVolume|ListsPausedContainer|ListsContainerWithDeletedTask|ExecStream)' -count=1 -timeout=3m
```

Multi-node integration uses the test/injected runtime and is separated in CI. The injected runtime is compiled into the node binary only under the `integration` build tag; the suite builds its own node binary with that tag:

```sh
go test -tags=integration ./cmd/trellis ./integration -count=1 -timeout=6m
```

Normal builds reject `--runtime injected`.

Tests beside each package document state-machine invariants, Raft persistence, scheduler behavior, network planning, durability, update regressions, and security validation.

From the repository root, `bash scripts/install-core_test.sh` exercises the installer's operator-access phase for first installs, replacement clusters, resumes, and joins. It uses the real CLI for context saving and mocks credential creation and host ownership operations; it does not install packages or start services.

## Linting

Run `golangci-lint run` from `orchestrator/` using the version pinned in CI.
The configuration includes the `integration` build tag and checks error wrapping,
JSON tags and encoding errors, enum coverage, context-aware networking, HTTP body
closure, security findings, and lightweight regression guards.

Persisted records, internal wire types, and execution-hash inputs need explicit
JSON tags. When tagging an existing field, preserve its current JSON name and
omission behavior; adding tags is not a storage-format or hash migration.
`errchkjson` permits ignoring encoding errors only for types it proves safe.

Security checks exclude G301/G302/G304/G306 because the orchestrator intentionally
manages host paths and file permissions. Tests are excluded from `gosec` and
`noctx`. Other suppressions must be local and explain the validated bound or
intentional behavior; `nolintlint` rejects unused directives.

## Three-node Vagrant demo

[`orchestrator/Vagrantfile`](../../orchestrator/Vagrantfile) provides a real three-node local demo cluster for development and for the multi-node public learning-path examples. It uses Vagrant's provider-independent private-network abstraction and guest mDNS rather than hostmanager or provider-specific addressing. With a compatible Vagrant provider configured:

```sh
cd orchestrator
vagrant up
```

The Vagrant environment provisions `control`, `worker-1`, and `worker-2` Debian 12 VMs, installs containerd and Trellis, joins the nodes, and applies the workloads in `demo/workloads.sh`. It is a disposable development/demo environment rather than a production deployment method.

## Design rules

- Put operator-API wire types in the public `api` package and client methods in the public `client` package; put node-to-node wire types in `internal/nodeapi`. Keep job YAML/JSON schema in `internal/spec`; the public API carries job specifications as canonical JSON.
- Validate user-controlled identifiers, paths, ports, resources, and enum values before persistence.
- Treat start/stop as retriable and idempotent; preserve epoch and generation checks.
- Never log or return secret plaintext. Clear temporary byte slices where feasible.
- Keep desired-state mutations Raft-backed and deterministic. Raft FSM application must not depend on wall-clock or unordered map traversal.
- Do not make the scheduler mutate inputs; deterministic order makes failures reproducible.
- Add a focused unit test and, for manifests, place examples under `examples/` so `TestExampleManifestsValidate` covers them.

## Entry points

- `cmd/trellis`: production node composition and flags.
- `cmd/trellisctl`: CLI, precedence-aware config, TLS setup.
