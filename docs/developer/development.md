# Development and testing

## Toolchains

The Go module `github.com/overfold/trellis` is rooted at the repository (`go.mod` in the repository root) and targets Go 1.26.9. All of its code lives under `orchestrator/`, so its packages are `github.com/overfold/trellis/orchestrator/...` and release tags (`vX.Y.Z`) are module versions. Run commands from `orchestrator/`, as CI does; `go test ./...` from the repository root is equivalent. `tutorial/` is a separate module.

```sh
cd orchestrator
go test ./...
go vet ./...
golangci-lint run
go run ./cmd/generate-schemas --check

go build ./cmd/trellis ./cmd/trellisctl
CGO_ENABLED=0 go build ./cmd/trellis-health-probe
```

Containerd end-to-end tests need a Linux host, a running containerd, and root privileges for namespace and snapshot mounts; socket access alone is insufficient. Build the task-local probe statically and pass its host path and socket address explicitly through `sudo` (adjust the address if needed):

```sh
CGO_ENABLED=0 go build -o /tmp/trellis-health-probe ./cmd/trellis-health-probe
sudo env CONTAINERD_ADDRESS=/run/containerd/containerd.sock TRELLIS_HEALTH_PROBE=/tmp/trellis-health-probe "$(command -v go)" test -v -tags=containerd_e2e ./internal/runtime -run '^TestContainerd' -count=1 -timeout=6m
```

Read the output for skipped tests: a green suite that skipped runtime tests is not containerd verification. These tests create and remove test containers and managed-volume fixtures; use a disposable development host.

The volume kernel regression verifies recursive read-only staging with a real writable nested tmpfs, a writable-volume baseline, and continued host-source writes. Run it in a private mount namespace (unlike a production agent, this test never passes its mounts to a separate containerd daemon). With `strace` installed, the second invocation injects `ENOSYS` on `mount_setattr` and checks fail-closed behavior and staging cleanup:

```sh
go test -c -o /tmp/trellis-agent.test ./internal/agent
sudo unshare -m env TRELLIS_VOLUME_E2E=1 /tmp/trellis-agent.test -test.run '^TestKernelReadOnlyVolumeIncludesWritableSubmount$' -test.v
sudo unshare -m env TRELLIS_VOLUME_E2E=1 TRELLIS_VOLUME_NO_MOUNT_SETATTR=1 strace -f -e trace=mount_setattr -e inject=mount_setattr:error=ENOSYS /tmp/trellis-agent.test -test.run '^TestKernelReadOnlyVolumeIncludesWritableSubmount$' -test.v
```

The containerd suite additionally verifies parent and nested-mount writes inside a real task before and after restart, and recovers a real `Created` task through the budgeted agent reconciler.

With a complete gVisor bundle installed and both `runsc` and
`containerd-shim-runsc-v1` in the node/containerd service PATH, add
`TRELLIS_RUNSC_E2E=1` to the `sudo env` command above. This enables
`TestContainerdRunsc`, which starts and execs a real sandbox through the direct
Runtime v2 shim, without Docker or CRI configuration. Once opted in, missing
binaries and runtime failures fail the test rather than skip it.

Verify dependency advisories from `orchestrator/` with
`go run golang.org/x/vuln/cmd/govulncheck@latest ./...`, and run `go test -race ./...`
for security/concurrency changes. The SDK regression
`TestContainerdPullGraphAmplificationBound` exercises a finite repeated OCI index
graph on the actual `images.Dispatch` pull path: valid repeated references stay
valid, while expansion beyond the SDK's 10,000-reference budget is rejected.

Namespace-network kernel regressions need root with network/mount namespace
privileges, IPv6 and WireGuard kernel support, and `ip`, `wg`, `iptables`,
`unshare`, `mount`, `umount`, and `curl`. They isolate network tables, sysfs, and named
network namespaces in a child process, and do not require containerd:

```sh
go test -c -o /tmp/trellis-network.test ./internal/network
sudo env TRELLIS_NETWORK_E2E=1 /tmp/trellis-network.test -test.run '^TestKernelNamespaceAudit$' -test.v
```

The suite proves old 40-bit collision separation, refuses foreign ownership,
exercises IPv6 link-local and real WireGuard host-input denial with working
pre-fix baselines, preserves node/host-initiated and namespace-peer connections,
repairs API reachability after a firewall flush, and checks final teardown.
It also crashes after real bridge/WireGuard/veth creation and netns mounting,
restarts cleanup before ownership aliases are installed, reuses the resources
in namespace `plans`, refuses foreign veths and replacement namespaces, and
repairs published-port connections after deleting all NAT chains and jumps.
Normal unit runs skip this opt-in suite; a failure after opting in is not skipped.

Multi-node integration uses the test/injected runtime and is separated in CI. The injected runtime is compiled into the node binary only under the `integration` build tag; the suite builds its own node binary with that tag:

```sh
go test -tags=integration ./cmd/trellis ./integration -count=1 -timeout=6m
```

The deterministic promotion-stage removal fault test uses real Raft, Bolt,
TCP/TLS and node authorization in-process, with a test-only Joiner hook after
tombstone commit and before promotion. It covers loss of the old voter and the
replacement, quorum recovery, retry, permanent revocation and convergence:

```sh
go test -race -tags=integration ./internal/server -run '^TestRemovalPromotionLossAndRecovery$' -count=3 -timeout=2m
```

Normal builds reject `--runtime injected`.

Tests beside each package document state-machine invariants, Raft persistence, scheduler behavior, network planning, durability, update regressions, and security validation.

From the repository root, `bash scripts/install-core_test.sh` checks secret/config permissions before initial writes and atomic publication on traversable custom paths, resume behavior, and Vagrant control/worker provisioning with mocked host operations. It also exercises the installer's operator-access phase for first installs, replacement clusters, resumes, and joins, using the real CLI for context saving. It does not install packages or start services. Maintenance shell tests require `jq` and cover pretty/compact node-list JSON, labels named `id`, and fail-closed malformed-output handling.

`bash scripts/upgrade_test.sh` exercises the full upgrade script with mocked releases, CLI calls, and host services. It checks local-context selection, single- and multi-node maintenance, explicit configuration paths, missing or rejected credentials, drain timeouts, and rollback without changing host services.

`bash scripts/uninstall_test.sh` exercises evacuation, membership removal, local resource cleanup, archiving, force/purge modes, and failures with mocked host services. Like the upgrade tests, it does not remove a real installation.

`bash scripts/release_test.sh` uses real digest validation, hashing, and extraction with mocked downloads. It covers valid archives, substituted same-version binaries, absent/malformed/mismatching digests, exact platform/asset selection, and full install/upgrade failures before host mutation. `python3 scripts/install_test.py` runs the interactive `install.sh` wrapper in a controlling pseudo-terminal with a mocked engine, covering confirmation, cancellation, customization, fixed resume settings, flag forwarding, invalid roles, and the completed-install fast path.

`bash scripts/security_test.sh` pipes all four lifecycle entrypoints into Bash from an attacker-controlled working directory, checks HTTPS-only helper downloads, and exercises verified full gVisor bundles, checksum failures, missing sidecars, incomplete/unrelated installations, resumed publication, owned removal, and unchanged Docker configuration. It uses real hashing/extraction and temporary installation paths, with mocked downloads and no package or service changes.

The `installer-test` workflow runs these five shell suites and wrapper tests on pull requests and pushes to `main`, as both the runner user and root (including permission/ownership assertions). It explicitly installs Go, `jq`, OpenSSL, curl/CA certificates, tar/bzip2, GNU core utilities/text tools, and Python 3. Test output is grouped by suite and privilege; a ten-minute job timeout bounds failures. These tests use temporary paths and mocked host services, not live systemd/containerd or Vagrant VMs.

## Linting

Run `golangci-lint run` from `orchestrator/` using the version pinned in CI.
The configuration includes the `integration` build tag and checks error wrapping,
JSON tags and encoding errors, enum coverage, context-aware networking, HTTP body
closure, security findings, and lightweight regression guards. `modernize` keeps
code using current Go language and standard-library idioms; review automatic
fixes for behavior changes, especially JSON omission and test cleanup timing.

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

## README terminal demo

[`docs/images/demo.tape`](../images/demo.tape) records plan, apply, status, and delete using the [hello example](../../examples/hello/). Install [VHS](https://github.com/charmbracelet/vhs) (rendered with v0.10.0), `ttyd`, `ffmpeg`, and [JetBrains Mono](https://www.jetbrains.com/lp/mono/) (rendered with v2.304). Select a **disposable cluster** context with write access and no existing `default/hello` job. From the repository root:

```sh
go build -o /tmp/trellisctl ./orchestrator/cmd/trellisctl
PATH="/tmp:$PATH" vhs docs/images/demo.tape
```

The tape creates and deletes `default/hello`; never use a production cluster. Idle polling is cut from playback. The committed GIF uses the real CLI and a Trellis process with the integration-only injected runtime (simulated containers, real planning and reconciliation). Inspect all four commands and commit the tape and GIF together.

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
