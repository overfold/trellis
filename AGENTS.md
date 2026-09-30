# AGENTS.md

This file contains repository-wide instructions for coding agents working on Trellis. A more specific `AGENTS.md` in a subdirectory takes precedence for files under that directory.

## Start here

Before making changes:

1. Read `README.md` for the problem statement and design principles.
2. Read `docs/developer/development.md` for toolchains, test commands, and implementation rules.
3. Read the relevant developer or public docs for the subsystem you are changing.
4. Inspect nearby code and tests before introducing a new pattern.

Prefer small, coherent changes. Do not perform unrelated refactors, rename unrelated symbols, or reformat untouched files.

## Project direction

Trellis is a focused container orchestrator built on containerd. Preserve the design principles in `README.md`:

- Keep the core modular, understandable, and narrow in scope. Do not introduce Kubernetes-style resources or opinionated platform abstractions unless the task explicitly calls for them.
- Keep Trellis non-opinionated. Reverse proxies and similar infrastructure are ordinary workloads; higher-level deployment opinions belong outside the core.
- Consumers own representation; Trellis owns meaning. The control-plane API consumes canonical JSON. YAML is a first-party human-authoring format and must be converted before reaching the API.
- Keep first-party terminology aligned across the API, `trellisctl`, docs, schemas, and examples.

Trellis is experimental and pre-1.0. Prefer a clean current design over speculative compatibility. Do not add aliases, migrations, fallback paths, or compatibility shims solely for hypothetical older clients or persisted state unless the task explicitly requires them.

## Architecture invariants

Respect these boundaries when changing orchestrator code:

- `orchestrator/api`: public operator-API wire types. `orchestrator/internal/nodeapi`: node-to-node wire types.
- `orchestrator/internal/spec`: job authoring decode, canonical spec types, validation, defaults, and execution hashing.
- `orchestrator/internal/server`: domain state, HTTP handlers, scheduling, reconciliation, allocation queries, metrics, and secret delivery.
- `orchestrator/internal/agent`: node-side execution and local reconciliation.
- `orchestrator/internal/runtime`: runtime abstraction, containerd implementation, injected test runtime, and logs.
- `orchestrator/internal/state`: state-store abstraction, Bolt persistence, Raft FSM, and snapshots.
- `orchestrator/internal/election`: leadership events.
- `orchestrator/internal/network` and `orchestrator/internal/dns`: namespace networking and service discovery.
- `orchestrator/internal/catalog`: healthy service endpoint index.
- `orchestrator/internal/health` and `orchestrator/internal/lifecycle`: health and allocation lifecycle semantics.
- `orchestrator/internal/secrets` and `orchestrator/internal/auth`: secret storage/delivery and authorization.
- `orchestrator/client`: the public Go client for the operator API, used by `trellisctl`, `trellis-proxy-sync`, and external consumers. `orchestrator/internal/client`: the node-internal agent and node clients. `orchestrator/internal/transport`: their shared HTTP transport.

Do not blur durable desired state with renewable observations. Allocation lifecycle and health are separate concepts.

Leader-to-agent operations must remain safe under retries, agent restarts, and leadership changes. Preserve allocation identity, generation, control epoch, job revision, and execution-hash fencing where applicable. Start/stop operations must remain idempotent.

Raft-backed mutations must be deterministic. Do not make FSM application depend on wall-clock time, random iteration order, or other node-local nondeterminism. Do not mutate scheduler inputs; deterministic scheduling makes failures reproducible.

## Go changes

The Go module `github.com/overfold/trellis` is rooted at the repository (`go.mod` at the root) and targets the version declared there; its code lives in `orchestrator/`. External modules import the public `orchestrator/api` and `orchestrator/client` packages, so keep them free of internal types in their exported API and do not make them depend on server-side packages.

- Follow idiomatic Go and existing package structure.
- Run `gofmt` on modified Go files.
- Validate user-controlled identifiers, paths, ports, resources, and enum values before persistence or execution.
- Return useful errors with enough context to diagnose the failed operation; do not expose secret material.
- Never log or return secret plaintext. Clear temporary secret byte slices where practical.
- Prefer focused tests beside the package being changed.
- Do not weaken, delete, or broadly skip tests merely to make a change pass.

When changing public wire behavior, update the relevant server handler, `api` types, `client` behavior, `trellisctl`, tests, and `docs/developer/api.md` together.

When changing manifest semantics, update `internal/spec`, validation/defaulting, tests, generated schemas, `docs/public/job-specification.md`, and affected examples together.

## Documentation and examples

`docs/README.md` is the authoritative documentation index.

- `docs/public/getting-started.md` is the only installation/first-workload walkthrough.
- `examples/hello/` is the only first-workload example.
- Keep beginner, intermediate, and advanced examples distinct; do not present architectural patterns as beginner defaults.
- Keep documented commands and output descriptions consistent with the current CLI.
- Internal Raft, RPC, storage, and execution mechanics belong in developer docs or clearly advanced operator material.
- If a user-facing behavior changes, update the relevant docs in the same change.

Example manifests are validated by the Go test suite. Put reusable manifest examples under `examples/` so they are covered by `TestExampleManifestsValidate`.

## Generated schemas

Checked-in schemas under `schemas/` are generated from the orchestrator spec. Do not hand-edit generated schema output as the source of truth.

From `orchestrator/`:

```sh
go run ./cmd/generate-schemas
```

Verify generated files are current with:

```sh
go run ./cmd/generate-schemas --check
```

## Validation commands

Run the smallest relevant checks while iterating, then run the checks affected by your final diff before handing off the change.

### Orchestrator

From `orchestrator/`:

```sh
go test ./...
go vet ./...
golangci-lint run
go run ./cmd/generate-schemas --check
go build ./cmd/trellis ./cmd/trellisctl ./cmd/trellis-proxy-sync
```

For distributed behavior that crosses server/agent/node boundaries, also run when relevant:

```sh
go test -tags=integration ./cmd/trellis ./integration -count=1 -timeout=6m
```

For containerd-specific allocation adoption/runtime behavior, run the containerd E2E test on a suitable Linux host with containerd and the required privileges:

```sh
sudo "$(command -v go)" test -tags=containerd_e2e ./internal/runtime -run TestContainerdAllocationAdoption -count=1 -timeout=3m
```

If an environment-specific suite cannot be run locally, say so explicitly in the handoff; do not imply it passed.

## Change checklist

Before considering a change complete:

- The implementation follows the README design principles and the architecture boundaries above.
- New behavior has focused tests, including regression coverage for bug fixes where practical.
- Public API, CLI, manifest, docs, schema, and example surfaces remain consistent where the change crosses those boundaries.
- Generated schema files are regenerated when their source changes and pass the schema check.
- Relevant unit, lint, build, and integration checks have been run, or any environment-limited checks are called out.
- The diff contains no unrelated cleanup or compatibility code that the task did not require.
- No secrets, credentials, generated local state, build artifacts, or machine-specific files are committed.
