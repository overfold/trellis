# Trellis agent guidance

Repository-wide instructions. Read any nested `AGENTS.md` before changing files in its scope; more specific guidance takes precedence where it conflicts with this file.

## Start here

- Read [README.md](README.md) for product scope and design principles, and [architecture](docs/developer/architecture.md) before changing subsystem boundaries.
- Use [docs/README.md](docs/README.md) to find the relevant subsystem docs. [Development and testing](docs/developer/development.md) owns toolchain and environment-specific test setup.
- Inspect the owning package and nearby tests before editing. Keep changes scoped to the request; do not reformat or refactor unrelated code.

## Design principles

Use the [README design principles](README.md#design-principles) to judge feature scope and implementation tradeoffs, not just package placement:

- **Modular and extensible, but focused.** Keep the containerd-based orchestrator lean and understandable. Expose clean primitives that consumers can build on; do not add speculative extension points, a plugin framework, or Kubernetes-style complexity.
- **Non-opinionated and flexible.** Keep application architecture and environment-specific policy outside the core. Reverse proxies are ordinary workloads, not special ingress resources. Do not add team/project abstractions or prescribe a platform architecture unless explicitly requested.
- **Consumers own representation; Trellis owns meaning.** The operator API accepts canonical JSON. `trellisctl` converts human-authored YAML; authoring conveniences must not create separate validation, defaulting, planning, or revision semantics in each consumer.
- **Declarative, with open-ended delivery.** Accept desired state without coupling orchestration to how it is produced or submitted. Keep CLI, CI/CD, custom frontends, and other API consumers equally viable; do not make a particular delivery workflow a core requirement.
- **Easy to use through clarity.** Prefer useful errors, thorough documentation, and progressive first-party examples over opinionated defaults that hide behavior. Make the primitives understandable without turning advanced application patterns into beginner defaults.
- **Open-source and inspectable.** Keep implementation and extension paths understandable to operators who need to read, modify, and run Trellis themselves.

## Architecture rules

Apply the design principles through clear ownership boundaries:

- **Keep wire contracts separate from domain and execution types.** Operator wire types belong in `orchestrator/api`; node-to-node wire types belong in `orchestrator/internal/nodeapi`. Public `api` and `client` exports must be usable by external Go modules without importing internal types, and must not depend on server-side packages. Shared internal transport helpers are permitted.
- **Change behavior at its owner.** Reuse existing package APIs rather than duplicating semantics in handlers or the CLI. Add a helper, interface, or package only for a coherent responsibility or a real consumer need, not a speculative extension point.
- **Prefer a clean current design.** Trellis is experimental and pre-1.0. Do not add aliases, migrations, fallbacks, or compatibility shims for hypothetical older clients or persisted state unless required by the task.

Key ownership boundaries (paths relative to `orchestrator/`; see the architecture guide for the full package map):

| Owner | Responsibility |
|---|---|
| `internal/spec` / `internal/plan` | Authoring decode, canonical specs, defaults, validation, execution hashing / semantic job planning |
| `internal/server` | Domain state, operator handlers, scheduling, leader reconciliation, allocation queries, metrics, secret delivery |
| `internal/agent` / `internal/runtime` | Node-side execution and local reconciliation / runtime abstraction, containerd, integration-only injected runtime, logs |
| `internal/state` / `internal/election` | State-store abstraction, Bolt persistence, Raft FSM and snapshots / leadership events |
| `internal/network`, `internal/dns`, `internal/catalog` | Namespace networking, service discovery, healthy endpoint index |
| `internal/health` / `internal/lifecycle` | Health probing / allocation lifecycle vocabulary and transitions |
| `internal/secrets` / `internal/auth` | Encrypted secret storage and delivery / credentials and authorization |
| `client` / `internal/client` / `internal/transport` | Public operator client / node-internal clients / shared HTTP transport |

### State and distributed execution invariants

- Keep durable desired state separate from renewable observations such as heartbeats and runtime status. Allocation lifecycle and health are separate concepts; do not infer one from the other.
- Keep desired-state mutations Raft-backed. FSM application must be deterministic: no wall-clock reads, randomness, or order-dependent map traversal during application. Supply required timestamps or identifiers in the replicated command.
- Do not mutate scheduler inputs; preserve deterministic placement and ordering.
- Leader-to-agent start/stop operations must be retriable and idempotent across agent restarts and leadership changes. Preserve allocation identity, generation, control epoch, job revision, and execution-hash fencing where applicable; reject stale operations and observations.
- Namespaces separate resources, discovery, and workload networks, **not API credential authority or arbitrary workload admission**. Credentials are cluster-scoped; do not treat namespaces as a security sandbox. See [trust boundaries](docs/public/multitenancy.md).

## Change contracts

- Validate user-controlled identifiers, paths, ports, resources, and enum values before persistence or execution. Errors must provide useful context without exposing secrets; never log secret plaintext, and clear temporary secret bytes where practical.
- Run `gofmt` on modified Go files. Add focused tests beside the owning package, including regression cases for bugs. Do not weaken or skip tests to make a change pass.
- For public wire behavior, update affected handlers, `api`, public `client`, `trellisctl`, tests, and [API docs](docs/developer/api.md) together.
- For manifest semantics, update `internal/spec`, planning when affected, validation/defaulting tests, generated schemas, [job reference](docs/public/job-specification.md), and affected examples together. Do not hand-edit generated schemas: from `orchestrator/`, run `go run ./cmd/generate-schemas`.
- Keep terminology consistent across API, CLI, docs, schemas, and examples: cluster → namespaces → jobs → task groups → tasks and allocations; nodes belong to the cluster.
- Update relevant docs when user-facing behavior changes. [Getting Started](docs/public/getting-started.md) and `examples/hello/` own the installation/first-workload walkthrough; link to them rather than adding competing quick starts. Keep advanced patterns clearly labelled and internals in developer docs or advanced operator material.
- Put reusable YAML job examples under `examples/`; `TestExampleManifestsValidate` validates its `.yaml` manifests recursively.

## Verification

The root `go.mod` owns the `github.com/overfold/trellis` module; code lives under `orchestrator/`. Use the tool versions declared in `go.mod` and `mise.toml`. `tutorial/` is a separate Go module and is not covered by the orchestrator checks.

Run focused package tests while iterating. For Go changes, run the relevant final checks below **from `orchestrator/`**, as CI does:

```sh
go test ./...
go vet ./...
golangci-lint run
go run ./cmd/generate-schemas --check
go build ./cmd/trellis ./cmd/trellisctl
CGO_ENABLED=0 go build ./cmd/trellis-health-probe
```

- Distributed server/agent/node changes: also run `go test -tags=integration ./cmd/trellis ./integration -count=1 -timeout=6m`. The injected runtime is available only with the `integration` build tag; normal builds reject it.
- Containerd runtime changes: use the containerd E2E commands in [development and testing](docs/developer/development.md). They require Linux, running containerd, root privileges for snapshot mounts, and a statically built health probe for health-probe tests; socket access alone is insufficient.
- Installer changes: run `bash scripts/install-core_test.sh` **from the repository root**.
- Documentation-only changes: check referenced paths and commands against the code and CI; a full Go suite is unnecessary unless examples or behavior changed.

Before handing off, review the diff for scope and consistency. Do not commit secrets, local state, or build artifacts. Report which checks actually ran and any failures, skips, or environment limitations; do not claim unexecuted suites passed.
