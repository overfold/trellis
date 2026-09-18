# CLI workflows

The `trellisctl` CLI is the first-party operator interface to the [Trellis user model](user-model.md). Resource commands remain available, but routine usage is organized around a small workflow: select a cluster context, check or preview desired state when needed, apply it, inspect status, read logs, and delete the job when it is no longer desired.

## Named cluster contexts

A **context** stores the connection information needed to operate one cluster/namespace: API address, cluster token, namespace, CA certificate, and optional client certificate/key paths.

Save the effective connection and select it:

```sh
export TRELLIS_TOKEN='replace-me'
trellisctl --server-addr trellis.example:8128 \
  --namespace production \
  --ca-cert ./cluster-ca.pem \
  context save production --use
```

After that, ordinary commands need no connection flags:

```sh
trellisctl context current
trellisctl nodes list
trellisctl jobs list
```

Useful context commands:

```sh
trellisctl context list
trellisctl context show production
trellisctl context use staging
trellisctl --context production jobs list   # one command only
trellisctl context delete old-cluster
```

`context show` never prints the stored token. The user config is written with mode `0600` because saved contexts can contain credentials.

Effective connection precedence is:

```text
local node run file
< selected named context
< TRELLIS_* environment variables
< explicit command-line flags
```

The selected context itself comes from `current_context`, then `TRELLIS_CONTEXT`, then the explicit `--context` flag.

## Discover known namespaces

Namespaces are isolation and authorization boundaries, not lifecycle-managed objects. Trellis therefore does not require a separate create/delete step before applying a job to a namespace.

To discover namespace names currently referenced by desired jobs and visible to the current credential:

```sh
trellisctl namespaces list
```

A namespace-scoped credential sees only its own namespace. A cluster-scoped credential sees the known desired-job namespaces across the cluster. Applying a job to a new valid namespace is still allowed; after the job exists, that namespace appears in discovery. Use `--output json` when automation needs the array directly.

## Apply manifest sources

`jobs apply` accepts a local manifest path or a GitHub repository as its optional positional source:

```sh
trellisctl jobs apply ./trellis.yaml
trellisctl jobs apply github.com/overfold/example-app
trellisctl jobs apply https://github.com/overfold/example-app
```

For a GitHub repository, `trellisctl` looks for `trellis.yml` at the repository root and falls back to `trellis.yaml`. The repository's default branch is used unless a ref is pinned:

```sh
trellisctl jobs apply github.com/overfold/example-app@v1.4.0
trellisctl jobs apply https://github.com/overfold/example-app/tree/v1.4.0
```

Pin a tag or commit when reproducibility matters. Public repositories need no GitHub credentials. For private repositories, set `GH_TOKEN` or `GITHUB_TOKEN` to a token that can read the repository. Remote manifests use exactly the same parser, validator, plan, and apply path as local manifests; the control plane still receives the canonical job model rather than YAML or a repository URL.

The existing explicit `--file` form remains supported:

```sh
trellisctl jobs apply --file trellis.yaml
```

Do not combine a positional source with `--file`.

## Check and preview a manifest

Local validation is a mode of `apply`; it does not modify or contact the cluster:

```sh
trellisctl jobs apply --check --file trellis.yaml
```

Preview what would change compared with the current job:

```sh
trellisctl jobs apply --dry-run --file trellis.yaml
```

The CLI parses the human-authored YAML locally, converts it to canonical JSON, and, unless `--check` was requested, sends that model to the control plane. `--dry-run` calls `POST /v1/jobs/plan`, where Trellis validates the canonical model and computes the semantic plan against authoritative current state; `trellisctl` does not maintain a second planning implementation.

The plan is semantic rather than a textual YAML diff. Task groups are identified by name, so merely reordering them does not look like a deployment. Ordered fields inside a group remain positional where order participates in Trellis semantics. Example output:

```text
Plan: update production/web from revision 7
  ~ task_groups[frontend].tasks[0].image: "registry.example/app:v7" -> "registry.example/app:v8"
  ~ task_groups[frontend].update.max_parallel: 1 -> 2
```

A normal `apply` also asks the server for a plan first and uses its `none` result for the no-op decision.

## Apply and observe convergence

A normal apply reports whether it created a job, changed its revision, or was already up to date:

```sh
trellisctl jobs apply --file trellis.yaml
```

To make deployment completion part of the command result, wait for desired capacity to become healthy:

```sh
trellisctl jobs apply --file trellis.yaml --wait --timeout 5m
```

The command prints only meaningful state changes while the revision converges. The same observer is available from `status`:

```sh
trellisctl jobs status web --watch --timeout 5m
```

A job is reported as `ready` when at least its desired allocation count from the current revision is running and healthy. Old or draining allocations cannot make a new revision look complete. `converging` means Trellis is still placing, starting, or replacing work. `degraded` means a current allocation explicitly reports an unhealthy, failed, or lost state.

## Inspect status and history

Start at the job level:

```sh
trellisctl jobs list
trellisctl jobs status web
```

Table output shows short allocation references and node addresses instead of requiring full internal UUIDs. Full IDs and API fields remain available through `--output json`.

`jobs status` is also the diagnostic view. When a job is not ready, the normal status output automatically includes allocations that need attention, their lifecycle and health states, reason codes, human-readable messages, retry timing, and attempt count. Healthy allocations and old draining allocations do not create diagnostic noise.

When the current state is not enough to explain what happened, inspect the recorded allocation lifecycle transitions:

```sh
trellisctl jobs status web --history
```

The history view combines lifecycle transitions from the job's allocations in timestamp order and shows the allocation, task group, phase, reason, and message for every transition. Narrow it to one allocation using the short reference printed by `jobs status`:

```sh
trellisctl jobs status web --history --allocation a1b2c3d4
```

Lifecycle history is control-plane/runtime state such as `placed`, `starting`, `running`, `failed`, or `lost`. It is deliberately separate from task logs: use history to answer **how the allocation moved through Trellis**, and logs to answer **what the process wrote to stdout/stderr**.

## Read logs by job, allocation, group, or task

For non-following output, a job name is enough. Trellis prints every matching task stream, with headers when more than one stream matches:

```sh
trellisctl jobs logs web --tail 200
```

Narrow the streams by task group or task when appropriate:

```sh
trellisctl jobs logs web --group frontend
trellisctl jobs logs web --task app
```

Following needs exactly one task stream. Combine the short allocation reference displayed by `jobs status` with a task selector when a task group has multiple tasks:

```sh
trellisctl jobs logs web --allocation a1b2c3d4 --task app --follow
```

## Run commands and open an allocation terminal

`trellisctl exec` targets a Trellis allocation directly. Without a TTY it runs one command, writes the remote stdout/stderr to the matching local streams, and returns the remote exit status:

```sh
trellisctl exec a1b2c3d4 -- /app/bin/migrate --check
```

When the allocation contains multiple tasks, select one explicitly:

```sh
trellisctl exec --task app a1b2c3d4 -- /app/bin/status
```

For an interactive container terminal, add `-it` (or `--tty --stdin`) and provide the shell or program to start:

```sh
trellisctl exec -it --task app a1b2c3d4 -- /bin/sh
```

Trellis deliberately does not choose a shell for the caller, so the command after `--` is always required. TTY mode uses the persistent exec-session API, forwards terminal bytes in both directions, restores the local terminal on exit, and tracks local terminal-size changes. The remote `TERM` value defaults to the local `TERM`, then `xterm-256color` when the local environment does not provide one; override it with `--term` when needed.

## Delete and wait for removal

```sh
trellisctl jobs delete web
```

Use `--wait` when a script should not continue until the job resource has disappeared:

```sh
trellisctl jobs delete web --wait --timeout 2m
```

## Inspect and maintain nodes without UUID copying

`nodes list` puts the human-meaningful address first, shows a short ID, and formats CPU/memory for humans. When placement depends on a node's labels or an existing local-volume registration, inspect that node directly:

```sh
trellisctl nodes status worker-2
```

`nodes status` shows the full ID, scheduling state, version, CPU/memory capacity, last heartbeat, labels, and locally known volume registrations. The Raft-backed registration is authoritative for placement; the node view is useful for confirming what backing volumes the node itself has recorded. Add `--output json` when automation needs the API representation.

Node references for status, drain, undrain, and remove may be any of:

- the node host, such as `worker-2`
- the displayed address, such as `worker-2:8127`
- a unique UUID prefix
- the complete UUID

For example:

```sh
trellisctl nodes status worker-2
trellisctl nodes drain worker-2
trellisctl nodes undrain worker-2
trellisctl nodes remove 9cf13a2b
```

Ambiguous prefixes are rejected and the CLI shows the matching nodes rather than guessing.

## Structured output and automation

`--output` / `-o` is deliberately **command-local**, not a global promise. Commands that can return one coherent structured result expose `--output table|json`; streaming or action-oriented commands do not expose the flag and therefore cannot silently ignore `--output json` while printing prose.

Current structured-output commands are:

```text
jobs list, status
namespaces list
nodes list, status
secrets set, list, describe
credentials create
```

For example:

```sh
trellisctl jobs status web --output json
trellisctl jobs status web --history --output json
trellisctl namespaces list --output json
trellisctl nodes status worker-2 -o json
```

`jobs logs` remains a log byte stream and `exec` remains a command/terminal stream, while `jobs apply`, `jobs status --watch`, `jobs delete`, node mutation commands, backup operations, and context mutation commands remain human/action workflows rather than pretending to produce a stable JSON document.

Explicit `--server-addr`, `--token`, `--namespace`, TLS flags, and `TRELLIS_*` environment variables override saved context values. Named contexts are therefore an interactive convenience, not a hidden requirement for automation.

[Documentation index](../README.md) · [Previous: Job manifest reference](job-specification.md) · [Next: Operations](operations.md)
