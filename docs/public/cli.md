# CLI workflows

The `trellisctl` CLI is the first-party operator interface to the [Trellis user model](user-model.md). Resource commands remain available, but routine usage is organized around a small workflow: select a cluster context, check or preview desired state when needed, apply it, inspect status, read logs, and delete the job when it is no longer desired.

## Named cluster contexts

A **context** stores the connection information needed to operate one cluster/namespace: API address, bearer token, namespace, an embedded CA certificate or CA file path, and optional client certificate/key paths.

Administrator private keys are deliberately not stored in named contexts. Root-only commands accept a PKCS#8 Ed25519 key through `--administrator-key PATH` or unpadded base64 PKCS#8 DER through `TRELLIS_ADMINISTRATOR_KEY`; `trellisctl` performs challenge acquisition, request signing, and leader-change retry automatically.

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

`context show` never prints the stored token and identifies the CA as embedded or file-backed. The user config is written with mode `0600` because saved contexts can contain credentials.

Saving with `--ca-cert PATH` stores an absolute `ca_cert_file` path, not a copy of the certificate. Each command reads that file anew: replacing it changes which CA the context trusts, and a missing or unreadable file is an error, not a reason to fall back to another CA. Hand-authored relative `ca_cert_file` paths are resolved relative to the user config file. Keep the file in a stable location controlled by a trusted operator.

For a self-contained, portable remote context, embed the CA instead:

```sh
TRELLIS_CA_CERT="$(cat ./cluster-ca.pem)" \
  trellisctl --server-addr trellis.example:8128 --namespace production \
  context save production --use
```

The config uses `ca_cert` for inline PEM and `ca_cert_file` for a path; a context must not set both. The installer-created `local` context uses `/run/trellis/ca.crt`, the public CA published by the running daemon. This context intentionally follows the cluster on this machine, including a replacement cluster after reinstall. It is not a portable cluster identity pin; use an embedded CA for that purpose. Changing the CA source does not refresh the context's bearer token.

Effective connection precedence is:

```text
local node run file
< selected named context
< TRELLIS_* environment variables
< explicit command-line flags
```

The selected context itself comes from `current_context`, then `TRELLIS_CONTEXT`, then the explicit `--context` flag.

Job, exec, and secret commands act on one namespace. The effective namespace is `--namespace` (or `TRELLIS_NAMESPACE`, or the context's saved namespace). When none is set, the command fails with `--namespace is required`; credentials never infer a namespace. `jobs apply` uses the manifest's `namespace` and rejects a manifest whose namespace differs from an explicitly selected one.

## Discover known namespaces

Namespaces are resource, networking, and discovery boundaries, not lifecycle-managed objects or API authorization boundaries. Trellis therefore does not require a separate create/delete step before applying a job to a namespace.

To discover namespace names that currently have desired jobs or secrets:

```sh
trellisctl namespaces list
```

The result is the cluster-wide union of those names. Applying a job to or storing a secret in a new valid namespace makes it appear in discovery; credentials do not. Use `--output json` when automation needs the array directly.

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

The CLI parses the human-authored YAML locally, converts it to canonical JSON, and, unless `--check` was requested, sends that model to the control plane. `--dry-run` calls `POST /v1/namespaces/{namespace}/jobs/plan`, where Trellis validates the canonical model and computes the semantic plan against authoritative current state; `trellisctl` does not maintain a second planning implementation.

The plan is semantic rather than a textual YAML diff. Task groups are identified by name, so merely reordering them does not look like a deployment. Ordered fields inside a group remain positional where order participates in Trellis semantics. Example output:

```text
Plan: update production/web from version 12 (revision 7)
  ~ task_groups[frontend].tasks[0].image: "registry.example/app:v7" -> "registry.example/app:v8"
  ~ task_groups[frontend].update.max_parallel: 1 -> 2
```

A normal `apply` also asks the server for a plan first and uses its `none` result for the no-op decision. Planning resolves image tags; the image changes shown in the plan include digest-qualified references, so reapplying an unchanged manifest can update a moved tag. The CLI submits the plan's exact image pins along with the manifest, conditioned on the plan's incarnation and version. A tag moving after planning cannot change the submitted deployment. If another apply changed, created, or deleted the job in between—including deleting and recreating it under the same name, which restarts its version at 1—the command fails with a version conflict and changes nothing; run it again to review the new plan. `jobs status` prints the job's `Incarnation`, which identifies the job from its creation until it is deleted.

## Apply and observe convergence

A normal apply reports whether it created a job, advanced its version (and, for execution changes, its revision), or was already up to date. A scale-only change reports, for example, `Applied job production/web: version 12 -> 13, revision 7 unchanged.`

```sh
trellisctl jobs apply --file trellis.yaml
```

To make deployment completion part of the command result, wait for desired capacity to become healthy:

```sh
trellisctl jobs apply --file trellis.yaml --wait --timeout 5m
```

The command prints only meaningful state changes while the job converges. Apply waiting is pinned to the submitted incarnation, version, and revision (or the planned identity for an unchanged manifest). A concurrent change, deletion, or delete/recreate reports that the deployment was superseded rather than succeeding against different desired state. To follow the named job's latest state instead, use `status`:

```sh
trellisctl jobs status web --watch --timeout 5m
```

A detailed job status is reported as `ready` when every task group has at least its desired allocation count running and healthy. Allocations must match the canonical namespace, job, incarnation, and current revision; old, duplicate, or draining allocations cannot make a deployment look complete. `converging` means Trellis is still placing, starting, or replacing work. `degraded` means a current allocation explicitly reports an unhealthy, failed, or lost state.

## Inspect status and history

Start at the job level:

```sh
trellisctl jobs list
trellisctl jobs status web
```

Table output shows full allocation IDs that can be copied into `--allocation`. The Node column shows the node UUID prefix accepted by `trellisctl nodes status`, not the task's private network address; unplaced allocations show `—`. Full node IDs and API fields remain available through `--output json`.

`jobs status` is also the diagnostic view. When a job is not ready, the normal status output automatically includes allocations that need attention, their lifecycle and health states, reason codes, human-readable messages, retry timing, and attempt count. An unmet replica appears as a pending allocation with a placement reason such as `no_healthy_nodes`, `constraint_mismatch`, `volume_owner_unavailable`, `missing_capability`, `host_port_conflict`, or `insufficient_capacity`; its message identifies the relevant scheduler filter. Healthy allocations and old draining allocations do not create diagnostic noise.

When the current state is not enough to explain what happened, inspect the recorded allocation lifecycle transitions:

```sh
trellisctl jobs status web --history
```

The history view combines lifecycle transitions from the job's allocations in timestamp order and shows the allocation, task group, phase, reason, and message for every transition. Narrow it to one allocation using the ID printed by `jobs status` (a unique ID prefix is also accepted):

```sh
trellisctl jobs status web --history --allocation default-web-frontend-a1b2c3d4
```

When failed allocations put a task group into replacement backoff, `jobs status` prints a **Replacement backoff** table with the failure count, next replacement time, and latest failure. After fixing a cause outside the job manifest, reset the backoff so the failed allocations are replaced without waiting:

```sh
trellisctl jobs reset-backoff web api
```

Lifecycle history is control-plane/runtime state such as `placed`, `starting`, `running`, `failed`, or `lost`. It is deliberately separate from task logs: use history to answer **how the allocation moved through Trellis**, and logs to answer **what the process wrote to stdout/stderr**.

## Read logs by job, allocation, group, or task

For non-following output, a job name is enough. Trellis prints every matching task stream, with headers when more than one stream matches:

```sh
trellisctl jobs logs web --tail 200
```

The default is the last 100 lines per selected task stream. Use `--tail 0` for all retained output. Logs are node-local and bounded: each task keeps about the node's `task_log_limit` (64 MiB by default) of its newest output, and older output is discarded (see [Task log limit](operations.md#task-log-limit)).

Without `--allocation`, selection prefers active allocations after applying `--group`; terminal allocations are selected only when no active allocation matches. An explicit allocation ID or unique prefix selects from all retained matching allocations, even when an active replacement exists. Ambiguous prefixes are rejected rather than choosing a replacement.

Narrow the streams by task group or task when appropriate:

```sh
trellisctl jobs logs web --group frontend
trellisctl jobs logs web --task app
```

Following needs exactly one task stream. Combine the allocation ID displayed by `jobs status` with a task selector when a task group has multiple tasks:

```sh
trellisctl jobs logs web --allocation default-web-frontend-a1b2c3d4 --task app --follow
```

Task selection uses each allocation's own task inventory, not the current job spec. This includes historical tasks and removed groups while their allocation history is retained. Against a server without allocation task metadata, `--task` is passed through for server validation; without it, the server resolves a single task or rejects an ambiguous multi-task allocation. Use an explicit `--task` for those older-server multi-task allocations.

## Run commands and open an allocation terminal

`trellisctl exec` targets a Trellis allocation directly. It runs one command over a single bidirectional stream: output is written to the matching local streams as it is produced, and the remote exit status becomes `trellisctl`'s exit status:

```sh
trellisctl exec a1b2c3d4 -- /app/bin/migrate --check
```

An absent, null, or non-integer remote exit status is a protocol error, never a successful exit. Valid zero and nonzero exit statuses retain their usual meaning.

When the allocation contains multiple tasks, select one explicitly:

```sh
trellisctl exec --task app a1b2c3d4 -- /app/bin/status
```

Add `-i` (`--stdin`) to forward local stdin to the command; the end of local input closes the command's stdin:

```sh
trellisctl exec -i --task app a1b2c3d4 -- sh -c 'cat > /tmp/seed.sql' < seed.sql
```

For an interactive container terminal, add `-it` (`--stdin --tty`) and provide the shell or program to start:

```sh
trellisctl exec -it --task app a1b2c3d4 -- /bin/sh
```

Exec starts the command in the selected task's container process context: it inherits the task's environment variables, OCI user, and working directory, with or without a terminal. Trellis deliberately does not choose a shell for the caller, so the command after `--` is always required. `-t` requires a local terminal on stdin; with `-it`, the local terminal is switched to raw mode and restored on exit, and local terminal-size changes are sent to the remote terminal. With a terminal, remote stdout and stderr are one stream. For TTY sessions, `TERM` is the one intentional environment override: it defaults to the local `TERM`, then `xterm-256color` when the local environment does not provide one; override it with `--term` when needed.

Interrupting `trellisctl` or losing its connection kills the remote command. A session with no input, output, or resize for 30 minutes is closed, and a session lasts at most eight hours even while active. A node permits up to 64 exec sessions, with at most 8 for one allocation; if either limit is full, `exec` reports the overload so you can end another session or retry later. If control-plane leadership changes during a session, the session ends with an error rather than hanging; run the command again once a leader is available.

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

`nodes status` shows the full ID, scheduling state, version, CPU/memory capacity, last heartbeat, labels, and locally known volume registration identities. The Raft-backed registration is authoritative for placement; this view does not expose backing paths. Add `--output json` when automation needs the API representation.

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
trellisctl --administrator-key ./trellis-administrator.pem nodes remove 9cf13a2b
```

Ambiguous prefixes are rejected and the CLI shows the matching nodes rather than guessing.

Use `trellisctl nodes leader` to show the serving control-plane leader's full UUID, or add `--output json` for `{"leader_id":"<node UUID>"}`. The query needs cluster-scoped read access and works through a follower's API. Before gracefully uninstalling a multi-node cluster's leader, transfer leadership explicitly with the administrator credential and verify that it moved:

```sh
trellisctl --context local nodes leader
trellisctl --context local --administrator-key ./trellis-administrator.pem nodes transfer-leadership
trellisctl --context local nodes leader
```

Transfer selects another voter; failure to transfer or a subsequent election requires operator intervention. Uninstall refuses a known leader before draining, and still fails safely if the target becomes leader after that check. See [Uninstall a node](operations.md#uninstall-a-node) for exceptions and interrupted-removal recovery.

## Inspect and change cluster settings

Job limits, reconciliation settings, and namespace-network settings apply to the whole cluster and are replicated with the rest of its state, so they do not change when leadership moves. Show them with:

```sh
trellisctl cluster settings
```

Change job limits with the administrator key. Only the flags you pass change; memory accepts human sizes:

```sh
trellisctl --administrator-key ./trellis-administrator.pem cluster set-job-limits --max-replicas-per-task-group 1000 --max-task-memory 2TiB
```

The leader refuses limits that would stop admitting a job that is already applied.

Change how the leader replaces lost and failed allocations the same way. Durations use Go syntax:

```sh
trellisctl --administrator-key ./trellis-administrator.pem cluster set-reconciliation --allocation-loss-timeout 2m --terminal-allocation-retention 10
```

The flags are `--allocation-loss-timeout` (30s–24h, default `45s`), `--replacement-backoff-base` (1s–24h, default `10s`), `--replacement-backoff-max` (at least the base, at most 24h, default `5m`), `--replacement-stable-after` (10s–24h, default `10m`), and `--terminal-allocation-retention` (0–100, default `5`). See [cluster settings](operations.md#cluster-settings) for what each controls. The WireGuard pool and port count are fixed when the cluster is created.

## Structured output and automation

`--output` / `-o` is deliberately **command-local**, not a global promise. Commands that can return one coherent structured result expose `--output table|json`; streaming or action-oriented commands do not expose the flag and therefore cannot silently ignore `--output json` while printing prose.

Current structured-output commands are:

```text
jobs list, status
namespaces list
nodes list, status, leader
cluster settings
secrets set, list, describe
credentials create, list
nodes join-token create, list
```

For example:

```sh
trellisctl jobs status web --output json
trellisctl jobs status web --history --output json
trellisctl namespaces list --output json
trellisctl nodes status worker-2 -o json
```

`jobs logs` remains a log byte stream and `exec` remains a command/terminal stream, while `jobs apply`, `jobs status --watch`, `jobs delete`, `jobs reset-backoff`, node mutation commands, backup operations, and context mutation commands remain human/action workflows rather than pretending to produce a stable JSON document.

Explicit `--server-addr`, `--token`, `--namespace`, TLS flags, and `TRELLIS_*` environment variables override saved context values. `TRELLIS_CA_CERT` contains inline PEM, matching workload API injection; `--ca-cert` accepts a PEM file path. Named contexts are therefore an interactive convenience, not a hidden requirement for automation.

[Documentation index](../README.md) · [Previous: Job manifest reference](job-specification.md) · [Next: Operations](operations.md)
