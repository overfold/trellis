# Operations dashboard

The `ui/` directory is a deliberately thin Next.js operations client for Trellis. It exposes the same jobs, nodes, allocations, diagnostics, logs, namespaces, and YAML manifests as `trellisctl`; it does not add application-platform abstractions or choose reverse proxies, ingress models, or deployment architecture for you.

The Operations page prioritizes failures and changing state. Job creation/editing uses the same YAML authoring representation as `trellisctl`, while the dashboard sends only canonical JSON to the Trellis API. **Review Plan** calls the control plane for the semantic plan; the browser does not maintain an independent implementation of Trellis diff semantics.

## Configure and run

```sh
cd ui
cp .env.example .env.local
npm ci
npm run dev
```

Server-side variables:

| Variable | Purpose |
|---|---|
| `TRELLIS_API_URL` | Cluster control-plane URL. |
| `TRELLIS_API_TOKEN` | Scoped bearer credential. `TRELLIS_TOKEN` from workload `api_access` is also accepted. |
| `TRELLIS_API_ACCESS` | Optional `namespace` or `cluster` scope hint. |
| `TRELLIS_CLUSTER_NAME` | Human-readable cluster name. |
| `TRELLIS_NAMESPACE` | Initial/default namespace for namespaced views. |
| `TRELLIS_NAMESPACES` | Optional comma-separated namespace allowlist for cluster scope. |
| `TRELLIS_ALLOW_WRITES` | Set exactly `true` to expose mutation controls. |

The bearer token stays in server-side Next.js route handlers. The browser sends only the selected namespace and desired-state payloads.

## Authorization and namespace selection

Trellis credentials have independent **scope** and **access**:

- `namespace/read` — observe one namespace;
- `namespace/write` — observe and mutate one namespace;
- `cluster/read` — observe cluster-wide state and select namespaces;
- `cluster/write` — perform ordinary cluster/operator mutations.

The administrator signing key is more privileged than `cluster/write`: it is reserved for Raft administration, backup/restore, and minting scoped credentials. The dashboard should never receive it. The separate managed-enrollment credential is likewise never a dashboard credential.

The setup script deploys a read-only dashboard with:

```yaml
api_access:
  scope: cluster
  access: read
```

Selecting read-write mode instead gives the dashboard `cluster/write` and sets `TRELLIS_ALLOW_WRITES=true`. This means the normal installer aligns the UI controls with the server-enforced credential. `TRELLIS_ALLOW_WRITES` remains a presentation guard, not a substitute for API authorization; if a dashboard is configured manually, its token must still have the permissions required for the operations it exposes.

With namespace scope, the namespace selector is pinned to the credential's namespace. With cluster scope and no `TRELLIS_NAMESPACES` allowlist, the dashboard asks `GET /v1/namespaces` for namespace names already referenced by desired jobs and offers them as autocomplete choices. Those names are discovery assistance, not namespace objects: an operator may still type a new valid namespace and apply the first job there. When `TRELLIS_NAMESPACES` is configured, that explicit list remains an allowlist and discovery does not broaden it.

Secret values are never readable through the API. `cluster/read` may list and inspect secret metadata, so the default read-only dashboard can render the Secrets page without holding mutation authority. Creating, rotating, or deleting a secret requires `cluster/write`.

## Manifest editing

The dashboard manifest editor is still deliberately YAML-first rather than a form abstraction, but it provides editor ergonomics around that representation:

- line numbers and synchronized scrolling;
- YAML-aware indentation for Enter and two-space Tab insertion;
- live YAML parse and authoring-schema structural feedback, with the relevant source line called out when it can be located;
- context-sensitive manifest-key completion with `Ctrl+Space` / `⌘+Space`;
- schema-driven completion for enum and boolean values such as network mode, runtime, update strategy, API scope/access, and true/false fields;
- field explanations from the generated authoring schema alongside completion choices;
- formatting through **Format YAML**;
- server-owned semantic review through **Review Plan** before apply.

The editor accepts the same first-party YAML conveniences as `trellisctl`, including durations such as `10s` and memory such as `64MiB`. Those are consumer-side representation details. The generated authoring schema identifies fields whose human representation differs from canonical JSON; before an API call, the dashboard follows that schema to convert durations to nanoseconds and memory sizes to bytes.

The new-job editor starts from the same canonical `hello` workload used by Getting Started and `examples/hello`; a Go synchronization test prevents those surfaces from silently drifting. Closing an editor after changing its manifest also requires explicit confirmation, including Escape and backdrop dismissal, and the browser warns before a page unload with unapplied edits.

The published authoring schema at [`schemas/trellis-job.schema.json`](../../schemas/trellis-job.schema.json) is generated from the canonical Go model and contains field descriptions plus structural validation rules. The schema generator also writes the byte-identical dashboard asset at `ui/public/trellis-job.schema.json`, and CI checks both copies together. The dashboard therefore does not maintain a separate handwritten catalogue of manifest keys, descriptions, or human-value paths. Schema diagnostics remain editing assistance only; Trellis server validation and planning are authoritative.

Allocation logs are task-specific. A single-task allocation selects its task automatically; multi-task groups require choosing a task, matching `trellisctl jobs logs JOB --task TASK`.

## Cluster connection

The configured API URL may point at any reachable Trellis server node. Followers proxy control-plane requests to the active leader, so normal leadership changes do not require reconfiguring the dashboard.

## Production

```sh
npm run build
npm start
# or: docker build -t trellis-dashboard .
```

Place the dashboard behind HTTPS and your own identity-aware proxy. Trellis intentionally does not prescribe that proxy or identity system. Treat `cluster/write` dashboard deployments as administrative surfaces and protect them accordingly.

[Documentation index](../README.md) · [Previous: Cookbook](cookbook.md)
