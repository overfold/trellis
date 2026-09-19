# In-cluster API access

**Level:** Advanced · **Prerequisites:** complete the intermediate examples and use a reviewed controller image

This example shows the normal least-privilege pattern: a trusted workload discovers and automates resources in its own namespace without embedding a cluster-wide operator credential in an image.

## Choose scope and access

`api_access` is an explicit task-group privilege request with two dimensions:

| Scope/access | Credential | Intended use |
|---|---|---|
| omitted | None | Ordinary application workloads |
| `namespace/read` | Persistent read-only token restricted to the job's own namespace | Discovery, observers, and namespace-local read-only controllers |
| `namespace/write` | Persistent read/write token restricted to the job's own namespace | Trusted namespace-local reconcilers |
| `cluster/read` | Cluster-wide read-only operator token | Trusted cluster observers |
| `cluster/write` | Cluster-wide read/write operator token | Trusted operator/control-plane workloads |

This example requests:

```yaml
api_access:
  scope: namespace
  access: read
```

There is intentionally no namespace selector inside `api_access`: a job in `default` gets namespace scope for `default`; a job in `payments` gets namespace scope for `payments`.

Use the narrowest pair that works. Cluster scope is for workloads that genuinely need cross-namespace or cluster-level visibility, and write access is only for controllers that deliberately mutate state. The injected `TRELLIS_NAMESPACE` still defaults to the job's namespace even with cluster scope; that default does not reduce a cluster-scoped token's authority.

A submitting credential cannot delegate authority it does not have. Namespace-scoped callers cannot request cluster scope, and read-only callers cannot request write access.

## What Trellis injects

With API access enabled, Trellis adds these variables to every task in the group:

| Variable | Meaning |
|---|---|
| `TRELLIS_ADDR` | Workload-reachable address of the Trellis control-plane API. |
| `TRELLIS_TOKEN` | Workload bearer token with the requested effective scope/access. |
| `TRELLIS_NAMESPACE` | The job's namespace; use it as the default scope for namespace-aware requests. |
| `TRELLIS_CA_CERT` | Cluster CA certificate (inline PEM) for TLS verification when configured. |

This is a group-level privilege boundary: every task in the group can read the injected environment and act with the token. Use a reviewed, pinned image and do not mix an untrusted sidecar into the group. This is especially important for cluster/write, because compromise of any task in that group exposes broad operator authority.

The bootstrap credential remains separate and is never injected into workloads.

## Build a useful client image

The stock nginx image in `trellis.yaml` only makes the privilege visible in the manifest; it does not contain `list-jobs.sh` or curl. For a real controller, copy the helper into an image:

```dockerfile
FROM curlimages/curl:8.12.1
COPY --chmod=0755 list-jobs.sh /usr/local/bin/list-jobs
ENTRYPOINT ["/usr/local/bin/list-jobs"]
```

Build and push that image, then replace the manifest's image. The helper validates the injected address, token, and namespace, sends Bearer authentication and `X-Trellis-Namespace`, treats an address without an explicit scheme as HTTPS, and uses `TRELLIS_CA_CERT` as a curl trust root when TLS is configured.

## Deploy and verify

```sh
trellisctl jobs apply --file examples/api-access/trellis.yaml
trellisctl --namespace default jobs status api-client
```

Use allocation logs to inspect the controller's non-sensitive result. Never print the token, dump the complete environment, return it to browser JavaScript, or include it in metrics and traces.

## Controller behavior

API clients should set request deadlines, retry transient transport/5xx failures with backoff, and tolerate resources changing between reads. Prefer read-only discovery loops unless mutation is essential. A namespace-scoped token cannot be broadened by changing the namespace header; broader operations require the appropriate cluster-scoped credential.

For a long-running process, poll only as often as needed and preserve the last known-good generated configuration through temporary API outages. The reverse-proxy recipe in the public cookbook applies this exact namespace-controller pattern.

[Examples index](../README.md) · [Learning path](../../docs/public/learning-path.md)
