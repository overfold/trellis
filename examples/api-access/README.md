# In-cluster API access

**Level:** Advanced · **Prerequisites:** complete the intermediate examples and use a reviewed controller image

This example shows a fully trusted task using cluster-wide read access while routing its sample request to the job's namespace. This is not tenant isolation: enabling `api_access` gives that task a cluster-wide credential.

## Choose access

`api_access` is an explicit task-level privilege request:

| Access | Credential | Intended use |
|---|---|---|
| omitted | None | Ordinary application workloads |
| `read` | Cluster-wide read-only token | Trusted cluster observers |
| `write` | Cluster-wide read/write token | Trusted operator/control-plane workloads |

This example requests:

```yaml
api_access:
  access: read
```

Place this object on the task, not the task group. There is no scope or namespace selector inside `api_access`.

Use the narrowest access level that works. API access is for tasks that genuinely need cluster-wide visibility, and write access is only for controllers that deliberately mutate state. The injected `TRELLIS_NAMESPACE` defaults to the job's namespace; that default does not reduce the token's authority.

A submitting credential cannot delegate write access when it has only read access.

## What Trellis injects

With API access enabled, Trellis adds these variables only to the requesting task:

| Variable | Meaning |
|---|---|
| `TRELLIS_ADDR` | Workload-reachable address of the Trellis control-plane API. |
| `TRELLIS_TOKEN` | Workload bearer token with the requested cluster-wide access. |
| `TRELLIS_NAMESPACE` | The job's namespace; use it as a routing default for namespaced request paths, never as authorization. |
| `TRELLIS_CA_CERT` | Cluster CA certificate (inline PEM) for TLS verification when configured. |

Sibling tasks do not receive the injected environment or token. Colocation is not a security sandbox: use reviewed, pinned images and do not mix an untrusted sidecar into the group. Compromise of the API-enabled task exposes its cluster-wide authority.

Each enabled task in an allocation receives its own token, bound to namespace, job, task group, and task. Start retries re-deliver the same task token, and Trellis revokes it when the allocation is replaced and pruned, when the job or task is deleted, or when the task's `api_access` is removed or narrowed. API access requires the servers' secrets encryption key, which the installer configures.

The bootstrap credential remains separate and is never injected into workloads.

## Build a useful client image

The stock nginx image in `trellis.yaml` only makes the privilege visible in the manifest; it does not contain `list-jobs.sh` or curl. For a real controller, copy the helper into an image:

```dockerfile
FROM curlimages/curl:8.12.1
COPY --chmod=0755 list-jobs.sh /usr/local/bin/list-jobs
ENTRYPOINT ["/usr/local/bin/list-jobs"]
```

Build and push that image, then replace the manifest's image. The helper validates the injected address, token, and namespace, sends Bearer authentication, addresses `/v1/namespaces/$TRELLIS_NAMESPACE/jobs`, treats an address without an explicit scheme as HTTPS, refuses plaintext HTTP, applies connection and overall request deadlines, and uses `TRELLIS_CA_CERT` as a curl trust root when TLS is configured. That path choice does not narrow the token: it can read jobs and other readable state in every namespace.

## Deploy and verify

```sh
trellisctl jobs apply --file examples/api-access/trellis.yaml
trellisctl --namespace default jobs status api-client
```

Use allocation logs to inspect the controller's non-sensitive result. Never print the token, dump the complete environment, return it to browser JavaScript, or include it in metrics and traces.

## Controller behavior

API clients should set request deadlines, retry transient transport/5xx failures with backoff, and tolerate resources changing between reads. Prefer read-only discovery loops unless mutation is essential. Do not expose this token to browser code or untrusted workloads; a different namespace path is authorized by the same cluster token.

For a long-running process, poll only as often as needed and preserve the last known-good generated configuration through temporary API outages. The reverse-proxy recipe in the public cookbook describes the same trusted cluster-controller pattern.

[Examples index](../README.md) · [Learning path](../../docs/public/learning-path.md)
