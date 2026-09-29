# Secret delivery and rotation

**Level:** Intermediate · **Prerequisites:** complete `hello` and configure the same separately managed secrets-encryption key on every server

This example injects one secret as an environment variable and one as a read-only file into a long-running nginx task. It demonstrates delivery mechanics; the container deliberately does not read or print either value.

## Prerequisites

Every server must use the same separately managed secrets encryption key and key ID. The key must be a root-readable 32-byte value (or its accepted base64 representation) supplied with `--secrets-key`. Losing it makes encrypted secret records—including records in a Trellis backup—unrecoverable.

Create the namespace-scoped values before applying the job:

```sh
openssl rand -base64 32 | trellisctl --namespace default secrets set api-token --stdin
trellisctl --namespace default secrets set tls-key --file ./server.key
trellisctl jobs apply --check --file examples/secrets/trellis.yaml
trellisctl jobs apply --file examples/secrets/trellis.yaml --wait
```

`API_TOKEN` receives the first value. The second appears at `/run/trellis-secrets/tls.key`; decimal mode `256` is octal `0400`. File targets must use a clean path below `/run/trellis-secrets/` and may use mode `0400` or `0600`.

## Inspect metadata, not plaintext

```sh
trellisctl --namespace default secrets list
trellisctl --namespace default secrets describe api-token
trellisctl --namespace default jobs status secrets-demo
```

Secret APIs and the dashboard return name, version, update time, and key ID—not plaintext. Avoid `env`, diagnostic dumps, shell tracing, or application logging that could reveal an environment-delivered value. Prefer file delivery when the application supports it.

## Rotate safely

Use the current metadata version as a compare-and-swap guard:

```sh
openssl rand -base64 32 | trellisctl --namespace default secrets set api-token \
  --stdin --expected-version 1
```

A successful write increments the version. Running allocations retain bytes already delivered, so coordinate any upstream credential change and replace consumers by applying an execution-affecting job revision. During a compatibility window, applications may need to accept both old and new credentials. Deleting a secret prevents future delivery but does not erase it from already-running containers.

Secret values are capped at 65,536 bytes. Use Trellis secrets for runtime credentials, not large configuration bundles or a general-purpose PKI lifecycle.

[Examples index](../README.md) · [Next: Volumes](../volumes/)
