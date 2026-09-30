# WordPress development stack

**Level:** Advanced composition · **Prerequisites:** understand task groups, task-level host networking, secrets, health checks, and local volume registration

This example colocates WordPress and MariaDB in one allocation for a compact demonstration. It exercises sidecars, environment and secret injection, local volumes, health checks, restart policy, and service routing in one manifest.

It is **not** a recommended production topology: the database and web tier share placement and lifecycle, and the group cannot be scaled safely by increasing `count`.

## Prepare the node

The manifest uses Trellis-managed `@/` paths for both local volumes, so no node startup flags are required. Trellis creates the namespaced backing directories on the node selected for first placement. If you switch to absolute `host_path` values, create those directories with ownership appropriate for the container images before applying the job.

```yaml
volumes:
  - name: db-data
    host_path: "@/wordpress/db"
    container_path: /var/lib/mysql
```

The example uses host networking so WordPress can reach MariaDB at `127.0.0.1:3306`. MariaDB reserves host port 3306 and WordPress reserves host port 80, so the selected node must have both ports free. Host networking is an explicit tradeoff: the containers share the node's network surface rather than receiving normal isolation.

## Create credentials

Use independent values for the application account and MariaDB root account:

```sh
openssl rand -base64 32 | \
  trellisctl --namespace default secrets set wordpress-db-password --stdin
openssl rand -base64 32 | \
  trellisctl --namespace default secrets set mariadb-root-password --stdin
```

Trellis injects the application password into both tasks under the variable names each image expects. The secret value is not stored in the manifest.

## Deploy and observe

```sh
trellisctl jobs apply --file examples/wordpress/trellis.yaml
trellisctl --namespace default jobs status wordpress
trellisctl nodes list
```

Browse to `http://NODE_ADDRESS` after the allocation becomes healthy. MariaDB's TCP health check establishes reachability, while the WordPress HTTP check gates overall readiness. First-time database initialization may take longer than steady-state startup; adjust thresholds for your hardware rather than weakening the check indefinitely.

## Operate and tear down

Back up both host paths using database-aware procedures; copying a live MariaDB directory is not automatically a consistent backup. Deleting the job stops the containers but does not make local volume data portable:

```sh
trellisctl --namespace default jobs delete wordpress
```

Before production, separate MariaDB into a managed or replicated database service, use namespace networking and TLS, place the web tier behind the reverse-proxy pattern, pin images by digest, and test password rotation and restores. Scaling only the stateless WordPress tier requires separate task groups/jobs because a Trellis task group scales every contained task together.

[Examples index](../README.md) · [Next: Patroni architecture](../patroni/)
