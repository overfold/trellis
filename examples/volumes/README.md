# Volume patterns

**Level:** Intermediate · **Prerequisites:** complete `hello`; for the absolute-path example, prepare the path and matching node label

This example demonstrates the two host-path forms used by Trellis named volumes. Both volumes have a namespace-scoped logical `name`; the difference is only how their backing `host_path` is resolved.

## Storage in the manifest

| Volume | Host path | Meaning |
|---|---|---|
| `scratch` | `@/scratch` | Resolves below Trellis's volume root for the `default` namespace. Trellis creates the directory when the first allocation is realized. |
| `database` | `/srv/trellis/app-data` | Uses that host directory verbatim. The directory must already exist on the selected node. |

The first allocation of each previously unseen `(namespace, name)` establishes the owning node. That node persists and advertises the registration; later allocations using the same name in the same namespace are scheduled back to it. The logical name is independent of the backing path, so changing `host_path` does not change volume identity or move the volume to a different node.

The group requires node label `storage=fast` because the explicit `/srv/trellis/app-data` path is only prepared on those nodes. Trellis does not probe arbitrary absolute paths during scheduling, so a constraint is the normal way to steer first placement when an explicit path is not present everywhere.

## Prepare a node

Create and secure the explicit path. Add the placement label to that node's durable `/etc/trellis/trellis.yaml` configuration, preserving any existing labels, then restart the service:

```yaml
labels:
  - storage=fast
```

```sh
sudo install -d -m 0750 /srv/trellis/app-data
sudo systemctl restart trellis
```

There is no volume-registration flag or node-side volume map. Registration happens when the allocation is first realized. Ensure explicit host paths have ownership compatible with the UID/GID used by the container image.

## Deploy and verify placement

```sh
trellisctl jobs apply --check --file examples/volumes/trellis.yaml
trellisctl jobs apply --file examples/volumes/trellis.yaml --wait
trellisctl --namespace default jobs status volumes-demo
trellisctl nodes list
trellisctl nodes status NODE
```

The allocation should land on a healthy node reporting `storage=fast`. After it is realized, that node advertises the `default/scratch` and `default/database` registrations. Later allocations that request those identities are pinned to the same node.

## Namespace isolation, recovery, and scaling

`@/` is a path prefix into Trellis's namespace volume root. With the default data directory, `@/scratch` in namespace `default` resolves below `/var/lib/trellis/data/volumes/namespaces/default/scratch`. This gives Trellis-rooted volumes filesystem-level namespace separation by construction.

An absolute path such as `/srv/trellis/app-data` is an intentional escape hatch. Trellis still scopes the logical `database` registration by namespace, but it uses the host path verbatim, so filesystem-level namespace isolation is the operator's responsibility. Different namespaces can point at the same absolute directory if their manifests say so.

A registration represents locality, not replication. If its owning node disappears, Trellis does not create another copy under the same name elsewhere. Trellis also does not replicate, snapshot, back up, or migrate the bytes. Back up important data independently.

Multiple allocations using the same namespace/name intentionally share one node registration. That is useful when several tasks need the same local data, but it means replicated stateful members that require independent disks on separate nodes must use distinct volume names.

[Examples index](../README.md) · [Next: Sidecars](../sidecar/)
