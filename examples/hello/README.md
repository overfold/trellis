# Hello Trellis

**Level:** Beginner · **Prerequisites:** one healthy node and a `trellisctl` context in the `default` namespace

This is the deliberately small first workload used by [Getting Started](../../docs/public/getting-started.md). It contains one job, one task group, one replica, and one tiny first-party tutorial task. There are no ports, health-check settings, volumes, secrets, or update-strategy settings to understand yet.

The tutorial image logs what version is running, so the first deployment and first update are both immediately observable without introducing networking. Trellis treats a running task without an explicit health check as healthy. Later examples reuse the same application and add application-aware readiness.

## Complete the workload lifecycle

From the repository root:

```sh
trellisctl jobs apply --check --file examples/hello/trellis.yaml
trellisctl jobs apply --dry-run --file examples/hello/trellis.yaml
trellisctl jobs apply --file examples/hello/trellis.yaml --wait
trellisctl jobs status hello
trellisctl jobs logs hello --tail 100
```

The first logs include `Trellis tutorial · v1` and confirm that the workload is running.

To deploy a real second application version, change:

```diff
-        image: ghcr.io/overfold/trellis-tutorial:v1
+        image: ghcr.io/overfold/trellis-tutorial:v2
```

Then preview and apply it:

```sh
trellisctl jobs apply --dry-run --file examples/hello/trellis.yaml
trellisctl jobs apply --file examples/hello/trellis.yaml --wait
trellisctl jobs status hello
trellisctl jobs logs hello --tail 100
```

The replacement allocation now logs `Trellis tutorial · v2` and `Nice — your new application version is running.`

Remove the desired state and wait for the job to disappear:

```sh
trellisctl jobs delete hello --wait
```

Next, continue to the [`web-service` example](../web-service/) to expose the same tutorial application on port 8080 and add an HTTP health check. Replication/placement and rolling replacement are introduced separately in the examples after that.
