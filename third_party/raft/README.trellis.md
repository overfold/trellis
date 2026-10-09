# Trellis Raft admission patch

This directory contains Hashicorp Raft **v1.7.3**, including its original tests
and MPL-2.0 license. The root module replaces `github.com/hashicorp/raft` with
this local module. Keep unchanged upstream files intact when updating it.

The patch is limited to `api.go`, `configuration.go`, `future.go`, and `raft.go`:

- `ApplyInTerm` and `ChangeConfigurationInTerm` carry an originating term and
  context to the leader loop. Cancellation and term mismatch reject the request
  before appending any entry. Group commit checks every request individually.
- Configuration changes still use upstream's `prevIndex` compare-and-set check.
- The atomic latest-configuration cache stores configuration and index together,
  fixing upstream v1.7.3's always-zero `GetConfiguration().Index()`.
- Heartbeats use the ordinary RPC Consumer channel rather than a transport
  callback that can advance the term concurrently with main-loop admission.
  This deliberately gives up fast-path heartbeat responsiveness during slow
  disk/snapshot work and may increase leadership churn under those conditions.

Caller-side `CurrentTerm` checks cannot replace leader-loop admission: a node
can lose and reacquire leadership between the check and enqueue. Membership
entries bypass the application FSM, so an FSM-only term fence is insufficient.

These additions do not change Raft transport, log, configuration, or snapshot
encoding. Existing upstream APIs retain their semantics; Trellis uses the fenced
APIs for mutations. Cancellation after admission does not undo a committed write.

Build daemon and CLI releases from a repository checkout, as the release workflow
does. Go's version-qualified `go install ...@version` does not accept a module
with a local replacement. Public operator API/client exports remain independent
of Raft; their wire contract is unchanged.

Run this module's tests separately from Trellis:

```sh
go test ./...
go test -race ./...
```

`TestRaft_FollowerRemovalNoElection` has an intermittent restart/transport timing
failure also reproduced against the unmodified v1.7.3 module. Do not skip it or
interpret a failed full run as passing.
