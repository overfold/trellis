// Package api defines the wire types of the Trellis control-plane operator
// API: the JSON bodies of the /v1 endpoints that operators, trellisctl, and
// integrations use. Package github.com/overfold/trellis/orchestrator/client
// sends these requests.
//
// Job specifications cross the API as canonical JSON documents
// (json.RawMessage). Their format is published as JSON Schema in the
// repository's schemas directory and documented in the job manifest
// reference; the control plane decodes, validates, and canonicalizes them.
//
// Node-to-node protocols (agent operations, heartbeats, enrollment, Raft
// membership, internal discovery) are not part of this package.
//
// Trellis is pre-1.0, and the wire format may change between releases.
package api
