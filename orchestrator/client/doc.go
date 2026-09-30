// Package client is the Go client for the Trellis control-plane operator API.
//
// A Client sends requests to any control-plane node; followers forward them
// to the current leader. It authenticates with a scoped API credential (a
// bearer token minted by "trellisctl credentials create" or delivered to a
// workload through api_access) or with the cluster administrator key, which
// signs each request with a fresh leader-issued challenge.
//
// Cluster-scoped operations, such as listing nodes or namespaces, are methods
// on Client. Namespaced operations, such as applying jobs or listing
// allocations, address the namespace the Client was configured with; use
// WithNamespace to address another one. A namespaced call on a Client without
// a namespace fails with ErrNamespaceRequired.
//
// A request the server rejects returns an error that wraps *HTTPError, which
// carries the response status and the server's message:
//
//	status, err := c.GetJob(ctx, "web")
//	var httpErr *client.HTTPError
//	if errors.As(err, &httpErr) && httpErr.Status == http.StatusNotFound {
//		// The job does not exist.
//	}
//
// Wire types are defined in package github.com/overfold/trellis/orchestrator/api.
// Job specifications are passed as canonical JSON documents; see the job
// manifest reference and the JSON Schema in the repository's schemas
// directory.
//
// Trellis is pre-1.0; this package and the wire format may change between
// releases.
package client
