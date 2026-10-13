//go:build integration

package integration

import (
	"crypto/tls"
	"strings"
	"testing"
	"time"

	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/client"
)

// The two binaries share today's semantics but have distinct compiled release
// declarations. This verifies the machinery, not historical pre-v1 compatibility.
func TestAdjacentReleaseRollingUpgrade(t *testing.T) {
	h := newReleaseHarness(t, 3, "1")
	defer h.close()
	oldBinary, nextBinary := h.bin, buildReleaseBinary(t, "2")
	h.waitNodes(3)
	h.waitVoters(3, 3)
	worker := h.addWorker(h.createJoinToken(api.NodeRoleWorker))
	h.start(worker)
	h.waitHTTP(worker)
	h.waitNodes(4)
	h.submit(job("web", "v1", 2, "rolling"))
	h.waitJob("web", 1, 2)
	adminAt := func(i int) *client.Client {
		c, err := client.New(client.Config{Address: addr(h.nodes[i].ports[1]), AdministratorKey: h.adminKey, TLSConfig: &tls.Config{InsecureSkipVerify: true}})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	restart := func(i int, binary string) {
		h.stop(i)
		h.bin = binary
		h.start(i)
		h.waitHTTP(i)
	}
	checkActive := func(want uint64) {
		t.Helper()
		status, err := h.administrator().ClusterCompatibility(t.Context())
		if err != nil || status.ActiveRelease != want {
			t.Fatalf("compatibility status = %+v, %v; want active %d", status, err, want)
		}
	}
	oldLeader := h.leader()
	upgraded := (oldLeader + 1) % 3
	restart(upgraded, nextBinary)
	checkActive(1)
	h.submit(job("web", "v2", 2, "rolling"))
	h.waitJob("web", 2, 2)
	// A real loss elects the upgraded node or the other old voter. Retain
	// quorum, then transfer if necessary to exercise new-binary leadership.
	h.stop(oldLeader)
	other := 3 - oldLeader - upgraded
	_ = h.leader()
	status, err := h.administrator().ClusterCompatibility(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status.BinaryRelease != 2 {
		if err := h.administrator().TransferLeadership(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	h.eventually(20*time.Second, func() bool {
		status, err := adminAt(upgraded).ClusterCompatibility(t.Context())
		return err == nil && status.BinaryRelease == 2
	}, "upgraded binary did not become leader")
	checkActive(1)
	if _, err := adminAt(upgraded).ActivateRelease(t.Context(), 2); err == nil {
		t.Fatal("activated while old members were still admitted")
	}
	h.bin = oldBinary
	h.start(oldLeader)
	h.waitHTTP(oldLeader)
	// Rollback before activation remains possible, including the upgraded
	// leader's persisted state and cached node execution documents.
	restart(upgraded, oldBinary)
	h.waitJob("web", 2, 2)
	checkActive(1)
	for _, i := range []int{oldLeader, other, upgraded} {
		restart(i, nextBinary)
	}
	if _, err := h.administrator().ActivateRelease(t.Context(), 2); err == nil {
		t.Fatal("old worker did not block activation")
	}
	restart(worker, nextBinary)
	h.waitNodes(4)
	status, err = h.administrator().ActivateRelease(t.Context(), 2)
	if err != nil || status.ActiveRelease != 2 {
		t.Fatalf("activation = %+v, %v", status, err)
	}
	checkActive(2)
	h.waitJob("web", 2, 2)
	// Restart the finalized cluster and refuse an old binary before it serves.
	restart(upgraded, nextBinary)
	checkActive(2)
	h.stop(upgraded)
	h.bin = oldBinary
	h.start(upgraded)
	if err := h.waitExit(upgraded, 15*time.Second); err == nil {
		t.Fatal("old binary restarted after activation")
	}
	h.bin = nextBinary
	h.start(upgraded)
	h.waitHTTP(upgraded)
	checkActive(2)
	// Stable baseline rejects experimental enrollment without consuming the
	// token, and skips are rejected while level 2 is active.
	token := h.createJoinToken(api.NodeRoleControlPlane)
	joining := h.addNode(token)
	h.bin = buildReleaseBinary(t, "0")
	h.start(joining)
	if err := h.waitExit(joining, 15*time.Second); err == nil {
		t.Fatal("experimental binary joined a stable cluster")
	}
	h.bin = buildReleaseBinary(t, "4")
	h.start(joining)
	if err := h.waitExit(joining, 15*time.Second); err == nil {
		t.Fatal("skipped release joined the cluster")
	}
	// The same unconsumed token works for the adjacent successor.
	h.bin = buildReleaseBinary(t, "3")
	h.start(joining)
	h.waitHTTP(joining)
	h.waitNodes(5)
	checkActive(2)
	if _, err := h.administrator().ActivateRelease(t.Context(), 1); err == nil || !strings.Contains(err.Error(), "activation") {
		t.Fatalf("downgrade activation = %v", err)
	}
}
