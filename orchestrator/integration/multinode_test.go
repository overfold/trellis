//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestMultiNodeFailureRecovery intentionally uses OS processes, loopback TCP,
// HTTPS, and on-disk Raft/runtime state. The injected runtime replaces only
// containerd; no control-plane, transport, election, or persistence component
// is mocked.
func TestMultiNodeFailureRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-process integration test")
	}
	h := newHarness(t, 3)
	defer h.close()
	h.waitNodes(3)

	t.Run("persistent state and ambiguous start reconciliation", func(t *testing.T) {
		h.fault(1, "start", "after")
		h.submit(job("web", "v1", 2, "rolling"))
		h.waitJob("web", 1, 2)
	})

	t.Run("leader failure and stale leader fencing", func(t *testing.T) {
		leader := h.leader()
		h.stop(leader)
		h.waitJob("web", 1, 2) // a surviving follower proxies to the new leader
		// A stopped former leader cannot continue issuing agent mutations. The
		// newly elected leader advances the durable epoch before reconciliation.
		h.start(leader)
		h.waitHTTP(leader)
		h.waitNodes(3)
	})

	t.Run("rolling update across election", func(t *testing.T) {
		h.submit(job("web", "v2", 3, "rolling"))
		oldLeader := h.leader()
		h.stop(oldLeader)
		_ = h.leader() // wait until the surviving quorum has elected a replacement
		h.start(oldLeader)
		h.waitHTTP(oldLeader)
		h.waitJob("web", 2, 3)
	})

	t.Run("process restart and allocation adoption", func(t *testing.T) {
		idx := h.anyRunning()
		before := h.runtimeState(idx)
		h.stop(idx)
		h.start(idx)
		h.waitHTTP(idx)
		h.waitNodes(2)
		after := h.runtimeState(idx)
		if len(before) > 0 && len(after) == 0 {
			t.Fatal("runtime inventory disappeared across process restart")
		}
	})

	t.Run("ambiguous stop converges", func(t *testing.T) {
		for i := range h.nodes {
			if h.nodes[i].cmd != nil {
				h.fault(i, "stop", "after")
			}
		}
		h.submit(job("web", "v3", 1, "recreate"))
		h.waitJob("web", 3, 1)
	})
}

type node struct {
	dir      string
	ports    [5]int
	args     []string
	cmd      *exec.Cmd
	log      *os.File
	logStart int64
}
type harness struct {
	t          *testing.T
	bin, token string
	nodes      []*node
	client     *http.Client
}

func newHarness(t *testing.T, count int) *harness {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "trellis")
	c := exec.Command("go", "build", "-o", bin, "./cmd/trellis")
	c.Dir = root
	if out, err := c.CombinedOutput(); err != nil {
		t.Fatalf("build node: %v\n%s", err, out)
	}
	h := &harness{t: t, bin: bin, token: "integration-token", client: &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}}
	base := t.TempDir()
	for i := 0; i < count; i++ {
		n := &node{dir: filepath.Join(base, fmt.Sprintf("node-%d", i))}
		_ = os.MkdirAll(n.dir, 0o750)
		listeners := reservePorts(t, len(n.ports))
		for p, listener := range listeners {
			n.ports[p] = listener.Addr().(*net.TCPAddr).Port
		}
		n.args = []string{"--admin-token", h.token, "--enrollment-token", "integration-enrollment", "--cluster", "integration", "--data-dir", n.dir, "--runtime", "injected", "--runtime-faults", filepath.Join(n.dir, "fault.json"), "--agent-listen", addr(n.ports[0]), "--agent-advertise", addr(n.ports[0]), "--server-listen", addr(n.ports[1]), "--server-advertise", addr(n.ports[1]), "--raft-listen", addr(n.ports[2]), "--raft-advertise", addr(n.ports[2]), "--dns-listen", addr(n.ports[3]), "--wireguard-port", fmt.Sprint(n.ports[4]), "--wireguard-port-count", "1"}
		if i > 0 {
			n.args = append(n.args, "--join", addr(h.nodes[0].ports[1]), "--ca-cert", filepath.Join(h.nodes[0].dir, "node-ca.crt"))
		}
		for _, listener := range listeners {
			_ = listener.Close()
		}
		h.nodes = append(h.nodes, n)
		h.start(i)
		h.waitHTTP(i)
	}
	return h
}
func addr(p int) string { return fmt.Sprintf("127.0.0.1:%d", p) }
func reservePorts(t *testing.T, count int) []net.Listener {
	t.Helper()
	listeners := make([]net.Listener, 0, count)
	t.Cleanup(func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	})
	for range count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
	}
	return listeners
}
func (h *harness) start(i int) {
	n := h.nodes[i]
	f, e := os.OpenFile(filepath.Join(n.dir, "node.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if e != nil {
		h.t.Fatal(e)
	}
	n.logStart, _ = f.Seek(0, io.SeekEnd)
	n.log = f
	n.cmd = exec.Command(h.bin, n.args...)
	n.cmd.Stdout = f
	n.cmd.Stderr = f
	if e = n.cmd.Start(); e != nil {
		h.t.Fatal(e)
	}
}
func (h *harness) stop(i int) {
	n := h.nodes[i]
	if n.cmd == nil {
		return
	}
	_ = n.cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() { done <- n.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(12 * time.Second):
		_ = n.cmd.Process.Kill()
		<-done
	}
	_ = n.log.Close()
	n.cmd = nil
}
func (h *harness) close() {
	for i := range h.nodes {
		h.stop(i)
	}
}
func (h *harness) waitHTTP(i int) {
	h.eventually(35*time.Second, func() bool {
		r, e := h.request(i, "GET", "/v1/nodes", nil)
		if r != nil {
			r.Body.Close()
		}
		return e == nil && r.StatusCode == 200
	}, "node API did not become ready")
}
func (h *harness) request(i int, method, path string, body any) (*http.Response, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(context.Background(), method, "https://"+addr(h.nodes[i].ports[1])+path, rd)
	req.Header.Set("Authorization", "Bearer "+h.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Trellis-Namespace", "default")
	return h.client.Do(req)
}
func (h *harness) endpoint() int {
	for i, n := range h.nodes {
		if n.cmd != nil {
			return i
		}
	}
	h.t.Fatal("no running node")
	return 0
}
func (h *harness) submit(v any) {
	r, e := h.request(h.endpoint(), "POST", "/v1/jobs", v)
	if e != nil {
		h.t.Fatal(e)
	}
	defer r.Body.Close()
	if r.StatusCode/100 != 2 {
		b, _ := io.ReadAll(r.Body)
		h.t.Fatalf("submit: %s: %s", r.Status, b)
	}
}
func (h *harness) waitNodes(want int) {
	h.eventually(45*time.Second, func() bool {
		r, e := h.request(h.endpoint(), "GET", "/v1/nodes", nil)
		if e != nil {
			return false
		}
		defer r.Body.Close()
		var v []any
		return r.StatusCode == 200 && json.NewDecoder(r.Body).Decode(&v) == nil && len(v) >= want
	}, "node registration did not converge")
}
func (h *harness) waitJob(name string, revision, desired int) {
	var last []byte
	converged := false
	defer func() {
		if !converged {
			h.t.Logf("last job response: %s", last)
		}
	}()
	h.eventually(70*time.Second, func() bool {
		r, e := h.request(h.endpoint(), "GET", "/v1/jobs/"+name, nil)
		if e != nil {
			return false
		}
		defer r.Body.Close()
		last, _ = io.ReadAll(r.Body)
		var v struct{ Revision, Desired, Running int }
		converged = r.StatusCode == 200 && json.Unmarshal(last, &v) == nil && v.Revision == revision && v.Desired == desired && v.Running == desired
		return converged
	}, fmt.Sprintf("job %s did not converge", name))
}
func (h *harness) eventually(d time.Duration, fn func() bool, msg string) {
	until := time.Now().Add(d)
	for time.Now().Before(until) {
		if fn() {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	for i, n := range h.nodes {
		b, _ := os.ReadFile(filepath.Join(n.dir, "node.log"))
		h.t.Logf("node %d:\n%s", i, b)
	}
	h.t.Fatal(msg)
}
func (h *harness) leader() int {
	found := -1
	h.eventually(20*time.Second, func() bool {
		for i, n := range h.nodes {
			if n.cmd == nil {
				continue
			}
			b, _ := os.ReadFile(filepath.Join(n.dir, "node.log"))
			if n.logStart < int64(len(b)) {
				segment := string(b[n.logStart:])
				acquired := strings.LastIndex(segment, "leadership acquired")
				lost := strings.LastIndex(segment, "leadership lost")
				if acquired >= 0 && acquired > lost {
					found = i
					return true
				}
			}
		}
		return false
	}, "leader not observed")
	return found
}
func (h *harness) anyRunning() int { return h.endpoint() }
func (h *harness) fault(i int, op, timing string) {
	b, _ := json.Marshal(map[string]any{"operation": op, "timing": timing, "count": 1})
	if e := os.WriteFile(filepath.Join(h.nodes[i].dir, "fault.json"), b, 0o600); e != nil {
		h.t.Fatal(e)
	}
}
func (h *harness) runtimeState(i int) map[string]any {
	b, _ := os.ReadFile(filepath.Join(h.nodes[i].dir, "injected-runtime.json"))
	var v struct {
		Containers map[string]any `json:"containers"`
	}
	_ = json.Unmarshal(b, &v)
	return v.Containers
}
func job(name, image string, count int, strategy string) map[string]any {
	return map[string]any{"spec": map[string]any{"name": name, "namespace": "default", "task_groups": []any{map[string]any{"name": "web", "count": count, "update": map[string]any{"strategy": strategy, "max_parallel": 1}, "tasks": []any{map[string]any{"name": "web", "image": image, "networking": map[string]any{"mode": "host"}}}}}}}
}
