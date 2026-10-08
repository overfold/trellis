package agent

import (
	"context"
	"net"
	"slices"
	"strings"
	"testing"

	"github.com/overfold/trellis/orchestrator/internal/network"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// recordingAttachManager records the attach requests it receives.
type recordingAttachManager struct {
	staticNetworkManager
	requests []network.AttachRequest
}

func (m *recordingAttachManager) Attach(ctx context.Context, request network.AttachRequest) (*network.Attachment, error) {
	m.requests = append(m.requests, request)
	return m.staticNetworkManager.Attach(ctx, request)
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	return listener.Addr().(*net.TCPAddr).Port
}

func TestNamespaceTaskPublishesClaimedNodePorts(t *testing.T) {
	rt := &blockingStartRuntime{reconcilerRuntime: &reconcilerRuntime{}, started: make(chan string, 1), release: make(chan struct{}, 1), labels: map[string]map[string]string{}, created: map[string]runtime.CreateOptions{}}
	rt.release <- struct{}{}
	agent := newOperationTestAgent(t, rt)
	manager := &recordingAttachManager{}
	agent.SetNetworkManager(manager)
	hostPort := freeTCPPort(t)
	task := &spec.TaskSpec{Name: "web", Image: "image", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard, Ports: []spec.PortSpec{{Port: 8080, HostPort: hostPort}}}}
	if err := agent.startTask(context.Background(), &taskStart{ID: "alloc", AllocationID: "scheduler", Generation: 1, JobRevision: 1, ExecutionHash: "hash", Namespace: "default", JobName: "job", GroupName: "group", Spec: task, NetworkPlan: &network.Plan{}}); err != nil {
		t.Fatal(err)
	}
	if len(manager.requests) != 1 {
		t.Fatalf("attach requests = %d, want 1", len(manager.requests))
	}
	if got, want := manager.requests[0].Ports, []network.PortMapping{{HostPort: hostPort, ContainerPort: 8080}}; !slices.Equal(got, want) {
		t.Fatalf("attach ports = %v, want %v", got, want)
	}
	claim := agent.ports.claims[hostPort]
	if claim == nil || claim.ContainerPort != 8080 {
		t.Fatalf("node port claim = %+v, want %d->8080", claim, hostPort)
	}
	if _, claimed := agent.ports.claims[8080]; claimed && hostPort != 8080 {
		t.Fatal("namespace listen port was claimed on the node")
	}
	if got := rt.created["alloc"].NetworkNamespace; got != "/var/run/netns/alloc" {
		t.Fatalf("network namespace = %q, want the attachment namespace", got)
	}
}

func TestHostTaskReservesPortWithoutAttaching(t *testing.T) {
	rt := &blockingStartRuntime{reconcilerRuntime: &reconcilerRuntime{}, started: make(chan string, 1), release: make(chan struct{}, 1), labels: map[string]map[string]string{}, created: map[string]runtime.CreateOptions{}}
	rt.release <- struct{}{}
	agent := newOperationTestAgent(t, rt)
	manager := &recordingAttachManager{}
	agent.SetNetworkManager(manager)
	port := freeTCPPort(t)
	task := &spec.TaskSpec{Name: "web", Image: "image", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkHost, Ports: []spec.PortSpec{{Port: port}}}}
	if err := agent.startTask(context.Background(), &taskStart{ID: "alloc", AllocationID: "scheduler", Generation: 1, JobRevision: 1, ExecutionHash: "hash", Namespace: "default", JobName: "job", GroupName: "group", Spec: task}); err != nil {
		t.Fatal(err)
	}
	if len(manager.requests) != 0 {
		t.Fatalf("host task attached a namespace network: %+v", manager.requests)
	}
	if claim := agent.ports.claims[port]; claim == nil || claim.ContainerPort != port {
		t.Fatalf("node port claim = %+v, want %d", claim, port)
	}
	if got := rt.created["alloc"].NetworkNamespace; got != "/proc/1/ns/net" {
		t.Fatalf("network namespace = %q, want the node's", got)
	}
}

func TestTaskCannotClaimWireGuardPort(t *testing.T) {
	rt := &blockingStartRuntime{reconcilerRuntime: &reconcilerRuntime{}, started: make(chan string, 1), release: make(chan struct{}, 1), labels: map[string]map[string]string{}, created: map[string]runtime.CreateOptions{}}
	agent := newOperationTestAgent(t, rt)
	manager := &recordingAttachManager{}
	agent.SetNetworkManager(manager)
	agent.SetWireGuardIdentity("key", "node:51820", 51820, 4)
	task := &spec.TaskSpec{Name: "web", Image: "image", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard, Ports: []spec.PortSpec{{Port: 8080, HostPort: 51823}}}}
	err := agent.startTask(context.Background(), &taskStart{ID: "alloc", AllocationID: "scheduler", Generation: 1, JobRevision: 1, ExecutionHash: "hash", Namespace: "default", JobName: "job", GroupName: "group", Spec: task, NetworkPlan: &network.Plan{}})
	if err == nil || !strings.Contains(err.Error(), "reserved for namespace WireGuard") {
		t.Fatalf("startTask() error = %v, want WireGuard port refusal", err)
	}
	if len(manager.requests) != 0 || len(agent.ports.claims) != 0 {
		t.Fatalf("refused port was attached or claimed: requests=%v claims=%v", manager.requests, agent.ports.claims)
	}
}

func TestPortManagerTreatsUDPListenerAsTaken(t *testing.T) {
	conn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	port := conn.LocalAddr().(*net.UDPAddr).Port
	manager := NewPortManager(nil, 0, 0)
	if _, err := manager.Claim(spec.PortSpec{Port: 8080, HostPort: port}); err == nil {
		// The TCP port of the same number may be free; the UDP listener still
		// holds a port that publishing would forward.
		t.Fatalf("Claim(%d) succeeded while a UDP listener holds it", port)
	}
}
