package agent

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/network"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

type blockingPlanManager struct {
	entered chan struct{}
	release chan struct{}
}

func (m *blockingPlanManager) Attach(context.Context, network.AttachRequest) (*network.Attachment, error) {
	return nil, nil
}

func (m *blockingPlanManager) Detach(context.Context, *network.Attachment) error {
	return nil
}

func (m *blockingPlanManager) UpdatePlan(context.Context, string, network.Plan) error {
	m.entered <- struct{}{}
	<-m.release
	return nil
}

func TestUpdateNetworkPlanSerializesEpochAndApplication(t *testing.T) {
	agent := newOperationTestAgent(t, &reconcilerRuntime{})
	manager := &blockingPlanManager{entered: make(chan struct{}, 2), release: make(chan struct{})}
	agent.SetNetworkManager(manager)
	agent.allocations["allocation"] = &Allocation{Namespace: "default", Network: &network.Attachment{}}

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- agent.UpdateNetworkPlan(context.Background(), &nodeapi.NetworkPlanRequest{Epoch: 1, Namespace: "default"})
	}()
	<-manager.entered

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- agent.UpdateNetworkPlan(context.Background(), &nodeapi.NetworkPlanRequest{Epoch: 2, Namespace: "default"})
	}()
	select {
	case <-manager.entered:
		t.Fatal("higher-epoch plan applied before the in-flight plan completed")
	case <-time.After(30 * time.Millisecond):
	}

	manager.release <- struct{}{}
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	<-manager.entered
	manager.release <- struct{}{}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if err := agent.UpdateNetworkPlan(context.Background(), &nodeapi.NetworkPlanRequest{Epoch: 1, Namespace: "default"}); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("stale plan error = %v, want %v", err, ErrStaleEpoch)
	}
}

func TestUpdateNetworkPlanRejectsActiveSubnetChange(t *testing.T) {
	agent := newOperationTestAgent(t, &reconcilerRuntime{})
	agent.allocations["allocation"] = &Allocation{
		Namespace: "default",
		Network: &network.Attachment{
			Address: "10.42.1.23/24",
			Gateway: "10.42.1.1",
		},
	}
	err := agent.UpdateNetworkPlan(context.Background(), &nodeapi.NetworkPlanRequest{
		Epoch:     1,
		Namespace: "default",
		Plan: network.Plan{
			CIDR:    "10.42.2.0/24",
			Gateway: "10.42.2.1",
		},
	})
	if err == nil {
		t.Fatal("active namespace subnet change was accepted")
	}
}

func TestSlowNetworkPlanAllowsStartAndCancelledPlanWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		agent := newOperationTestAgent(t, &reconcilerRuntime{})
		manager := &blockingPlanManager{entered: make(chan struct{}, 2), release: make(chan struct{})}
		agent.SetNetworkManager(manager)
		agent.allocations["existing"] = &Allocation{Namespace: "default", Network: &network.Attachment{}}
		firstCtx, cancelFirst := context.WithCancel(context.Background())
		defer cancelFirst()
		first := make(chan error, 1)
		go func() {
			first <- agent.UpdateNetworkPlan(firstCtx, &nodeapi.NetworkPlanRequest{Epoch: 1, Namespace: "default"})
		}()
		<-manager.entered
		// Admission of an unrelated start (including a higher epoch) must not
		// wait for network I/O. Its background execution may need networking.
		request := singleTaskRequest()
		request.Epoch = 3
		startDone := make(chan error, 1)
		go func() { startDone <- agent.StartGroup(context.Background(), request) }()
		synctest.Wait()
		select {
		case err := <-startDone:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("network plan blocked unrelated start admission")
		}
		// Even if application ignores cancellation until late completion,
		// retain plan exclusion rather than overlapping external commands.
		cancelFirst()
		ctx, cancel := context.WithCancel(context.Background())
		second := make(chan error, 1)
		go func() {
			second <- agent.UpdateNetworkPlan(ctx, &nodeapi.NetworkPlanRequest{Epoch: 4, Namespace: "default"})
		}()
		synctest.Wait()
		cancel()
		if err := <-second; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled plan wait = %v", err)
		}
		select {
		case <-manager.entered:
			t.Fatal("cancelled queued plan reached network application")
		default:
		}
		// Completing an admitted old plan does not roll back the accepted epoch.
		close(manager.release)
		if err := <-first; !errors.Is(err, context.Canceled) {
			t.Fatalf("late cancelled plan = %v", err)
		}
		if err := agent.UpdateNetworkPlan(context.Background(), &nodeapi.NetworkPlanRequest{Epoch: 2, Namespace: "default"}); !errors.Is(err, ErrStaleEpoch) {
			t.Fatalf("late stale plan = %v", err)
		}
	})
}

func TestDelayedAttachmentUsesAuthoritativeTopology(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-first-attachment", true: "active-path"}[active], func(t *testing.T) {
			a := newOperationTestAgent(t, &reconcilerRuntime{})
			manager := newRecoveringNetworkManager()
			a.SetNetworkManager(manager)
			if active {
				a.allocations["existing"] = &Allocation{Namespace: "acme", Network: &network.Attachment{}}
			}
			newer := network.Plan{CIDR: "10.42.0.0/24", Gateway: "10.42.0.1", Peers: []network.PeerPlan{{PublicKey: "new-peer", AllowedIPs: []string{"10.42.3.0/24"}}}}
			if err := a.UpdateNetworkPlan(t.Context(), &nodeapi.NetworkPlanRequest{Epoch: 1, Namespace: "acme", Plan: newer}); err != nil {
				t.Fatal(err)
			}
			stale := newer
			stale.Peers = []network.PeerPlan{{PublicKey: "removed-peer", AllowedIPs: []string{"10.42.2.0/24"}}}
			stale.APIPort = 8126
			manager.beforeAttach = func(request network.AttachRequest) {
				if !reflect.DeepEqual(request.Plan.Peers, newer.Peers) || request.Plan.APIPort != 8126 {
					t.Fatalf("delayed start overwrote topology or lost its API grant: %#v", request.Plan)
				}
			}
			launch := &taskLaunch{task: &taskStart{ID: "delayed", Namespace: "acme", NetworkPlan: &stale}, alloc: &Allocation{ID: "delayed", Namespace: "acme"}}
			if err := a.attachTaskNetwork(t.Context(), launch); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAgentReadinessTracksDNSPrerequisite(t *testing.T) {
	a := newOperationTestAgent(t, &reconcilerRuntime{})
	ready := false
	a.SetReadiness(func() bool { return ready })
	if a.ready() {
		t.Fatal("agent advertised readiness without DNS listeners")
	}
	if err := a.StartGroup(t.Context(), singleTaskRequest()); err == nil {
		t.Fatal("agent accepted a new start without DNS listeners")
	}
	ready = true
	if !a.ready() {
		t.Fatal("agent did not recover readiness after listener restart")
	}
	ready = false
	if a.ready() {
		t.Fatal("agent retained readiness after DNS listener failure")
	}
}

func TestIdleNamespaceForgetsTopologyButPendingStartsAndCleanupRetainIt(t *testing.T) {
	a := newOperationTestAgent(t, &reconcilerRuntime{})
	a.networkPlans = map[string]network.Plan{"acme": {CIDR: "10.42.1.0/24"}}
	done := make(chan struct{})
	a.starts = map[string]*groupStart{"pulling": {namespace: "acme", done: done}}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.forgetIdleNetworkPlanLocked("acme")
	if _, ok := a.networkPlans["acme"]; !ok {
		t.Fatal("a pending image pull lost its authoritative topology")
	}
	close(done)
	a.allocations["cleanup"] = &Allocation{Namespace: "acme", NetworkIntent: &network.AttachmentIntent{}}
	a.forgetIdleNetworkPlanLocked("acme")
	if _, ok := a.networkPlans["acme"]; !ok {
		t.Fatal("retained network intent lost its authoritative topology")
	}
	delete(a.allocations, "cleanup")
	a.forgetIdleNetworkPlanLocked("acme")
	if _, ok := a.networkPlans["acme"]; ok {
		t.Fatal("recreated idle namespace would inherit its old subnet and peers")
	}
}
