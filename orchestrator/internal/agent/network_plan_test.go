package agent

import (
	"context"
	"errors"
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
