package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/network"
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
		firstDone <- agent.UpdateNetworkPlan(context.Background(), &api.NetworkPlanRequest{Epoch: 1, Namespace: "default"})
	}()
	<-manager.entered

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- agent.UpdateNetworkPlan(context.Background(), &api.NetworkPlanRequest{Epoch: 2, Namespace: "default"})
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
	if err := agent.UpdateNetworkPlan(context.Background(), &api.NetworkPlanRequest{Epoch: 1, Namespace: "default"}); !errors.Is(err, ErrStaleEpoch) {
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
	err := agent.UpdateNetworkPlan(context.Background(), &api.NetworkPlanRequest{
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
