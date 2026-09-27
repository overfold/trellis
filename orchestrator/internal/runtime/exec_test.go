package runtime

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
)

type fakeExecProcess struct {
	exitCh   chan containerd.ExitStatus
	startErr error

	mu      sync.Mutex
	killed  []syscall.Signal
	deleted chan error
}

func newFakeExecProcess() *fakeExecProcess {
	return &fakeExecProcess{exitCh: make(chan containerd.ExitStatus, 1), deleted: make(chan error, 1)}
}

func (p *fakeExecProcess) Wait(context.Context) (<-chan containerd.ExitStatus, error) {
	return p.exitCh, nil
}

func (p *fakeExecProcess) Start(context.Context) error { return p.startErr }

func (p *fakeExecProcess) Kill(_ context.Context, signal syscall.Signal, _ ...containerd.KillOpts) error {
	p.mu.Lock()
	p.killed = append(p.killed, signal)
	p.mu.Unlock()
	p.exitCh <- *containerd.NewExitStatus(137, time.Now(), nil)
	return nil
}

func (p *fakeExecProcess) Delete(ctx context.Context, _ ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error) {
	p.deleted <- ctx.Err()
	return nil, nil
}

func (p *fakeExecProcess) killSignals() []syscall.Signal {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]syscall.Signal(nil), p.killed...)
}

func TestRunExecProcessKillsAndDeletesOnCancellation(t *testing.T) {
	process := newFakeExecProcess()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := runExecProcess(ctx, process)
		done <- err
	}()
	cancel()

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want context canceled", err)
	}
	select {
	case err := <-process.deleted:
		if err != nil {
			t.Fatalf("delete used an ended context: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled exec process was not deleted")
	}
	if signals := process.killSignals(); len(signals) != 1 || signals[0] != syscall.SIGKILL {
		t.Fatalf("kill signals = %v, want [SIGKILL]", signals)
	}
}

func TestRunExecProcessReturnsExitWithoutKilling(t *testing.T) {
	process := newFakeExecProcess()
	process.exitCh <- *containerd.NewExitStatus(3, time.Now(), nil)

	status, err := runExecProcess(context.Background(), process)
	if err != nil {
		t.Fatal(err)
	}
	if code, _, _ := status.Result(); code != 3 {
		t.Fatalf("exit code = %d, want 3", code)
	}
	if signals := process.killSignals(); len(signals) != 0 {
		t.Fatalf("exited process was killed: %v", signals)
	}
	select {
	case <-process.deleted:
		t.Fatal("successful exit deleted the process before the caller collected output")
	default:
	}
}

func TestRunExecProcessDeletesAfterStartFailure(t *testing.T) {
	process := newFakeExecProcess()
	process.startErr = errors.New("start failed")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := runExecProcess(ctx, process); !errors.Is(err, process.startErr) {
		t.Fatalf("run error = %v, want start failure", err)
	}
	select {
	case err := <-process.deleted:
		if err != nil {
			t.Fatalf("delete used an ended context: %v", err)
		}
	default:
		t.Fatal("process was not deleted after start failure")
	}
}
