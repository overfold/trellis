package runtime

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
)

type fakeExecProcess struct {
	exitCh   chan containerd.ExitStatus
	startErr error
	// startLaunches makes a failed Start still leave the process running.
	startLaunches bool

	mu      sync.Mutex
	running bool
	killed  []syscall.Signal
	deleted chan error
}

func newFakeExecProcess() *fakeExecProcess {
	return &fakeExecProcess{exitCh: make(chan containerd.ExitStatus, 1), deleted: make(chan error, 2)}
}

func (p *fakeExecProcess) Wait(context.Context) (<-chan containerd.ExitStatus, error) {
	return p.exitCh, nil
}

func (p *fakeExecProcess) Start(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running = p.startErr == nil || p.startLaunches
	return p.startErr
}

func (p *fakeExecProcess) Kill(_ context.Context, signal syscall.Signal, _ ...containerd.KillOpts) error {
	p.mu.Lock()
	p.killed = append(p.killed, signal)
	running := p.running
	p.mu.Unlock()
	if !running {
		return errdefs.ErrNotFound
	}
	// The signal is delivered asynchronously; the process exits later.
	go func() {
		p.mu.Lock()
		p.running = false
		p.mu.Unlock()
		p.exitCh <- *containerd.NewExitStatus(137, time.Now(), nil)
	}()
	return nil
}

func (p *fakeExecProcess) Delete(ctx context.Context, _ ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error) {
	p.mu.Lock()
	running := p.running
	p.mu.Unlock()
	if running {
		return nil, errdefs.ErrFailedPrecondition
	}
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

func TestRunExecProcessCleansUpAfterStartFailure(t *testing.T) {
	for _, launched := range []bool{false, true} {
		process := newFakeExecProcess()
		process.startErr = errors.New("start failed")
		process.startLaunches = launched
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if _, err := runExecProcess(ctx, process); !errors.Is(err, process.startErr) {
			t.Fatalf("launched=%v: run error = %v, want start failure", launched, err)
		}
		select {
		case err := <-process.deleted:
			if err != nil {
				t.Fatalf("launched=%v: delete used an ended context: %v", launched, err)
			}
		default:
			t.Fatalf("launched=%v: process was not deleted after start failure", launched)
		}
		if launched {
			if signals := process.killSignals(); len(signals) != 1 || signals[0] != syscall.SIGKILL {
				t.Fatalf("launched process kill signals = %v, want [SIGKILL]", signals)
			}
		}
	}
}

type hangingDeleteProcess struct {
	*fakeExecProcess
	release chan struct{}
}

func (p *hangingDeleteProcess) Delete(context.Context, ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error) {
	<-p.release // containerd's output wait ignores the context.
	return nil, nil
}

func TestDeleteWithinGivesUpWhenContextEnds(t *testing.T) {
	process := &hangingDeleteProcess{fakeExecProcess: newFakeExecProcess(), release: make(chan struct{})}
	defer close(process.release)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	if err := deleteWithin(ctx, process); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delete error = %v, want deadline exceeded", err)
	}
}

type flakyKillProcess struct {
	*fakeExecProcess
	failures int
}

func (p *flakyKillProcess) Kill(ctx context.Context, signal syscall.Signal, opts ...containerd.KillOpts) error {
	p.mu.Lock()
	if p.failures > 0 {
		p.failures--
		p.killed = append(p.killed, signal)
		p.mu.Unlock()
		return errors.New("transient kill failure")
	}
	p.mu.Unlock()
	return p.fakeExecProcess.Kill(ctx, signal, opts...)
}

func TestKillExecProcessRetriesFailedKill(t *testing.T) {
	process := &flakyKillProcess{fakeExecProcess: newFakeExecProcess(), failures: 1}
	process.running = true

	killExecProcess(context.Background(), process, process.exitCh)
	select {
	case err := <-process.deleted:
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("process was not deleted after a retried kill")
	}
	if signals := process.killSignals(); len(signals) != 2 {
		t.Fatalf("kill attempts = %d, want 2", len(signals))
	}
}

func TestLockedBufferDiscardsWritesAfterSnapshot(t *testing.T) {
	var buffer lockedBuffer
	_, _ = buffer.Write([]byte("before"))
	if got := string(buffer.Bytes()); got != "before" {
		t.Fatalf("snapshot = %q", got)
	}
	if n, err := buffer.Write([]byte("after")); n != 5 || err != nil {
		t.Fatalf("detached write = %d, %v", n, err)
	}
	if got := len(buffer.Bytes()); got != 0 {
		t.Fatalf("detached buffer kept %d bytes", got)
	}
}
