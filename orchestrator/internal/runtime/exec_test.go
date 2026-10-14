package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"syscall"
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
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

func testExecCleanup(t *testing.T, process execProcess) func(bool) {
	t.Helper()
	cleanup := newExecCleanup()
	t.Cleanup(cleanup.cancel)
	if err := cleanup.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	return func(kill bool) { cleanup.cleanup(context.Background(), process, kill, "test", "test") }
}

func TestRunExecProcessKillsAndDeletesOnCancellation(t *testing.T) {
	process := newFakeExecProcess()
	cleanup := testExecCleanup(t, process)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := runExecProcess(ctx, process, cleanup)
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
	if signals := process.killSignals(); len(signals) == 0 || signals[0] != syscall.SIGKILL {
		t.Fatalf("kill signals = %v, want SIGKILL", signals)
	}
}

func TestRunExecProcessReturnsExitWithoutKilling(t *testing.T) {
	process := newFakeExecProcess()
	process.exitCh <- *containerd.NewExitStatus(3, time.Now(), nil)

	var cleanupRequested, killRequested bool
	status, err := runExecProcess(context.Background(), process, func(kill bool) {
		cleanupRequested, killRequested = true, kill
	})
	if err != nil {
		t.Fatal(err)
	}
	if code, _, _ := status.Result(); code != 3 {
		t.Fatalf("exit code = %d, want 3", code)
	}
	if signals := process.killSignals(); len(signals) != 0 {
		t.Fatalf("exited process was killed: %v", signals)
	}
	if !cleanupRequested || killRequested {
		t.Fatal("successful exit must request deletion without a kill")
	}
}

func TestRunExecProcessCleansUpAfterStartFailure(t *testing.T) {
	for _, launched := range []bool{false, true} {
		process := newFakeExecProcess()
		process.startErr = errors.New("start failed")
		process.startLaunches = launched
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if _, err := runExecProcess(ctx, process, testExecCleanup(t, process)); !errors.Is(err, process.startErr) {
			t.Fatalf("launched=%v: run error = %v, want start failure", launched, err)
		}
		select {
		case err := <-process.deleted:
			if err != nil {
				t.Fatalf("launched=%v: delete used an ended context: %v", launched, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("launched=%v: process was not deleted after start failure", launched)
		}
		if launched {
			if signals := process.killSignals(); len(signals) == 0 || signals[0] != syscall.SIGKILL {
				t.Fatalf("launched process kill signals = %v, want SIGKILL", signals)
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

func TestExecCleanupRetainsSlotDuringBlockedDelete(t *testing.T) {
	process := &hangingDeleteProcess{fakeExecProcess: newFakeExecProcess(), release: make(chan struct{})}
	defer close(process.release)
	cleanup := newExecCleanup()
	defer cleanup.cancel()
	cleanup.slots = make(chan struct{}, 1)
	if err := cleanup.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	cleanup.cleanup(context.Background(), process, false, "test", "test")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	if err := cleanup.acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("admission error = %v, want deadline exceeded while deletion is unresolved", err)
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

	testExecCleanup(t, process)(true)
	select {
	case err := <-process.deleted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("process was not deleted after a retried kill")
	}
	if signals := process.killSignals(); len(signals) < 2 {
		t.Fatalf("kill attempts = %d, want a retry after failed kill", len(signals))
	}
}

type outageExecProcess struct {
	*fakeExecProcess
	restored chan struct{}
	exits    bool
}

func (p *outageExecProcess) Start(ctx context.Context) error {
	if err := p.fakeExecProcess.Start(ctx); err != nil {
		return err
	}
	if p.exits {
		p.mu.Lock()
		p.running = false
		p.mu.Unlock()
		p.exitCh <- *containerd.NewExitStatus(3, time.Now(), nil)
	}
	return nil
}

func (p *outageExecProcess) Kill(ctx context.Context, signal syscall.Signal, opts ...containerd.KillOpts) error {
	select {
	case <-p.restored:
		return p.fakeExecProcess.Kill(ctx, signal, opts...)
	default:
		return errdefs.ErrUnavailable
	}
}

func (p *outageExecProcess) Delete(ctx context.Context, opts ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error) {
	select {
	case <-p.restored:
		return p.fakeExecProcess.Delete(ctx, opts...)
	default:
		return nil, errdefs.ErrUnavailable
	}
}

func TestExecCleanupSurvivesRuntimeOutage(t *testing.T) {
	for _, exits := range []bool{false, true} {
		t.Run(fmt.Sprintf("successful_exit=%t", exits), func(t *testing.T) {
			process := &outageExecProcess{fakeExecProcess: newFakeExecProcess(), restored: make(chan struct{}), exits: exits}
			cleanup := newExecCleanup()
			defer cleanup.cancel()
			cleanup.slots = make(chan struct{}, 1)
			if err := cleanup.acquire(context.Background()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			if !exits {
				cancel()
			}
			defer cancel()
			started := time.Now()
			status, err := runExecProcess(ctx, process, func(kill bool) {
				cleanup.cleanup(ctx, process, kill, "test", "test")
			})
			if exits {
				code, _, statusErr := status.Result()
				if err != nil || statusErr != nil || code != 3 {
					t.Fatalf("exit = %d, %v, %v; want 3 without errors", code, err, statusErr)
				}
			} else if !errors.Is(err, context.Canceled) {
				t.Fatalf("run error = %v, want cancellation", err)
			}
			if time.Since(started) > time.Second {
				t.Fatal("caller waited for runtime recovery")
			}
			// The former cleanup budget expired at five seconds. Admission
			// must remain blocked beyond that boundary, even for exited execs.
			blocked, stop := context.WithTimeout(context.Background(), execCleanupTimeout+100*time.Millisecond)
			defer stop()
			if err := cleanup.acquire(blocked); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("admission during outage = %v, want deadline exceeded", err)
			}
			close(process.restored)
			select {
			case err := <-process.deleted:
				if err != nil {
					t.Fatalf("cleanup inherited cancelled context: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("exec not deleted after runtime recovery")
			}
			retry, stopRetry := context.WithTimeout(context.Background(), time.Second)
			defer stopRetry()
			if err := cleanup.acquire(retry); err != nil {
				t.Fatalf("confirmed deletion did not restore admission: %v", err)
			}
			<-cleanup.slots
			if exits && len(process.killSignals()) != 0 {
				t.Fatal("successfully exited exec was killed during deletion retry")
			}
		})
	}
}

type drainingExecIO struct {
	cio.IO
	drained chan struct{}
}

func (d *drainingExecIO) Wait() { <-d.drained }

type drainingExecProcess struct {
	containerd.Process
	output    *drainingExecIO
	deleteErr error
}

func (p *drainingExecProcess) IO() cio.IO { return p.output }

func (p *drainingExecProcess) Delete(context.Context, ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error) {
	return nil, p.deleteErr
}

func TestStreamedExecCompletionWaitsForOutputAndReportsErrors(t *testing.T) {
	failure := errors.New("delivery failed")
	for _, test := range []struct {
		name                string
		writeErr, deleteErr error
	}{
		{name: "complete"},
		{name: "output failure", writeErr: failure},
		{name: "delete failure", deleteErr: failure},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			stdout := &execOutput{Writer: &output}
			stderr := &execOutput{Writer: io.Discard}
			process := &drainingExecProcess{output: &drainingExecIO{drained: make(chan struct{})}, deleteErr: test.deleteErr}
			cleanup := newExecCleanup()
			defer cleanup.cancel()
			if err := cleanup.acquire(context.Background()); err != nil {
				t.Fatal(err)
			}
			result := &containerdExecProcess{process: process, stdout: stdout, stderr: stderr, done: make(chan struct{}), cleanup: func(kill bool) <-chan error {
				return cleanup.cleanup(context.Background(), process, kill, "test", "test")
			}}
			go result.complete(context.Background(), *containerd.NewExitStatus(0, time.Now(), nil))
			stall := 10 * time.Millisecond
			if test.name == "complete" {
				// A slow consumer must survive the old five-second drain
				// timeout without premature successful completion.
				stall = execCleanupTimeout + 100*time.Millisecond
			}
			select {
			case <-result.Done():
				t.Fatal("exec completed before output drained")
			case <-time.After(stall):
			}
			_, _ = stdout.Write([]byte("last output"))
			if test.writeErr != nil {
				stderr.Writer = failingExecWriter{err: test.writeErr}
				_, _ = stderr.Write([]byte("stderr"))
			}
			close(process.output.drained)
			code, err := result.ExitCode()
			if code != 0 || output.String() != "last output" {
				t.Fatalf("completion lost output or exit code: %d, %q", code, output.String())
			}
			if wantError := test.writeErr != nil || test.deleteErr != nil; wantError != errors.Is(err, failure) {
				t.Fatalf("completion error = %v, want failure=%t", err, wantError)
			}
		})
	}
}

type failingExecWriter struct{ err error }

func (w failingExecWriter) Write([]byte) (int, error) { return 0, w.err }
