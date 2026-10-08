package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/overfold/trellis/orchestrator/internal/election"
	"github.com/spf13/cobra"
)

func TestLeaderTermCoalescedNotifications(t *testing.T) {
	for _, coalesced := range []bool{false, true} {
		t.Run(map[bool]string{false: "consecutive true", true: "false overwritten before drain"}[coalesced], func(t *testing.T) {
			var term leaderTerm
			defer term.stop()
			var live atomic.Int32
			var contexts []context.Context
			run := func(ctx context.Context) <-chan struct{} {
				if n := live.Add(1); n != 1 {
					t.Fatalf("duplicate leader loops: %d live terms", n)
				}
				contexts = append(contexts, ctx)
				done := make(chan struct{})
				go func() {
					<-ctx.Done()
					live.Add(-1)
					close(done)
				}()
				return done
			}
			term.start(t.Context(), run)
			for range 5 {
				pending := make(chan election.Event, 1)
				if coalesced {
					pending <- election.Event{Elected: false}
					<-pending // Raft replaces the unread loss notification.
				}
				pending <- election.Event{Elected: true}
				event := <-pending
				term.stop() // Must happen before reload/epoch activation.
				if live.Load() != 0 || contexts[len(contexts)-1].Err() == nil {
					t.Fatal("prior term was not cancelled and joined before activation")
				}
				if event.Elected {
					term.start(t.Context(), run)
				}
			}
			term.stop()
			term.stop() // Repeated loss and shutdown are idempotent.
			for _, ctx := range contexts {
				if ctx.Err() == nil {
					t.Fatal("orphaned leadership context")
				}
			}
			if live.Load() != 0 {
				t.Fatal("loops survived final loss")
			}
		})
	}
}

func TestLeaderTermStartJoinsPriorTerm(t *testing.T) {
	var term leaderTerm
	cancelled, release := make(chan struct{}), make(chan struct{})
	term.start(t.Context(), func(ctx context.Context) <-chan struct{} {
		done := make(chan struct{})
		go func() {
			<-ctx.Done()
			close(cancelled)
			<-release
			close(done)
		}()
		return done
	})
	started := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		term.start(t.Context(), func(ctx context.Context) <-chan struct{} {
			close(started)
			done := make(chan struct{})
			go func() { <-ctx.Done(); close(done) }()
			return done
		})
		close(finished)
	}()
	<-cancelled
	select {
	case <-started:
		t.Fatal("started new loops before old loops exited")
	default:
	}
	close(release)
	<-finished
	term.stop()
}

type activationControl struct {
	fault    string
	calls    []string
	contexts []context.Context
	onRun    func()
	cancel   context.CancelFunc
}

func (c *activationControl) step(stage string) error {
	c.calls = append(c.calls, stage)
	if c.fault == stage {
		if c.cancel != nil {
			c.cancel()
		}
		return errors.New("activation fault")
	}
	return nil
}

func (c *activationControl) Reload(context.Context) error            { return c.step("reload") }
func (c *activationControl) AcquireLeadership(context.Context) error { return c.step("epoch") }
func (c *activationControl) Run(ctx context.Context) <-chan struct{} {
	c.calls = append(c.calls, "run")
	c.contexts = append(c.contexts, ctx)
	done := make(chan struct{})
	go func() { <-ctx.Done(); close(done) }()
	if c.onRun != nil {
		c.onRun()
	}
	return done
}

func TestLeadershipActivationFailure(t *testing.T) {
	for _, stage := range []string{"barrier", "reload", "epoch"} {
		t.Run(stage, func(t *testing.T) {
			control := &activationControl{fault: stage}
			proxy := &controlPlaneProxy{}
			proxy.SetLeaderActive(true)
			events := make(chan election.Event, 2)
			events <- election.Event{Elected: true}
			events <- election.Event{Elected: true} // Must exit, not retry.
			err := runLeadership(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), events, nil, proxy, func() error { return control.step("barrier") }, control)
			if err == nil || !strings.Contains(err.Error(), "activation fault") {
				t.Fatalf("activation returned %v", err)
			}
			want := map[string]string{"barrier": "barrier", "reload": "barrier,reload", "epoch": "barrier,reload,epoch"}[stage]
			if got := strings.Join(control.calls, ","); got != want || proxy.leaderLive.Load() {
				t.Fatalf("calls=%s, API active=%t", got, proxy.leaderLive.Load())
			}
		})
	}
}

func TestLeadershipReactivationFailureStopsPriorTerm(t *testing.T) {
	for _, stage := range []string{"barrier", "reload", "epoch"} {
		t.Run(stage, func(t *testing.T) {
			events := make(chan election.Event, 1)
			events <- election.Event{Elected: true}
			control := &activationControl{}
			control.onRun = func() {
				control.fault = stage
				events <- election.Event{Elected: true}
			}
			proxy := &controlPlaneProxy{}
			err := runLeadership(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), events, nil, proxy, func() error {
				if len(control.contexts) != 0 && control.contexts[0].Err() == nil {
					t.Fatal("activation started before cancelling the old term")
				}
				return control.step("barrier")
			}, control)
			if err == nil || len(control.contexts) != 1 || control.contexts[0].Err() == nil || proxy.leaderLive.Load() {
				t.Fatalf("err=%v terms=%d API active=%t", err, len(control.contexts), proxy.leaderLive.Load())
			}
		})
	}
}

// Exercise the leadership runner across a process boundary with the same
// Cobra error-to-exit mapping used by main; normal cancellation is success.
func TestLeadershipExitStatus(t *testing.T) {
	if stage := os.Getenv("TRELLIS_TEST_ACTIVATION"); stage != "" {
		ctx, cancel := context.WithCancel(context.Background())
		control := &activationControl{fault: stage}
		if after, ok := strings.CutPrefix(stage, "shutdown-"); ok {
			control.fault = after
			control.cancel = cancel
		}
		events := make(chan election.Event, 1)
		if stage == "shutdown" {
			cancel()
		} else {
			events <- election.Event{Elected: true}
		}
		root := &cobra.Command{Use: "trellis", RunE: func(*cobra.Command, []string) error {
			defer cancel()
			return runLeadership(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), events, nil, &controlPlaneProxy{}, func() error { return control.step("barrier") }, control)
		}}
		root.SetArgs(nil)
		if err := root.Execute(); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	for _, stage := range []string{"barrier", "reload", "epoch", "shutdown", "shutdown-barrier", "shutdown-reload", "shutdown-epoch"} {
		t.Run(stage, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestLeadershipExitStatus$")
			cmd.Env = append(os.Environ(), "TRELLIS_TEST_ACTIVATION="+stage)
			output, err := cmd.CombinedOutput()
			if strings.HasPrefix(stage, "shutdown") {
				if err != nil {
					t.Fatalf("shutdown: %v\n%s", err, output)
				}
				return
			}
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(string(output), "activation fault") {
				t.Fatalf("activation: %v\n%s", err, output)
			}
		})
	}
}
