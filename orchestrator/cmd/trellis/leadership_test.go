package main

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/overfold/trellis/orchestrator/internal/election"
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
