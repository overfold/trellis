package server

import (
	"context"
	"errors"
)

// ErrStateUnavailable indicates that a request was valid but the control
// plane could not durably read or commit cluster state, for example because
// leadership changed or a Raft apply timed out. Callers may retry.
var ErrStateUnavailable = errors.New("cluster state unavailable")

// stateError marks err as a state-store failure without changing its message.
type stateError struct{ err error }

func (e *stateError) Error() string { return e.err.Error() }
func (e *stateError) Unwrap() error { return e.err }

// Is reports whether target is ErrStateUnavailable.
func (e *stateError) Is(target error) bool { return target == ErrStateUnavailable }

// stateUnavailable wraps a failed state-store operation so handlers can
// report it as a transient server fault rather than a client error.
func stateUnavailable(err error) error {
	if err == nil {
		return nil
	}
	return &stateError{err: err}
}

// isUnavailable reports whether err is a transient control-plane failure:
// a state-store failure or an ended request context.
func isUnavailable(err error) bool {
	return errors.Is(err, ErrStateUnavailable) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
