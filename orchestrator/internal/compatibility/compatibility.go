// Package compatibility owns the release sequence, independently of build tags
// and document revisions. Experimental releases are outside the stable sequence.
package compatibility

import (
	"fmt"
	"strconv"
)

// release is changed in source for each stable release, starting at 1 for v1.
// Integration tests override it at link time to exercise adjacent binaries.
// No pre-v1 tag is mapped into this sequence.
var release = "0"

// Current returns this binary's release sequence number. Zero is experimental.
func Current() uint64 {
	n, err := strconv.ParseUint(release, 10, 64)
	if err != nil {
		panic("invalid compiled compatibility release")
	}
	return n
}

// Supports reports whether this binary can implement an active cluster level.
// Stable binaries implement their own and their predecessor's semantics, except
// the v1 baseline, which deliberately does not recognize experimental releases.
func Supports(binary, active uint64) bool {
	return binary == active || binary > 1 && active == binary-1
}

// Check refuses unsupported release combinations. An active level admits only
// that release and its immediate successor, never three releases or a skip.
func Check(binary, active uint64) error {
	if !Supports(binary, active) {
		return fmt.Errorf("binary release %d cannot operate at cluster compatibility level %d", binary, active)
	}
	return nil
}
