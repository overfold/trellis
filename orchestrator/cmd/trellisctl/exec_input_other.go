//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd

package main

import (
	"fmt"
	"io"
	"os"
	"time"
)

func execFileInput(input *os.File) (io.Reader, func(), func(), error) {
	if err := input.SetReadDeadline(time.Time{}); err != nil {
		return nil, nil, nil, fmt.Errorf("exec stdin must support interruptible reads: %w", err)
	}
	return input, func() { _ = input.SetReadDeadline(time.Now()) }, func() { _ = input.SetReadDeadline(time.Time{}) }, nil
}
