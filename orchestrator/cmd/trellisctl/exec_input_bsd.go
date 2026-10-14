//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// execFileInput owns only a duplicate. Nonblocking mode is shared with the
// borrowed descriptor; restore its original flags after the reader is joined.
func execFileInput(input *os.File) (io.Reader, func(), func(), error) {
	raw, err := input.SyscallConn()
	if err != nil {
		return nil, nil, nil, err
	}
	fd, flags := -1, 0
	var setupErr error
	err = raw.Control(func(original uintptr) {
		flags, setupErr = unix.FcntlInt(original, unix.F_GETFL, 0)
		if setupErr != nil {
			return
		}
		fd, setupErr = unix.FcntlInt(original, unix.F_DUPFD_CLOEXEC, 0)
		if setupErr == nil {
			setupErr = unix.SetNonblock(fd, true)
		}
	})
	if err != nil || setupErr != nil {
		if fd >= 0 {
			_ = unix.Close(fd)
		}
		return nil, nil, nil, fmt.Errorf("prepare exec stdin: %w", errors.Join(err, setupErr))
	}
	reader := os.NewFile(uintptr(fd), "exec-stdin")
	reset := func() {
		_ = reader.Close()
		_ = raw.Control(func(original uintptr) { _, _ = unix.FcntlInt(original, unix.F_SETFL, flags) })
	}
	if err := reader.SetReadDeadline(time.Time{}); err != nil {
		reset()
		return nil, nil, nil, fmt.Errorf("exec stdin must support interruptible reads: %w", err)
	}
	return reader, func() { _ = reader.Close() }, reset, nil
}
