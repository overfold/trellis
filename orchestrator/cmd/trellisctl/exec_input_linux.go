package main

import (
	"fmt"
	"io"
	"os"
	"syscall"
	"time"
)

// Reopening gives us an independent file description: unlike dup, making it
// nonblocking cannot affect the caller's stdin or a shared terminal stdout.
func execFileInput(input *os.File) (io.Reader, func(), func(), error) {
	raw, err := input.SyscallConn()
	if err != nil {
		return nil, nil, nil, err
	}
	var reader *os.File
	var openErr error
	err = raw.Control(func(fd uintptr) {
		reader, openErr = os.OpenFile(fmt.Sprintf("/proc/self/fd/%d", fd), os.O_RDONLY|syscall.O_NONBLOCK, 0)
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if openErr != nil {
		return nil, nil, nil, fmt.Errorf("prepare exec stdin: %w", openErr)
	}
	closeInput := func() { _ = reader.Close() }
	if err := reader.SetReadDeadline(time.Time{}); err != nil {
		closeInput()
		return nil, nil, nil, fmt.Errorf("exec stdin must support interruptible reads: %w", err)
	}
	return reader, closeInput, closeInput, nil
}
