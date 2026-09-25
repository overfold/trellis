package health

import (
	"context"
	"fmt"
	"net"
	"os"
	goruntime "runtime"

	"golang.org/x/sys/unix"
)

func dialInNamespace(ctx context.Context, path, network, address string) (conn net.Conn, err error) {
	current, err := os.Open("/proc/self/ns/net")
	if err != nil {
		return nil, fmt.Errorf("open agent network namespace: %w", err)
	}
	defer current.Close()
	target, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open task network namespace: %w", err)
	}
	defer target.Close()

	entered := false
	goruntime.LockOSThread()
	defer func() {
		if entered {
			if restoreErr := unix.Setns(int(current.Fd()), unix.CLONE_NEWNET); restoreErr != nil {
				if conn != nil {
					_ = conn.Close()
					conn = nil
				}
				err = fmt.Errorf("restore agent network namespace: %w", restoreErr)
				return // A goroutine exiting while locked discards this thread.
			}
		}
		goruntime.UnlockOSThread()
	}()
	if err := unix.Setns(int(target.Fd()), unix.CLONE_NEWNET); err != nil {
		return nil, fmt.Errorf("enter task network namespace: %w", err)
	}
	entered = true
	return (&net.Dialer{}).DialContext(ctx, network, address)
}
