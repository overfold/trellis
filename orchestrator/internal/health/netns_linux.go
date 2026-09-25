package health

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"syscall"

	"golang.org/x/sys/unix"
)

// taskNamespaceDialer creates each socket in the task's network namespace.
// The socket keeps that namespace after the creating thread returns to the host.
func taskNamespaceDialer(pid uint32) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, _, address string) (net.Conn, error) {
		_, portText, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		port, err := strconv.Atoi(portText)
		if err != nil {
			return nil, err
		}
		fd, err := socketInTaskNamespace(pid)
		if err != nil {
			return nil, err
		}
		file := os.NewFile(uintptr(fd), "health-check")
		defer file.Close()

		err = unix.Connect(fd, &unix.SockaddrInet4{Port: port, Addr: [4]byte{127, 0, 0, 1}})
		if err != nil && err != unix.EINPROGRESS {
			return nil, err
		}
		if err == unix.EINPROGRESS {
			for {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				ready, err := unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLOUT}}, 50)
				if err == unix.EINTR {
					continue
				}
				if err != nil {
					return nil, err
				}
				if ready == 0 {
					continue
				}
				code, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_ERROR)
				if err != nil {
					return nil, err
				}
				if code != 0 {
					return nil, syscall.Errno(code)
				}
				break
			}
		}
		conn, err := net.FileConn(file)
		if err != nil {
			return nil, err
		}
		if deadline, ok := ctx.Deadline(); ok {
			_ = conn.SetDeadline(deadline)
		}
		return conn, nil
	}
}

func socketInTaskNamespace(pid uint32) (int, error) {
	type result struct {
		fd  int
		err error
	}
	done := make(chan result, 1)
	go func() {
		runtime.LockOSThread()
		host, err := os.Open("/proc/self/ns/net")
		if err != nil {
			runtime.UnlockOSThread()
			done <- result{-1, err}
			return
		}
		defer host.Close()
		task, err := os.Open(fmt.Sprintf("/proc/%d/ns/net", pid))
		if err != nil {
			runtime.UnlockOSThread()
			done <- result{-1, err}
			return
		}
		defer task.Close()
		if err = unix.Setns(int(task.Fd()), unix.CLONE_NEWNET); err != nil {
			runtime.UnlockOSThread()
			done <- result{-1, err}
			return
		}
		fd, socketErr := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
		restoreErr := unix.Setns(int(host.Fd()), unix.CLONE_NEWNET)
		if restoreErr == nil {
			runtime.UnlockOSThread()
		}
		if socketErr != nil || restoreErr != nil {
			if fd >= 0 {
				_ = unix.Close(fd)
			}
			if restoreErr != nil {
				done <- result{-1, fmt.Errorf("restore host network namespace: %w", restoreErr)}
				return // A locked goroutine's thread is discarded on exit.
			}
			done <- result{-1, socketErr}
			return
		}
		done <- result{fd: fd}
	}()
	result := <-done
	return result.fd, result.err
}
