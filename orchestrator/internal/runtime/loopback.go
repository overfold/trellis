package runtime

import (
	"fmt"
	"os"
	goruntime "runtime"
	"unsafe"

	"github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
)

func hasPrivateNetworkNamespace(spec *specs.Spec) bool {
	if spec == nil || spec.Linux == nil {
		return false
	}
	for _, ns := range spec.Linux.Namespaces {
		if ns.Type == specs.NetworkNamespace {
			return ns.Path == ""
		}
	}
	return false
}

// bringUpLoopback configures the runc-created namespace before the task starts.
func bringUpLoopback(path string) (err error) {
	current, err := os.Open("/proc/self/ns/net")
	if err != nil {
		return fmt.Errorf("open runtime network namespace: %w", err)
	}
	defer current.Close()
	target, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open task network namespace: %w", err)
	}
	defer target.Close()

	entered := false
	goruntime.LockOSThread()
	defer func() {
		if entered {
			if restoreErr := unix.Setns(int(current.Fd()), unix.CLONE_NEWNET); restoreErr != nil {
				err = fmt.Errorf("restore runtime network namespace: %w", restoreErr)
				return // The locked thread is discarded when this goroutine exits.
			}
		}
		goruntime.UnlockOSThread()
	}()
	if err := unix.Setns(int(target.Fd()), unix.CLONE_NEWNET); err != nil {
		return fmt.Errorf("enter task network namespace: %w", err)
	}
	entered = true
	if err := setLoopbackUp(); err != nil {
		return fmt.Errorf("set lo up: %w", err)
	}
	return nil
}

func setLoopbackUp() error {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)

	// Linux ifreq starts with IFNAMSIZ bytes for the name and a uint16 flag field.
	// Leave room for the full union so the kernel can copy its native ifreq size.
	var request struct {
		Name  [unix.IFNAMSIZ]byte
		Flags uint16
		_     [22]byte
	}
	copy(request.Name[:], "lo")
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), uintptr(unix.SIOCGIFFLAGS), uintptr(unsafe.Pointer(&request))); errno != 0 {
		return errno
	}
	request.Flags |= unix.IFF_UP
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), uintptr(unix.SIOCSIFFLAGS), uintptr(unsafe.Pointer(&request))); errno != 0 {
		return errno
	}
	return nil
}
