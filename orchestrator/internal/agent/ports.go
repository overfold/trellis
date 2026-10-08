package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"syscall"

	"github.com/overfold/trellis/orchestrator/internal/runtime"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// PortManager reserves host ports for allocations.
type PortManager struct {
	runtime runtime.ContainerRuntime

	claims map[int]*runtime.Port

	min    int
	max    int
	cursor int
	mu     sync.Mutex
}

// NewPortManager creates a port manager for an inclusive range.
func NewPortManager(containerRuntime runtime.ContainerRuntime, minPort int, maxPort int) *PortManager {
	if minPort == 0 {
		minPort = 20000
	}
	if maxPort == 0 {
		maxPort = 40000
	}

	return &PortManager{
		runtime: containerRuntime,

		claims: make(map[int]*runtime.Port),

		min:    minPort,
		max:    maxPort,
		cursor: minPort,
	}
}

// Check reports whether a host port is available.
func (p *PortManager) Check(hostPort int) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.check(hostPort)
}

func (p *PortManager) check(hostPort int) (bool, error) {
	_, ok := p.claims[hostPort]
	if ok {
		return true, nil
	}

	addr := fmt.Sprintf(":%d", hostPort)
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
	if err != nil {
		var errno syscall.Errno
		if errors.As(err, &errno) && errno == syscall.EADDRINUSE {
			return true, nil
		}

		return true, err
	}
	_ = listener.Close()

	// Published ports forward UDP too, so a UDP listener also holds the port.
	conn, err := (&net.ListenConfig{}).ListenPacket(context.Background(), "udp", addr)
	if err != nil {
		var errno syscall.Errno
		if errors.As(err, &errno) && errno == syscall.EADDRINUSE {
			return true, nil
		}

		return true, err
	}
	_ = conn.Close()

	return false, nil
}

// Claim reserves the node port of portSpec: its host_port when published
// from a task network namespace, otherwise the port bound on the node.
func (p *PortManager) Claim(portSpec spec.PortSpec) (*runtime.Port, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	hostPort := portSpec.NodePort()
	if hostPort == 0 {

		for {

			if p.cursor > p.max {
				return nil, fmt.Errorf("no free ports")
			}

			taken, err := p.check(p.cursor)

			if err != nil {
				return nil, err
			}

			if taken {
				p.cursor++
				continue
			}

			hostPort = p.cursor
			break

		}

	} else {

		taken, err := p.check(hostPort)
		if err != nil {
			return nil, err
		} else if taken {
			return nil, fmt.Errorf("port %d taken", hostPort)
		}

	}

	containerPort := portSpec.Port
	if containerPort == 0 {
		containerPort = hostPort
	}
	port := &runtime.Port{
		HostPort:      hostPort,
		ContainerPort: containerPort,
	}

	p.claims[hostPort] = port

	return port, nil
}

// Release releases a reserved host port.
func (p *PortManager) Release(port *runtime.Port) error {
	if port == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	hostPort := port.HostPort

	_, ok := p.claims[hostPort]
	if !ok {
		return nil
	}

	delete(p.claims, hostPort)

	return nil
}

// Adopt records an existing host-port reservation.
func (p *PortManager) Adopt(port *runtime.Port) error {
	if port == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if existing := p.claims[port.HostPort]; existing != nil && existing.ContainerPort != port.ContainerPort {
		return fmt.Errorf("port %d already belongs to another allocation", port.HostPort)
	}
	portCopy := *port
	p.claims[port.HostPort] = &portCopy
	return nil
}
