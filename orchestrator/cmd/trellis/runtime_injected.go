//go:build integration

package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"net/netip"
	"path/filepath"

	"github.com/overfold/trellis/internal/network"
	containerruntime "github.com/overfold/trellis/internal/runtime"
	"github.com/spf13/pflag"
)

// The injected runtime reports workloads as running without executing them. It
// is compiled into the node binary only for the multi-process integration suite
// (go build -tags=integration); normal builds reject --runtime injected.
func init() {
	var faults string
	buildTestRuntime = &testRuntime{
		name: "injected",
		addFlags: func(f *pflag.FlagSet) {
			if runtimeFlag := f.Lookup("runtime"); runtimeFlag != nil {
				runtimeFlag.Usage += " or injected (integration builds only)"
			}
			f.StringVar(&faults, "runtime-faults", "", "Injected runtime fault-control file (integration builds only)")
		},
		open: func(dataDir string) (containerruntime.ContainerRuntime, io.Closer, error) {
			r, err := containerruntime.NewInjectedRuntime(filepath.Join(dataDir, "injected-runtime.json"), faults)
			if err != nil {
				return nil, nil, fmt.Errorf("init injected runtime: %w", err)
			}
			return r, r, nil
		},
		network: injectedNetwork{},
	}
}

// injectedNetwork stands in for namespace networking beside the injected
// runtime: it reports an address in the planned subnet without changing the
// host, since the injected runtime runs no processes that could use it.
type injectedNetwork struct{}

func (injectedNetwork) Attach(_ context.Context, request network.AttachRequest) (*network.Attachment, error) {
	address := ""
	if prefix, err := netip.ParsePrefix(request.Plan.CIDR); err == nil && prefix.Addr().Is4() && prefix.Bits() <= 29 {
		size := uint32(1) << (32 - prefix.Bits())
		sum := sha256.Sum256([]byte(request.AllocationID))
		base := binary.BigEndian.Uint32(prefix.Masked().Addr().AsSlice())
		host := base + 2 + binary.BigEndian.Uint32(sum[:4])%(size-3)
		var octets [4]byte
		binary.BigEndian.PutUint32(octets[:], host)
		address = netip.PrefixFrom(netip.AddrFrom4(octets), prefix.Bits()).String()
	}
	return &network.Attachment{
		AllocationID: request.AllocationID,
		Namespace:    request.Namespace,
		Network:      request.Network,
		Gateway:      request.Plan.Gateway,
		APIPort:      request.Plan.APIPort,
		Address:      address,
		Ports:        append([]network.PortMapping(nil), request.Ports...),
	}, nil
}

func (injectedNetwork) Detach(context.Context, *network.Attachment) error { return nil }

func (injectedNetwork) UpdatePlan(context.Context, string, network.Plan) error { return nil }
