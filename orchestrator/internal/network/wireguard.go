package network

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

// Peer describes a WireGuard peer configuration.
type Peer struct {
	PublicKey  string   `json:"public_key"`
	Endpoint   string   `json:"endpoint"`
	AllowedIPs []string `json:"allowed_ips"`
}

// Config describes a WireGuard network.
type Config struct {
	CIDR             string `json:"cidr"`
	Gateway          string `json:"gateway"`
	WireGuardAddress string `json:"wireguard_address"`
	PrivateKeyFile   string `json:"private_key_file"`
	ListenPort       int    `json:"listen_port"`
	Peers            []Peer `json:"peers"`
}

type commandRunner interface {
	Run(context.Context, string, ...string) error
}
type execRunner struct{}

func (execRunner) Run(ctx context.Context, name string, args ...string) error {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

// WorkloadDNSAddress is the reserved node-local resolver address injected into workloads.
const WorkloadDNSAddress = "198.18.0.53"

// WireGuardManager manages allocation networking with WireGuard.
type WireGuardManager struct {
	configDir  string
	stateDir   string
	run        commandRunner
	mu         sync.Mutex
	dnsAddress string
}

// ConfigureWorkloadDNS reserves an internal address on loopback for the Trellis
// workload resolver. Namespace firewall rules allow only DNS traffic to this
// address; it is not exposed on external interfaces.
func (m *WireGuardManager) ConfigureWorkloadDNS(ctx context.Context, address string) error {
	ip, err := netip.ParseAddr(address)
	if err != nil || !ip.Is4() {
		return fmt.Errorf("workload DNS address must be IPv4: %s", address)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.run.Run(ctx, "ip", "addr", "replace", address+"/32", "dev", "lo"); err != nil {
		return fmt.Errorf("configure workload DNS address: %w", err)
	}
	m.dnsAddress = address
	return nil
}

// NewAutomatedWireGuardManager creates a manager with an automatically generated identity.
func NewAutomatedWireGuardManager(stateDir string) (*WireGuardManager, error) {
	m := &WireGuardManager{stateDir: stateDir, run: execRunner{}}
	if _, err := m.Identity(); err != nil {
		return nil, err
	}
	return m, nil
}

// Identity returns the manager public key, generating its key pair if needed.
func (m *WireGuardManager) Identity() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := os.MkdirAll(m.stateDir, 0o700); err != nil {
		return "", err
	}
	privatePath, publicPath := filepath.Join(m.stateDir, "identity.key"), filepath.Join(m.stateDir, "identity.pub")
	if raw, err := os.ReadFile(publicPath); err == nil {
		return strings.TrimSpace(string(raw)), nil
	}
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return "", err
	}
	private, public := base64.StdEncoding.EncodeToString(key.Bytes()), base64.StdEncoding.EncodeToString(key.PublicKey().Bytes())
	if err := os.WriteFile(privatePath, []byte(private+"\n"), 0o600); err != nil {
		return "", err
	}
	if err := os.WriteFile(publicPath, []byte(public+"\n"), 0o644); err != nil {
		return "", err
	}
	return public, nil
}

// NewWireGuardManager creates a manager that loads named network configurations.
func NewWireGuardManager(configDir string) *WireGuardManager {
	return &WireGuardManager{configDir: configDir, stateDir: "/var/lib/trellis/network", run: execRunner{}}
}

var safeName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$`)
var safeAllocation = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,238}$`)

func (m *WireGuardManager) load(name string) (*Config, error) {
	if !safeName.MatchString(name) {
		return nil, fmt.Errorf("invalid network name %q", name)
	}
	raw, err := os.ReadFile(filepath.Join(m.configDir, name+".json"))
	if err != nil {
		return nil, fmt.Errorf("read network config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse network config: %w", err)
	}
	prefix, err := netip.ParsePrefix(cfg.CIDR)
	if err != nil || !prefix.Addr().Is4() {
		return nil, fmt.Errorf("network CIDR must be IPv4: %q", cfg.CIDR)
	}
	if _, err := netip.ParseAddr(cfg.Gateway); err != nil {
		return nil, fmt.Errorf("invalid gateway: %w", err)
	}
	if gateway, _ := netip.ParseAddr(cfg.Gateway); !prefix.Contains(gateway) {
		return nil, fmt.Errorf("gateway must be within CIDR")
	}
	if _, err := netip.ParsePrefix(cfg.WireGuardAddress); err != nil {
		return nil, fmt.Errorf("invalid wireguard_address: %w", err)
	}
	if cfg.ListenPort < 1 || cfg.ListenPort > 65535 || cfg.PrivateKeyFile == "" {
		return nil, fmt.Errorf("private_key_file and valid listen_port are required")
	}
	keyInfo, err := os.Stat(cfg.PrivateKeyFile)
	if err != nil {
		return nil, fmt.Errorf("stat private key: %w", err)
	}
	if keyInfo.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("private key file must not be accessible by group or others")
	}
	for _, peer := range cfg.Peers {
		if strings.TrimSpace(peer.PublicKey) == "" || len(peer.AllowedIPs) == 0 {
			return nil, fmt.Errorf("peer public_key and allowed_ips are required")
		}
		for _, allowed := range peer.AllowedIPs {
			if _, err := netip.ParsePrefix(allowed); err != nil {
				return nil, fmt.Errorf("invalid peer allowed IP %q: %w", allowed, err)
			}
		}
	}
	return &cfg, nil
}

func short(prefix, value string) string {
	h := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%s%x", prefix, h[:5])
}

func allocationAddress(cidr, allocation string) (string, error) {
	return allocationAddressAt(cidr, allocation, 0)
}

func allocationAddressAt(cidr, allocation string, probe uint32) (string, error) {
	p, err := netip.ParsePrefix(cidr)
	if err != nil || !p.Addr().Is4() {
		if err == nil {
			err = fmt.Errorf("CIDR must be IPv4")
		}
		return "", err
	}
	h := sha256.Sum256([]byte(allocation))
	host := binary.BigEndian.Uint32(h[:4])
	base := binary.BigEndian.Uint32(p.Addr().AsSlice())
	bits := uint32(32 - p.Bits())
	if bits < 3 {
		return "", fmt.Errorf("CIDR %s has no allocation space", cidr)
	}
	mask := uint32((uint64(1) << bits) - 1)
	host = (host+probe)%(mask-2) + 2
	a := netip.AddrFrom4([4]byte{byte(base >> 24), byte(base >> 16), byte(base >> 8), byte(base)}).Next()
	// Add host-1 without depending on platform integer address APIs.
	b := binary.BigEndian.Uint32(a.AsSlice()) + host - 1
	return fmt.Sprintf("%d.%d.%d.%d/%d", byte(b>>24), byte(b>>16), byte(b>>8), byte(b), p.Bits()), nil
}

func reserveAddress(leaseDir, cidr, allocation string) (address, lease string, err error) {
	prefix, err := netip.ParsePrefix(cidr)
	if err != nil {
		return "", "", err
	}
	if !prefix.Addr().Is4() || prefix.Bits() > 29 {
		return "", "", fmt.Errorf("CIDR %s has no IPv4 allocation space", cidr)
	}
	capacity := uint32((uint64(1) << uint(32-prefix.Bits())) - 3)
	for probe := uint32(0); probe < capacity; probe++ {
		address, err = allocationAddressAt(cidr, allocation, probe)
		if err != nil {
			return "", "", err
		}
		lease = filepath.Join(leaseDir, strings.ReplaceAll(address, "/", "_"))
		f, openErr := os.OpenFile(lease, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if openErr == nil {
			if _, err = f.WriteString(allocation); err == nil {
				err = f.Close()
			} else {
				_ = f.Close()
			}
			if err != nil {
				_ = os.Remove(lease)
				return "", "", fmt.Errorf("persist address lease: %w", err)
			}
			return address, lease, nil
		}
		if !os.IsExist(openErr) {
			return "", "", fmt.Errorf("reserve address %s: %w", address, openErr)
		}
		owner, readErr := os.ReadFile(lease)
		if readErr != nil {
			return "", "", fmt.Errorf("read address lease %s: %w", address, readErr)
		}
		if string(owner) == allocation {
			return address, lease, nil
		}
	}
	return "", "", fmt.Errorf("network %s has no free allocation addresses", cidr)
}

func (m *WireGuardManager) ensureLink(ctx context.Context, name string, args ...string) error {
	if err := m.run.Run(ctx, "ip", append([]string{"link", "add", name}, args...)...); err != nil {
		if showErr := m.run.Run(ctx, "ip", "link", "show", "dev", name); showErr != nil {
			return err
		}
	}
	return nil
}

func (m *WireGuardManager) reconcilePeers(ctx context.Context, wg, namespace, networkName string, peers []Peer) error {
	planDir := filepath.Join(m.stateDir, "plans")
	if err := os.MkdirAll(planDir, 0o700); err != nil {
		return fmt.Errorf("create network plan state: %w", err)
	}
	path := m.planPath(namespace, networkName)
	var previous []Peer
	if raw, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(raw, &previous); err != nil {
			return fmt.Errorf("parse applied network plan: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("read applied network plan: %w", err)
	}
	desiredPeers, desiredRoutes := map[string]bool{}, map[string]bool{}
	for _, peer := range peers {
		desiredPeers[peer.PublicKey] = true
		for _, route := range peer.AllowedIPs {
			desiredRoutes[route] = true
		}
	}
	for _, peer := range previous {
		if !desiredPeers[peer.PublicKey] {
			if err := m.run.Run(ctx, "wg", "set", wg, "peer", peer.PublicKey, "remove"); err != nil {
				return fmt.Errorf("remove stale WireGuard peer: %w", err)
			}
		}
		for _, route := range peer.AllowedIPs {
			if !desiredRoutes[route] {
				if err := m.run.Run(ctx, "ip", "route", "del", route, "dev", wg); err != nil {
					return fmt.Errorf("remove stale WireGuard route: %w", err)
				}
			}
		}
	}
	raw, err := json.Marshal(peers)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return fmt.Errorf("persist applied network plan: %w", err)
	}
	return nil
}

func (m *WireGuardManager) planPath(namespace, networkName string) string {
	return filepath.Join(m.stateDir, "plans", short("", namespace+"\x00"+networkName)+".json")
}

// UpdatePlan reconciles peers and routes without touching running allocations.
func (m *WireGuardManager) UpdatePlan(ctx context.Context, namespace string, plan Plan) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !safeName.MatchString(namespace) {
		return fmt.Errorf("namespace must be a safe identifier")
	}
	entries, err := os.ReadDir(filepath.Join(m.stateDir, namespace))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read network leases: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}
	wg := short("tw", namespace+"\x00"+namespace)
	peers := make([]Peer, len(plan.Peers))
	for i, peer := range plan.Peers {
		peers[i] = Peer(peer)
	}
	if err := m.reconcilePeers(ctx, wg, namespace, namespace, peers); err != nil {
		return err
	}
	for _, peer := range peers {
		args := []string{"set", wg, "peer", peer.PublicKey, "allowed-ips", strings.Join(peer.AllowedIPs, ",")}
		if peer.Endpoint != "" {
			args = append(args, "endpoint", peer.Endpoint)
		}
		if err := m.run.Run(ctx, "wg", args...); err != nil {
			return fmt.Errorf("configure WireGuard peer: %w", err)
		}
		for _, route := range peer.AllowedIPs {
			if err := m.run.Run(ctx, "ip", "route", "replace", route, "dev", wg); err != nil {
				return fmt.Errorf("configure WireGuard route: %w", err)
			}
		}
	}
	return nil
}

// Attach configures networking for an allocation.
func (m *WireGuardManager) Attach(ctx context.Context, request AttachRequest) (_ *Attachment, retErr error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	namespace, networkName, allocation := request.Namespace, request.Network, request.AllocationID
	if !safeName.MatchString(namespace) || !safeAllocation.MatchString(allocation) {
		return nil, fmt.Errorf("namespace and allocation must be safe identifiers")
	}
	var cfg *Config
	var err error
	if request.Plan.CIDR == "" {
		cfg, err = m.load(networkName)
	} else {
		cfg = &Config{CIDR: request.Plan.CIDR, Gateway: request.Plan.Gateway, WireGuardAddress: request.Plan.WireGuardAddress,
			PrivateKeyFile: filepath.Join(m.stateDir, "identity.key"), ListenPort: request.Plan.ListenPort}
		for _, peer := range request.Plan.Peers {
			cfg.Peers = append(cfg.Peers, Peer(peer))
		}
	}
	if err != nil {
		return nil, err
	}
	wg, bridge, hostVeth, peerVeth := short("tw", namespace+"\x00"+networkName), short("tb", namespace+"\x00"+networkName), short("vh", allocation), short("vc", allocation)
	ns := filepath.Join("/var/run/netns", allocation)
	leaseDir := filepath.Join(m.stateDir, networkName)
	if err := os.MkdirAll(leaseDir, 0o700); err != nil {
		return nil, fmt.Errorf("create IPAM state: %w", err)
	}
	address, lease, err := reserveAddress(leaseDir, cfg.CIDR, allocation)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			_ = os.Remove(lease)
		}
	}()
	// Every command is idempotently reconciled; "replace" is used for routes.
	if err = m.ensureLink(ctx, bridge, "type", "bridge"); err != nil {
		return nil, fmt.Errorf("create bridge: %w", err)
	}
	if err = m.run.Run(ctx, "ip", "addr", "replace", cfg.Gateway+"/"+strings.Split(cfg.CIDR, "/")[1], "dev", bridge); err != nil {
		return nil, fmt.Errorf("configure bridge address: %w", err)
	}
	if err = m.run.Run(ctx, "ip", "link", "set", bridge, "up"); err != nil {
		return nil, err
	}
	if err = m.ensureLink(ctx, wg, "type", "wireguard"); err != nil {
		return nil, fmt.Errorf("create WireGuard interface: %w", err)
	}
	if err = m.run.Run(ctx, "ip", "addr", "replace", cfg.WireGuardAddress, "dev", wg); err != nil {
		return nil, fmt.Errorf("configure WireGuard address: %w", err)
	}
	if err = m.run.Run(ctx, "wg", "set", wg, "private-key", cfg.PrivateKeyFile, "listen-port", fmt.Sprint(cfg.ListenPort)); err != nil {
		return nil, err
	}
	if request.Plan.CIDR != "" {
		if err = m.reconcilePeers(ctx, wg, namespace, networkName, cfg.Peers); err != nil {
			return nil, err
		}
	}
	for _, p := range cfg.Peers {
		args := []string{"set", wg, "peer", p.PublicKey, "allowed-ips", strings.Join(p.AllowedIPs, ",")}
		if p.Endpoint != "" {
			args = append(args, "endpoint", p.Endpoint)
		}
		if err = m.run.Run(ctx, "wg", args...); err != nil {
			return nil, err
		}
		for _, route := range p.AllowedIPs {
			if err = m.run.Run(ctx, "ip", "route", "replace", route, "dev", wg); err != nil {
				return nil, fmt.Errorf("configure WireGuard route: %w", err)
			}
		}
	}
	if err = m.run.Run(ctx, "ip", "link", "set", wg, "up"); err != nil {
		return nil, err
	}
	if err = m.run.Run(ctx, "sysctl", "-w", "net.ipv4.ip_forward=1"); err != nil {
		return nil, err
	}
	if m.run.Run(ctx, "iptables", "-C", "FORWARD", "-i", bridge, "!", "-o", wg, "-j", "DROP") != nil {
		if err = m.run.Run(ctx, "iptables", "-A", "FORWARD", "-i", bridge, "!", "-o", wg, "-j", "DROP"); err != nil {
			return nil, err
		}
	}
	if m.run.Run(ctx, "iptables", "-C", "FORWARD", "-o", bridge, "!", "-i", wg, "-j", "DROP") != nil {
		if err = m.run.Run(ctx, "iptables", "-A", "FORWARD", "-o", bridge, "!", "-i", wg, "-j", "DROP"); err != nil {
			return nil, err
		}
	}
	if m.dnsAddress != "" {
		for _, protocol := range []string{"udp", "tcp"} {
			args := []string{"INPUT", "-i", bridge, "-d", m.dnsAddress, "-p", protocol, "--dport", "53", "-j", "ACCEPT"}
			if m.run.Run(ctx, "iptables", append([]string{"-C"}, args...)...) != nil {
				if err = m.run.Run(ctx, "iptables", append([]string{"-I"}, args...)...); err != nil {
					return nil, err
				}
			}
		}
	}
	if request.Plan.APIPort > 0 {
		args := []string{"INPUT", "-i", bridge, "-d", cfg.Gateway, "-p", "tcp", "--dport", fmt.Sprint(request.Plan.APIPort), "-j", "ACCEPT"}
		if m.run.Run(ctx, "iptables", append([]string{"-C"}, args...)...) != nil {
			if err = m.run.Run(ctx, "iptables", append([]string{"-I"}, args...)...); err != nil {
				return nil, err
			}
		}
	}
	_ = m.run.Run(ctx, "iptables", "-D", "INPUT", "-i", bridge, "!", "-d", cfg.Gateway, "-j", "DROP")
	if m.run.Run(ctx, "iptables", "-C", "INPUT", "-i", bridge, "-j", "DROP") != nil {
		if err = m.run.Run(ctx, "iptables", "-A", "INPUT", "-i", bridge, "-j", "DROP"); err != nil {
			return nil, err
		}
	}
	if err = m.run.Run(ctx, "ip", "netns", "add", allocation); err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			_ = m.run.Run(ctx, "ip", "link", "del", hostVeth)
			_ = m.run.Run(ctx, "ip", "netns", "del", allocation)
		}
	}()
	if err = m.run.Run(ctx, "ip", "link", "add", hostVeth, "type", "veth", "peer", "name", peerVeth); err != nil {
		_ = m.run.Run(ctx, "ip", "netns", "del", allocation)
		return nil, err
	}
	if err = m.run.Run(ctx, "ip", "link", "set", hostVeth, "master", bridge); err != nil {
		return nil, err
	}
	if err = m.run.Run(ctx, "ip", "link", "set", hostVeth, "up"); err != nil {
		return nil, err
	}
	if err = m.run.Run(ctx, "ip", "link", "set", peerVeth, "netns", allocation); err != nil {
		return nil, err
	}
	if err = m.run.Run(ctx, "ip", "-n", allocation, "link", "set", "lo", "up"); err != nil {
		return nil, err
	}
	if err = m.run.Run(ctx, "ip", "-n", allocation, "link", "set", peerVeth, "name", "eth0"); err != nil {
		return nil, err
	}
	if err = m.run.Run(ctx, "ip", "-n", allocation, "addr", "add", address, "dev", "eth0"); err != nil {
		return nil, err
	}
	if err = m.run.Run(ctx, "ip", "-n", allocation, "link", "set", "eth0", "up"); err != nil {
		return nil, err
	}
	if err = m.run.Run(ctx, "ip", "-n", allocation, "route", "replace", "default", "via", cfg.Gateway); err != nil {
		return nil, err
	}
	return &Attachment{
		AllocationID:       allocation,
		Namespace:          namespace,
		Network:            networkName,
		NetworkNamespace:   ns,
		HostVeth:           hostVeth,
		Bridge:             bridge,
		WireGuardInterface: wg,
		Gateway:            cfg.Gateway,
		APIPort:            request.Plan.APIPort,
		Address:            address,
		LeasePath:          lease,
	}, nil
}

// Detach removes networking resources for an allocation.
func (m *WireGuardManager) Detach(ctx context.Context, a *Attachment) error {
	if a == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	_ = m.run.Run(ctx, "ip", "link", "del", a.HostVeth)
	_ = m.run.Run(ctx, "ip", "netns", "del", a.AllocationID)
	if a.LeasePath != "" {
		if err := os.Remove(a.LeasePath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	leaseDir := filepath.Join(m.stateDir, a.Network)
	entries, err := os.ReadDir(leaseDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read network leases: %w", err)
	}
	if len(entries) != 0 {
		return nil
	}

	bridge := a.Bridge
	if bridge == "" {
		bridge = short("tb", a.Namespace+"\x00"+a.Network)
	}
	wg := a.WireGuardInterface
	if wg == "" {
		wg = short("tw", a.Namespace+"\x00"+a.Network)
	}
	_ = m.run.Run(ctx, "iptables", "-D", "FORWARD", "-i", bridge, "!", "-o", wg, "-j", "DROP")
	_ = m.run.Run(ctx, "iptables", "-D", "FORWARD", "-o", bridge, "!", "-i", wg, "-j", "DROP")
	if m.dnsAddress != "" {
		for _, protocol := range []string{"udp", "tcp"} {
			_ = m.run.Run(ctx, "iptables", "-D", "INPUT", "-i", bridge, "-d", m.dnsAddress, "-p", protocol, "--dport", "53", "-j", "ACCEPT")
		}
	}
	if a.APIPort > 0 {
		_ = m.run.Run(ctx, "iptables", "-D", "INPUT", "-i", bridge, "-d", a.Gateway, "-p", "tcp", "--dport", fmt.Sprint(a.APIPort), "-j", "ACCEPT")
	}
	_ = m.run.Run(ctx, "iptables", "-D", "INPUT", "-i", bridge, "-j", "DROP")
	_ = m.run.Run(ctx, "ip", "link", "del", wg)
	_ = m.run.Run(ctx, "ip", "link", "del", bridge)
	if err := os.Remove(m.planPath(a.Namespace, a.Network)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove applied network plan: %w", err)
	}
	if err := os.Remove(leaseDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove empty network lease directory: %w", err)
	}
	return nil
}
