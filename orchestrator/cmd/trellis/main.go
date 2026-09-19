// Command trellis runs a Trellis orchestrator and allocation agent.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	goruntime "runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/clofour/trellis/internal/agent"
	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/auth"
	"github.com/clofour/trellis/internal/client"
	trellisdns "github.com/clofour/trellis/internal/dns"
	"github.com/clofour/trellis/internal/election"
	"github.com/clofour/trellis/internal/health"
	"github.com/clofour/trellis/internal/localconfig"
	"github.com/clofour/trellis/internal/network"
	containerruntime "github.com/clofour/trellis/internal/runtime"
	secretstore "github.com/clofour/trellis/internal/secrets"
	"github.com/clofour/trellis/internal/server"
	"github.com/clofour/trellis/internal/spec"
	"github.com/clofour/trellis/internal/state"
	"github.com/clofour/trellis/internal/storage"
	"github.com/clofour/trellis/internal/tlsutil"
	"github.com/clofour/trellis/internal/version"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
)

const shutdownTime = 10 * time.Second

type config struct {
	ConfigFile                                                 string
	AgentListen, AgentAdvertise, ServerListen, ServerAdvertise string
	RaftListen, RaftAdvertise, Join                            string
	DataDir, Cluster, ClusterToken, ContainerdSock             string
	Runtime, RuntimeFaults                                     string
	WireGuardPool, WireGuardEndpoint                           string
	WireGuardPort, WireGuardPortCount                          int
	DNSListen                                                  string
	CACert, CAKey, Cert, Key                                   string
	SecretsKey, SecretsKeyID                                   string
	Labels                                                     []string
}

func main() {
	cfg := &config{}
	root := &cobra.Command{
		Use:     "trellis",
		Short:   "Trellis node",
		Version: version.Current(),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if cfg.ConfigFile != "" {
				if err := loadNodeConfig(cfg.ConfigFile, cfg, cmd.Flags()); err != nil {
					return err
				}
			}
			return run(cmd.Context(), cfg)
		},
	}
	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the trellis version",
		Args:  cobra.NoArgs,
		Run:   func(_ *cobra.Command, _ []string) { fmt.Println(version.Current()) },
	})
	f := root.Flags()
	f.StringVar(&cfg.ConfigFile, "config", "", "Path to Trellis node configuration YAML")
	f.StringVar(&cfg.AgentListen, "agent-listen", ":8127", "Agent API listen address")
	f.StringVar(&cfg.AgentAdvertise, "agent-advertise", "", "Agent address advertised to the cluster")
	f.StringVar(&cfg.ServerListen, "server-listen", ":8128", "Control-plane API listen address")
	f.StringVar(&cfg.ServerAdvertise, "server-advertise", "", "Control-plane API address advertised to the cluster")
	f.StringVar(&cfg.RaftListen, "raft-listen", ":8129", "Raft consensus transport listen address")
	f.StringVar(&cfg.RaftAdvertise, "raft-advertise", "", "Raft consensus transport advertised address")
	f.StringVar(&cfg.Join, "join", "", "Address of an existing cluster member to join (server API address)")
	f.StringVar(&cfg.DataDir, "data-dir", "/var/lib/trellis/data", "Directory for local state and volumes")
	f.StringVar(&cfg.Cluster, "cluster", "default", "Cluster name")
	f.StringVar(&cfg.ClusterToken, "bootstrap-token", "", "Bootstrap credential shared by Trellis nodes")
	f.StringVar(&cfg.ContainerdSock, "containerd-sock", "/run/containerd/containerd.sock", "Containerd socket path")
	f.StringVar(&cfg.Runtime, "runtime", "containerd", "Workload runtime: containerd or injected (test only)")
	f.StringVar(&cfg.RuntimeFaults, "runtime-faults", "", "Injected runtime fault-control file")
	f.StringVar(&cfg.WireGuardPool, "wireguard-pool", "10.64.0.0/10", "Cluster address pool used for automatic namespace networking")
	f.StringVar(&cfg.WireGuardEndpoint, "wireguard-endpoint", "", "Externally reachable WireGuard host or base host:port")
	f.IntVar(&cfg.WireGuardPort, "wireguard-port", 51820, "First UDP port in the per-namespace WireGuard range")
	f.IntVar(&cfg.WireGuardPortCount, "wireguard-port-count", 256, "Number of consecutive UDP ports available for namespace WireGuard networks")
	f.StringVar(&cfg.DNSListen, "dns-listen", net.JoinHostPort(network.WorkloadDNSAddress, "53"), "Workload DNS resolver listen address")
	f.StringVar(&cfg.CACert, "ca-cert", "", "Path to cluster CA certificate (PEM)")
	f.StringVar(&cfg.CAKey, "ca-key", "", "Path to cluster CA private key (PEM)")
	f.StringVar(&cfg.Cert, "cert", "", "Path to node certificate (PEM)")
	f.StringVar(&cfg.Key, "key", "", "Path to node private key (PEM)")
	f.StringVar(&cfg.SecretsKey, "secrets-key", "", "Path to a root-readable 32-byte or base64-encoded secrets encryption key")
	f.StringVar(&cfg.SecretsKeyID, "secrets-key-id", "", "Identifier for the active secrets encryption key")
	f.StringArrayVar(&cfg.Labels, "label", nil, "Node label in key=value form (repeatable)")
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(parent context.Context, cfg *config) error {
	ctx, stop := signal.NotifyContext(parent, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if cfg.ClusterToken == "" && cfg.Join == "" {
		return fmt.Errorf("bootstrap_token or --bootstrap-token is required")
	}
	if cfg.WireGuardPort < 1 || cfg.WireGuardPort > 65535 {
		return fmt.Errorf("--wireguard-port must be between 1 and 65535")
	}
	if cfg.WireGuardPortCount < 1 || cfg.WireGuardPort+cfg.WireGuardPortCount-1 > 65535 {
		return fmt.Errorf("--wireguard-port-count must be positive and fit between --wireguard-port and 65535")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	id, err := acquireNodeID(cfg.DataDir)
	if err != nil {
		return err
	}
	if cfg.AgentAdvertise == "" || cfg.ServerAdvertise == "" || cfg.RaftAdvertise == "" {
		advertiseHost, err := detectAdvertiseHost()
		if err != nil {
			return fmt.Errorf("determine advertise address: %w; configure agent_advertise, server_advertise, and raft_advertise explicitly", err)
		}
		if cfg.AgentAdvertise == "" {
			cfg.AgentAdvertise = net.JoinHostPort(advertiseHost, "8127")
		}
		if cfg.ServerAdvertise == "" {
			cfg.ServerAdvertise = net.JoinHostPort(advertiseHost, "8128")
		}
		if cfg.RaftAdvertise == "" {
			cfg.RaftAdvertise = net.JoinHostPort(advertiseHost, "8129")
		}
	}
	agentHost, agentPort, err := splitAddress(cfg.AgentAdvertise)
	if err != nil {
		return fmt.Errorf("agent advertise address: %w", err)
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	local := storage.NewLocalStorage(cfg.DataDir)
	if err := local.Init(); err != nil {
		return fmt.Errorf("init local storage: %w", err)
	}
	// A node that already has its mTLS identity has completed enrollment. Never
	// retain or reuse an enrollment secret supplied by a stale config, flag, or
	// environment-derived invocation on subsequent restarts.
	if cfg.Join != "" {
		if _, err := loadTLSFromStorage(local); err == nil {
			cfg.ClusterToken = ""
			if err := discardEnrollmentCredential(cfg.ConfigFile); err != nil {
				log := slog.Default()
				log.Warn("could not remove stale enrollment credential", "error", err)
			}
		}
	}

	tlsMaterials, err := loadOrBootstrapTLS(ctx, log, cfg, local, id)
	if err != nil {
		return fmt.Errorf("TLS bootstrap: %w", err)
	}

	_, serverPort, err := splitAddress(cfg.ServerListen)
	if err != nil {
		return fmt.Errorf("server listen address: %w", err)
	}
	runFile := localconfig.DefaultPath
	if writeErr := localconfig.Write(runFile, &localconfig.Config{
		ServerAddr: net.JoinHostPort("localhost", strconv.Itoa(serverPort)),
		CACert:     string(tlsMaterials.CACert),
	}); writeErr != nil {
		log.Warn("could not write local connection file", "path", runFile, "error", writeErr)
	} else {
		defer func() { _ = os.Remove(runFile) }()
	}

	peerTLS, err := tlsutil.PeerTLSConfig(tlsMaterials)
	if err != nil {
		return fmt.Errorf("peer TLS config: %w", err)
	}
	agentServerTLS, err := tlsutil.ServerTLSConfig(tlsMaterials)
	if err != nil {
		return fmt.Errorf("agent server TLS config: %w", err)
	}
	leaderServerTLS, err := tlsutil.LeaderTLSConfig(tlsMaterials)
	if err != nil {
		return fmt.Errorf("leader server TLS config: %w", err)
	}
	clientTLS, err := tlsutil.ClientTLSConfig(tlsMaterials)
	if err != nil {
		return fmt.Errorf("client TLS config: %w", err)
	}

	raftStore, err := state.NewRaftStore(state.RaftConfig{
		DataDir:   cfg.DataDir,
		BindAddr:  cfg.RaftListen,
		Advertise: cfg.RaftAdvertise,
		ServerID:  raftServerID(id, cfg.ServerAdvertise),
		Bootstrap: cfg.Join == "",
		TLS:       peerTLS,
	})
	if err != nil {
		return fmt.Errorf("init raft store: %w", err)
	}
	defer func() { _ = raftStore.Close() }()

	if cfg.Join != "" && !raftStore.HadExistingState() {
		log.Info("joining cluster", "address", cfg.Join)
		if err := discardEnrollmentCredential(cfg.ConfigFile); err != nil {
			log.Warn("could not remove consumed enrollment credential", "error", err)
		}
		cfg.ClusterToken = ""
	}
	// Raft construction and joining are asynchronous. Reading the local FSM
	// before it has applied the leader's committed log can make an existing
	// cluster look uninitialized, causing a follower to attempt a write that can
	// never succeed. Do not initialize the control plane (or advertise readiness)
	// until this member has heard from a leader and applied its local log.
	if err := waitForRaftSync(ctx, raftStore); err != nil {
		return fmt.Errorf("wait for raft synchronization: %w", err)
	}

	stateCtl := server.NewStateController(raftStore, cfg.Cluster)
	control := server.NewServer(log, local, stateCtl, raftStore, cfg.Cluster, cfg.ServerAdvertise)
	if cfg.SecretsKey != "" {
		key, keyID, err := loadSecretsKey(cfg.SecretsKey, cfg.SecretsKeyID)
		if err != nil {
			return err
		}
		store, err := secretstore.NewStore(raftStore, cfg.Cluster, keyID, key)
		clear(key)
		if err != nil {
			return fmt.Errorf("configure secrets: %w", err)
		}
		control.SetSecretStore(store)
		log.Info("secrets enabled", "key_id", keyID)
	}
	control.SetClusterJoiner(raftStore)
	control.SetClientTLS(clientTLS)
	if err := control.SetNetworkPool(cfg.WireGuardPool); err != nil {
		return err
	}
	if err := control.SetWireGuardPortCount(cfg.WireGuardPortCount); err != nil {
		return err
	}

	server.RegisterMetrics(control, prometheus.DefaultRegisterer)

	for i := 0; ; i++ {
		if _, err := control.InitWithToken(ctx, cfg.ClusterToken); err == nil {
			break
		} else if i >= 30 {
			return fmt.Errorf("initialize control plane: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}

	var runtimeClient containerruntime.ContainerRuntime
	var runtimeCloser io.Closer
	switch cfg.Runtime {
	case "containerd":
		r, err := containerruntime.NewContainerdRuntime(cfg.ContainerdSock)
		if err != nil {
			return fmt.Errorf("init runtime: %w", err)
		}
		runtimeClient, runtimeCloser = r, r
	case "injected":
		r, err := containerruntime.NewInjectedRuntime(filepath.Join(cfg.DataDir, "injected-runtime.json"), cfg.RuntimeFaults)
		if err != nil {
			return fmt.Errorf("init injected runtime: %w", err)
		}
		runtimeClient, runtimeCloser = r, r
	default:
		return fmt.Errorf("unsupported runtime %q", cfg.Runtime)
	}
	defer func() {
		if err := runtimeCloser.Close(); err != nil {
			log.Error("close runtime", "error", err)
		}
	}()
	healthMgr := health.NewHealthManager(log, runtimeClient, nil)
	restartCtl := agent.NewAllocationReconciler(runtimeClient, nil)
	leaderClient := client.NewServerClient("", "", clientTLS)
	volumeManager := agent.NewVolumeManager(cfg.DataDir)
	ag := agent.NewAgent(log, runtimeClient, healthMgr, restartCtl, agent.NewPortManager(runtimeClient, 0, 0, 0), volumeManager, leaderClient, id)
	ag.SetVersion(version.Current())
	ag.ConfigureDurability(local, cfg.Cluster)
	networkManager, err := network.NewAutomatedWireGuardManager(filepath.Join(cfg.DataDir, "network"))
	if err != nil {
		return fmt.Errorf("initialize WireGuard identity: %w", err)
	}
	dnsHost, dnsPort, err := splitAddress(cfg.DNSListen)
	if err != nil {
		return fmt.Errorf("dns listen address: %w", err)
	}
	if dnsHost == "" || dnsHost == "0.0.0.0" {
		dnsHost = network.WorkloadDNSAddress
		cfg.DNSListen = net.JoinHostPort(dnsHost, strconv.Itoa(dnsPort))
	}
	if cfg.Runtime == "containerd" {
		if dnsPort != 53 {
			return fmt.Errorf("workload DNS must listen on port 53; resolv.conf nameserver entries cannot include a custom port")
		}
		if err := networkManager.ConfigureWorkloadDNS(ctx, dnsHost); err != nil {
			return err
		}
	}
	ag.SetDNSServers([]string{dnsHost})
	ag.SetNetworkManager(networkManager)
	endpoint := cfg.WireGuardEndpoint
	if endpoint == "" {
		endpoint = net.JoinHostPort(agentHost, strconv.Itoa(cfg.WireGuardPort))
	} else if _, _, err := net.SplitHostPort(endpoint); err != nil {
		endpoint = net.JoinHostPort(endpoint, strconv.Itoa(cfg.WireGuardPort))
	}
	_, externalBaseRaw, err := net.SplitHostPort(endpoint)
	if err != nil {
		return fmt.Errorf("WireGuard endpoint: %w", err)
	}
	externalBase, err := strconv.Atoi(externalBaseRaw)
	if err != nil || externalBase < 1 || externalBase+cfg.WireGuardPortCount-1 > 65535 {
		return fmt.Errorf("WireGuard endpoint base port and namespace port count must fit within 1-65535")
	}
	publicKey, err := networkManager.Identity()
	if err != nil {
		return err
	}
	ag.SetWireGuardIdentity(publicKey, endpoint, cfg.WireGuardPort, cfg.WireGuardPortCount)
	ag.SetAdvertiseAddress(agentHost, agentPort)
	var sysinfo syscall.Sysinfo_t
	memory := int64(0)
	if syscall.Sysinfo(&sysinfo) == nil {
		memory = int64(sysinfo.Totalram) * int64(sysinfo.Unit)
	}
	ag.SetResources(goruntime.NumCPU()*1000, memory, goruntime.GOOS, goruntime.GOARCH)
	ag.SetCapabilities(detectNodeCapabilities(cfg.Runtime))
	if len(cfg.Labels) > 0 {
		labels, err := parseLabels(cfg.Labels)
		if err != nil {
			return err
		}
		ag.SetLabels(labels)
	}
	ag.Init(ctx)

	upstreams, upstreamErr := trellisdns.SystemResolvers("/etc/resolv.conf")
	if upstreamErr != nil {
		log.Warn("load DNS upstreams", "error", upstreamErr)
	}
	filteredUpstreams := upstreams[:0]
	for _, upstream := range upstreams {
		host, _, splitErr := net.SplitHostPort(upstream)
		if splitErr == nil && host == dnsHost {
			continue
		}
		filteredUpstreams = append(filteredUpstreams, upstream)
	}
	dnsResolver := trellisdns.NewResolver(log, leaderClient, trellisdns.DefaultDomain, filteredUpstreams...)
	go func() {
		if err := dnsResolver.Run(ctx, cfg.DNSListen); err != nil && ctx.Err() == nil {
			log.Error("dns resolver stopped", "error", err)
		}
	}()

	agentHTTP := echo.New()
	agentHTTP.Use(middleware.Recover(), leaderAgentAuthMiddleware(raftStore.Raft()))
	agent.NewHandler(ag).Register(agentHTTP)
	go func() {
		if err := (echo.StartConfig{Address: cfg.AgentListen, TLSConfig: agentServerTLS, GracefulTimeout: shutdownTime}).Start(ctx, agentHTTP); err != nil && ctx.Err() == nil {
			log.Error("agent API stopped", "error", err)
			stop()
		}
	}()

	elector := election.NewRaftElector(raftStore.Raft(), election.Leader{NodeID: id, Address: cfg.ServerAdvertise})
	leaderHTTP := echo.New()
	leaderHTTP.Use(middleware.Recover(), leaderAuthMiddleware(cfg.ClusterToken, control.TokenManager()))
	leaderHTTP.GET("/v1/auth/whoami", server.HandleWhoAmI)
	server.NewHandler(control).Register(leaderHTTP)
	apiProxy := newControlPlaneProxy(elector, cfg.ServerAdvertise, leaderHTTP, newHTTPTransport(clientTLS), log)
	go func() {
		if err := (echo.StartConfig{Address: cfg.ServerListen, TLSConfig: leaderServerTLS, GracefulTimeout: shutdownTime}).Start(ctx, apiProxy); err != nil && ctx.Err() == nil {
			log.Error("control-plane API stopped", "error", err)
			stop()
		}
	}()

	events := make(chan election.Event)
	go func() {
		if err := elector.Run(ctx, events); err != nil {
			log.Error("leader election stopped", "error", err)
			stop()
		}
	}()
	go watchLeader(ctx, log, elector, leaderClient)

	var leaderCancel context.CancelFunc
	for {
		select {
		case <-ctx.Done():
			if leaderCancel != nil {
				leaderCancel()
			}
			return nil
		case event, ok := <-events:
			if !ok {
				return fmt.Errorf("leader election event stream closed")
			}
			if event.Elected {
				// LeaderCh can fire before this node's FSM has applied every
				// committed entry inherited from the previous leader. Reloading at
				// that point resurrects stale jobs and allocations in memory. A
				// barrier makes the leadership snapshot include all prior commits.
				if err := raftStore.Raft().Barrier(10 * time.Second).Error(); err != nil {
					log.Error("raft leadership barrier failed", "error", err)
					stop()
					continue
				}
				if err := control.Reload(ctx); err != nil {
					log.Error("load leader state failed", "error", err)
					stop()
					continue
				}
				if err := control.AcquireLeadership(ctx); err != nil {
					log.Error("advance leadership epoch failed", "error", err)
					stop()
					continue
				}
				termCtx, cancel := context.WithCancel(ctx)
				leaderCancel = cancel
				log.Info("leadership acquired", "node_id", id, "address", cfg.ServerAdvertise)
				control.Run(termCtx)
				apiProxy.SetLeaderActive(true)
			} else if leaderCancel != nil {
				log.Warn("leadership lost", "node_id", id)
				apiProxy.SetLeaderActive(false)
				leaderCancel()
				leaderCancel = nil
			}
		}
	}
}

func detectAdvertiseHost() (string, error) {
	if conn, err := net.Dial("udp4", "1.1.1.1:53"); err == nil {
		defer func() { _ = conn.Close() }()
		if address, ok := conn.LocalAddr().(*net.UDPAddr); ok {
			if ip := address.IP.To4(); ip != nil && !ip.IsLoopback() && !ip.IsUnspecified() {
				return ip.String(), nil
			}
		}
	}
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, address := range addresses {
		var ip net.IP
		switch value := address.(type) {
		case *net.IPNet:
			ip = value.IP
		case *net.IPAddr:
			ip = value.IP
		}
		if ip = ip.To4(); ip != nil && !ip.IsLoopback() && !ip.IsUnspecified() {
			return ip.String(), nil
		}
	}
	return "", fmt.Errorf("no non-loopback IPv4 address found")
}

func detectNodeCapabilities(runtimeName string) []spec.NodeCapability {
	if runtimeName == "injected" {
		return []spec.NodeCapability{spec.CapabilityRunsc, spec.CapabilityNamespaceNetworking}
	}
	var capabilities []spec.NodeCapability
	if _, err := exec.LookPath("runsc"); err == nil {
		if config, err := os.ReadFile("/etc/containerd/config.toml"); err == nil && bytes.Contains(config, []byte("io.containerd.runsc.v1")) {
			capabilities = append(capabilities, spec.CapabilityRunsc)
		}
	}
	if goruntime.GOOS == "linux" && commandsAvailable("wg", "ip", "iptables") {
		capabilities = append(capabilities, spec.CapabilityNamespaceNetworking)
	}
	return capabilities
}

func commandsAvailable(commands ...string) bool {
	for _, command := range commands {
		if _, err := exec.LookPath(command); err != nil {
			return false
		}
	}
	return true
}

func waitForRaftSync(ctx context.Context, store *state.RaftStore) error {
	const timeout = 30 * time.Second
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		r := store.Raft()
		_, leaderID := r.LeaderWithID()
		if leaderID != "" && r.AppliedIndex() >= r.LastIndex() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timed out after %s (leader=%q applied=%d last=%d)", timeout, leaderID, r.AppliedIndex(), r.LastIndex())
		case <-ticker.C:
		}
	}
}

func loadSecretsKey(path, configuredID string) ([]byte, string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, "", fmt.Errorf("stat secrets key: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, "", fmt.Errorf("secrets key must not be accessible by group or others")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("read secrets key: %w", err)
	}
	key := raw
	if decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw))); err == nil && len(decoded) == 32 {
		key = decoded
	}
	if len(key) != 32 {
		return nil, "", fmt.Errorf("secrets key must contain exactly 32 raw bytes or their base64 encoding")
	}
	key = append([]byte(nil), key...)
	keyID := configuredID
	if keyID == "" {
		sum := sha256.Sum256(key)
		keyID = hex.EncodeToString(sum[:8])
	}
	return key, keyID, nil
}

func loadOrBootstrapTLS(ctx context.Context, log *slog.Logger, cfg *config, local *storage.LocalStorage, nodeID uuid.UUID) (*tlsutil.Materials, error) {
	if cfg.CACert != "" {
		return loadTLSFromFiles(cfg)
	}
	m, err := loadTLSFromStorage(local)
	if err == nil {
		return m, nil
	}
	if cfg.Join != "" {
		resp, err := joinClusterTLS(ctx, log, cfg.Join, cfg.ClusterToken, raftServerID(nodeID, cfg.ServerAdvertise), nodeID.String(), cfg.RaftAdvertise)
		if err != nil {
			return nil, fmt.Errorf("join cluster for TLS: %w", err)
		}
		caCert := []byte(resp.CACert)
		m := &tlsutil.Materials{CACert: caCert, Cert: []byte(resp.Cert), Key: []byte(resp.Key)}
		if err := saveTLSToStorage(local, m); err != nil {
			return nil, fmt.Errorf("save TLS materials: %w", err)
		}
		log.Info("TLS materials received from cluster and stored")
		return m, nil
	}
	caCert, caKey, err := tlsutil.GenerateCA()
	if err != nil {
		return nil, fmt.Errorf("generate CA: %w", err)
	}
	nodeCert, nodeKey, err := tlsutil.GenerateNodeCert(caCert, caKey, nodeID.String(), cfg.ServerAdvertise, cfg.AgentAdvertise)
	if err != nil {
		return nil, fmt.Errorf("generate node cert: %w", err)
	}
	m = &tlsutil.Materials{CACert: caCert, CAKey: caKey, Cert: nodeCert, Key: nodeKey}
	if err := saveTLSToStorage(local, m); err != nil {
		return nil, fmt.Errorf("save TLS materials: %w", err)
	}
	log.Info("cluster CA and node certificate generated")
	return m, nil
}

func loadTLSFromFiles(cfg *config) (*tlsutil.Materials, error) {
	caCert, err := os.ReadFile(cfg.CACert)
	if err != nil {
		return nil, fmt.Errorf("read CA cert: %w", err)
	}
	caKey, err := os.ReadFile(cfg.CAKey)
	if err != nil {
		return nil, fmt.Errorf("read CA key: %w", err)
	}
	cert, err := os.ReadFile(cfg.Cert)
	if err != nil {
		return nil, fmt.Errorf("read cert: %w", err)
	}
	key, err := os.ReadFile(cfg.Key)
	if err != nil {
		return nil, fmt.Errorf("read key: %w", err)
	}
	return &tlsutil.Materials{CACert: caCert, CAKey: caKey, Cert: cert, Key: key}, nil
}

func loadTLSFromStorage(local *storage.LocalStorage) (*tlsutil.Materials, error) {
	var caCert, cert, key string
	if err := local.Get("tls/ca-cert", &caCert); err != nil {
		return nil, err
	}
	if err := local.Get("tls/node-cert", &cert); err != nil {
		return nil, err
	}
	if err := local.Get("tls/node-key", &key); err != nil {
		return nil, err
	}
	return &tlsutil.Materials{
		CACert: []byte(caCert),
		Cert:   []byte(cert),
		Key:    []byte(key),
	}, nil
}

func saveTLSToStorage(local *storage.LocalStorage, m *tlsutil.Materials) error {
	if err := local.Put("tls/ca-cert", string(m.CACert)); err != nil {
		return err
	}
	if len(m.CAKey) > 0 {
		if err := local.Put("tls/ca-key", string(m.CAKey)); err != nil {
			return err
		}
	}
	if err := local.Put("tls/node-cert", string(m.Cert)); err != nil {
		return err
	}
	return local.Put("tls/node-key", string(m.Key))
}

func joinClusterTLS(ctx context.Context, log *slog.Logger, joinAddr, clusterToken, serverID, nodeID, raftAddr string) (*api.RaftJoinResponse, error) {
	body, err := json.Marshal(api.RaftJoinRequest{ID: serverID, NodeID: nodeID, RaftAddress: raftAddr})
	if err != nil {
		return nil, err
	}
	base := joinAddr
	if !strings.Contains(base, "://") {
		base = "https://" + base
	}
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13},
		},
	}
	for i := 0; ; i++ {
		req, _ := http.NewRequestWithContext(ctx, "POST", base+"/v1/raft/join", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if clusterToken != "" {
			req.Header.Set("Authorization", "Bearer "+clusterToken)
		}
		resp, err := httpClient.Do(req)
		if err == nil {
			respBody, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				var joinResp api.RaftJoinResponse
				if err := json.Unmarshal(respBody, &joinResp); err != nil {
					return nil, fmt.Errorf("decode join response: %w", err)
				}
				log.Info("received TLS materials from cluster")
				return &joinResp, nil
			}
			err = fmt.Errorf("join returned status %d", resp.StatusCode)
		}
		if i >= 30 {
			return nil, err
		}
		log.Warn("join attempt failed, retrying", "error", err, "attempt", i+1)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(min(i+1, 5)) * time.Second):
		}
	}
}

func joinClusterRaft(ctx context.Context, log *slog.Logger, joinAddr, clusterToken, serverID, raftAddr string, tlsConfig *tls.Config) error {
	body, err := json.Marshal(api.RaftJoinRequest{ID: serverID, RaftAddress: raftAddr})
	if err != nil {
		return err
	}
	base := joinAddr
	if !strings.Contains(base, "://") {
		base = "https://" + base
	}
	httpClient := &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsConfig},
	}
	for i := 0; ; i++ {
		req, _ := http.NewRequestWithContext(ctx, "POST", base+"/v1/raft/join", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := httpClient.Do(req)
		if err == nil {
			_, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				log.Info("joined cluster successfully")
				return nil
			}
			err = fmt.Errorf("join returned status %d", resp.StatusCode)
		}
		if i >= 30 {
			return err
		}
		log.Warn("join attempt failed, retrying", "error", err, "attempt", i+1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(min(i+1, 5)) * time.Second):
		}
	}
}

func watchLeader(ctx context.Context, log *slog.Logger, elector election.Elector, target *client.ServerClient) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		leader, err := elector.Current(ctx)
		if err != nil {
			log.Error("discover leader failed", "error", err)
		} else if leader != nil {
			target.SetAddress(leader.Address)
		} else {
			target.SetAddress("")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// controlPlaneProxy keeps the public API available on every node while ensuring
// that only the current leader executes requests. Authentication remains an
// end-to-end concern of the leader: the proxy neither interprets credentials nor
// derives an identity from the request's network origin.
type controlPlaneProxy struct {
	elector    election.Elector
	self       string
	local      http.Handler
	transport  http.RoundTripper
	log        *slog.Logger
	leaderLive atomic.Bool
}

func newControlPlaneProxy(elector election.Elector, self string, local http.Handler, transport http.RoundTripper, log *slog.Logger) *controlPlaneProxy {
	return &controlPlaneProxy{elector: elector, self: normalizeAPIAddress(self), local: local, transport: transport, log: log}
}

func (p *controlPlaneProxy) SetLeaderActive(active bool) { p.leaderLive.Store(active) }

func (p *controlPlaneProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	leader, err := p.elector.Current(r.Context())
	if err != nil || leader == nil || leader.Address == "" {
		http.Error(w, "control-plane leader unavailable", http.StatusServiceUnavailable)
		return
	}
	target := normalizeAPIAddress(leader.Address)
	if target == p.self {
		if !p.leaderLive.Load() {
			http.Error(w, "control-plane leader is not ready", http.StatusServiceUnavailable)
			return
		}
		p.local.ServeHTTP(w, r)
		return
	}

	targetURL, err := url.Parse(target)
	if err != nil {
		http.Error(w, "invalid control-plane leader address", http.StatusServiceUnavailable)
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.Transport = p.transport
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = targetURL.Scheme
		req.URL.Host = targetURL.Host
		req.Host = targetURL.Host
		// A follower's mTLS certificate authenticates the proxy connection, not
		// the original caller. Never allow it to become a request principal.
		req.Header.Set("X-Trellis-Forwarded-By-Proxy", "1")
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		p.log.Error("proxy request to leader failed", "leader", target, "error", err)
		http.Error(w, "control-plane leader unavailable", http.StatusServiceUnavailable)
	}
	proxy.ServeHTTP(w, r)
}

func normalizeAPIAddress(address string) string {
	address = strings.TrimRight(address, "/")
	if !strings.HasPrefix(address, "http://") && !strings.HasPrefix(address, "https://") {
		address = "https://" + address
	}
	return address
}

func newHTTPTransport(tlsConfig *tls.Config) *http.Transport {
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConns:          100,
		TLSClientConfig:       tlsConfig,
	}
}

func leaderAuthMiddleware(bootstrapToken string, tokenManager *auth.TokenManager) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if c.Request().URL.Path == "/metrics" {
				return next(c)
			}
			key := strings.TrimPrefix(c.Request().Header.Get("Authorization"), "Bearer ")
			if c.Request().URL.Path == "/v1/raft/join" && bootstrapToken != "" && key != "" && subtle.ConstantTimeCompare([]byte(key), []byte(bootstrapToken)) == 1 {
				ctx := context.WithValue(c.Request().Context(), server.EnrollmentContextKey, true)
				c.SetRequest(c.Request().WithContext(ctx))
				return next(c)
			}
			principal, err := tokenManager.ValidateToken(c.Request().Context(), key)
			if principal != nil {
				ctx := context.WithValue(c.Request().Context(), server.NamespaceContextKey, auth.EncodeScope(principal.Scope, principal.Access, principal.Namespace))
				ctx = context.WithValue(ctx, server.PrincipalContextKey, *principal)
				if principal.Admin {
					ctx = context.WithValue(ctx, server.AdminContextKey, true)
				}
				c.SetRequest(c.Request().WithContext(ctx))
				return next(c)
			}
			if c.Request().Header.Get("X-Trellis-Forwarded-By-Proxy") == "" && isNodeEndpoint(c.Request().URL.Path) {
				if nodeID, ok := tlsNodeID(c.Request()); ok {
					ctx := context.WithValue(c.Request().Context(), server.NodeContextKey, nodeID)
					c.SetRequest(c.Request().WithContext(ctx))
					return next(c)
				}
			}
			_ = err
			return echo.NewHTTPError(http.StatusUnauthorized, "invalid API credential")
		}
	}
}

func isNodeEndpoint(path string) bool {
	return path == "/v1/nodes" || path == "/v1/internal/discovery" || strings.HasPrefix(path, "/v1/nodes/") && strings.HasSuffix(path, "/heartbeat")
}

// leaderAgentAuthMiddleware authorizes commands only from the certificate
// presented by the node Raft currently reports as leader. A certificate proves
// node identity; it is not standing agent authority.
func leaderAgentAuthMiddleware(r *raft.Raft) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if c.Request().TLS == nil {
				return echo.NewHTTPError(http.StatusUnauthorized, "node certificate required")
			}
			certs := c.Request().TLS.PeerCertificates
			if len(certs) == 0 {
				return echo.NewHTTPError(http.StatusUnauthorized, "node certificate required")
			}
			_, leader := r.LeaderWithID()
			leaderNodeID, _, ok := parseRaftServerID(string(leader))
			if !ok {
				return echo.NewHTTPError(http.StatusServiceUnavailable, "Raft leader unavailable")
			}
			if certs[0].Subject.CommonName != leaderNodeID.String() {
				return echo.NewHTTPError(http.StatusForbidden, "caller is not the current Raft leader")
			}
			return next(c)
		}
	}
}

func raftServerID(nodeID uuid.UUID, serverAddress string) string {
	return nodeID.String() + "@" + serverAddress
}

func parseRaftServerID(value string) (uuid.UUID, string, bool) {
	rawID, address, ok := strings.Cut(value, "@")
	id, err := uuid.Parse(rawID)
	return id, address, ok && err == nil && address != ""
}

func tlsNodeID(r *http.Request) (uuid.UUID, bool) {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(r.TLS.PeerCertificates[0].Subject.CommonName)
	return id, err == nil
}

func splitAddress(address string) (string, int, error) {
	host, rawPort, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		return "", 0, err
	}
	return host, port, nil
}

func parseLabels(raw []string) (map[string]string, error) {
	labels := make(map[string]string, len(raw))
	for _, s := range raw {
		k, v, ok := strings.Cut(s, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("label %q must be in key=value form", s)
		}
		labels[k] = v
	}
	return labels, nil
}

func acquireNodeID(dataDir string) (uuid.UUID, error) {
	path := filepath.Join(dataDir, "node-id")
	raw, err := os.ReadFile(path)
	if err == nil {
		id, parseErr := uuid.Parse(strings.TrimSpace(string(raw)))
		if parseErr != nil {
			return uuid.Nil, fmt.Errorf("parse node ID: %w", parseErr)
		}
		return id, nil
	}
	if !os.IsNotExist(err) {
		return uuid.Nil, fmt.Errorf("read node ID: %w", err)
	}
	id := uuid.New()
	if err := os.WriteFile(path, []byte(id.String()), 0o600); err != nil {
		return uuid.Nil, fmt.Errorf("write node ID: %w", err)
	}
	return id, nil
}

// discardEnrollmentCredential removes the bootstrap credential from the
// daemon's YAML configuration once the node has its own certificate. It leaves
// comments and every other setting intact. Flag- and environment-supplied
// enrollment credentials are process-only and require no cleanup.
func discardEnrollmentCredential(configFile string) error {
	if configFile == "" {
		return nil
	}
	raw, err := os.ReadFile(configFile)
	if err != nil {
		return err
	}
	lines := strings.Split(string(raw), "\n")
	kept := lines[:0]
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "bootstrap_token:") {
			continue
		}
		kept = append(kept, line)
	}
	return os.WriteFile(configFile, []byte(strings.Join(kept, "\n")), 0o600)
}
