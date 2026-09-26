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
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
)

const shutdownTime = 10 * time.Second

type config struct {
	ConfigFile                                                                     string
	AgentListen, AgentAdvertise, ServerListen, ServerAdvertise                     string
	RaftListen, RaftAdvertise, Join                                                string
	DataDir, Cluster, AdminTokenHash, EnrollmentToken, SigningMode, ContainerdSock string
	Runtime, RuntimeFaults                                                         string
	WireGuardPool, WireGuardEndpoint                                               string
	WireGuardPort, WireGuardPortCount                                              int
	DNSListen                                                                      string
	CACert, CAKey, Cert, Key                                                       string
	SecretsKey, SecretsKeyID                                                       string
	Labels                                                                         []string
	MaxReplicasPerTaskGroup, MaxTaskGroupsPerJob                                   int
	MaxTasksPerTaskGroup, MaxDesiredAllocations, MaxDesiredAllocationsPerNamespace int
	DefaultTaskCPU                                                                 int
	DefaultTaskMemory                                                              string
	MaxTaskCPU                                                                     int
	MaxTaskMemory                                                                  string
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
	f.StringVar(&cfg.AdminTokenHash, "admin-token-hash", "", "SHA-256 hash used to initialize administrator API verification")
	f.StringVar(&cfg.EnrollmentToken, "enrollment-token", "", "Managed-mode node enrollment credential")
	f.StringVar(&cfg.SigningMode, "node-signing-mode", "managed", "Node certificate signing mode: managed or external")
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
	defaults := spec.DefaultLimits()
	f.IntVar(&cfg.MaxReplicasPerTaskGroup, "max-replicas-per-task-group", defaults.MaxReplicasPerTaskGroup, "Maximum replicas allowed in one task group")
	f.IntVar(&cfg.MaxTaskGroupsPerJob, "max-task-groups-per-job", defaults.MaxTaskGroupsPerJob, "Maximum task groups allowed in one job")
	f.IntVar(&cfg.MaxTasksPerTaskGroup, "max-tasks-per-task-group", defaults.MaxTasksPerTaskGroup, "Maximum tasks allowed in one task group")
	f.IntVar(&cfg.MaxDesiredAllocations, "max-desired-allocations", defaults.MaxDesiredAllocations, "Maximum desired allocations allowed in one job")
	f.IntVar(&cfg.MaxDesiredAllocationsPerNamespace, "max-desired-allocations-per-namespace", defaults.MaxDesiredAllocationsPerNamespace, "Maximum desired allocations allowed in one namespace")
	f.IntVar(&cfg.DefaultTaskCPU, "default-task-cpu", defaults.DefaultTaskCPU, "Default task CPU request in millicores")
	f.StringVar(&cfg.DefaultTaskMemory, "default-task-memory", fmt.Sprintf("%d", defaults.DefaultTaskMemory), "Default task memory request in bytes")
	f.IntVar(&cfg.MaxTaskCPU, "max-task-cpu", defaults.MaxTaskCPU, "Maximum task CPU request in millicores")
	f.StringVar(&cfg.MaxTaskMemory, "max-task-memory", fmt.Sprintf("%d", defaults.MaxTaskMemory), "Maximum task memory request in bytes")
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(parent context.Context, cfg *config) error {
	ctx, stop := signal.NotifyContext(parent, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if cfg.Join == "" && cfg.AdminTokenHash == "" {
		return fmt.Errorf("admin_token_hash or --admin-token-hash is required when creating a cluster")
	}
	if cfg.SigningMode != "managed" && cfg.SigningMode != "external" {
		return fmt.Errorf("node_signing_mode must be managed or external")
	}
	if cfg.SigningMode == "managed" && cfg.EnrollmentToken == "" {
		return fmt.Errorf("enrollment_token or --enrollment-token is required in managed mode")
	}
	if cfg.WireGuardPort < 1 || cfg.WireGuardPort > 65535 {
		return fmt.Errorf("--wireguard-port must be between 1 and 65535")
	}
	if cfg.WireGuardPortCount < 1 || cfg.WireGuardPort+cfg.WireGuardPortCount-1 > 65535 {
		return fmt.Errorf("--wireguard-port-count must be positive and fit between --wireguard-port and 65535")
	}
	defaultMemory, err := spec.ParseByteSize(cfg.DefaultTaskMemory)
	if err != nil {
		return fmt.Errorf("--default-task-memory: %w", err)
	}
	maxMemory, err := spec.ParseByteSize(cfg.MaxTaskMemory)
	if err != nil {
		return fmt.Errorf("--max-task-memory: %w", err)
	}
	limits := spec.Limits{MaxReplicasPerTaskGroup: cfg.MaxReplicasPerTaskGroup, MaxTaskGroupsPerJob: cfg.MaxTaskGroupsPerJob, MaxTasksPerTaskGroup: cfg.MaxTasksPerTaskGroup, MaxDesiredAllocations: cfg.MaxDesiredAllocations, MaxDesiredAllocationsPerNamespace: cfg.MaxDesiredAllocationsPerNamespace, DefaultTaskCPU: cfg.DefaultTaskCPU, DefaultTaskMemory: defaultMemory, MaxTaskCPU: cfg.MaxTaskCPU, MaxTaskMemory: maxMemory}
	if err := spec.ValidateLimits(limits); err != nil {
		return fmt.Errorf("job limits: %w", err)
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

	tlsMaterials, err := loadOrBootstrapTLS(ctx, log, cfg, local, id)
	if err != nil {
		return fmt.Errorf("TLS bootstrap: %w", err)
	}
	if err := os.WriteFile(filepath.Join(cfg.DataDir, "node-ca.crt"), tlsMaterials.CACert, 0o644); err != nil {
		return fmt.Errorf("write trusted node CA certificate: %w", err)
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
		ServerID:  id.String(),
		Bootstrap: cfg.Join == "",
		TLS:       peerTLS,
	})
	if err != nil {
		return fmt.Errorf("init raft store: %w", err)
	}
	defer func() { _ = raftStore.Close() }()

	if cfg.Join != "" && !raftStore.HadExistingState() {
		log.Info("joining cluster", "address", cfg.Join)
		if err := joinClusterRaft(ctx, log, cfg.Join, cfg.ServerAdvertise, raftStore.LocalAddr(), clientTLS); err != nil {
			return fmt.Errorf("join cluster: %w", err)
		}
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
	if err := control.SetJobLimits(limits); err != nil {
		return err
	}
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
	control.SetNodeID(id)
	control.SetClientTLS(clientTLS)
	if err := control.SetNetworkPool(cfg.WireGuardPool); err != nil {
		return err
	}
	if err := control.SetWireGuardPortCount(cfg.WireGuardPortCount); err != nil {
		return err
	}

	server.RegisterMetrics(control, prometheus.DefaultRegisterer)

	for i := 0; ; i++ {
		if err := control.Init(ctx, cfg.AdminTokenHash); err == nil {
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
	if cfg.Join == "" {
		if _, err := control.NodeServerAddress(ctx, id.String()); err != nil {
			if err := control.RecordNodeServerAddress(ctx, id, cfg.ServerAdvertise); err != nil {
				return fmt.Errorf("record local control-plane address: %w", err)
			}
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
	agentHTTP.Use(middleware.Recover(), nodeAuthMiddleware(ag.AuthorizeLeader))
	agent.NewHandler(ag).Register(agentHTTP)
	go func() {
		if err := (echo.StartConfig{Address: cfg.AgentListen, TLSConfig: agentServerTLS, GracefulTimeout: shutdownTime}).Start(ctx, agentHTTP); err != nil && ctx.Err() == nil {
			log.Error("agent API stopped", "error", err)
			stop()
		}
	}()

	elector := election.NewRaftElector(raftStore.Raft(), election.Leader{NodeID: id, Address: cfg.ServerAdvertise}, control.NodeServerAddress)
	leaderHTTP := echo.New()
	leaderHTTP.Use(middleware.Recover(), leaderAuthMiddleware(control.ValidateAPIToken, cfg.EnrollmentToken, control.TokenManager()))
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
	if cfg.Cert != "" || cfg.Key != "" || cfg.SigningMode == "external" {
		m, err := loadTLSFromFiles(cfg)
		if err != nil {
			return nil, err
		}
		if cfg.SigningMode == "external" && len(m.CAKey) != 0 {
			return nil, fmt.Errorf("external signing mode must not configure a CA private key")
		}
		if err := tlsutil.ValidateMaterials(m, nodeID); err != nil {
			return nil, err
		}
		if err := saveTLSToStorage(local, m); err != nil {
			return nil, fmt.Errorf("save TLS materials: %w", err)
		}
		return m, nil
	}
	m, err := loadTLSFromStorage(local)
	if err == nil {
		if cfg.SigningMode == "managed" && len(m.CAKey) == 0 {
			return nil, fmt.Errorf("managed signing mode requires the stored CA private key")
		}
		if err := tlsutil.ValidateMaterials(m, nodeID); err != nil {
			return nil, err
		}
		return m, nil
	}
	if cfg.Join != "" {
		if cfg.CACert == "" {
			return nil, fmt.Errorf("managed enrollment requires ca_cert to pin the existing cluster CA")
		}
		caCert, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("read pinned CA cert: %w", err)
		}
		resp, err := joinClusterTLS(ctx, log, cfg.Join, cfg.EnrollmentToken, caCert, nodeID, cfg.ServerAdvertise, cfg.AgentAdvertise, cfg.RaftAdvertise)
		if err != nil {
			return nil, fmt.Errorf("join cluster for TLS: %w", err)
		}
		m := &tlsutil.Materials{CACert: []byte(resp.CACert), CAKey: []byte(resp.CAKey), Cert: []byte(resp.Cert), Key: []byte(resp.Key)}
		if err := tlsutil.ValidateMaterials(m, nodeID); err != nil {
			return nil, fmt.Errorf("validate enrolled node certificate: %w", err)
		}
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
	nodeCert, nodeKey, err := tlsutil.GenerateNodeCert(caCert, caKey, nodeID, cfg.ServerAdvertise, cfg.AgentAdvertise)
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
	if cfg.CACert == "" || cfg.Cert == "" || cfg.Key == "" {
		return nil, fmt.Errorf("ca_cert, cert, and key are required for configured node certificates")
	}
	caCert, err := os.ReadFile(cfg.CACert)
	if err != nil {
		return nil, fmt.Errorf("read CA cert: %w", err)
	}
	var caKey []byte
	if cfg.CAKey != "" {
		caKey, err = os.ReadFile(cfg.CAKey)
		if err != nil {
			return nil, fmt.Errorf("read CA key: %w", err)
		}
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
	var caCert, caKey, cert, key string
	if err := local.Get("tls/ca-cert", &caCert); err != nil {
		return nil, err
	}
	_ = local.Get("tls/ca-key", &caKey)
	if err := local.Get("tls/node-cert", &cert); err != nil {
		return nil, err
	}
	if err := local.Get("tls/node-key", &key); err != nil {
		return nil, err
	}
	return &tlsutil.Materials{
		CACert: []byte(caCert),
		CAKey:  []byte(caKey),
		Cert:   []byte(cert),
		Key:    []byte(key),
	}, nil
}

func saveTLSToStorage(local *storage.LocalStorage, m *tlsutil.Materials) error {
	if err := local.Put("tls/ca-cert", string(m.CACert)); err != nil {
		return err
	}
	if len(m.CAKey) != 0 {
		if err := local.Put("tls/ca-key", string(m.CAKey)); err != nil {
			return err
		}
	} else if err := local.Delete("tls/ca-key"); err != nil {
		return err
	}
	if err := local.Put("tls/node-cert", string(m.Cert)); err != nil {
		return err
	}
	return local.Put("tls/node-key", string(m.Key))
}

func joinClusterTLS(ctx context.Context, log *slog.Logger, joinAddr, enrollmentToken string, caCert []byte, nodeID uuid.UUID, serverAdvertise, agentAdvertise, raftAdvertise string) (*api.NodeEnrollmentResponse, error) {
	body, err := json.Marshal(api.NodeEnrollmentRequest{NodeID: nodeID, ServerAdvertise: serverAdvertise, AgentAdvertise: agentAdvertise, RaftAdvertise: raftAdvertise})
	if err != nil {
		return nil, err
	}
	base := joinAddr
	if !strings.Contains(base, "://") {
		base = "https://" + base
	}
	tlsConfig, err := tlsutil.CAClientTLSConfig(caCert)
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}
	for i := 0; ; i++ {
		req, _ := http.NewRequestWithContext(ctx, "POST", base+"/v1/nodes/enroll", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+enrollmentToken)
		resp, err := httpClient.Do(req)
		if err == nil {
			respBody, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusCreated {
				var joinResp api.NodeEnrollmentResponse
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

func joinClusterRaft(ctx context.Context, log *slog.Logger, joinAddr, serverAddr, raftAddr string, tlsConfig *tls.Config) error {
	body, err := json.Marshal(api.RaftJoinRequest{ServerAddress: serverAddr, RaftAddress: raftAddr})
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
			if resp.StatusCode == http.StatusNoContent {
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
	// Raft join authorization is bound to the joining node certificate. Redirect
	// rather than proxy so the leader verifies that original certificate instead
	// of the forwarding member's transport identity.
	if r.Method == http.MethodPost && r.URL.Path == "/v1/raft/join" {
		http.Redirect(w, r, target+r.URL.RequestURI(), http.StatusTemporaryRedirect)
		return
	}

	targetURL, err := url.Parse(target)
	if err != nil {
		http.Error(w, "invalid control-plane leader address", http.StatusServiceUnavailable)
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.Transport = p.transport
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

func authenticatedNodeID(r *http.Request) (uuid.UUID, bool) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		return uuid.Nil, false
	}
	id, err := tlsutil.NodeID(r.TLS.VerifiedChains[0][0])
	return id, err == nil
}

func nodeAuthMiddleware(authorize func(uuid.UUID) bool) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			id, ok := authenticatedNodeID(c.Request())
			if !ok {
				return echo.NewHTTPError(http.StatusUnauthorized, "authenticated node certificate required")
			}
			if authorize != nil && !authorize(id) {
				return echo.NewHTTPError(http.StatusForbidden, "agent operation requires the current Raft leader identity")
			}
			ctx := context.WithValue(c.Request().Context(), server.NodeContextKey, id)
			c.SetRequest(c.Request().WithContext(ctx))
			return next(c)
		}
	}
}

func nodeControlPlaneRoute(r *http.Request) bool {
	path := r.URL.Path
	return (r.Method == http.MethodPost && path == "/v1/nodes") ||
		(r.Method == http.MethodPost && strings.HasPrefix(path, "/v1/nodes/") && strings.HasSuffix(path, "/heartbeat")) ||
		(r.Method == http.MethodGet && path == "/v1/internal/discovery") ||
		(r.Method == http.MethodPost && path == "/v1/raft/join")
}

func leaderAuthMiddleware(validateAdmin func(string) bool, enrollmentToken string, tokenManager *auth.TokenManager) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if c.Request().URL.Path == "/metrics" {
				return next(c)
			}
			key := strings.TrimPrefix(c.Request().Header.Get("Authorization"), "Bearer ")
			if enrollmentToken != "" && c.Request().URL.Path == "/v1/nodes/enroll" && subtle.ConstantTimeCompare([]byte(key), []byte(enrollmentToken)) == 1 {
				ctx := context.WithValue(c.Request().Context(), server.EnrollmentContextKey, true)
				c.SetRequest(c.Request().WithContext(ctx))
				return next(c)
			}
			if key != "" && validateAdmin != nil && validateAdmin(key) {
				principal := auth.AdministratorPrincipal()
				ctx := context.WithValue(c.Request().Context(), server.AdminContextKey, true)
				ctx = context.WithValue(ctx, server.PrincipalContextKey, principal)
				c.SetRequest(c.Request().WithContext(ctx))
				return next(c)
			}
			if key != "" && tokenManager != nil {
				principal, err := tokenManager.ValidateToken(c.Request().Context(), key)
				if err == nil && principal != nil {
					ctx := context.WithValue(c.Request().Context(), server.NamespaceContextKey, auth.EncodeScope(principal.Scope, principal.Access, principal.Namespace))
					ctx = context.WithValue(ctx, server.PrincipalContextKey, *principal)
					c.SetRequest(c.Request().WithContext(ctx))
					return next(c)
				}
			}
			if id, ok := authenticatedNodeID(c.Request()); ok {
				if !nodeControlPlaneRoute(c.Request()) {
					return echo.NewHTTPError(http.StatusForbidden, "node identity is not authorized for this API operation")
				}
				ctx := context.WithValue(c.Request().Context(), server.NodeContextKey, id)
				c.SetRequest(c.Request().WithContext(ctx))
				return next(c)
			}
			return echo.NewHTTPError(http.StatusUnauthorized, "invalid credential")
		}
	}
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
