// Command trellis runs a Trellis orchestrator and allocation agent.
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
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

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"github.com/overfold/trellis/internal/agent"
	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/auth"
	"github.com/overfold/trellis/internal/client"
	trellisdns "github.com/overfold/trellis/internal/dns"
	"github.com/overfold/trellis/internal/election"
	"github.com/overfold/trellis/internal/health"
	"github.com/overfold/trellis/internal/localconfig"
	"github.com/overfold/trellis/internal/network"
	containerruntime "github.com/overfold/trellis/internal/runtime"
	secretstore "github.com/overfold/trellis/internal/secrets"
	"github.com/overfold/trellis/internal/server"
	"github.com/overfold/trellis/internal/spec"
	"github.com/overfold/trellis/internal/state"
	"github.com/overfold/trellis/internal/storage"
	"github.com/overfold/trellis/internal/tlsutil"
	"github.com/overfold/trellis/internal/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const shutdownTime = 10 * time.Second

type config struct {
	ConfigFile                                                                     string
	AgentListen, AgentAdvertise, ServerListen, ServerAdvertise                     string
	RaftListen, RaftAdvertise, Join                                                string
	DataDir, Cluster, AdminPublicKey, EnrollmentToken, SigningMode, ContainerdSock string
	Runtime                                                                        string
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
	TaskPidsLimit                                                                  int64
	AllocationLossTimeout                                                          time.Duration
	// Explicit records which cluster settings the operator set on this node,
	// through flags or the configuration file. Cluster settings initialize a
	// new cluster; on an existing cluster the replicated values win.
	Explicit explicitClusterSettings
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
			recordExplicitClusterSettingFlags(cfg, cmd.Flags())
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
	f.StringVar(&cfg.AdminPublicKey, "administrator-public-key", "", "Base64 PKIX Ed25519 public key used to initialize administrator request verification")
	f.StringVar(&cfg.EnrollmentToken, "enrollment-token", "", "Managed-mode node enrollment credential")
	f.StringVar(&cfg.SigningMode, "node-signing-mode", "managed", "Node certificate signing mode: managed or external")
	f.StringVar(&cfg.ContainerdSock, "containerd-sock", "/run/containerd/containerd.sock", "Containerd socket path")
	f.StringVar(&cfg.Runtime, "runtime", "containerd", "Workload runtime: containerd")
	if buildTestRuntime != nil {
		buildTestRuntime.addFlags(f)
	}
	f.StringVar(&cfg.WireGuardPool, "wireguard-pool", server.DefaultWireGuardPool, "Namespace network address pool of a new cluster (existing clusters use their replicated pool)")
	f.StringVar(&cfg.WireGuardEndpoint, "wireguard-endpoint", "", "Externally reachable WireGuard host or base host:port")
	f.IntVar(&cfg.WireGuardPort, "wireguard-port", 51820, "First UDP port in the per-namespace WireGuard range")
	f.IntVar(&cfg.WireGuardPortCount, "wireguard-port-count", server.DefaultWireGuardPortCount, "Number of consecutive UDP ports for namespace WireGuard networks; must match the cluster's replicated count")
	f.StringVar(&cfg.DNSListen, "dns-listen", net.JoinHostPort(network.WorkloadDNSAddress, "53"), "Workload DNS resolver listen address")
	f.StringVar(&cfg.CACert, "ca-cert", "", "Path to cluster CA certificate (PEM)")
	f.StringVar(&cfg.CAKey, "ca-key", "", "Path to cluster CA private key (PEM)")
	f.StringVar(&cfg.Cert, "cert", "", "Path to node certificate (PEM)")
	f.StringVar(&cfg.Key, "key", "", "Path to node private key (PEM)")
	f.StringVar(&cfg.SecretsKey, "secrets-key", "", "Path to a root-readable 32-byte or base64-encoded secrets encryption key")
	f.StringVar(&cfg.SecretsKeyID, "secrets-key-id", "", "Identifier for the active secrets encryption key")
	f.StringArrayVar(&cfg.Labels, "label", nil, "Node label in key=value form (repeatable)")
	f.Int64Var(&cfg.TaskPidsLimit, "task-pids-limit", agent.DefaultTaskPidsLimit, "Maximum processes and threads in each task container created on this node")
	f.DurationVar(&cfg.AllocationLossTimeout, "allocation-loss-timeout", server.DefaultAllocationLossTimeout, "How long a node may miss heartbeats before its allocations become lost and are replaced")
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
	if !spec.ValidIdentifier(cfg.Cluster) {
		return fmt.Errorf("cluster or --cluster must be a safe identifier (1-63 ASCII letters, digits, dots, underscores, or hyphens; must start with a letter or digit)")
	}
	ctx, stop := signal.NotifyContext(parent, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if cfg.Join == "" && cfg.AdminPublicKey == "" {
		return fmt.Errorf("administrator_public_key or --administrator-public-key is required when creating a cluster")
	}
	if cfg.SigningMode != "managed" && cfg.SigningMode != "external" {
		return fmt.Errorf("node_signing_mode must be managed or external")
	}
	if err := validateRuntime(cfg.Runtime); err != nil {
		return err
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
	if err := server.ValidateAllocationLossTimeout(cfg.AllocationLossTimeout); err != nil {
		return fmt.Errorf("allocation_loss_timeout or --allocation-loss-timeout: %w", err)
	}
	if err := agent.ValidateTaskPidsLimit(cfg.TaskPidsLimit); err != nil {
		return fmt.Errorf("resources.task_pids_limit or --task-pids-limit: %w", err)
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
	pool, err := server.ParseWireGuardPool(cfg.WireGuardPool)
	if err != nil {
		return fmt.Errorf("wireguard_pool or --wireguard-pool: %w", err)
	}
	bootstrapSettings := server.ClusterSettings{JobLimits: limits, WireGuardPool: pool, WireGuardPortCount: cfg.WireGuardPortCount}
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

	tlsMaterials, enrolledID, err := loadOrBootstrapTLS(ctx, log, cfg, local, id)
	if err != nil {
		return fmt.Errorf("TLS bootstrap: %w", err)
	}
	id = enrolledID
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

	if cfg.Join != "" && (!raftStore.HadExistingState() || (cfg.SigningMode == "managed" && len(tlsMaterials.CAKey) == 0)) {
		log.Info("joining cluster", "address", cfg.Join)
		joinResponse, err := joinClusterRaft(ctx, log, cfg.Join, cfg.ServerAdvertise, raftStore.LocalAddr(), clientTLS)
		if err != nil {
			return fmt.Errorf("join cluster: %w", err)
		}
		if cfg.SigningMode == "managed" {
			if joinResponse.CAKey == "" {
				return fmt.Errorf("join cluster: managed signing key was not returned after admission")
			}
			tlsMaterials.CAKey = []byte(joinResponse.CAKey)
			if err := saveTLSToStorage(local, tlsMaterials); err != nil {
				return fmt.Errorf("save managed signing key: %w", err)
			}
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
	if err := control.SetAllocationLossTimeout(cfg.AllocationLossTimeout); err != nil {
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

	server.RegisterMetrics(control, prometheus.DefaultRegisterer)

	for i := 0; ; i++ {
		if err := control.Init(ctx, server.ClusterBootstrap{AdministratorPublicKey: cfg.AdminPublicKey, Settings: bootstrapSettings}); err == nil {
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
	if err := applyClusterSettings(log, cfg, bootstrapSettings, control.ClusterSettings()); err != nil {
		return err
	}
	if cfg.Join == "" {
		localCertificate, err := x509.ParseCertificate(peerTLS.Certificates[0].Certificate[0])
		if err != nil {
			return fmt.Errorf("parse local node certificate: %w", err)
		}
		if err := control.BindNodeCertificate(ctx, id, localCertificate); err != nil {
			return fmt.Errorf("bind local node certificate: %w", err)
		}
		if _, err := control.NodeServerAddress(ctx, id.String()); err != nil {
			if err := control.RecordNodeServerAddress(ctx, id, cfg.ServerAdvertise); err != nil {
				return fmt.Errorf("record local control-plane address: %w", err)
			}
		}
	}

	runtimeClient, runtimeCloser, capabilities, err := openRuntime(cfg)
	if err != nil {
		return err
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
	if err := ag.SetTaskPidsLimit(cfg.TaskPidsLimit); err != nil {
		return fmt.Errorf("resources.task_pids_limit or --task-pids-limit: %w", err)
	}
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
		if containerruntime.SwapUncapped() {
			log.Warn("swap is active but swap accounting was not detected in the host memory cgroup; task memory limits may not cap swap")
		}
		if !containerruntime.PidsControllerDetected() {
			log.Warn("pids cgroup controller not detected; task containers with a pids limit may fail to start")
		}
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
	if err := ag.SetResources(goruntime.NumCPU()*1000, memory, goruntime.GOOS, goruntime.GOARCH); err != nil {
		return fmt.Errorf("configure node resources: %w", err)
	}
	ag.SetCapabilities(capabilities)
	if len(cfg.Labels) > 0 {
		labels, err := parseLabels(cfg.Labels)
		if err != nil {
			return err
		}
		ag.SetLabels(labels)
	}
	if err := ag.Init(ctx); err != nil {
		return fmt.Errorf("initialize allocation agent: %w", err)
	}
	// Runs before the runtime client closes, so terminals can still be killed.
	defer ag.CloseExecSessions(context.Background())

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
	dnsResolver := trellisdns.NewResolver(log, leaderClient, networkManager, trellisdns.DefaultDomain, filteredUpstreams...)
	go func() {
		if err := dnsResolver.Run(ctx, cfg.DNSListen); err != nil && ctx.Err() == nil {
			log.Error("dns resolver stopped", "error", err)
		}
	}()

	elector := election.NewRaftElector(raftStore.Raft(), election.Leader{NodeID: id, Address: cfg.ServerAdvertise}, control.NodeServerAddress)
	agentHTTP := echo.New()
	agentHTTP.Use(middleware.Recover(), nodeAuthMiddleware(currentLeaderAuthorizer(elector, control.AuthorizeNodeCertificate)))
	agent.NewHandler(ag).Register(agentHTTP)
	go func() {
		if err := (echo.StartConfig{Address: cfg.AgentListen, TLSConfig: agentServerTLS, GracefulTimeout: shutdownTime}).Start(ctx, agentHTTP); err != nil && ctx.Err() == nil {
			log.Error("agent API stopped", "error", err)
			stop()
		}
	}()

	leaderHTTP := echo.New()
	leaderHTTP.Use(middleware.Recover(), leaderAuthMiddleware(auth.NewAdministratorAuthenticator(), control.AdministratorVerification, cfg.EnrollmentToken, control.TokenManager(), control.AuthorizeNodeCertificate))
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
		case err := <-ag.Failed():
			if leaderCancel != nil {
				leaderCancel()
			}
			return fmt.Errorf("allocation agent: %w", err)
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

// testRuntime describes a runtime that exists only for tests. It is nil in
// normal builds and set by runtime_injected.go in integration builds, so a
// production node cannot select a runtime that reports workloads as running
// without executing them.
type testRuntime struct {
	name     string
	addFlags func(*pflag.FlagSet)
	open     func(dataDir string) (containerruntime.ContainerRuntime, io.Closer, error)
}

var buildTestRuntime *testRuntime

func validateRuntime(name string) error {
	if name == "containerd" || (buildTestRuntime != nil && name == buildTestRuntime.name) {
		return nil
	}
	return fmt.Errorf("unsupported runtime %q", name)
}

// openRuntime opens the configured workload runtime and returns the node
// capabilities it can honestly advertise.
func openRuntime(cfg *config) (containerruntime.ContainerRuntime, io.Closer, []spec.NodeCapability, error) {
	if err := validateRuntime(cfg.Runtime); err != nil {
		return nil, nil, nil, err
	}
	if cfg.Runtime == "containerd" {
		r, err := containerruntime.NewContainerdRuntime(cfg.ContainerdSock)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("init runtime: %w", err)
		}
		return r, r, detectNodeCapabilities(), nil
	}
	// Test runtimes execute nothing, so they advertise no optional capabilities.
	r, closer, err := buildTestRuntime.open(cfg.DataDir)
	if err != nil {
		return nil, nil, nil, err
	}
	return r, closer, nil, nil
}

func detectNodeCapabilities() []spec.NodeCapability {
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
	key, err := decodeSecretsKey(raw)
	if err != nil {
		return nil, "", err
	}
	keyID := configuredID
	if keyID == "" {
		sum := sha256.Sum256(key)
		keyID = hex.EncodeToString(sum[:8])
	}
	return key, keyID, nil
}

func decodeSecretsKey(raw []byte) ([]byte, error) {
	defer clear(raw)
	key := append([]byte(nil), raw...)
	encoded := bytes.TrimSpace(raw)
	decoded := make([]byte, base64.StdEncoding.DecodedLen(len(encoded)))
	if n, decodeErr := base64.StdEncoding.Decode(decoded, encoded); decodeErr == nil && n == 32 {
		clear(key)
		key = append([]byte(nil), decoded[:n]...)
	}
	clear(decoded)
	if len(key) != 32 {
		clear(key)
		return nil, fmt.Errorf("secrets key must contain exactly 32 raw bytes or their base64 encoding")
	}
	return key, nil
}

func loadOrBootstrapTLS(ctx context.Context, log *slog.Logger, cfg *config, local *storage.LocalStorage, nodeID uuid.UUID) (*tlsutil.Materials, uuid.UUID, error) {
	if cfg.Cert != "" || cfg.Key != "" || cfg.SigningMode == "external" {
		m, err := loadTLSFromFiles(cfg)
		if err != nil {
			return nil, uuid.Nil, err
		}
		if cfg.SigningMode == "external" && len(m.CAKey) != 0 {
			return nil, uuid.Nil, fmt.Errorf("external signing mode must not configure a CA private key")
		}
		if err := tlsutil.ValidateMaterials(m, nodeID); err != nil {
			return nil, uuid.Nil, err
		}
		if err := saveTLSToStorage(local, m); err != nil {
			return nil, uuid.Nil, fmt.Errorf("save TLS materials: %w", err)
		}
		return m, nodeID, nil
	}
	m, err := loadTLSFromStorage(local)
	if err == nil {
		if cfg.SigningMode == "managed" && len(m.CAKey) == 0 && cfg.Join == "" {
			return nil, uuid.Nil, fmt.Errorf("managed signing mode requires the stored CA private key")
		}
		if err := tlsutil.ValidateMaterials(m, nodeID); err != nil {
			return nil, uuid.Nil, err
		}
		return m, nodeID, nil
	}
	if cfg.Join != "" {
		if cfg.CACert == "" {
			return nil, uuid.Nil, fmt.Errorf("managed enrollment requires ca_cert to pin the existing cluster CA")
		}
		caCert, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, uuid.Nil, fmt.Errorf("read pinned CA cert: %w", err)
		}
		resp, err := joinClusterTLS(ctx, log, cfg.Join, cfg.EnrollmentToken, caCert, cfg.ServerAdvertise, cfg.AgentAdvertise, cfg.RaftAdvertise)
		if err != nil {
			return nil, uuid.Nil, fmt.Errorf("join cluster for TLS: %w", err)
		}
		m := &tlsutil.Materials{CACert: []byte(resp.CACert), Cert: []byte(resp.Cert), Key: []byte(resp.Key)}
		if err := tlsutil.ValidateMaterials(m, resp.NodeID); err != nil {
			return nil, uuid.Nil, fmt.Errorf("validate enrolled node certificate: %w", err)
		}
		if err := saveNodeID(cfg.DataDir, resp.NodeID); err != nil {
			return nil, uuid.Nil, err
		}
		if err := saveTLSToStorage(local, m); err != nil {
			return nil, uuid.Nil, fmt.Errorf("save TLS materials: %w", err)
		}
		log.Info("TLS materials received from cluster and stored")
		return m, resp.NodeID, nil
	}
	caCert, caKey, err := tlsutil.GenerateCA()
	if err != nil {
		return nil, uuid.Nil, fmt.Errorf("generate CA: %w", err)
	}
	nodeCert, nodeKey, err := tlsutil.GenerateNodeCert(caCert, caKey, nodeID, cfg.ServerAdvertise, cfg.AgentAdvertise, cfg.RaftAdvertise)
	if err != nil {
		return nil, uuid.Nil, fmt.Errorf("generate node cert: %w", err)
	}
	m = &tlsutil.Materials{CACert: caCert, CAKey: caKey, Cert: nodeCert, Key: nodeKey}
	if err := saveTLSToStorage(local, m); err != nil {
		return nil, uuid.Nil, fmt.Errorf("save TLS materials: %w", err)
	}
	log.Info("cluster CA and node certificate generated")
	return m, nodeID, nil
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

func joinClusterTLS(ctx context.Context, log *slog.Logger, joinAddr, enrollmentToken string, caCert []byte, serverAdvertise, agentAdvertise, raftAdvertise string) (*api.NodeEnrollmentResponse, error) {
	body, err := json.Marshal(api.NodeEnrollmentRequest{ServerAdvertise: serverAdvertise, AgentAdvertise: agentAdvertise, RaftAdvertise: raftAdvertise})
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

func joinClusterRaft(ctx context.Context, log *slog.Logger, joinAddr, serverAddr, raftAddr string, tlsConfig *tls.Config) (*api.RaftJoinResponse, error) {
	body, err := json.Marshal(api.RaftJoinRequest{ServerAddress: serverAddr, RaftAddress: raftAddr})
	if err != nil {
		return nil, err
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
			respBody, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				var joinResponse api.RaftJoinResponse
				if err := json.Unmarshal(respBody, &joinResponse); err != nil {
					return nil, fmt.Errorf("decode Raft join response: %w", err)
				}
				log.Info("joined cluster successfully")
				return &joinResponse, nil
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
	// Node authorization is bound to the caller's certificate. Redirect rather
	// than proxy so the leader verifies the original identity instead of the
	// forwarding member's transport identity.
	if nodeControlPlaneRoute(r) {
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

func authenticatedNodeIdentity(r *http.Request) (uuid.UUID, *x509.Certificate, bool) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		return uuid.Nil, nil, false
	}
	certificate := r.TLS.VerifiedChains[0][0]
	id, err := tlsutil.NodeID(certificate)
	return id, certificate, err == nil
}

func nodeAuthMiddleware(authorize func(context.Context, uuid.UUID, *x509.Certificate) bool) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			id, certificate, ok := authenticatedNodeIdentity(c.Request())
			if !ok {
				return echo.NewHTTPError(http.StatusUnauthorized, "authenticated node certificate required")
			}
			if authorize != nil && !authorize(c.Request().Context(), id, certificate) {
				return echo.NewHTTPError(http.StatusForbidden, "agent operation requires the current Raft leader identity")
			}
			ctx := context.WithValue(c.Request().Context(), server.NodeContextKey, id)
			c.SetRequest(c.Request().WithContext(ctx))
			return next(c)
		}
	}
}

func currentLeaderAuthorizer(elector election.Elector, authorizeCertificate func(context.Context, uuid.UUID, *x509.Certificate) bool) func(context.Context, uuid.UUID, *x509.Certificate) bool {
	return func(ctx context.Context, id uuid.UUID, certificate *x509.Certificate) bool {
		leaderID, err := elector.CurrentID()
		return err == nil && id != uuid.Nil && id == leaderID && (authorizeCertificate == nil || authorizeCertificate(ctx, id, certificate))
	}
}

func nodeControlPlaneRoute(r *http.Request) bool {
	path := r.URL.Path
	return (r.Method == http.MethodPost && path == "/v1/nodes") ||
		(r.Method == http.MethodPost && strings.HasPrefix(path, "/v1/nodes/") && strings.HasSuffix(path, "/heartbeat")) ||
		(r.Method == http.MethodGet && path == "/v1/internal/discovery") ||
		(r.Method == http.MethodPost && path == "/v1/raft/join")
}

func leaderAuthMiddleware(administrator *auth.AdministratorAuthenticator, administratorVerification func() (ed25519.PublicKey, uint64, bool), enrollmentToken string, tokenManager *auth.TokenManager, authorizeNodeCertificate func(context.Context, uuid.UUID, *x509.Certificate) bool) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if c.Request().URL.Path == "/metrics" {
				return next(c)
			}
			if c.Request().URL.Path == "/v1/auth/administrator/challenge" {
				if c.Request().Method != http.MethodPost {
					return echo.NewHTTPError(http.StatusMethodNotAllowed, "administrator challenges require POST")
				}
				_, epoch, ok := administratorVerification()
				if !ok {
					return echo.NewHTTPError(http.StatusServiceUnavailable, "administrator verification is unavailable")
				}
				challenge, expiresAt, err := administrator.Issue(epoch)
				if err != nil {
					return echo.NewHTTPError(http.StatusServiceUnavailable, "unable to issue administrator challenge")
				}
				c.Response().Header().Set("Cache-Control", "no-store")
				return c.JSON(http.StatusCreated, api.AdministratorChallengeResponse{Challenge: challenge, ExpiresAt: expiresAt})
			}
			key := strings.TrimPrefix(c.Request().Header.Get("Authorization"), "Bearer ")
			if enrollmentToken != "" && c.Request().URL.Path == "/v1/nodes/enroll" && subtle.ConstantTimeCompare([]byte(key), []byte(enrollmentToken)) == 1 {
				ctx := context.WithValue(c.Request().Context(), server.EnrollmentContextKey, true)
				c.SetRequest(c.Request().WithContext(ctx))
				return next(c)
			}
			challenge := c.Request().Header.Get(auth.AdministratorChallengeHeader)
			signature := c.Request().Header.Get(auth.AdministratorSignatureHeader)
			if challenge != "" || signature != "" {
				if challenge != "" {
					defer administrator.Consume(challenge)
				}
				body, err := io.ReadAll(io.LimitReader(c.Request().Body, (64<<20)+1))
				if err != nil {
					c.Response().Header().Set(auth.AdministratorChallengeStatusHeader, auth.AdministratorChallengeInvalid)
					return echo.NewHTTPError(http.StatusBadRequest, "unable to read signed request body")
				}
				if len(body) > 64<<20 {
					c.Response().Header().Set(auth.AdministratorChallengeStatusHeader, auth.AdministratorChallengeInvalid)
					return echo.NewHTTPError(http.StatusRequestEntityTooLarge, "signed request body exceeds 64 MiB")
				}
				c.Request().Body = io.NopCloser(bytes.NewReader(body))
				publicKey, epoch, ok := administratorVerification()
				payload := auth.AdministratorSigningPayload(challenge, c.Request().Method, c.Request().URL.RequestURI(), body)
				if ok && challenge != "" && signature != "" && administrator.Verify(publicKey, epoch, challenge, signature, payload) {
					principal := auth.AdministratorPrincipal()
					ctx := context.WithValue(c.Request().Context(), server.AdminContextKey, true)
					ctx = context.WithValue(ctx, server.PrincipalContextKey, principal)
					c.SetRequest(c.Request().WithContext(ctx))
					return next(c)
				}
				c.Response().Header().Set(auth.AdministratorChallengeStatusHeader, auth.AdministratorChallengeInvalid)
				return echo.NewHTTPError(http.StatusUnauthorized, "invalid administrator challenge or signature")
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
			if id, certificate, ok := authenticatedNodeIdentity(c.Request()); ok {
				if !nodeControlPlaneRoute(c.Request()) {
					return echo.NewHTTPError(http.StatusForbidden, "node identity is not authorized for this API operation")
				}
				// External-signing nodes establish their certificate binding while
				// joining. The handler rejects attempts to replace an existing UUID.
				isRaftJoin := c.Request().Method == http.MethodPost && c.Request().URL.Path == "/v1/raft/join"
				if !isRaftJoin && authorizeNodeCertificate != nil && !authorizeNodeCertificate(c.Request().Context(), id, certificate) {
					return echo.NewHTTPError(http.StatusForbidden, "node identity certificate does not match its enrolled binding")
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
	if err := saveNodeID(dataDir, id); err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

func saveNodeID(dataDir string, id uuid.UUID) error {
	if id == uuid.Nil {
		return fmt.Errorf("write node ID: ID is required")
	}
	if err := os.WriteFile(filepath.Join(dataDir, "node-id"), []byte(id.String()), 0o600); err != nil {
		return fmt.Errorf("write node ID: %w", err)
	}
	return nil
}
