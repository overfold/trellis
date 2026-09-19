package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/clofour/trellis/internal/localconfig"
	"github.com/clofour/trellis/internal/version"
	"github.com/spf13/cobra"
)

type CLIConfig struct {
	Context      string
	ServerAddr   string
	ClusterToken string
	Namespace    string
	CACert       string
	CACertPEM    string
	Cert         string
	Key          string
	Output       string
}

var config CLIConfig

type contextFileConfig struct {
	ServerAddr   string `yaml:"server_addr,omitempty"`
	ClusterToken string `yaml:"token,omitempty"`
	Namespace    string `yaml:"namespace,omitempty"`
	CACert       string `yaml:"ca_cert,omitempty"`
	Cert         string `yaml:"cert,omitempty"`
	Key          string `yaml:"key,omitempty"`
}

type fileConfig struct {
	CurrentContext string                       `yaml:"current_context,omitempty"`
	Contexts       map[string]contextFileConfig `yaml:"contexts,omitempty"`
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		var exitErr *execExitError
		if errors.As(err, &exitErr) && exitErr.code > 0 && exitErr.code <= 255 {
			os.Exit(exitErr.code)
		}
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:     "trellisctl",
		Short:   "Operate Trellis clusters",
		Version: version.Current(),
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			return loadConfig(cmd)
		},
	}

	persistentFlags := root.PersistentFlags()
	persistentFlags.StringVar(&config.Context, "context", "", "Named cluster context to use for this command")
	persistentFlags.StringVar(&config.ServerAddr, "server-addr", "localhost:8128", "Cluster API address")
	persistentFlags.StringVar(&config.ClusterToken, "token", "", "Bearer API credential")
	persistentFlags.StringVar(&config.Namespace, "namespace", "", "Namespace scope for jobs, allocations, and secrets")
	persistentFlags.StringVar(&config.CACert, "ca-cert", "", "Path to cluster CA certificate (PEM)")
	persistentFlags.StringVar(&config.Cert, "cert", "", "Path to client certificate (PEM)")
	persistentFlags.StringVar(&config.Key, "key", "", "Path to client private key (PEM)")

	root.AddCommand(NewContextCmd())
	root.AddCommand(NewJobsCmd())
	root.AddCommand(NewExecCmd())
	root.AddCommand(NewNamespacesCmd())
	root.AddCommand(NewNodesCmd())
	root.AddCommand(NewSecretsCmd())
	root.AddCommand(NewBackupCmd())
	root.AddCommand(NewCredentialsCmd())
	root.AddCommand(NewVersionCmd())
	addStructuredOutputFlags(root)
	return root
}

var structuredOutputCommands = [][]string{
	{"jobs", "list"},
	{"jobs", "status"},
	{"namespaces", "list"},
	{"nodes", "list"},
	{"nodes", "status"},
	{"secrets", "set"},
	{"secrets", "list"},
	{"secrets", "describe"},
	{"credentials", "create"},
}

// addStructuredOutputFlags keeps --output local to commands that deliberately
// implement both human table/text output and a single structured JSON value.
// Streaming and action-oriented commands therefore cannot silently ignore
// "--output json" and emit prose instead.
func addStructuredOutputFlags(root *cobra.Command) {
	for _, path := range structuredOutputCommands {
		command, _, err := root.Find(path)
		if err != nil || command == root {
			panic(fmt.Sprintf("structured output command %v is not registered", path))
		}
		command.Flags().StringVarP(&config.Output, "output", "o", "table", "Output format (table or json)")
	}
}

func buildCLITLSConfig() (*tls.Config, error) {
	if (config.Cert == "") != (config.Key == "") {
		return nil, fmt.Errorf("client certificate and private key must be provided together")
	}
	var caPEM []byte
	switch {
	case config.CACert != "":
		data, err := os.ReadFile(config.CACert)
		if err != nil {
			return nil, fmt.Errorf("read CA cert: %w", err)
		}
		caPEM = data
	case config.CACertPEM != "":
		caPEM = []byte(config.CACertPEM)
	case config.Cert == "":
		return nil, nil
	}
	cfg := &tls.Config{ServerName: "trellis"}
	if caPEM != nil {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		cfg.RootCAs = pool
	}
	if config.Cert != "" {
		cert, err := tls.LoadX509KeyPair(config.Cert, config.Key)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func loadConfig(cmd *cobra.Command) error {
	flagConfig := config
	merged := CLIConfig{ServerAddr: "localhost:8128", CACert: flagConfig.CACert, Cert: flagConfig.Cert, Key: flagConfig.Key, Output: flagConfig.Output}

	if lc, err := localconfig.Read(localconfig.DefaultPath); err == nil {
		merged.ServerAddr = lc.ServerAddr
		merged.ClusterToken = lc.ClusterToken
		merged.CACertPEM = lc.CACert
	} else if !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintf(os.Stderr, "warning: could not read %s: %v\n", localconfig.DefaultPath, err)
	}

	cfgPath, err := userConfigPath()
	if err != nil {
		return err
	}
	file, err := readUserConfig(cfgPath)
	if err == nil {
		selected := file.CurrentContext
		if value, ok := os.LookupEnv("TRELLIS_CONTEXT"); ok {
			selected = value
		}
		flags := cmd.Root().PersistentFlags()
		if flags.Changed("context") {
			selected = flagConfig.Context
		}
		if selected != "" {
			ctx, ok := file.Contexts[selected]
			if !ok {
				return fmt.Errorf("context %q is not defined in %s", selected, cfgPath)
			}
			merged.Context = selected
			merged.ServerAddr = ctx.ServerAddr
			merged.ClusterToken = ctx.ClusterToken
			merged.Namespace = ctx.Namespace
			merged.CACert = ""
			merged.CACertPEM = ctx.CACert
			merged.Cert = ctx.Cert
			merged.Key = ctx.Key
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read config file %s: %w", cfgPath, err)
	} else {
		selected := ""
		if value, ok := os.LookupEnv("TRELLIS_CONTEXT"); ok {
			selected = value
		}
		if cmd.Root().PersistentFlags().Changed("context") {
			selected = flagConfig.Context
		}
		if selected != "" {
			return fmt.Errorf("context %q is not defined: %s does not exist", selected, cfgPath)
		}
	}

	if value, ok := os.LookupEnv("TRELLIS_ADDR"); ok {
		merged.ServerAddr = value
	}
	if value, ok := os.LookupEnv("TRELLIS_TOKEN"); ok {
		merged.ClusterToken = value
	}
	if value, ok := os.LookupEnv("TRELLIS_NAMESPACE"); ok {
		merged.Namespace = value
	}
	if value, ok := os.LookupEnv("TRELLIS_CA_CERT"); ok {
		merged.CACert = value
	}
	if value, ok := os.LookupEnv("TRELLIS_CERT"); ok {
		merged.Cert = value
	}
	if value, ok := os.LookupEnv("TRELLIS_KEY"); ok {
		merged.Key = value
	}

	flags := cmd.Root().PersistentFlags()
	if flags.Changed("server-addr") {
		merged.ServerAddr = flagConfig.ServerAddr
	}
	if flags.Changed("token") {
		merged.ClusterToken = flagConfig.ClusterToken
	}
	if flags.Changed("namespace") {
		merged.Namespace = flagConfig.Namespace
	}
	if flags.Changed("ca-cert") {
		merged.CACert = flagConfig.CACert
	}
	if flags.Changed("cert") {
		merged.Cert = flagConfig.Cert
	}
	if flags.Changed("key") {
		merged.Key = flagConfig.Key
	}

	if merged.Output == "" {
		merged.Output = "table"
	}
	if merged.Output != "table" && merged.Output != "json" {
		return fmt.Errorf("invalid output format %q: must be table or json", merged.Output)
	}
	config = merged
	return nil
}

func writeJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
