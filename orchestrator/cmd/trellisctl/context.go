package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func NewContextCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "context",
		Short: "Manage named cluster contexts",
		Long:  "Save and select named cluster connections so normal Trellis commands do not need repeated address, token, namespace, or TLS flags.",
	}
	cmd.AddCommand(newContextListCmd())
	cmd.AddCommand(newContextCurrentCmd())
	cmd.AddCommand(newContextShowCmd())
	cmd.AddCommand(newContextSaveCmd())
	cmd.AddCommand(newContextUseCmd())
	cmd.AddCommand(newContextDeleteCmd())
	return cmd
}

func newContextListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List saved contexts",
		RunE: func(cmd *cobra.Command, _ []string) error {
			path, err := userConfigPath()
			if err != nil {
				return err
			}
			file, err := readUserConfig(path)
			if os.IsNotExist(err) {
				_, err = fmt.Fprintln(cmd.OutOrStdout(), "No saved contexts")
				return err
			}
			if err != nil {
				return err
			}
			if len(file.Contexts) == 0 {
				_, err = fmt.Fprintln(cmd.OutOrStdout(), "No saved contexts")
				return err
			}
			names := make([]string, 0, len(file.Contexts))
			for name := range file.Contexts {
				names = append(names, name)
			}
			sort.Strings(names)
			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			if _, err := fmt.Fprintln(w, "Current\tName\tAddress\tNamespace\tTLS"); err != nil {
				return err
			}
			for _, name := range names {
				ctx := file.Contexts[name]
				current := ""
				if name == file.CurrentContext {
					current = "*"
				}
				ns := ctx.Namespace
				if ns == "" {
					ns = "(cluster)"
				}
				tlsState := "no"
				if ctx.CACert != "" || ctx.CACertFile != "" || ctx.Cert != "" || ctx.Key != "" {
					tlsState = "yes"
				}
				if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", current, name, ctx.ServerAddr, ns, tlsState); err != nil {
					return err
				}
			}
			return w.Flush()
		},
	}
}

func newContextCurrentCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "current",
		Short: "Show the active context and effective connection",
		RunE: func(cmd *cobra.Command, _ []string) error {
			name := config.Context
			if name == "" {
				name = "(none)"
			}
			ns := config.Namespace
			if ns == "" {
				ns = "(cluster)"
			}
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "Context: %s\nAddress: %s\nNamespace: %s\nToken: %s\n", name, config.ServerAddr, ns, configured(config.ClusterToken))
			return err
		},
	}
}

func newContextShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show [NAME]",
		Args:  cobra.MaximumNArgs(1),
		Short: "Show a saved context without revealing its token",
		RunE: func(cmd *cobra.Command, args []string) error {
			name := config.Context
			if len(args) == 1 {
				name = args[0]
			}
			if name == "" {
				return fmt.Errorf("no context selected; pass a name or use 'trellis context use NAME'")
			}
			path, err := userConfigPath()
			if err != nil {
				return err
			}
			file, err := readUserConfig(path)
			if err != nil {
				return err
			}
			ctx, ok := file.Contexts[name]
			if !ok {
				return fmt.Errorf("context %q not found", name)
			}
			ns := ctx.Namespace
			if ns == "" {
				ns = "(cluster)"
			}
			ca := "not configured"
			if ctx.CACertFile != "" {
				ca = "file: " + ctx.CACertFile
			} else if ctx.CACert != "" {
				ca = "embedded"
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Name: %s\nAddress: %s\nNamespace: %s\nToken: %s\nCA: %s\nClient certificate: %s\n", name, ctx.ServerAddr, ns, configured(ctx.ClusterToken), ca, configured(ctx.Cert))
			return err
		},
	}
}

func newContextSaveCmd() *cobra.Command {
	var selectContext bool
	cmd := &cobra.Command{
		Use:   "save NAME",
		Args:  cobra.ExactArgs(1),
		Short: "Save the effective connection as a named context",
		Long:  "Save the currently effective address, token, namespace, and TLS configuration. A CA supplied with --ca-cert is saved as an absolute file path; an inline CA is embedded. Combine this with global flags for first-time setup, for example:\n\n  trellisctl --server-addr cluster.example:8128 --token TOKEN --namespace default --ca-cert ./cluster-ca.pem context save production --use",
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if err := validateContextName(name); err != nil {
				return err
			}
			path, err := userConfigPath()
			if err != nil {
				return err
			}
			file, err := readUserConfig(path)
			if os.IsNotExist(err) {
				file = fileConfig{}
			} else if err != nil {
				return err
			}
			if file.Contexts == nil {
				file.Contexts = make(map[string]contextFileConfig)
			}
			ctx, err := effectiveContextFileConfig()
			if err != nil {
				return err
			}
			file.Contexts[name] = ctx
			if selectContext {
				file.CurrentContext = name
			}
			if err := writeUserConfig(path, file); err != nil {
				return err
			}
			if selectContext {
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "Saved and selected context %s.\n", name)
			} else {
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "Saved context %s. Select it with 'trellis context use %s'.\n", name, name)
			}
			return err
		},
	}
	cmd.Flags().BoolVar(&selectContext, "use", false, "Select this context after saving it")
	return cmd
}

func newContextUseCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "use NAME",
		Args:  cobra.ExactArgs(1),
		Short: "Select the default context",
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			path, err := userConfigPath()
			if err != nil {
				return err
			}
			file, err := readUserConfig(path)
			if err != nil {
				return err
			}
			if _, ok := file.Contexts[name]; !ok {
				return fmt.Errorf("context %q not found", name)
			}
			file.CurrentContext = name
			if err := writeUserConfig(path, file); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Using context %s.\n", name)
			return err
		},
	}
}

func newContextDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete NAME",
		Args:  cobra.ExactArgs(1),
		Short: "Delete a saved context",
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			path, err := userConfigPath()
			if err != nil {
				return err
			}
			file, err := readUserConfig(path)
			if err != nil {
				return err
			}
			if _, ok := file.Contexts[name]; !ok {
				return fmt.Errorf("context %q not found", name)
			}
			delete(file.Contexts, name)
			if file.CurrentContext == name {
				file.CurrentContext = ""
			}
			if err := writeUserConfig(path, file); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Deleted context %s.\n", name)
			return err
		},
	}
}

func configured(value string) string {
	if value == "" {
		return "not configured"
	}
	return "configured"
}

func validateContextName(name string) error {
	if name == "" {
		return fmt.Errorf("context name is required")
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return fmt.Errorf("context name %q contains unsupported character %q", name, r)
	}
	return nil
}

func effectiveContextFileConfig() (contextFileConfig, error) {
	caPEM := config.CACertPEM
	caFile := ""
	if config.CACert != "" {
		if _, err := os.ReadFile(config.CACert); err != nil {
			return contextFileConfig{}, fmt.Errorf("read CA cert: %w", err)
		}
		var err error
		caFile, err = filepath.Abs(config.CACert)
		if err != nil {
			return contextFileConfig{}, fmt.Errorf("resolve CA cert path: %w", err)
		}
		caPEM = ""
	}
	return contextFileConfig{
		ServerAddr:   config.ServerAddr,
		ClusterToken: config.ClusterToken,
		Namespace:    config.Namespace,
		CACert:       caPEM,
		CACertFile:   caFile,
		Cert:         config.Cert,
		Key:          config.Key,
	}, nil
}

func userConfigPath() (string, error) {
	if path := os.Getenv("TRELLIS_CONFIG"); path != "" {
		return path, nil
	}
	configDir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("determine config directory: %w", err)
	}
	return filepath.Join(configDir, "trellis", "config.yaml"), nil
}

func readUserConfig(path string) (fileConfig, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return fileConfig{}, err
	}
	var file fileConfig
	if err := yaml.Unmarshal(content, &file); err != nil {
		return fileConfig{}, fmt.Errorf("parse config file %s: %w", path, err)
	}
	for name, ctx := range file.Contexts {
		if ctx.CACert != "" && ctx.CACertFile != "" {
			return fileConfig{}, fmt.Errorf("context %q in %s must use only one of ca_cert and ca_cert_file", name, path)
		}
	}
	return file, nil
}

func writeUserConfig(path string, file fileConfig) error {
	content, err := yaml.Marshal(&file)
	if err != nil {
		return fmt.Errorf("encode config: %w", err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".trellis-config-*")
	if err != nil {
		return fmt.Errorf("create temporary config: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("protect config file: %w", err)
	}
	if _, err := tmp.Write(content); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write config: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close config: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace config file: %w", err)
	}
	return nil
}
