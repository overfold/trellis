package main

import (
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func NewSecretsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "secrets", Short: "Manage namespace-scoped secrets"}
	cmd.AddCommand(newSecretsSetCmd(), newSecretsListCmd(), newSecretsDescribeCmd(), newSecretsDeleteCmd())
	return cmd
}

func newSecretsSetCmd() *cobra.Command {
	var file string
	var stdin bool
	var expected uint64
	cmd := &cobra.Command{Use: "set NAME", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if (file == "") == !stdin {
			return fmt.Errorf("exactly one of --file or --stdin is required")
		}
		var value []byte
		var err error
		if stdin {
			value, err = readSecret(cmd.InOrStdin())
		} else {
			value, err = readSecretFile(file)
		}
		if err != nil {
			return fmt.Errorf("read secret: %w", err)
		}
		defer clear(value)
		if len(value) > 64<<10 {
			return fmt.Errorf("secret exceeds 65536 bytes")
		}
		c, err := namespaceClient(cmd.Context())
		if err != nil {
			return err
		}
		var expectedPtr *uint64
		if cmd.Flags().Changed("expected-version") {
			expectedPtr = &expected
		}
		meta, err := c.SetSecret(cmd.Context(), args[0], value, expectedPtr)
		if err != nil {
			return err
		}
		if config.Output == "json" {
			return writeJSON(cmd.OutOrStdout(), meta)
		}
		_, err = fmt.Fprintf(cmd.OutOrStdout(), "Secret %s stored (version %d).\n", meta.Name, meta.Version)
		return err
	}}
	cmd.Flags().StringVar(&file, "file", "", "Read the secret value from a file")
	cmd.Flags().BoolVar(&stdin, "stdin", false, "Read the secret value from standard input")
	cmd.Flags().Uint64Var(&expected, "expected-version", 0, "Require the current version (0 creates only)")
	return cmd
}

func readSecret(reader io.Reader) ([]byte, error) {
	value, err := io.ReadAll(io.LimitReader(reader, (64<<10)+1))
	if err != nil {
		clear(value)
		return nil, err
	}
	if len(value) > 64<<10 {
		clear(value)
		return nil, fmt.Errorf("secret exceeds 65536 bytes")
	}
	return value, nil
}

func readSecretFile(path string) ([]byte, error) {
	input, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = input.Close() }()
	info, err := input.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() > 64<<10 {
		return nil, fmt.Errorf("secret exceeds 65536 bytes")
	}
	return readSecret(input)
}

func newSecretsListCmd() *cobra.Command {
	return &cobra.Command{Use: "list", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		c, err := namespaceClient(cmd.Context())
		if err != nil {
			return err
		}
		items, err := c.ListSecrets(cmd.Context())
		if err != nil {
			return err
		}
		if config.Output == "json" {
			return writeJSON(cmd.OutOrStdout(), items)
		}
		w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
		if _, err := fmt.Fprintln(w, "Name\tVersion\tUpdated\tKey"); err != nil {
			return err
		}
		for _, item := range *items {
			if _, err := fmt.Fprintf(w, "%s\t%d\t%s\t%s\n", item.Name, item.Version, item.UpdatedAt.Format("2006-01-02T15:04:05Z"), item.KeyID); err != nil {
				return err
			}
		}
		return w.Flush()
	}}
}

func newSecretsDescribeCmd() *cobra.Command {
	return &cobra.Command{Use: "describe NAME", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		c, err := namespaceClient(cmd.Context())
		if err != nil {
			return err
		}
		meta, err := c.GetSecretMetadata(cmd.Context(), args[0])
		if err != nil {
			return err
		}
		if config.Output == "json" {
			return writeJSON(cmd.OutOrStdout(), meta)
		}
		_, err = fmt.Fprintf(cmd.OutOrStdout(), "Name: %s\nNamespace: %s\nVersion: %d\nUpdated: %s\nKey: %s\n", meta.Name, meta.Namespace, meta.Version, meta.UpdatedAt.Format("2006-01-02T15:04:05Z"), meta.KeyID)
		return err
	}}
}

func newSecretsDeleteCmd() *cobra.Command {
	return &cobra.Command{Use: "delete NAME", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		c, err := namespaceClient(cmd.Context())
		if err != nil {
			return err
		}
		if err := c.DeleteSecret(cmd.Context(), args[0]); err != nil {
			return err
		}
		_, err = fmt.Fprintln(cmd.OutOrStdout(), "Secret deleted. Running allocations retain values already delivered.")
		return err
	}}
}
