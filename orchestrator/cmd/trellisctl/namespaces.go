package main

import (
	"fmt"

	"github.com/overfold/trellis/internal/client"
	"github.com/spf13/cobra"
)

func NewNamespacesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "namespaces",
		Short: "Discover namespaces known to the cluster",
		Long:  "List namespace names currently referenced by desired jobs. Namespaces are not lifecycle-managed resources; applying a job to a new namespace creates no separate namespace object.",
	}
	cmd.AddCommand(NewNamespacesListCmd())
	return cmd
}

func NewNamespacesListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List namespaces visible to the current credential",
		RunE: func(cmd *cobra.Command, _ []string) error {
			tlsCfg, err := buildCLITLSConfig()
			if err != nil {
				return err
			}
			namespaces, err := client.NewServerClient(config.ClusterToken, config.ServerAddr, tlsCfg).ListNamespaces(cmd.Context())
			if err != nil {
				return err
			}
			if config.Output == "json" {
				return writeJSON(cmd.OutOrStdout(), namespaces)
			}
			if len(*namespaces) == 0 {
				_, err = fmt.Fprintln(cmd.OutOrStdout(), "No known namespaces")
				return err
			}
			for _, namespace := range *namespaces {
				if _, err := fmt.Fprintln(cmd.OutOrStdout(), namespace); err != nil {
					return err
				}
			}
			return nil
		},
	}
}
