package main

import (
	"fmt"

	"github.com/clofour/trellis/internal/api"
	"github.com/spf13/cobra"
)

func NewCredentialsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "credentials",
		Short: "Mint scoped operator API credentials",
		Long:  "Mint scoped operator API credentials. This command requires the administrator signing key; ordinary cluster/write operator credentials cannot mint additional credentials.",
	}
	cmd.AddCommand(newCredentialsCreateCmd())
	return cmd
}

func newCredentialsCreateCmd() *cobra.Command {
	var scope, access, namespace string
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a scoped operator API credential",
		Long:  "Create an operator credential with namespace or cluster scope and read or write access. The caller must authenticate with the administrator signing key.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if scope != "cluster" && scope != "namespace" {
				return fmt.Errorf("--scope must be cluster or namespace")
			}
			if access != "read" && access != "write" {
				return fmt.Errorf("--access must be read or write")
			}
			if scope == "namespace" && namespace == "" {
				return fmt.Errorf("--namespace-scope is required for namespace scope")
			}
			if scope == "cluster" && namespace != "" {
				return fmt.Errorf("--namespace-scope cannot be used with cluster scope")
			}
			serverClient, err := administratorServerClient()
			if err != nil {
				return err
			}
			response, err := serverClient.CreateCredential(cmd.Context(), &api.CredentialCreateRequest{
				Scope: scope, Access: access, Namespace: namespace,
			})
			if err != nil {
				return err
			}
			if config.Output == "json" {
				return writeJSON(cmd.OutOrStdout(), response)
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), response.Token)
			return err
		},
	}
	cmd.Flags().StringVar(&scope, "scope", "", "Credential scope (cluster or namespace)")
	cmd.Flags().StringVar(&access, "access", "", "Credential access (read or write)")
	cmd.Flags().StringVar(&namespace, "namespace-scope", "", "Namespace for namespace-scoped credentials")
	return cmd
}
