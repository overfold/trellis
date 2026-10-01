package main

import (
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/overfold/trellis/orchestrator/api"
	"github.com/spf13/cobra"
)

func NewCredentialsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "credentials",
		Short: "Manage scoped operator API credentials",
		Long:  "Mint, list, and revoke scoped operator API credentials. These commands require the administrator signing key; ordinary cluster/write operator credentials cannot manage credentials.",
	}
	cmd.AddCommand(newCredentialsCreateCmd())
	cmd.AddCommand(newCredentialsListCmd())
	cmd.AddCommand(newCredentialsRevokeCmd())
	return cmd
}

func newCredentialsCreateCmd() *cobra.Command {
	var scope, access string
	var ttl time.Duration
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a scoped operator API credential",
		Long:  "Create an operator credential with explicit cluster scope and read or write access, optionally expiring after --ttl. The token is printed once; the cluster stores only its hash. The caller must authenticate with the administrator signing key.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if scope != "cluster" {
				return fmt.Errorf("--scope must be cluster")
			}
			if access != "read" && access != "write" {
				return fmt.Errorf("--access must be read or write")
			}
			ttlSeconds, err := ttlSeconds(ttl)
			if err != nil {
				return err
			}
			serverClient, err := administratorServerClient()
			if err != nil {
				return err
			}
			response, err := serverClient.CreateCredential(cmd.Context(), &api.CredentialCreateRequest{
				Scope: scope, Access: access, TTLSeconds: ttlSeconds,
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
	cmd.Flags().StringVar(&scope, "scope", "", "Credential scope (cluster)")
	cmd.Flags().StringVar(&access, "access", "", "Credential access (read or write)")
	cmd.Flags().DurationVar(&ttl, "ttl", 0, "Expire the credential after this duration, such as 720h (default: no expiry)")
	return cmd
}

func newCredentialsListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List operator API credentials",
		Long:  "List operator credential metadata, including expired credentials. Tokens are never shown; the cluster stores only their hashes.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			serverClient, err := administratorServerClient()
			if err != nil {
				return err
			}
			credentials, err := serverClient.ListCredentials(cmd.Context())
			if err != nil {
				return err
			}
			if config.Output == "json" {
				return writeJSON(cmd.OutOrStdout(), credentials)
			}
			if len(credentials) == 0 {
				_, err = fmt.Fprintln(cmd.OutOrStdout(), "No credentials")
				return err
			}
			now := time.Now()
			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			if _, err := fmt.Fprintln(w, "ID\tScope\tAccess\tCreated\tExpires"); err != nil {
				return err
			}
			for _, credential := range credentials {
				if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", credential.ID, credential.Scope, credential.Access, credential.CreatedAt.Format(time.RFC3339), formatExpiry(credential.ExpiresAt, now)); err != nil {
					return err
				}
			}
			return w.Flush()
		},
	}
}

func newCredentialsRevokeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "revoke ID",
		Short: "Revoke an operator API credential",
		Long:  "Revoke an operator credential by the ID shown by 'trellisctl credentials list'. Requests using it are rejected immediately.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			serverClient, err := administratorServerClient()
			if err != nil {
				return err
			}
			if err := serverClient.RevokeCredential(cmd.Context(), args[0]); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Credential %s revoked.\n", args[0])
			return err
		},
	}
}

// ttlSeconds converts a --ttl flag to whole seconds for the API. Zero keeps
// the server's default.
func ttlSeconds(ttl time.Duration) (int64, error) {
	if ttl < 0 || (ttl > 0 && ttl < time.Second) || ttl%time.Second != 0 {
		return 0, fmt.Errorf("--ttl must be a positive whole number of seconds, such as 90s, 1h, or 720h")
	}
	return int64(ttl / time.Second), nil
}

func formatExpiry(expiresAt *time.Time, now time.Time) string {
	if expiresAt == nil {
		return "never"
	}
	if !now.Before(*expiresAt) {
		return expiresAt.Format(time.RFC3339) + " (expired)"
	}
	return expiresAt.Format(time.RFC3339)
}
