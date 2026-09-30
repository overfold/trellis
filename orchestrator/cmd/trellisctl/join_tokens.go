package main

import (
	"fmt"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/overfold/trellis/internal/api"
	"github.com/spf13/cobra"
)

// NewNodesJoinTokenCmd manages the administrator-minted tokens that new nodes
// present once to enroll in a managed-mode cluster.
func NewNodesJoinTokenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "join-token",
		Short: "Manage node join tokens",
		Long:  "Mint, list, and revoke the short-lived tokens new nodes use to enroll in a managed-mode cluster. These commands require the administrator signing key.",
	}
	cmd.AddCommand(newJoinTokenCreateCmd())
	cmd.AddCommand(newJoinTokenListCmd())
	cmd.AddCommand(newJoinTokenRevokeCmd())
	return cmd
}

func newJoinTokenCreateCmd() *cobra.Command {
	var ttl time.Duration
	var maxUses int
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a node join token",
		Long:  "Create a join token for enrolling new nodes. It expires after --ttl (default 1h, at most 168h) and, with --max-uses, after that many enrollments. The token is printed once; the cluster stores only its hash.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			seconds, err := ttlSeconds(ttl)
			if err != nil {
				return err
			}
			if maxUses < 0 {
				return fmt.Errorf("--max-uses must not be negative")
			}
			serverClient, err := administratorServerClient()
			if err != nil {
				return err
			}
			response, err := serverClient.CreateJoinToken(cmd.Context(), &api.JoinTokenCreateRequest{TTLSeconds: seconds, MaxUses: maxUses})
			if err != nil {
				return err
			}
			if config.Output == "json" {
				return writeJSON(cmd.OutOrStdout(), response)
			}
			if _, err := fmt.Fprintln(cmd.OutOrStdout(), response.Token); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.ErrOrStderr(), "Join token %s expires at %s (%s).\n", response.ID, response.ExpiresAt.Format(time.RFC3339), formatJoinTokenUses(response.JoinTokenResponse))
			return err
		},
	}
	cmd.Flags().DurationVar(&ttl, "ttl", 0, "Token lifetime, such as 30m or 24h (default 1h, maximum 168h)")
	cmd.Flags().IntVar(&maxUses, "max-uses", 0, "Maximum number of nodes that may enroll with the token (default: unlimited until expiry)")
	return cmd
}

func newJoinTokenListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List unexpired node join tokens",
		Long:  "List unexpired join token metadata. Tokens are never shown; the cluster stores only their hashes.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			serverClient, err := administratorServerClient()
			if err != nil {
				return err
			}
			tokens, err := serverClient.ListJoinTokens(cmd.Context())
			if err != nil {
				return err
			}
			if config.Output == "json" {
				return writeJSON(cmd.OutOrStdout(), tokens)
			}
			if len(tokens) == 0 {
				_, err = fmt.Fprintln(cmd.OutOrStdout(), "No join tokens")
				return err
			}
			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			if _, err := fmt.Fprintln(w, "ID\tCreated\tExpires\tUses"); err != nil {
				return err
			}
			for _, token := range tokens {
				if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", token.ID, token.CreatedAt.Format(time.RFC3339), token.ExpiresAt.Format(time.RFC3339), formatJoinTokenUses(token)); err != nil {
					return err
				}
			}
			return w.Flush()
		},
	}
}

func newJoinTokenRevokeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "revoke ID",
		Short: "Revoke a node join token",
		Long:  "Revoke a join token by the ID shown by 'trellisctl nodes join-token list'. Nodes that already enrolled with it are not affected; remove them with 'trellisctl nodes remove'.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			serverClient, err := administratorServerClient()
			if err != nil {
				return err
			}
			if err := serverClient.RevokeJoinToken(cmd.Context(), args[0]); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Join token %s revoked.\n", args[0])
			return err
		},
	}
}

func formatJoinTokenUses(token api.JoinTokenResponse) string {
	if token.MaxUses == 0 {
		return strconv.Itoa(token.Uses) + " used, unlimited"
	}
	return fmt.Sprintf("%d of %d used", token.Uses, token.MaxUses)
}
