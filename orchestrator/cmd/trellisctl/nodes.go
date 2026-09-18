package main

import (
	"fmt"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/client"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

func NewNodesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "nodes",
		Short: "Manage cluster nodes",
		Long:  "List, inspect, and maintain cluster nodes. Node commands accept an address, host, full UUID, or unique UUID prefix so routine maintenance does not require copying long internal identifiers.",
	}

	cmd.AddCommand(NewNodesListCmd())
	cmd.AddCommand(NewNodesStatusCmd())
	cmd.AddCommand(NewNodesDrainCmd())
	cmd.AddCommand(NewNodesUndrainCmd())
	cmd.AddCommand(NewNodesRemoveCmd())
	cmd.AddCommand(NewNodesLeadershipTransferCmd())
	return cmd
}

func NewNodesLeadershipTransferCmd() *cobra.Command {
	return &cobra.Command{Use: "transfer-leadership", Args: cobra.NoArgs, Hidden: true, Short: "Transfer control-plane leadership to another voter", RunE: func(cmd *cobra.Command, _ []string) error {
		tlsCfg, err := buildCLITLSConfig()
		if err != nil {
			return err
		}
		if err := client.NewServerClient(config.ClusterToken, config.ServerAddr, tlsCfg).TransferLeadership(cmd.Context()); err != nil {
			return err
		}
		_, err = fmt.Fprintln(cmd.OutOrStdout(), "Control-plane leadership transfer started.")
		return err
	}}
}

func NewNodesUndrainCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "undrain NODE",
		Args:  cobra.ExactArgs(1),
		Short: "Allow scheduling on a drained node",
		RunE: func(cmd *cobra.Command, args []string) error {
			serverClient, node, err := resolveNodeClient(cmd, args[0])
			if err != nil {
				return err
			}
			if err := serverClient.UndrainNode(cmd.Context(), node.ID); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Node %s undrained.\n", nodeDisplay(node))
			return err
		},
	}
}

func NewNodesRemoveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "remove NODE",
		Args:  cobra.ExactArgs(1),
		Short: "Permanently remove a node from the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			serverClient, node, err := resolveNodeClient(cmd, args[0])
			if err != nil {
				return err
			}
			if err := serverClient.RemoveRaftMember(cmd.Context(), node.ID.String()); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Node %s removed from the cluster.\n", nodeDisplay(node))
			return err
		},
	}
}

func NewNodesDrainCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "drain NODE",
		Args:  cobra.ExactArgs(1),
		Short: "Drain a node and migrate its allocations",
		RunE: func(cmd *cobra.Command, args []string) error {
			serverClient, node, err := resolveNodeClient(cmd, args[0])
			if err != nil {
				return err
			}
			if err := serverClient.DrainNode(cmd.Context(), node.ID); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Drain started for %s.\n", nodeDisplay(node))
			return err
		},
	}
}

func NewNodesListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List nodes in the cluster",
		RunE: func(cmd *cobra.Command, _ []string) error {
			tlsCfg, err := buildCLITLSConfig()
			if err != nil {
				return err
			}
			serverClient := client.NewServerClient(config.ClusterToken, config.ServerAddr, tlsCfg)
			nodes, err := serverClient.ListNodes(cmd.Context())
			if err != nil {
				return fmt.Errorf("list nodes: %w", err)
			}
			if config.Output == "json" {
				return writeJSON(cmd.OutOrStdout(), nodes)
			}
			if len(*nodes) == 0 {
				_, err = fmt.Fprintln(cmd.OutOrStdout(), "No nodes")
				return err
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			if _, err := fmt.Fprintln(w, "Node\tID\tStatus\tVersion\tCPU\tMemory\tHeartbeat"); err != nil {
				return err
			}
			for _, node := range *nodes {
				heartbeat := node.LastHeartbeat.Format(time.RFC3339)
				version := node.Version
				if version == "" {
					version = "unknown"
				}
				if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%dm\t%s\t%s\n", nodeDisplay(node), shortID(node.ID.String()), node.Status, version, node.CPU, formatByteCount(node.Memory), heartbeat); err != nil {
					return err
				}
			}
			return w.Flush()
		},
	}
}

func NewNodesStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status NODE",
		Args:  cobra.ExactArgs(1),
		Short: "Inspect a node and its placement-relevant metadata",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, node, err := resolveNodeClient(cmd, args[0])
			if err != nil {
				return err
			}
			if config.Output == "json" {
				return writeJSON(cmd.OutOrStdout(), node)
			}
			return printNodeStatus(cmd.OutOrStdout(), node)
		},
	}
}

func printNodeStatus(w interface{ Write([]byte) (int, error) }, node api.NodeResponse) error {
	version := node.Version
	if version == "" {
		version = "unknown"
	}
	platform := "unknown"
	if node.OS != "" || node.Arch != "" {
		platform = fmt.Sprintf("%s/%s", valueOrUnknown(node.OS), valueOrUnknown(node.Arch))
	}
	if _, err := fmt.Fprintf(w, "Node: %s\nID: %s\nStatus: %s\nVersion: %s\nPlatform: %s\nCPU: %dm\nMemory: %s (%d bytes)\nHeartbeat: %s\n", nodeDisplay(node), node.ID, node.Status, version, platform, node.CPU, formatByteCount(node.Memory), node.Memory, node.LastHeartbeat.Format(time.RFC3339)); err != nil {
		return err
	}

	labelKeys := make([]string, 0, len(node.Labels))
	for key := range node.Labels {
		labelKeys = append(labelKeys, key)
	}
	sort.Strings(labelKeys)
	if len(labelKeys) == 0 {
		if _, err := fmt.Fprintln(w, "Labels: none"); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintln(w, "Labels:"); err != nil {
			return err
		}
		for _, key := range labelKeys {
			if _, err := fmt.Fprintf(w, "  %s=%s\n", key, node.Labels[key]); err != nil {
				return err
			}
		}
	}

	volumes := append([]string(nil), node.Volumes...)
	sort.Strings(volumes)
	if len(volumes) == 0 {
		if _, err := fmt.Fprintln(w, "Volume registrations: none"); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintln(w, "Volume registrations:"); err != nil {
			return err
		}
		for _, volume := range volumes {
			if _, err := fmt.Fprintf(w, "  %s\n", volume); err != nil {
				return err
			}
		}
	}
	if len(node.Capabilities) == 0 {
		_, err := fmt.Fprintln(w, "Capabilities: none")
		return err
	}
	if _, err := fmt.Fprintln(w, "Capabilities:"); err != nil {
		return err
	}
	for _, capability := range node.Capabilities {
		if _, err := fmt.Fprintf(w, "  %s\n", capability); err != nil {
			return err
		}
	}
	return nil
}

func valueOrUnknown(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}

func formatByteCount(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	}
	value := float64(bytes)
	units := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	unit := units[0]
	value /= 1024
	for i := 1; i < len(units) && value >= 1024; i++ {
		value /= 1024
		unit = units[i]
	}
	return fmt.Sprintf("%.1f %s", value, unit)
}

func resolveNodeClient(cmd *cobra.Command, ref string) (*client.ServerClient, api.NodeResponse, error) {
	tlsCfg, err := buildCLITLSConfig()
	if err != nil {
		return nil, api.NodeResponse{}, err
	}
	serverClient := client.NewServerClient(config.ClusterToken, config.ServerAddr, tlsCfg)
	nodes, err := serverClient.ListNodes(cmd.Context())
	if err != nil {
		return nil, api.NodeResponse{}, err
	}
	node, err := resolveNodeReference(*nodes, ref)
	if err != nil {
		return nil, api.NodeResponse{}, err
	}
	return serverClient, node, nil
}

func resolveNodeReference(nodes api.NodeListResponse, ref string) (api.NodeResponse, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return api.NodeResponse{}, fmt.Errorf("node reference is required")
	}
	if id, err := uuid.Parse(ref); err == nil {
		for _, node := range nodes {
			if node.ID == id {
				return node, nil
			}
		}
		return api.NodeResponse{}, fmt.Errorf("node %s is not in the cluster", ref)
	}

	var matches []api.NodeResponse
	for _, node := range nodes {
		address := nodeDisplay(node)
		id := node.ID.String()
		if node.Host == ref || address == ref || strings.HasPrefix(id, ref) {
			matches = append(matches, node)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) == 0 {
		return api.NodeResponse{}, fmt.Errorf("no node matches %q; use 'trellisctl nodes list' to see addresses and ID prefixes", ref)
	}
	labels := make([]string, 0, len(matches))
	for _, node := range matches {
		labels = append(labels, fmt.Sprintf("%s (%s)", nodeDisplay(node), shortID(node.ID.String())))
	}
	sort.Strings(labels)
	return api.NodeResponse{}, fmt.Errorf("node reference %q is ambiguous: %s", ref, strings.Join(labels, ", "))
}

func nodeDisplay(node api.NodeResponse) string {
	return fmt.Sprintf("%s:%d", node.Host, node.Port)
}
