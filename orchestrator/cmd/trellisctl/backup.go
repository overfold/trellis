// Command trellis provides the Trellis command-line client.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/clofour/trellis/internal/api"
	"github.com/spf13/cobra"
)

const maxBackupFileSize = 64 << 20

func NewBackupCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "backup", Short: "Create and restore desired-state backups"}
	cmd.AddCommand(newBackupCreateCmd(), newBackupRestoreCmd())
	return cmd
}

func newBackupCreateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "create FILE",
		Short: "Take a live backup of jobs and encrypted secrets",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			serverClient, err := administratorServerClient()
			if err != nil {
				return err
			}
			snapshot, err := serverClient.CreateBackup(cmd.Context())
			if err != nil {
				return err
			}
			data, err := json.MarshalIndent(snapshot, "", "  ")
			if err != nil {
				return fmt.Errorf("encode backup: %w", err)
			}
			data = append(data, '\n')
			if args[0] == "-" {
				_, err = cmd.OutOrStdout().Write(data)
				return err
			}
			return writeBackupFile(args[0], data)
		},
	}
}

func writeBackupFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".trellis-backup-*")
	if err != nil {
		return fmt.Errorf("create backup file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write backup: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync backup: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close backup: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("install backup: %w", err)
	}
	return nil
}

func newBackupRestoreCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "restore FILE",
		Short: "Restore jobs and encrypted secrets into a fresh cluster",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var reader io.Reader
			if args[0] == "-" {
				reader = cmd.InOrStdin()
			} else {
				file, err := os.Open(args[0])
				if err != nil {
					return fmt.Errorf("open backup: %w", err)
				}
				defer func() { _ = file.Close() }()
				reader = file
			}
			var snapshot api.BackupSnapshot
			decoder := json.NewDecoder(io.LimitReader(reader, maxBackupFileSize+1))
			if err := decoder.Decode(&snapshot); err != nil {
				return fmt.Errorf("decode backup: %w", err)
			}
			if err := ensureJSONEOF(decoder); err != nil {
				return err
			}
			serverClient, err := administratorServerClient()
			if err != nil {
				return err
			}
			if err := serverClient.RestoreBackup(cmd.Context(), &snapshot); err != nil {
				return err
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), "Backup restored successfully; jobs will be scheduled fresh.")
			return err
		},
	}
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("decode backup: multiple JSON values")
		}
		return fmt.Errorf("decode backup: %w", err)
	}
	return nil
}
