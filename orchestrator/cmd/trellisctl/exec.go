package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/client"
	"github.com/spf13/cobra"
)

const (
	execOutputPollInterval = 40 * time.Millisecond
	execResizePollInterval = 250 * time.Millisecond
)

type execExitError struct {
	code int
}

func (e *execExitError) Error() string {
	return fmt.Sprintf("remote command exited with status %d", e.code)
}

func NewExecCmd() *cobra.Command {
	var task string
	var attachStdin bool
	var tty bool
	var terminalType string

	cmd := &cobra.Command{
		Use:          "exec [flags] ALLOCATION -- COMMAND [ARG...]",
		Short:        "Run a command in an allocation task",
		Long:         "Run a command in an allocation task. By default the command is non-interactive. Use -it or --tty to attach a real terminal backed by Trellis's persistent exec session API.",
		Args:         cobra.MinimumNArgs(2),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if attachStdin && !tty {
				return fmt.Errorf("--stdin requires --tty because interactive Trellis exec sessions are TTY-backed")
			}
			if cmd.Flags().Changed("term") && !tty {
				return fmt.Errorf("--term requires --tty")
			}

			tlsCfg, err := buildCLITLSConfig()
			if err != nil {
				return err
			}
			serverClient := client.NewNamespaceServerClient(config.ClusterToken, config.ServerAddr, config.Namespace, tlsCfg)
			allocationID := args[0]
			command := args[1:]

			if !tty {
				return runExecCommand(cmd, serverClient, allocationID, task, command)
			}

			if terminalType == "" {
				terminalType = os.Getenv("TERM")
			}
			if terminalType == "" {
				terminalType = "xterm-256color"
			}
			return runExecTerminal(cmd, serverClient, allocationID, task, command, terminalType)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&task, "task", "", "Task name when the allocation contains multiple tasks")
	flags.BoolVarP(&attachStdin, "stdin", "i", false, "Attach stdin to a TTY session (requires --tty)")
	flags.BoolVarP(&tty, "tty", "t", false, "Allocate a TTY, attach stdin, and use an interactive exec session")
	flags.StringVar(&terminalType, "term", "", "TERM value for the remote TTY (defaults to local TERM, then xterm-256color)")
	return cmd
}

func runExecCommand(cmd *cobra.Command, serverClient *client.ServerClient, allocationID, task string, command []string) error {
	result, err := serverClient.ExecAllocation(cmd.Context(), allocationID, task, command)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(cmd.OutOrStdout(), result.Stdout); err != nil {
		return fmt.Errorf("write command stdout: %w", err)
	}
	if _, err := io.WriteString(cmd.ErrOrStderr(), result.Stderr); err != nil {
		return fmt.Errorf("write command stderr: %w", err)
	}
	if result.ExitCode != 0 {
		return &execExitError{code: result.ExitCode}
	}
	return nil
}

func runExecTerminal(cmd *cobra.Command, serverClient *client.ServerClient, allocationID, task string, command []string, terminalType string) error {
	input := cmd.InOrStdin()
	inputFile, ok := input.(*os.File)
	if !ok || !terminalIsTTY(inputFile.Fd()) {
		return fmt.Errorf("--tty requires stdin to be a terminal")
	}

	var outputFD uintptr
	if outputFile, ok := cmd.OutOrStdout().(*os.File); ok {
		outputFD = outputFile.Fd()
	}

	cols, rows, hasSize := terminalSize(inputFile.Fd(), outputFD)
	if hasSize {
		cols, rows = boundTerminalSize(cols, rows)
	}

	request := &api.ExecSessionCreateRequest{
		Task:    task,
		Command: command,
		Term:    terminalType,
	}
	if hasSize {
		request.Cols = cols
		request.Rows = rows
	}
	session, err := serverClient.CreateExecSession(cmd.Context(), allocationID, request)
	if err != nil {
		return err
	}

	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = serverClient.CloseExecSession(closeCtx, allocationID, session.ID)
	}()

	restore, err := terminalMakeRaw(inputFile.Fd(), outputFD)
	if err != nil {
		return fmt.Errorf("configure local terminal: %w", err)
	}
	defer func() { _ = restore() }()

	resizeCtx, cancelResize := context.WithCancel(cmd.Context())
	defer cancelResize()
	go watchTerminalSize(resizeCtx, serverClient, allocationID, session.ID, inputFile.Fd(), outputFD, cols, rows)

	inputErrors := make(chan error, 1)
	go forwardTerminalInput(cmd.Context(), serverClient, allocationID, session.ID, input, inputErrors)

	var offset int64
	for {
		response, err := serverClient.ReadExecSession(cmd.Context(), allocationID, session.ID, offset)
		if err != nil {
			return err
		}
		if response.NextOffset < offset {
			return fmt.Errorf("exec session returned invalid output offset %d after %d", response.NextOffset, offset)
		}
		if response.DataBase64 != "" {
			data, err := base64.StdEncoding.DecodeString(response.DataBase64)
			if err != nil {
				return fmt.Errorf("decode terminal output: %w", err)
			}
			if _, err := cmd.OutOrStdout().Write(data); err != nil {
				return fmt.Errorf("write terminal output: %w", err)
			}
		}
		offset = response.NextOffset

		if response.Exited {
			if response.ExitCode != nil && *response.ExitCode != 0 {
				return &execExitError{code: *response.ExitCode}
			}
			return nil
		}

		if response.DataBase64 != "" {
			select {
			case err := <-inputErrors:
				if err != nil {
					return err
				}
				inputErrors = nil
			default:
			}
			continue
		}

		timer := time.NewTimer(execOutputPollInterval)
		select {
		case err := <-inputErrors:
			if !timer.Stop() {
				<-timer.C
			}
			if err != nil {
				return err
			}
			inputErrors = nil
		case <-cmd.Context().Done():
			if !timer.Stop() {
				<-timer.C
			}
			return cmd.Context().Err()
		case <-timer.C:
		}
	}
}

func forwardTerminalInput(ctx context.Context, serverClient *client.ServerClient, allocationID, sessionID string, input io.Reader, result chan<- error) {
	buffer := make([]byte, 32*1024)
	for {
		n, err := input.Read(buffer)
		if n > 0 {
			request := &api.ExecSessionInputRequest{
				DataBase64: base64.StdEncoding.EncodeToString(buffer[:n]),
			}
			if writeErr := serverClient.WriteExecSession(ctx, allocationID, sessionID, request); writeErr != nil {
				result <- fmt.Errorf("send terminal input: %w", writeErr)
				return
			}
		}
		if err != nil {
			if err == io.EOF {
				result <- nil
			} else {
				result <- fmt.Errorf("read terminal input: %w", err)
			}
			return
		}
		select {
		case <-ctx.Done():
			result <- ctx.Err()
			return
		default:
		}
	}
}

func watchTerminalSize(ctx context.Context, serverClient *client.ServerClient, allocationID, sessionID string, inputFD, outputFD uintptr, lastCols, lastRows uint32) {
	ticker := time.NewTicker(execResizePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cols, rows, ok := terminalSize(inputFD, outputFD)
			if !ok {
				continue
			}
			cols, rows = boundTerminalSize(cols, rows)
			if cols == lastCols && rows == lastRows {
				continue
			}
			request := &api.ExecSessionResizeRequest{Cols: cols, Rows: rows}
			if err := serverClient.ResizeExecSession(ctx, allocationID, sessionID, request); err != nil {
				continue
			}
			lastCols, lastRows = cols, rows
		}
	}
}

func boundTerminalSize(cols, rows uint32) (uint32, uint32) {
	if cols == 0 {
		cols = 1
	}
	if rows == 0 {
		rows = 1
	}
	if cols > 1000 {
		cols = 1000
	}
	if rows > 1000 {
		rows = 1000
	}
	return cols, rows
}
