package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/client"
	"github.com/overfold/trellis/internal/execstream"
	"github.com/spf13/cobra"
)

// execResizePollInterval is how often the local terminal size is sampled.
// Sampling is local; only changes are sent on the stream.
const execResizePollInterval = 250 * time.Millisecond

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
		Use:   "exec [flags] ALLOCATION -- COMMAND [ARG...]",
		Short: "Run a command in an allocation task",
		Long: "Run a command in an allocation task over one bidirectional stream. Output is streamed as it is produced and the remote exit status becomes trellisctl's exit status. " +
			"Use -i to forward local stdin, -t to allocate a remote terminal, and -it for an interactive shell.",
		Args:         cobra.MinimumNArgs(2),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("term") && !tty {
				return fmt.Errorf("--term requires --tty")
			}
			request := api.ExecRequest{Task: task, Command: args[1:], TTY: tty, Stdin: attachStdin}
			var terminal *execTerminal
			if tty {
				var err error
				if terminal, err = newExecTerminal(cmd); err != nil {
					return err
				}
				if terminalType == "" {
					terminalType = os.Getenv("TERM")
				}
				if terminalType == "" {
					terminalType = "xterm-256color"
				}
				request.Term = terminalType
				request.Cols, request.Rows = terminal.cols, terminal.rows
			}

			serverClient, err := namespaceClient(cmd.Context())
			if err != nil {
				return err
			}
			return runExec(cmd, serverClient, args[0], request, terminal)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&task, "task", "", "Task name when the allocation contains multiple tasks")
	flags.BoolVarP(&attachStdin, "stdin", "i", false, "Forward local stdin to the command; its end closes the command's stdin")
	flags.BoolVarP(&tty, "tty", "t", false, "Allocate a remote terminal (requires a local terminal on stdin)")
	flags.StringVar(&terminalType, "term", "", "TERM value for the remote TTY (defaults to local TERM, then xterm-256color)")
	return cmd
}

// execTerminal is the local terminal of a TTY session.
type execTerminal struct {
	inputFD, outputFD uintptr
	cols, rows        uint32
}

func newExecTerminal(cmd *cobra.Command) (*execTerminal, error) {
	inputFile, ok := cmd.InOrStdin().(*os.File)
	if !ok || !terminalIsTTY(inputFile.Fd()) {
		return nil, fmt.Errorf("--tty requires stdin to be a terminal")
	}
	terminal := &execTerminal{inputFD: inputFile.Fd()}
	if outputFile, ok := cmd.OutOrStdout().(*os.File); ok {
		terminal.outputFD = outputFile.Fd()
	}
	if cols, rows, ok := terminalSize(terminal.inputFD, terminal.outputFD); ok {
		terminal.cols, terminal.rows = boundTerminalSize(cols, rows)
	}
	return terminal, nil
}

func runExec(cmd *cobra.Command, serverClient *client.ServerClient, allocationID string, request api.ExecRequest, terminal *execTerminal) error {
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()
	stream, err := serverClient.Exec(ctx, allocationID, request)
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()

	if terminal != nil {
		if request.Stdin {
			restore, err := terminalMakeRaw(terminal.inputFD, terminal.outputFD)
			if err != nil {
				return fmt.Errorf("configure local terminal: %w", err)
			}
			defer func() { _ = restore() }()
		}
		go watchTerminalSize(ctx, stream, terminal)
	}
	if request.Stdin {
		go forwardExecInput(stream, cmd.InOrStdin())
	}

	code, err := stream.Wait(cmd.OutOrStdout(), cmd.ErrOrStderr())
	if err != nil {
		return err
	}
	if code != 0 {
		return &execExitError{code: code}
	}
	return nil
}

// forwardExecInput sends local input until it ends, then closes the remote
// command's stdin. A failed send means the stream has ended, which Wait
// reports.
func forwardExecInput(stream *client.ExecStream, input io.Reader) {
	buffer := make([]byte, execstream.MaxPayload)
	for {
		n, err := input.Read(buffer)
		if n > 0 {
			if _, writeErr := stream.Write(buffer[:n]); writeErr != nil {
				return
			}
		}
		if err != nil {
			_ = stream.CloseStdin()
			return
		}
	}
}

func watchTerminalSize(ctx context.Context, stream *client.ExecStream, terminal *execTerminal) {
	ticker := time.NewTicker(execResizePollInterval)
	defer ticker.Stop()
	lastCols, lastRows := terminal.cols, terminal.rows
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cols, rows, ok := terminalSize(terminal.inputFD, terminal.outputFD)
			if !ok {
				continue
			}
			cols, rows = boundTerminalSize(cols, rows)
			if cols == lastCols && rows == lastRows {
				continue
			}
			if err := stream.Resize(cols, rows); err != nil {
				return
			}
			lastCols, lastRows = cols, rows
		}
	}
}

func boundTerminalSize(cols, rows uint32) (uint32, uint32) {
	return min(max(cols, 1), execstream.MaxTerminalDimension), min(max(rows, 1), execstream.MaxTerminalDimension)
}
