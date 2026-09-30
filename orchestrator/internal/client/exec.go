package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/execstream"
)

// ExecError reports a stream that the server ended with an error frame
// instead of an exit status.
type ExecError struct {
	Message string
}

func (e *ExecError) Error() string { return e.Message }

// ExecStream is an open exec session: one bidirectional stream carrying
// input, output, resizes, and the exit status. Input methods may be called
// concurrently with Wait.
type ExecStream struct {
	conn   io.ReadWriteCloser
	reader *execstream.Reader
	writer *execstream.Writer
}

func newExecStream(conn io.ReadWriteCloser) *ExecStream {
	return &ExecStream{conn: conn, reader: execstream.NewReader(conn), writer: execstream.NewWriter(conn, 0)}
}

// Write sends process input.
func (s *ExecStream) Write(p []byte) (int, error) {
	if err := s.writer.WriteData(execstream.FrameStdin, p); err != nil {
		return 0, fmt.Errorf("send exec input: %w", err)
	}
	return len(p), nil
}

// CloseStdin ends process input.
func (s *ExecStream) CloseStdin() error {
	if err := s.writer.WriteFrame(execstream.FrameStdinClose, nil); err != nil {
		return fmt.Errorf("close exec input: %w", err)
	}
	return nil
}

// Resize changes the terminal size of a TTY session.
func (s *ExecStream) Resize(cols, rows uint32) error {
	if err := s.writer.WriteJSON(execstream.FrameResize, api.ExecResize{Cols: cols, Rows: rows}); err != nil {
		return fmt.Errorf("resize exec terminal: %w", err)
	}
	return nil
}

// Wait copies process output to stdout and stderr until the stream ends and
// returns the process's exit status. An error frame returns an *ExecError; a
// stream that ends without an exit status returns an error.
func (s *ExecStream) Wait(stdout, stderr io.Writer) (int, error) {
	for {
		frame, err := s.reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, errors.New("exec stream ended without an exit status")
			}
			return 0, fmt.Errorf("read exec stream: %w", err)
		}
		switch frame.Type {
		case execstream.FrameStdout:
			if _, err := stdout.Write(frame.Payload); err != nil {
				return 0, fmt.Errorf("write exec stdout: %w", err)
			}
		case execstream.FrameStderr:
			if _, err := stderr.Write(frame.Payload); err != nil {
				return 0, fmt.Errorf("write exec stderr: %w", err)
			}
		case execstream.FrameExit:
			var exit api.ExecExit
			if err := json.Unmarshal(frame.Payload, &exit); err != nil {
				return 0, fmt.Errorf("decode exec exit status: %w", err)
			}
			return exit.ExitCode, nil
		case execstream.FrameError:
			var streamErr api.ExecStreamError
			if err := json.Unmarshal(frame.Payload, &streamErr); err != nil || streamErr.Message == "" {
				return 0, &ExecError{Message: "exec stream failed"}
			}
			return 0, &ExecError{Message: streamErr.Message}
		default:
			return 0, fmt.Errorf("%w: unexpected type %d from server", execstream.ErrInvalidFrame, frame.Type)
		}
	}
}

// Close ends the stream. Closing a stream whose process is still running
// terminates the process.
func (s *ExecStream) Close() error { return s.conn.Close() }
