package api

// ExecRequest starts an exec stream in an allocation task. It is carried in
// the query string of the WebSocket request rather than in a JSON body.
type ExecRequest struct {
	// Task selects the task; it may be empty when the allocation has one task.
	Task string
	// Command is the argv to run. Trellis never adds a shell.
	Command []string
	// TTY allocates a terminal. Terminal output is delivered as stdout.
	TTY bool
	// Stdin attaches the stream's stdin frames to the process. Without it
	// the process has no standard input.
	Stdin bool
	// Term sets TERM in a TTY process.
	Term string
	// Cols and Rows are the initial terminal size of a TTY process.
	Cols uint32
	Rows uint32
}

// ExecResize is the payload of an exec stream resize frame.
type ExecResize struct {
	Cols uint32 `json:"cols"`
	Rows uint32 `json:"rows"`
}

// ExecExit is the payload of an exec stream exit frame.
type ExecExit struct {
	ExitCode int `json:"exit_code"`
}

// ExecStreamError is the payload of an exec stream error frame. It ends the
// stream without an exit status.
type ExecStreamError struct {
	Message string `json:"message"`
}
