package execstream

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/overfold/trellis/orchestrator/internal/api"
)

const (
	// DefaultCols is the width of a terminal whose size was not given.
	DefaultCols = 80
	// DefaultRows is the height of a terminal whose size was not given.
	DefaultRows = 24
	// MaxTerminalDimension bounds each terminal dimension.
	MaxTerminalDimension = 1000
	maxTermLength        = 64
	maxCommandArgs       = 1024
)

// EncodeRequest returns the query string of an exec upgrade request.
func EncodeRequest(request api.ExecRequest) url.Values {
	query := url.Values{}
	if request.Task != "" {
		query.Set("task", request.Task)
	}
	for _, arg := range request.Command {
		query.Add("command", arg)
	}
	if request.TTY {
		query.Set("tty", "true")
	}
	if request.Stdin {
		query.Set("stdin", "true")
	}
	if request.Term != "" {
		query.Set("term", request.Term)
	}
	if request.Cols != 0 {
		query.Set("cols", strconv.FormatUint(uint64(request.Cols), 10))
	}
	if request.Rows != 0 {
		query.Set("rows", strconv.FormatUint(uint64(request.Rows), 10))
	}
	return query
}

// DecodeRequest parses and validates the query string of an exec upgrade
// request, defaulting the size of a terminal.
func DecodeRequest(query url.Values) (api.ExecRequest, error) {
	var request api.ExecRequest
	var err error
	request.Task = query.Get("task")
	request.Command = query["command"]
	if len(request.Command) == 0 || request.Command[0] == "" {
		return request, errors.New("command is required")
	}
	if len(request.Command) > maxCommandArgs {
		return request, fmt.Errorf("command has more than %d arguments", maxCommandArgs)
	}
	if request.TTY, err = parseBool(query, "tty"); err != nil {
		return request, err
	}
	if request.Stdin, err = parseBool(query, "stdin"); err != nil {
		return request, err
	}
	request.Term = query.Get("term")
	if request.Cols, err = parseDimension(query, "cols"); err != nil {
		return request, err
	}
	if request.Rows, err = parseDimension(query, "rows"); err != nil {
		return request, err
	}
	if !request.TTY {
		if request.Term != "" || request.Cols != 0 || request.Rows != 0 {
			return request, errors.New("term, cols, and rows require tty")
		}
		return request, nil
	}
	if err := validateTerm(request.Term); err != nil {
		return request, err
	}
	if request.Cols == 0 {
		request.Cols = DefaultCols
	}
	if request.Rows == 0 {
		request.Rows = DefaultRows
	}
	return request, nil
}

// EncodeAgentRequest returns the query string of a leader-to-agent exec
// upgrade request.
func EncodeAgentRequest(request api.AgentExecRequest) url.Values {
	query := EncodeRequest(request.ExecRequest)
	query.Set("epoch", strconv.FormatUint(request.Epoch, 10))
	return query
}

// DecodeAgentRequest parses and validates a leader-to-agent exec request.
func DecodeAgentRequest(query url.Values) (api.AgentExecRequest, error) {
	request, err := DecodeRequest(query)
	if err != nil {
		return api.AgentExecRequest{}, err
	}
	epoch, err := strconv.ParseUint(query.Get("epoch"), 10, 64)
	if err != nil || epoch == 0 {
		return api.AgentExecRequest{}, errors.New("epoch must be greater than zero")
	}
	return api.AgentExecRequest{ExecRequest: request, Epoch: epoch}, nil
}

// ValidateResize checks the dimensions of a resize frame.
func ValidateResize(resize api.ExecResize) error {
	if resize.Cols == 0 || resize.Rows == 0 || resize.Cols > MaxTerminalDimension || resize.Rows > MaxTerminalDimension {
		return fmt.Errorf("terminal dimensions must be between 1 and %d", MaxTerminalDimension)
	}
	return nil
}

func parseBool(query url.Values, name string) (bool, error) {
	value := query.Get(name)
	if value == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", name)
	}
	return parsed, nil
}

func parseDimension(query url.Values, name string) (uint32, error) {
	value := query.Get(name)
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil || parsed == 0 || parsed > MaxTerminalDimension {
		return 0, fmt.Errorf("%s must be between 1 and %d", name, MaxTerminalDimension)
	}
	return uint32(parsed), nil
}

// validateTerm accepts terminal names such as xterm-256color and
// screen.xterm-256color. The value becomes an environment variable.
func validateTerm(term string) error {
	if len(term) > maxTermLength {
		return fmt.Errorf("term must be at most %d characters", maxTermLength)
	}
	for _, r := range term {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.', r == '+':
		default:
			return fmt.Errorf("term contains invalid character %q", r)
		}
	}
	return nil
}
