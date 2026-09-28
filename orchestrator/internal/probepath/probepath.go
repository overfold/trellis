// Package probepath defines the HTTP health-check request targets that the
// control plane accepts and the task-local probe sends. It depends only on the
// standard library so the statically built probe can share it.
package probepath

import (
	"errors"
	"fmt"
)

// UsageExit is the probe's exit status for arguments it refuses. It is
// distinct from 1 (unhealthy) and from 2, which the Go runtime uses for a
// panic or fatal error.
const UsageExit = 64

// MaxLength bounds an HTTP health-check request target.
const MaxLength = 1024

// ErrTooLong reports a request target longer than MaxLength.
var ErrTooLong = fmt.Errorf("must be at most %d bytes", MaxLength)

// Pattern is the JSON Schema equivalent of Validate: empty, or an absolute
// path and optional query of RFC 3986 path/query characters and well-formed
// percent-encodings.
const Pattern = `^(?:/(?:[A-Za-z0-9._~!$&'()*+,;=:@/?-]|%[0-9A-Fa-f]{2})*)?$`

// Validate reports whether path is an origin-form request target (an absolute
// path plus optional query) that is sent unchanged. Such a target can never
// name a scheme, host, user information, or fragment. An empty path means /.
func Validate(path string) error {
	if path == "" {
		return nil
	}
	if len(path) > MaxLength {
		return ErrTooLong
	}
	if path[0] != '/' {
		return errors.New("must begin with /")
	}
	for i := 0; i < len(path); i++ {
		c := path[i]
		switch {
		case c == '%':
			if i+2 >= len(path) || !isHex(path[i+1]) || !isHex(path[i+2]) {
				return errors.New("must use %XX hexadecimal percent-encodings")
			}
			i += 2
		case !allowed(c):
			return errors.New("must contain only URL path and query characters; percent-encode others")
		}
	}
	return nil
}

func allowed(c byte) bool {
	switch {
	case 'A' <= c && c <= 'Z', 'a' <= c && c <= 'z', '0' <= c && c <= '9':
		return true
	}
	switch c {
	case '-', '.', '_', '~', '!', '$', '&', '\'', '(', ')', '*', '+', ',', ';', '=', ':', '@', '/', '?':
		return true
	}
	return false
}

func isHex(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
}
