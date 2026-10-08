package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"reflect"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/internal/auth"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// Request body limits. Administrator-only aggregate restores have no byte cap:
// legal cluster state and pretty-printed backups can exceed any per-write cap.
const (
	maxSmallRequestBytes    = 64 << 10
	maxDefaultRequestBytes  = 1 << 20
	maxJobRequestBytes      = 4 << 20
	maxSecretRequestBytes   = 96 << 10
	maxExecInputRequestSize = 128 << 10
	maxBackupRequestBytes   = 0
)

// decodeJSON strictly decodes exactly one JSON value from the request body
// into dst. The API is as strict as the published schemas and YAML decoding:
// unknown fields, trailing data, a missing body, and non-JSON media types are
// rejected. Error messages describe the JSON structure only, never field
// values, so a rejected secret write cannot echo its payload.
func decodeJSON(c *echo.Context, dst any, limit int64) error {
	request := c.Request()
	if mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type")); err != nil || mediaType != "application/json" {
		return echo.NewHTTPError(http.StatusUnsupportedMediaType, "request body must be application/json")
	}
	var reader io.Reader = request.Body
	if limit > 0 {
		reader = http.MaxBytesReader(c.Response(), request.Body, limit)
	}
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return decodeError(err, limit)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err != nil {
			return decodeError(err, limit)
		}
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body: unexpected data after the JSON value")
	}
	return nil
}

// decodeJobSpec strictly decodes the spec of a job request.
func decodeJobSpec(raw json.RawMessage, jobSpec *spec.JobSpec) error {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body: spec is required")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(jobSpec); err != nil {
		var mismatch *json.UnmarshalTypeError
		if errors.As(err, &mismatch) {
			mismatch.Field = strings.TrimSuffix("spec."+mismatch.Field, ".")
		}
		return decodeError(err, maxJobRequestBytes)
	}
	return nil
}

func decodeError(err error, limit int64) error {
	var tooLarge *http.MaxBytesError
	var syntax *json.SyntaxError
	var mismatch *json.UnmarshalTypeError
	switch {
	case errors.As(err, &tooLarge):
		return echo.NewHTTPError(http.StatusRequestEntityTooLarge, fmt.Sprintf("request body exceeds %d bytes", limit))
	case errors.Is(err, io.EOF):
		return echo.NewHTTPError(http.StatusBadRequest, "request body is empty")
	case errors.Is(err, io.ErrUnexpectedEOF):
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body: truncated JSON")
	case errors.As(err, &syntax):
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("invalid request body: malformed JSON at byte %d", syntax.Offset))
	case errors.As(err, &mismatch):
		field := mismatch.Field
		if field == "" {
			field = "request body"
		}
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("invalid request body: %s must be %s", field, jsonTypeName(mismatch.Type)))
	default:
		// encoding/json reports unknown fields and unmarshaler failures with
		// messages that name the field, not its value.
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body: "+err.Error())
	}
}

func jsonTypeName(t reflect.Type) string {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct, reflect.Map:
		return "an object"
	case reflect.Slice, reflect.Array:
		return "an array"
	case reflect.String:
		return "a string"
	case reflect.Bool:
		return "a boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "an integer in range"
	default:
		return "a number"
	}
}

// namespaceParam validates the {namespace} path parameter and authorizes the caller.
func namespaceParam(c *echo.Context) (string, error) {
	namespace := c.Param("namespace")
	if !spec.ValidDiscoveryIdentifier(namespace) {
		return "", echo.NewHTTPError(http.StatusBadRequest, "invalid namespace: must be a single identifier without dots")
	}
	authz := authorization(c)
	if authz.root || authz.scope == auth.AccessCluster {
		return namespace, nil
	}
	return "", echo.NewHTTPError(http.StatusForbidden, "namespaced resources require an authenticated scoped credential")
}

// namespaceWrite authorizes a mutation of the {namespace} path parameter.
func namespaceWrite(c *echo.Context, message string) (string, error) {
	namespace, err := namespaceParam(c)
	if err != nil {
		return "", err
	}
	if err := requireWrite(c, message); err != nil {
		return "", err
	}
	return namespace, nil
}
