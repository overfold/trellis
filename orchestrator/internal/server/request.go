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
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/auth"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// MaxRestoreRequestBytes bounds the aggregate restore body and its private spool.
const MaxRestoreRequestBytes = 256 << 20

// Request body limits.
const (
	maxSmallRequestBytes    = 64 << 10
	maxDefaultRequestBytes  = 1 << 20
	maxJobRequestBytes      = 4 << 20
	maxSecretRequestBytes   = 96 << 10
	maxExecInputRequestSize = 128 << 10
	maxBackupRequestBytes   = MaxRestoreRequestBytes
)

// RequestBodyLimit returns the route's upload budget for administrator signature
// verification, using the same limits as JSON decoding. Unmatched and bodyless
// routes receive only the small budget, never a large fallback.
func RequestBodyLimit(c *echo.Context) int64 {
	if c.Request().Method == http.MethodPost {
		switch c.Path() {
		case "/v1/backup/restore":
			return MaxRestoreRequestBytes
		case "/v1/namespaces/:namespace/jobs", "/v1/namespaces/:namespace/jobs/plan":
			return maxJobRequestBytes
		case "/v1/nodes":
			return maxDefaultRequestBytes
		case "/v1/nodes/:id/heartbeat":
			return maxHeartbeatBodyBytes
		}
	}
	if c.Request().Method == http.MethodPut && c.Path() == "/v1/namespaces/:namespace/secrets/:name" {
		return maxSecretRequestBytes
	}
	return maxSmallRequestBytes
}

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
	var raw json.RawMessage
	_, jobRequest := dst.(*api.JobRegistrationRequest)
	if err := decoder.Decode(&raw); err != nil {
		return decodeError(err, limit)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err != nil {
			return decodeError(err, limit)
		}
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body: unexpected data after the JSON value")
	}
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return decodeError(err, limit)
	}
	if jobRequest {
		if err := validateJSONShape(raw, reflect.TypeOf(dst).Elem(), "request body", true); err != nil {
			return decodeError(err, limit)
		}
	}
	valueDecoder := json.NewDecoder(bytes.NewReader(raw))
	valueDecoder.DisallowUnknownFields()
	if err := valueDecoder.Decode(dst); err != nil {
		return decodeError(err, limit)
	}
	return nil
}

// Walk tokens before map or typed decoding can discard duplicate object keys,
// including inside RawMessage fields. Do not echo keys or values in errors:
// arbitrary map keys can themselves contain sensitive author input.
func rejectDuplicateJSONKeys(raw json.RawMessage) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		switch token {
		case json.Delim('{'):
			keys := make(map[string]struct{})
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				name := key.(string)
				if _, exists := keys[name]; exists {
					return errors.New("duplicate JSON object key")
				}
				keys[name] = struct{}{}
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case json.Delim('['):
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return nil
		}
	}
	return walk()
}

// decodeJobSpec strictly decodes the spec of a job request.
func decodeJobSpec(raw json.RawMessage, jobSpec *spec.JobSpec) error {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body: spec is required")
	}
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return decodeError(err, maxJobRequestBytes)
	}
	if err := validateJSONShape(raw, reflect.TypeFor[spec.JobSpec](), "spec", false); err != nil {
		return decodeError(err, maxJobRequestBytes)
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

// encoding/json accepts case-folded struct names and null scalar values.
// Check the structural contract before decoding; RawMessage is owned by its
// subsequent decoder. Job specs forbid explicit null, just like their schema.
func validateJSONShape(raw json.RawMessage, t reflect.Type, path string, nullable bool) error {
	if t == reflect.TypeFor[json.RawMessage]() {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if nullable && (t.Kind() == reflect.Pointer || t.Kind() == reflect.Map || t.Kind() == reflect.Slice) {
			return nil
		}
		return fmt.Errorf("%s must not be null", path)
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct, reflect.Map:
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return err
		}
		for key, value := range fields {
			var child reflect.Type
			if t.Kind() == reflect.Map {
				child = t.Elem()
			} else {
				for field := range t.Fields() {
					name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
					if name == key {
						child = field.Type
						break
					}
				}
				if child == nil {
					return fmt.Errorf("unknown field %q in %s", key, path)
				}
			}
			if err := validateJSONShape(value, child, path+"."+key, nullable); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return err
		}
		for i, value := range items {
			if err := validateJSONShape(value, t.Elem(), fmt.Sprintf("%s[%d]", path, i), nullable); err != nil {
				return err
			}
		}
	default:
		// Scalar type/range checking remains encoding/json's responsibility.
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
