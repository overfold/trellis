package client

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/transport"
)

// Config configures a Client.
type Config struct {
	// Address is the control-plane API address of any node: host:port or an
	// http(s) URL. An address without a scheme uses HTTPS.
	Address string
	// Namespace is the namespace addressed by namespaced operations. It may
	// be empty for a client that only uses cluster-scoped operations.
	Namespace string
	// Token is a bearer API credential.
	Token string
	// AdministratorKey is the cluster administrator's Ed25519 private key.
	// A client with the key signs every request instead of sending a token,
	// so Token must be empty.
	AdministratorKey ed25519.PrivateKey
	// TLSConfig configures server verification and client certificates. A
	// nil TLSConfig uses the system roots.
	TLSConfig *tls.Config
}

// Client sends authenticated requests to the Trellis control-plane API. It
// is safe for concurrent use.
type Client struct {
	baseURL   string
	namespace string
	transport *transport.Client
}

// ErrNamespaceRequired reports a namespaced operation on a client without a
// namespace.
var ErrNamespaceRequired = errors.New("a namespace is required")

// New creates a client from config.
func New(config Config) (*Client, error) {
	baseURL := transport.NormalizeBaseURL(config.Address)
	if baseURL == "" {
		return nil, errors.New("trellis client: an address is required")
	}
	if _, err := url.Parse(baseURL); err != nil {
		return nil, fmt.Errorf("trellis client: invalid address: %w", err)
	}
	t := &transport.Client{Token: config.Token, HTTP: transport.NewHTTPClient(config.TLSConfig, 30*time.Second)}
	if config.AdministratorKey != nil {
		if config.Token != "" {
			return nil, errors.New("trellis client: a token and an administrator key are mutually exclusive")
		}
		if len(config.AdministratorKey) != ed25519.PrivateKeySize {
			return nil, fmt.Errorf("trellis client: administrator private key must be %d bytes", ed25519.PrivateKeySize)
		}
		t.AdministratorKey = append(ed25519.PrivateKey(nil), config.AdministratorKey...)
	}
	return &Client{baseURL: baseURL, namespace: config.Namespace, transport: t}, nil
}

// WithNamespace returns a client that shares c's connection and credentials
// and addresses namespace in namespaced operations.
func (c *Client) WithNamespace(namespace string) *Client {
	return &Client{baseURL: c.baseURL, namespace: namespace, transport: c.transport}
}

// Namespace returns the namespace addressed by namespaced operations.
func (c *Client) Namespace() string { return c.namespace }

// HTTPError is a response the server rejected with a non-2xx status.
type HTTPError struct {
	Status int
	Body   []byte
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("status %d: %s", e.Status, bytes.TrimSpace(e.Body))
}

// Message returns the server's error message: the response's JSON "message"
// field, or its trimmed body when the body carries no such string.
func (e *HTTPError) Message() string {
	var body struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(e.Body, &body) == nil && body.Message != "" {
		return body.Message
	}
	return string(bytes.TrimSpace(e.Body))
}

// publicError exposes a transport status error as an *HTTPError.
func publicError(err error) error {
	var httpErr *transport.HTTPError
	if errors.As(err, &httpErr) {
		return &HTTPError{Status: httpErr.Status, Body: httpErr.Body}
	}
	return err
}

// clusterPath returns the URL of a cluster-scoped path.
func (c *Client) clusterPath(format string, args ...any) string {
	return c.baseURL + fmt.Sprintf(format, args...)
}

// namespacedPath returns the URL of path within the client's namespace.
func (c *Client) namespacedPath(format string, args ...any) (string, error) {
	if c.namespace == "" {
		return "", ErrNamespaceRequired
	}
	return c.baseURL + "/v1/namespaces/" + url.PathEscape(c.namespace) + fmt.Sprintf(format, args...), nil
}

func (c *Client) request(ctx context.Context, method, target string, requestData, responseData any) error {
	return publicError(c.transport.Request(ctx, method, target, requestData, responseData))
}

func (c *Client) stream(ctx context.Context, target string) (io.ReadCloser, error) {
	body, err := c.transport.Stream(ctx, target)
	return body, publicError(err)
}
