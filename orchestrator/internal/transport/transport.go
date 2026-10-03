// Package transport sends authenticated HTTP requests to Trellis APIs. It is
// shared by the public client package and the node-internal clients.
package transport

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/adminsign"
	"github.com/overfold/trellis/orchestrator/internal/execstream"
	"github.com/overfold/trellis/orchestrator/internal/execwebsocket"
)

// MaxResponseBody bounds a buffered response body.
const MaxResponseBody = 64 << 20

// NewHTTPClient returns an HTTP client for Trellis APIs. A zero
// responseHeaderTimeout leaves waiting for response headers to the request
// context.
func NewHTTPClient(tlsConfig *tls.Config, responseHeaderTimeout time.Duration) *http.Client {
	return &http.Client{Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: responseHeaderTimeout,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConns:          100,
		TLSClientConfig:       tlsConfig,
	}}
}

// NormalizeBaseURL returns addr as a base URL without a trailing slash. An
// address without a scheme uses HTTPS.
func NormalizeBaseURL(addr string) string {
	addr = strings.TrimRight(strings.TrimSpace(addr), "/")
	if addr == "" {
		return ""
	}
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "https://" + addr
	}
	return addr
}

// Client authenticates requests with a bearer token or, when
// AdministratorKey is set, with administrator request signatures.
type Client struct {
	Token            string
	AdministratorKey ed25519.PrivateKey
	HTTP             *http.Client
}

// HTTPError contains a non-successful HTTP response.
type HTTPError struct {
	Status int
	Body   []byte
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("status %d: %s", e.Status, bytes.TrimSpace(e.Body))
}

// Message returns the response's JSON "message" field, or its trimmed body
// when the body carries no such string.
func (e *HTTPError) Message() string {
	var body struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(e.Body, &body) == nil && body.Message != "" {
		return body.Message
	}
	return string(bytes.TrimSpace(e.Body))
}

// Request sends requestData as a JSON body and decodes a JSON response into
// responseData. Either may be nil.
func (c *Client) Request(ctx context.Context, method string, url string, requestData any, responseData any) error {
	var requestBodyBytes []byte
	if requestData != nil {
		var err error
		requestBodyBytes, err = json.Marshal(requestData)
		if err != nil {
			return fmt.Errorf("marshal json: %w", err)
		}
	}
	return c.RequestBody(ctx, method, url, requestBodyBytes, responseData)
}

// RequestBody sends an encoded JSON body and decodes a JSON response into
// responseData, which may be nil. It clears requestBodyBytes before
// returning.
func (c *Client) RequestBody(ctx context.Context, method string, url string, requestBodyBytes []byte, responseData any) error {
	defer clear(requestBodyBytes)
	for attempt := 0; attempt < 2; attempt++ {
		request, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(requestBodyBytes))
		if err != nil {
			return fmt.Errorf("constructing request %s: %w", url, err)
		}
		request.Header.Set("Content-Type", "application/json")
		if err := c.authenticate(ctx, request, requestBodyBytes); err != nil {
			return err
		}

		response, err := c.HTTP.Do(request)
		if err != nil {
			return fmt.Errorf("executing request %s: %w", url, err)
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, MaxResponseBody+1))
		_ = response.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read response body: %w", readErr)
		}
		if len(responseBody) > MaxResponseBody {
			return fmt.Errorf("response body exceeds %d bytes", MaxResponseBody)
		}
		if c.AdministratorKey != nil && attempt == 0 && response.Header.Get(adminsign.ChallengeStatusHeader) == adminsign.ChallengeInvalid {
			continue
		}
		if checkStatusCode(response.StatusCode) {
			return &HTTPError{Status: response.StatusCode, Body: responseBody}
		}
		if responseData != nil {
			if err := json.Unmarshal(responseBody, responseData); err != nil {
				return fmt.Errorf("unmarshal json: %w", err)
			}
		}
		return nil
	}
	return fmt.Errorf("administrator challenge was rejected after retry")
}

// authenticate adds the client's credentials to request, signing it with a
// fresh administrator challenge when the client holds the administrator key.
func (c *Client) authenticate(ctx context.Context, request *http.Request, body []byte) error {
	if c.Token != "" {
		request.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if c.AdministratorKey != nil {
		challenge, err := c.administratorChallenge(ctx, request.URL.String())
		if err != nil {
			return err
		}
		payload := adminsign.Payload(challenge, request.Method, request.URL.RequestURI(), body)
		request.Header.Set(adminsign.ChallengeHeader, challenge)
		request.Header.Set(adminsign.SignatureHeader, base64.RawURLEncoding.EncodeToString(ed25519.Sign(c.AdministratorKey, payload)))
	}
	return nil
}

// Upgrade sends a GET request that the server switches to the exec stream
// protocol and returns the stream. The stream is closed when ctx ends.
func (c *Client) Upgrade(ctx context.Context, target string) (io.ReadWriteCloser, error) {
	for attempt := 0; attempt < 2; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, http.NoBody)
		if err != nil {
			return nil, fmt.Errorf("constructing request %s: %w", target, err)
		}
		if err := c.authenticate(ctx, request, nil); err != nil {
			return nil, err
		}
		execstream.SetUpgradeHeaders(request)
		response, err := c.HTTP.Do(request)
		if err != nil {
			return nil, fmt.Errorf("executing request %s: %w", target, err)
		}
		if response.StatusCode == http.StatusSwitchingProtocols {
			stream, err := execstream.Upgraded(response)
			if err != nil {
				_ = response.Body.Close()
				return nil, err
			}
			return closeWithContext(ctx, stream), nil
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, MaxResponseBody))
		_ = response.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read response body: %w", readErr)
		}
		if c.AdministratorKey != nil && attempt == 0 && response.Header.Get(adminsign.ChallengeStatusHeader) == adminsign.ChallengeInvalid {
			continue
		}
		if !checkStatusCode(response.StatusCode) {
			return nil, fmt.Errorf("server did not switch to %s (status %d)", execstream.Protocol, response.StatusCode)
		}
		return nil, &HTTPError{Status: response.StatusCode, Body: body}
	}
	return nil, fmt.Errorf("administrator challenge was rejected after retry")
}

// ExecWebSocket opens an authenticated public exec WebSocket. The stream is
// closed when ctx ends.
func (c *Client) ExecWebSocket(ctx context.Context, target string) (io.ReadWriteCloser, error) {
	for attempt := 0; attempt < 2; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, http.NoBody)
		if err != nil {
			return nil, fmt.Errorf("constructing request %s: %w", target, err)
		}
		if err := c.authenticate(ctx, request, nil); err != nil {
			return nil, err
		}
		conn, response, err := websocket.Dial(ctx, target, &websocket.DialOptions{
			HTTPClient:   c.HTTP,
			HTTPHeader:   request.Header,
			Subprotocols: []string{execwebsocket.Protocol},
		})
		if err == nil {
			stream, streamErr := execwebsocket.New(conn)
			if streamErr != nil {
				return nil, streamErr
			}
			return closeWithContext(ctx, stream), nil
		}
		if response == nil {
			return nil, fmt.Errorf("opening exec WebSocket %s: %w", target, err)
		}
		defer func() { _ = response.Body.Close() }()
		body, readErr := io.ReadAll(io.LimitReader(response.Body, MaxResponseBody))
		if readErr != nil {
			return nil, fmt.Errorf("read response body: %w", readErr)
		}
		if c.AdministratorKey != nil && attempt == 0 && response.Header.Get(adminsign.ChallengeStatusHeader) == adminsign.ChallengeInvalid {
			continue
		}
		if response.StatusCode != http.StatusSwitchingProtocols {
			return nil, &HTTPError{Status: response.StatusCode, Body: body}
		}
		return nil, fmt.Errorf("opening exec WebSocket: %w", err)
	}
	return nil, fmt.Errorf("administrator challenge was rejected after retry")
}

// contextStream closes its stream when the context it was opened with ends.
type contextStream struct {
	io.ReadWriteCloser
	once   sync.Once
	closed chan struct{}
	err    error
}

func closeWithContext(ctx context.Context, stream io.ReadWriteCloser) io.ReadWriteCloser {
	wrapped := &contextStream{ReadWriteCloser: stream, closed: make(chan struct{})}
	go func() {
		select {
		case <-ctx.Done():
			_ = wrapped.Close()
		case <-wrapped.closed:
		}
	}()
	return wrapped
}

func (s *contextStream) Close() error {
	s.once.Do(func() {
		s.err = s.ReadWriteCloser.Close()
		close(s.closed)
	})
	return s.err
}

func (c *Client) administratorChallenge(ctx context.Context, target string) (string, error) {
	targetURL, err := url.Parse(target)
	if err != nil {
		return "", fmt.Errorf("parse administrator request URL: %w", err)
	}
	targetURL.Path = "/v1/auth/administrator/challenge"
	targetURL.RawPath = ""
	targetURL.RawQuery = ""
	// target is intentionally the endpoint of the caller's original Trellis
	// request; administrator signing fetches its challenge from that same server.
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL.String(), http.NoBody) //nolint:gosec
	if err != nil {
		return "", fmt.Errorf("construct administrator challenge request: %w", err)
	}
	response, err := c.HTTP.Do(request) //nolint:gosec // The caller intentionally selects the Trellis API endpoint.
	if err != nil {
		return "", fmt.Errorf("request administrator challenge: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, 16<<10))
	if err != nil {
		return "", fmt.Errorf("read administrator challenge: %w", err)
	}
	if checkStatusCode(response.StatusCode) {
		return "", &HTTPError{Status: response.StatusCode, Body: body}
	}
	var challenge api.AdministratorChallengeResponse
	if err := json.Unmarshal(body, &challenge); err != nil || challenge.Challenge == "" {
		return "", fmt.Errorf("invalid administrator challenge response")
	}
	return challenge.Challenge, nil
}

// Stream sends a GET request and returns the response body for the caller
// to read and close.
func (c *Client) Stream(ctx context.Context, url string) (io.ReadCloser, error) {
	for attempt := 0; attempt < 2; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
		if err != nil {
			return nil, fmt.Errorf("constructing request %s: %w", url, err)
		}
		if err := c.authenticate(ctx, request, nil); err != nil {
			return nil, err
		}
		response, err := c.HTTP.Do(request)
		if err != nil {
			return nil, fmt.Errorf("executing request %s: %w", url, err)
		}
		if !checkStatusCode(response.StatusCode) {
			return response.Body, nil
		}
		body, _ := io.ReadAll(io.LimitReader(response.Body, MaxResponseBody))
		_ = response.Body.Close()
		if c.AdministratorKey != nil && attempt == 0 && response.Header.Get(adminsign.ChallengeStatusHeader) == adminsign.ChallengeInvalid {
			continue
		}
		return nil, &HTTPError{Status: response.StatusCode, Body: body}
	}
	return nil, fmt.Errorf("administrator challenge was rejected after retry")
}

func checkStatusCode(statusCode int) bool {
	return statusCode < 200 || statusCode >= 300
}
