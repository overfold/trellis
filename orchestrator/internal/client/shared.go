package client

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
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/auth"
)

const maxResponseBody = 64 << 20

func newHTTPClient(tlsConfig *tls.Config) *http.Client {
	return newHTTPClientWithResponseHeaderTimeout(tlsConfig, 30*time.Second)
}

func newHTTPClientWithResponseHeaderTimeout(tlsConfig *tls.Config, responseHeaderTimeout time.Duration) *http.Client {
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

type client struct {
	token            string
	namespace        string
	administratorKey ed25519.PrivateKey
	client           *http.Client
}

// HTTPError contains a non-successful HTTP response.
type HTTPError struct {
	Status int
	Body   []byte
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("status %d: %s", e.Status, bytes.TrimSpace(e.Body))
}

func (c *client) request(ctx context.Context, method string, url string, requestData any, responseData any) error {
	var requestBodyBytes []byte
	if requestData != nil {
		var err error
		requestBodyBytes, err = json.Marshal(requestData)
		if err != nil {
			return fmt.Errorf("marshal json: %w", err)
		}
	}

	for attempt := 0; attempt < 2; attempt++ {
		request, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(requestBodyBytes))
		if err != nil {
			return fmt.Errorf("constructing request %s: %w", url, err)
		}
		request.Header.Set("Content-Type", "application/json")
		if c.token != "" {
			request.Header.Set("Authorization", "Bearer "+c.token)
		}
		if c.namespace != "" {
			request.Header.Set("X-Trellis-Namespace", c.namespace)
		}
		if c.administratorKey != nil {
			challenge, err := c.administratorChallenge(ctx, url)
			if err != nil {
				return err
			}
			payload := auth.AdministratorSigningPayload(challenge, method, request.URL.RequestURI(), requestBodyBytes)
			request.Header.Set(auth.AdministratorChallengeHeader, challenge)
			request.Header.Set(auth.AdministratorSignatureHeader, base64.RawURLEncoding.EncodeToString(ed25519.Sign(c.administratorKey, payload)))
		}

		response, err := c.client.Do(request)
		if err != nil {
			return fmt.Errorf("executing request %s: %w", url, err)
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBody+1))
		_ = response.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read response body: %w", readErr)
		}
		if len(responseBody) > maxResponseBody {
			return fmt.Errorf("response body exceeds %d bytes", maxResponseBody)
		}
		if c.administratorKey != nil && attempt == 0 && response.Header.Get(auth.AdministratorChallengeStatusHeader) == auth.AdministratorChallengeInvalid {
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

func (c *client) administratorChallenge(ctx context.Context, target string) (string, error) {
	targetURL, err := url.Parse(target)
	if err != nil {
		return "", fmt.Errorf("parse administrator request URL: %w", err)
	}
	targetURL.Path = "/v1/auth/administrator/challenge"
	targetURL.RawPath = ""
	targetURL.RawQuery = ""
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL.String(), http.NoBody)
	if err != nil {
		return "", fmt.Errorf("construct administrator challenge request: %w", err)
	}
	response, err := c.client.Do(request)
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

func (c *client) stream(ctx context.Context, url string) (io.ReadCloser, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("constructing request %s: %w", url, err)
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	if c.namespace != "" {
		request.Header.Set("X-Trellis-Namespace", c.namespace)
	}
	response, err := c.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("executing request %s: %w", url, err)
	}
	if checkStatusCode(response.StatusCode) {
		defer func() { _ = response.Body.Close() }()
		body, _ := io.ReadAll(io.LimitReader(response.Body, maxResponseBody))
		return nil, fmt.Errorf("status %d: %s", response.StatusCode, bytes.TrimSpace(body))
	}
	return response.Body, nil
}

func checkStatusCode(statusCode int) bool {
	return statusCode < 200 || statusCode >= 300
}
