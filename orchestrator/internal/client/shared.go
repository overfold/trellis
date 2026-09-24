package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
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
	token     string
	namespace string
	client    *http.Client
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
	var requestBody io.Reader = http.NoBody
	if requestData != nil {
		requestBodyBytes, err := json.Marshal(requestData)
		if err != nil {
			return fmt.Errorf("marshal json: %w", err)
		}
		requestBody = bytes.NewReader(requestBodyBytes)
	}

	request, err := http.NewRequestWithContext(ctx, method, url, requestBody)
	if err != nil {
		return fmt.Errorf("constructing request %s: %w", url, err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+c.token)
	if c.namespace != "" {
		request.Header.Set("X-Trellis-Namespace", c.namespace)
	}

	response, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("executing request %s: %w", url, err)
	}
	defer func() {
		_ = response.Body.Close()
	}()

	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBody+1))
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	if len(responseBody) > maxResponseBody {
		return fmt.Errorf("response body exceeds %d bytes", maxResponseBody)
	}

	if checkStatusCode(response.StatusCode) {
		return &HTTPError{Status: response.StatusCode, Body: responseBody}
	}

	if responseData != nil {
		err = json.Unmarshal(responseBody, responseData)
		if err != nil {
			return fmt.Errorf("unmarshal json: %w", err)
		}
	}

	return nil
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
