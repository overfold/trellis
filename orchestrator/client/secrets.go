package client

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/overfold/trellis/orchestrator/api"
)

// SetSecret creates or updates a secret in the client's namespace. A
// non-nil expectedVersion makes the write conditional: 0 requires that the
// secret does not exist and N requires that it is at version N. A failed
// precondition returns an *HTTPError with status 409 Conflict. The encoded
// request is cleared after it is sent; value is not retained.
func (c *Client) SetSecret(ctx context.Context, name string, value []byte, expectedVersion *uint64) (*api.SecretMetadata, error) {
	path, err := c.namespacedPath("/secrets/%s", url.PathEscape(name))
	if err != nil {
		return nil, fmt.Errorf("set secret: %w", err)
	}
	// The body is encoded by hand so the only copy of the encoded value is
	// the request buffer, which the transport clears.
	request := make([]byte, 0, base64.StdEncoding.EncodedLen(len(value))+64)
	request = append(request, `{"value_base64":"`...)
	request = base64.StdEncoding.AppendEncode(request, value)
	request = append(request, '"')
	if expectedVersion != nil {
		request = append(request, `,"expected_version":`...)
		request = strconv.AppendUint(request, *expectedVersion, 10)
	}
	request = append(request, '}')
	var response api.SecretMetadata
	if err := publicError(c.transport.RequestBody(ctx, http.MethodPut, path, request, &response)); err != nil {
		return nil, fmt.Errorf("set secret: %w", err)
	}
	return &response, nil
}

// ListSecrets returns the metadata of the secrets in the client's
// namespace. Secret values are never returned.
func (c *Client) ListSecrets(ctx context.Context) (api.SecretListResponse, error) {
	path, err := c.namespacedPath("/secrets")
	if err != nil {
		return nil, fmt.Errorf("list secrets: %w", err)
	}
	var response api.SecretListResponse
	if err := c.request(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, fmt.Errorf("list secrets: %w", err)
	}
	return response, nil
}

// GetSecretMetadata returns the metadata of a secret.
func (c *Client) GetSecretMetadata(ctx context.Context, name string) (*api.SecretMetadata, error) {
	path, err := c.namespacedPath("/secrets/%s", url.PathEscape(name))
	if err != nil {
		return nil, fmt.Errorf("describe secret: %w", err)
	}
	var response api.SecretMetadata
	if err := c.request(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, fmt.Errorf("describe secret: %w", err)
	}
	return &response, nil
}

// DeleteSecret removes a secret from the client's namespace.
func (c *Client) DeleteSecret(ctx context.Context, name string) error {
	path, err := c.namespacedPath("/secrets/%s", url.PathEscape(name))
	if err != nil {
		return fmt.Errorf("delete secret: %w", err)
	}
	if err := c.request(ctx, http.MethodDelete, path, nil, nil); err != nil {
		return fmt.Errorf("delete secret: %w", err)
	}
	return nil
}
