package auth

import "strings"

const authorizationPrefix = "@trellis-auth:"

// EncodeRole returns the compact authorization value stored in request context.
func EncodeRole(role Role) string {
	return authorizationPrefix + string(role)
}

// DecodeRole decodes an authorization request-context value. Only generated
// credential roles are accepted; administrator authority has its own context.
func DecodeRole(value string) (Role, bool) {
	if !strings.HasPrefix(value, authorizationPrefix) {
		return "", false
	}
	role := Role(strings.TrimPrefix(value, authorizationPrefix))
	return role, role.Generated()
}
