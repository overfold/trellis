package auth

import "strings"

const authorizationPrefix = "@trellis-auth:"

// EncodeAccess returns the compact authorization value stored in request context.
func EncodeAccess(access AccessLevel) string {
	return authorizationPrefix + string(access)
}

// DecodeAccess decodes an authorization request-context value.
func DecodeAccess(value string) (AccessLevel, bool) {
	if !strings.HasPrefix(value, authorizationPrefix) {
		return "", false
	}
	access := AccessLevel(strings.TrimPrefix(value, authorizationPrefix))
	return access, access == AccessRead || access == AccessWrite
}
