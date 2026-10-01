package auth

import "strings"

const authorizationPrefix = "@trellis-auth:"

// EncodeScope returns the compact authorization value stored in request context.
func EncodeScope(scope AccessScope, access AccessLevel) string {
	return authorizationPrefix + string(scope) + ":" + string(access)
}

// DecodeScope decodes an authorization request-context value.
func DecodeScope(value string) (scope AccessScope, access AccessLevel, ok bool) {
	if !strings.HasPrefix(value, authorizationPrefix) {
		return "", "", false
	}
	parts := strings.Split(value[len(authorizationPrefix):], ":")
	switch {
	case len(parts) == 2 && parts[0] == string(AccessCluster):
		access = AccessLevel(parts[1])
		if access != AccessRead && access != AccessWrite {
			return "", "", false
		}
		return AccessCluster, access, true
	default:
		return "", "", false
	}
}
