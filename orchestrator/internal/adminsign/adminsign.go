// Package adminsign defines how administrator requests are signed: the
// headers that carry a leader-issued challenge and the Ed25519 signature,
// and the canonical bytes that are signed. It has no dependencies so API
// clients can sign requests without the server's authentication state.
package adminsign

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const (
	// ChallengeHeader carries the one-time challenge being signed.
	ChallengeHeader = "X-Trellis-Admin-Challenge"
	// SignatureHeader carries the Ed25519 request signature.
	SignatureHeader = "X-Trellis-Admin-Signature"
	// ChallengeStatusHeader tells clients to obtain a new challenge.
	ChallengeStatusHeader = "X-Trellis-Admin-Challenge-Status"
	// ChallengeInvalid is returned when a challenge cannot be used.
	ChallengeInvalid = "invalid"

	signingVersion = "trellis-admin-request-v1"
)

// Payload returns the canonical bytes signed by administrator clients.
func Payload(challenge, method, requestURI string, body []byte) []byte {
	digest := sha256.Sum256(body)
	return []byte(strings.Join([]string{
		signingVersion,
		challenge,
		strings.ToUpper(method),
		requestURI,
		hex.EncodeToString(digest[:]),
	}, "\n"))
}
