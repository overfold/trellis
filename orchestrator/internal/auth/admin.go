package auth

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"time"
)

const maxAdministratorChallenges = 4096

// AdministratorAuthenticator issues and consumes leader-local administrator challenges.
type AdministratorAuthenticator struct {
	mu         sync.Mutex
	challenges map[string]time.Time
	key        [32]byte
	now        func() time.Time
	ttl        time.Duration
}

// NewAdministratorAuthenticator creates an administrator request authenticator.
func NewAdministratorAuthenticator() *AdministratorAuthenticator {
	a := &AdministratorAuthenticator{
		challenges: make(map[string]time.Time),
		now:        time.Now,
		ttl:        30 * time.Second,
	}
	_, _ = rand.Read(a.key[:])
	return a
}

// Issue creates a short-lived challenge bound to the current leadership epoch.
func (a *AdministratorAuthenticator) Issue(epoch uint64) (string, time.Time, error) {
	raw := make([]byte, 48)
	if _, err := rand.Read(raw); err != nil {
		return "", time.Time{}, fmt.Errorf("generate administrator challenge: %w", err)
	}
	now := a.now()
	expiresAt := now.Add(a.ttl)
	binary.BigEndian.PutUint64(raw[32:40], epoch)
	binary.BigEndian.PutUint64(raw[40:48], uint64(expiresAt.UnixNano()))
	mac := hmac.New(sha256.New, a.key[:])
	_, _ = mac.Write(raw)
	challenge := base64.RawURLEncoding.EncodeToString(mac.Sum(raw))
	return challenge, expiresAt, nil
}

// Preflight rejects malformed proofs and invalid, expired or used challenges
// without consuming them. It does not authenticate the request body.
func (a *AdministratorAuthenticator) Preflight(publicKey ed25519.PublicKey, epoch uint64, challenge, signature string) bool {
	_, _, ok := a.proof(publicKey, epoch, challenge, signature)
	return ok
}

func (a *AdministratorAuthenticator) proof(publicKey ed25519.PublicKey, epoch uint64, challenge, signature string) (time.Time, []byte, bool) {
	if len(challenge) != base64.RawURLEncoding.EncodedLen(80) || len(signature) != base64.RawURLEncoding.EncodedLen(ed25519.SignatureSize) {
		return time.Time{}, nil, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(challenge)
	if err != nil || len(raw) != 80 || len(publicKey) != ed25519.PublicKeySize {
		return time.Time{}, nil, false
	}
	mac := hmac.New(sha256.New, a.key[:])
	_, _ = mac.Write(raw[:48])
	nanos := binary.BigEndian.Uint64(raw[40:48])
	if nanos > math.MaxInt64 {
		return time.Time{}, nil, false
	}
	expiresAt := time.Unix(0, int64(nanos))
	if !hmac.Equal(raw[48:], mac.Sum(nil)) || binary.BigEndian.Uint64(raw[32:40]) != epoch || !expiresAt.After(a.now()) {
		return time.Time{}, nil, false
	}
	rawSignature, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil || len(rawSignature) != ed25519.SignatureSize {
		return time.Time{}, nil, false
	}
	a.mu.Lock()
	_, used := a.challenges[challenge]
	a.mu.Unlock()
	return expiresAt, rawSignature, !used
}

// VerifySignature checks the signed payload without consuming the challenge.
// Call Verify after body validation to atomically enforce one-time use.
func (a *AdministratorAuthenticator) VerifySignature(publicKey ed25519.PublicKey, epoch uint64, challenge, signature string, payload []byte) bool {
	_, rawSignature, ok := a.proof(publicKey, epoch, challenge, signature)
	return ok && ed25519.Verify(publicKey, payload, rawSignature)
}

// Verify consumes challenge and verifies its signature for exactly one request.
func (a *AdministratorAuthenticator) Verify(publicKey ed25519.PublicKey, epoch uint64, challenge, signature string, payload []byte) bool {
	expiresAt, rawSignature, ok := a.proof(publicKey, epoch, challenge, signature)
	if !ok || !ed25519.Verify(publicKey, payload, rawSignature) {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for value, expiry := range a.challenges {
		if !expiry.After(a.now()) {
			delete(a.challenges, value)
		}
	}
	if _, used := a.challenges[challenge]; used || len(a.challenges) >= maxAdministratorChallenges || !expiresAt.After(a.now()) {
		return false
	}
	a.challenges[challenge] = expiresAt
	return true
}
