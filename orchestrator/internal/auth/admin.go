package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"sync"
	"time"
)

const maxAdministratorChallenges = 4096

type administratorChallenge struct {
	expiresAt time.Time
	epoch     uint64
}

// AdministratorAuthenticator issues and consumes leader-local administrator challenges.
type AdministratorAuthenticator struct {
	mu         sync.Mutex
	challenges map[string]administratorChallenge
	now        func() time.Time
	ttl        time.Duration
}

// NewAdministratorAuthenticator creates an administrator request authenticator.
func NewAdministratorAuthenticator() *AdministratorAuthenticator {
	return &AdministratorAuthenticator{
		challenges: make(map[string]administratorChallenge),
		now:        time.Now,
		ttl:        30 * time.Second,
	}
}

// Issue creates a short-lived challenge bound to the current leadership epoch.
func (a *AdministratorAuthenticator) Issue(epoch uint64) (string, time.Time, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", time.Time{}, fmt.Errorf("generate administrator challenge: %w", err)
	}
	now := a.now()
	expiresAt := now.Add(a.ttl)
	challenge := base64.RawURLEncoding.EncodeToString(raw)
	a.mu.Lock()
	defer a.mu.Unlock()
	for value, existing := range a.challenges {
		if !existing.expiresAt.After(now) || existing.epoch != epoch {
			delete(a.challenges, value)
			continue
		}
	}
	if len(a.challenges) >= maxAdministratorChallenges {
		return "", time.Time{}, fmt.Errorf("administrator challenge capacity exhausted")
	}
	a.challenges[challenge] = administratorChallenge{expiresAt: expiresAt, epoch: epoch}
	return challenge, expiresAt, nil
}

// Verify consumes challenge and verifies its signature for exactly one request.
func (a *AdministratorAuthenticator) Verify(publicKey ed25519.PublicKey, epoch uint64, challenge, signature string, payload []byte) bool {
	a.mu.Lock()
	issued, ok := a.challenges[challenge]
	delete(a.challenges, challenge)
	a.mu.Unlock()
	if !ok || issued.epoch != epoch || !issued.expiresAt.After(a.now()) || len(publicKey) != ed25519.PublicKeySize {
		return false
	}
	rawSignature, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil {
		return false
	}
	return ed25519.Verify(publicKey, payload, rawSignature)
}

// Consume invalidates a supplied challenge when an authentication attempt
// fails before signature verification can run.
func (a *AdministratorAuthenticator) Consume(challenge string) {
	a.mu.Lock()
	delete(a.challenges, challenge)
	a.mu.Unlock()
}
