package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"testing"
	"time"
)

func TestAdministratorChallengeIssuanceRemainsAvailableAtCapacity(t *testing.T) {
	authenticator := NewAdministratorAuthenticator()
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	authenticator.now = func() time.Time { return now }
	for i := 0; i < maxAdministratorChallenges; i++ {
		if _, _, err := authenticator.Issue(7); err != nil {
			t.Fatalf("fill challenge %d: %v", i, err)
		}
	}

	challenge, _, err := authenticator.Issue(7)
	if err != nil {
		t.Fatalf("issue challenge at capacity: %v", err)
	}
	if len(authenticator.challenges) != maxAdministratorChallenges {
		t.Fatalf("outstanding challenges = %d, want %d", len(authenticator.challenges), maxAdministratorChallenges)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	payload := AdministratorSigningPayload(challenge, "POST", "/v1/credentials", nil)
	signature := base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	if !authenticator.Verify(publicKey, 7, challenge, signature, payload) {
		t.Fatal("challenge issued at capacity did not verify")
	}
	if authenticator.Verify(publicKey, 7, challenge, signature, payload) {
		t.Fatal("consumed challenge verified twice")
	}
}

func TestAdministratorChallengeEpochAndExpiryRemainEnforced(t *testing.T) {
	authenticator := NewAdministratorAuthenticator()
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	authenticator.now = func() time.Time { return now }
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	verify := func(challenge string, epoch uint64) bool {
		payload := AdministratorSigningPayload(challenge, "DELETE", "/v1/raft/members/node-2", nil)
		signature := base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
		return authenticator.Verify(publicKey, epoch, challenge, signature, payload)
	}

	oldEpoch, _, err := authenticator.Issue(7)
	if err != nil {
		t.Fatal(err)
	}
	if verify(oldEpoch, 8) {
		t.Fatal("challenge verified in a different control epoch")
	}

	expired, _, err := authenticator.Issue(8)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(authenticator.ttl)
	if verify(expired, 8) {
		t.Fatal("challenge verified at its expiry time")
	}
}
