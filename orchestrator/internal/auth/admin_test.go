package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/adminsign"
)

func TestAdministratorChallengeFloodPreservesInflightChallenges(t *testing.T) {
	authenticator := NewAdministratorAuthenticator()
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	authenticator.now = func() time.Time { return now }
	oldest := ""
	for i := range maxAdministratorChallenges * 2 {
		challenge, _, err := authenticator.Issue(7)
		if err != nil {
			t.Fatalf("fill challenge %d: %v", i, err)
		}
		if i == 0 {
			oldest = challenge
		}
	}

	for range 10 {
		if _, _, err := authenticator.Issue(7); err != nil {
			t.Fatal("public issuance exhausted capacity", err)
		}
	}
	if len(authenticator.challenges) != 0 {
		t.Fatalf("public issuance allocated replay state: %d", len(authenticator.challenges))
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	oldestPayload := adminsign.Payload(oldest, "POST", "/v1/credentials", nil)
	oldestSignature := base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, oldestPayload))
	if !authenticator.Verify(publicKey, 7, oldest, oldestSignature, oldestPayload) {
		t.Fatal("flood evicted an in-flight challenge")
	}
	challenge, _, err := authenticator.Issue(7)
	if err != nil {
		t.Fatal(err)
	}
	payload := adminsign.Payload(challenge, "POST", "/v1/credentials", nil)
	signature := base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	if !authenticator.Verify(publicKey, 7, challenge, signature, payload) {
		t.Fatal("challenge issued at capacity did not verify")
	}
	if authenticator.Verify(publicKey, 7, challenge, signature, payload) {
		t.Fatal("consumed challenge verified twice")
	}
	// Invalid signatures cannot allocate replay entries or burn a challenge.
	fresh, _, err := authenticator.Issue(7)
	if err != nil {
		t.Fatal(err)
	}
	freshPayload := adminsign.Payload(fresh, "GET", "/v1/backup", nil)
	if authenticator.Verify(publicKey, 7, fresh, "invalid", freshPayload) || len(authenticator.challenges) != 2 {
		t.Fatal("invalid proof changed replay state")
	}
	if !authenticator.Verify(publicKey, 7, fresh, base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, freshPayload)), freshPayload) {
		t.Fatal("invalid proof burned fresh challenge")
	}
	now = now.Add(authenticator.ttl)
	if _, _, err := authenticator.Issue(7); err != nil {
		t.Fatal("expired capacity was not reclaimed", err)
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
		payload := adminsign.Payload(challenge, "DELETE", "/v1/raft/members/node-2", nil)
		signature := base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
		return authenticator.Verify(publicKey, epoch, challenge, signature, payload)
	}

	oldEpoch, _, err := authenticator.Issue(7)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(oldEpoch)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] ^= 1
	if verify(base64.RawURLEncoding.EncodeToString(raw), 7) {
		t.Fatal("tampered challenge verified even with a valid administrator proof")
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

func TestAdministratorReplayCacheBoundAndConcurrentReplay(t *testing.T) {
	a := NewAdministratorAuthenticator()
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return now }
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	challenge, _, err := a.Issue(7)
	if err != nil {
		t.Fatal(err)
	}
	payload := adminsign.Payload(challenge, "GET", "/v1/backup", nil)
	signature := base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	var admitted atomic.Int32
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			if a.Verify(publicKey, 7, challenge, signature, payload) {
				admitted.Add(1)
			}
		})
	}
	workers.Wait()
	if admitted.Load() != 1 {
		t.Fatalf("concurrent replay admitted %d requests", admitted.Load())
	}
	for range maxAdministratorChallenges - 1 {
		value, _, err := a.Issue(7)
		if err != nil {
			t.Fatal(err)
		}
		body := adminsign.Payload(value, "GET", "/v1/backup", nil)
		proof := base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, body))
		if !a.Verify(publicKey, 7, value, proof, body) {
			t.Fatal("valid request failed below replay capacity")
		}
	}
	fresh, _, err := a.Issue(7)
	if err != nil {
		t.Fatal("public issuance failed at replay capacity", err)
	}
	freshPayload := adminsign.Payload(fresh, "GET", "/v1/backup", nil)
	freshSignature := base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, freshPayload))
	if a.Verify(publicKey, 7, fresh, freshSignature, freshPayload) || a.Verify(publicKey, 7, challenge, signature, payload) {
		t.Fatal("replay capacity allowed growth or evicted an unexpired replay entry")
	}
	now = now.Add(a.ttl)
	value, _, err := a.Issue(7)
	if err != nil {
		t.Fatal(err)
	}
	body := adminsign.Payload(value, "GET", "/v1/backup", nil)
	if !a.Verify(publicKey, 7, value, base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, body)), body) {
		t.Fatal("expired replay capacity was not reclaimed")
	}
}
