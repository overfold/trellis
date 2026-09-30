package api

import "time"

// AdministratorChallengeResponse contains a short-lived one-time signing challenge.
type AdministratorChallengeResponse struct {
	Challenge string    `json:"challenge"`
	ExpiresAt time.Time `json:"expires_at"`
}
