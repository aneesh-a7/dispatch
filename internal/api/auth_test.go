package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestValidToken(t *testing.T) {
	tests := []struct {
		name   string
		want   string
		header string
		ok     bool
	}{
		{"exact match", "s3cret", "Bearer s3cret", true},
		{"scheme is case-insensitive", "s3cret", "bearer s3cret", true},
		{"wrong token", "s3cret", "Bearer nope", false},
		{"empty header", "s3cret", "", false},
		{"missing scheme", "s3cret", "s3cret", false},
		{"scheme only", "s3cret", "Bearer ", false},
		{"prefix of real token is not enough", "s3cret", "Bearer s3c", false},
		{"token with extra suffix", "s3cret", "Bearer s3cretX", false},
		{"wrong scheme", "s3cret", "Basic s3cret", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validToken(tt.want, tt.header); got != tt.ok {
				t.Errorf("validToken(%q, %q) = %v, want %v", tt.want, tt.header, got, tt.ok)
			}
		})
	}
}

func TestAuthMiddleware_NoTokenConfiguredAllowsEverything(t *testing.T) {
	h := authMiddleware("", okHandler())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/jobs", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (auth disabled should not block)", rec.Code)
	}
}

func TestAuthMiddleware_RejectsUnauthenticated(t *testing.T) {
	h := authMiddleware("s3cret", okHandler())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/jobs", nil))

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
	if got := rec.Header().Get("WWW-Authenticate"); got == "" {
		t.Error("WWW-Authenticate header missing on 401")
	}
}

func TestAuthMiddleware_AcceptsValidToken(t *testing.T) {
	h := authMiddleware("s3cret", okHandler())

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer s3cret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 for a valid token", rec.Code)
	}
}

// Health checks have to stay reachable without credentials, or every
// uptime monitor and load balancer in front of this needs the secret.
func TestAuthMiddleware_HealthzStaysOpen(t *testing.T) {
	h := authMiddleware("s3cret", okHandler())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (healthz must not require auth)", rec.Code)
	}
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}
