package api

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// authMiddleware gates every route behind a shared bearer token.
//
// Auth is opt-in: with no token configured the control plane behaves
// exactly as it did before, which keeps the single-user localhost setup
// this project started as a one-command affair. Configure a token and the
// same binary becomes safe to point at a network other people can reach.
//
// One shared secret (rather than per-user credentials) matches the shape
// of the problem: a small set of machines and people who already trust
// each other. Per-worker tokens, rotation, and roles are deliberately not
// here; see docs/ROADMAP.md.
func authMiddleware(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Health checks stay open so uptime monitors and load balancers do
		// not need credentials to answer "is this process up?". The handler
		// reports liveness only, no cluster state.
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		if !validToken(token, r.Header.Get("Authorization")) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="dispatch"`)
			writeError(w, http.StatusUnauthorized, "missing or invalid bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// validToken compares the configured token against an Authorization
// header value in constant time. A plain == would return as soon as it
// hit a differing byte, so the time it takes to fail leaks how many
// leading bytes a guess got right, which is enough to recover a secret
// one byte at a time given enough attempts over a network.
func validToken(want, header string) bool {
	const prefix = "Bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return false
	}
	got := header[len(prefix):]
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}
