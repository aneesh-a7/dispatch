// Package webui serves the dashboard's static assets. They are embedded
// into the compiled binary with go:embed, so the control plane remains a
// single self-contained executable, with no separate frontend build step, no
// static file server to deploy alongside it, nothing to go stale.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/*
var staticFiles embed.FS

// Handler returns an http.Handler that serves the dashboard's static
// assets rooted at "/". The dashboard talks to the existing /v1/* JSON
// API from client-side JS. It has no server-side templating and no
// dedicated backend endpoints of its own.
func Handler() http.Handler {
	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		// Only possible if the embed directive above is wrong, which
		// would be caught by `go build` in the first place. This is a
		// belt-and-suspenders panic, not a real runtime failure mode.
		panic("webui: static assets not embedded correctly: " + err.Error())
	}
	return http.FileServer(http.FS(sub))
}
