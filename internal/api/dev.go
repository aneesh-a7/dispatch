package api

import (
	"log"
	"net/http"
	"os"
)

// handleSpawnWorker opens a new terminal window on the control plane's own
// machine, sitting in the project directory with a worker ready to run.
// It backs the dashboard's "Add worker" button so a new worker can be
// started without leaving the browser.
//
// This is the one endpoint that reaches past the shared /v1 API and asks
// the control-plane process to do something local to its host. That only
// makes sense in the single-user setup this project targets, where the
// browser, the control plane, and the worker all live on the same laptop.
// It is deliberately not something to expose on a shared or remote
// control plane: an unauthenticated "open a terminal here" endpoint would
// be a real problem the moment the API is reachable by anyone else.
func (s *Server) handleSpawnWorker(w http.ResponseWriter, r *http.Request) {
	dir, err := os.Getwd()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot determine working directory: "+err.Error())
		return
	}
	// openWorkerTerminal is platform-specific (see dev_windows.go and
	// dev_unix.go). It only fires off the terminal; it does not wait for
	// the worker, which compiles via `go run` and registers on its own a
	// few seconds later, showing up through the normal /v1/workers poll.
	if err := openWorkerTerminal(dir); err != nil {
		writeError(w, http.StatusInternalServerError, "could not open a worker terminal: "+err.Error())
		return
	}
	log.Printf("api: opened a worker terminal in %s", dir)
	writeJSON(w, http.StatusOK, map[string]string{"status": "spawning", "dir": dir})
}
