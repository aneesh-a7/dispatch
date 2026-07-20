// Package idgen generates IDs that are both unique and lexicographically
// sortable by creation time — useful for skimming a data directory or
// log file and immediately seeing chronological order, without needing
// a real UUID library as a dependency for something this small.
package idgen

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// New returns an id of the form "<prefix>_<millis-since-epoch>_<8 random hex chars>".
// The timestamp prefix makes IDs sort chronologically as plain strings;
// the random suffix makes collisions between two IDs minted in the same
// millisecond astronomically unlikely.
func New(prefix string) string {
	ms := time.Now().UTC().UnixMilli()
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is effectively unheard of on a real OS; if
		// it ever does, fall back to a still-unique (if less random) id
		// rather than panicking a control plane over ID generation.
		return fmt.Sprintf("%s_%d_%d", prefix, ms, time.Now().UnixNano())
	}
	return fmt.Sprintf("%s_%d_%s", prefix, ms, hex.EncodeToString(b[:]))
}
