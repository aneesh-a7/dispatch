// Package config loads optional JSON config files for the control plane
// and worker.
//
// Flags alone are fine for one process on one machine. They get brittle
// once several workers with different capacities, tokens, and control
// plane addresses have to be restarted by hand. A file fixes that without
// taking on a config-language dependency: encoding/json is already used
// everywhere else in this repo.
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
)

// ControlPlane mirrors the control plane's flags. Every field is a
// pointer so "absent from the file" is distinguishable from "present and
// set to the zero value": without that, a config file omitting a field
// would silently stomp a flag with false/0/"".
type ControlPlane struct {
	Addr            *string `json:"addr"`
	DataDir         *string `json:"data_dir"`
	Token           *string `json:"token"`
	WebhookURL      *string `json:"webhook_url"`
	TLSCert         *string `json:"tls_cert"`
	TLSKey          *string `json:"tls_key"`
	HeartbeatTTL    *string `json:"heartbeat_ttl"`
	ReapInterval    *string `json:"reap_interval"`
	CompactInterval *string `json:"compact_interval"`
}

// Worker mirrors the worker's flags.
type Worker struct {
	ControlPlane *string `json:"control_plane"`
	Token        *string `json:"token"`
	Address      *string `json:"address"`
	CPU          *int    `json:"cpu"`
	Memory       *int    `json:"memory"`
	PollInterval *string `json:"poll_interval"`
	JobTimeout   *string `json:"job_timeout"`
}

// Load reads and decodes a JSON config file into v.
func Load(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("config: reading %s: %w", path, err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	// Unknown fields are an error rather than a shrug: a typo'd key in a
	// config file is otherwise invisible, and the symptom (a setting that
	// silently did not apply) is much harder to debug than a startup error.
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("config: parsing %s: %w", path, err)
	}
	return nil
}
