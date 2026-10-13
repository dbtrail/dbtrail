// Package telemetry implements bintrail's usage telemetry: a metadata-only,
// spool-based reporter for command mix and error classes.
//
// Two invariants make the privacy claims verifiable from source rather than
// from documentation:
//
//   - The wire payload is a closed allowlist (Event below, AllowedFields).
//     Nothing reaches the network that is not one of those keys.
//   - This package must not import internal/config, internal/parser,
//     internal/indexer, internal/query, internal/recovery, internal/byos or
//     any server-identity package. A package that cannot reach them cannot
//     read a DSN, a row, or a server UUID — a structural guarantee that
//     survives a careless future contributor, which a field allowlist alone
//     does not (an allowlist is blind to where a value came from).
//
// There is no PERSISTENT identifier: nothing that identifies the install is
// created. Telemetry writes two files outside the spool, and neither carries
// an id: the consent record, and a daemon's install record (install.go) — a
// calendar day and a major.minor version, both shared by every install that
// started that day or runs that release. RunID is
// generated per process, lives in the spooled event until that batch is
// delivered or aged out (so ingestion can dedup a re-sent file), and never
// appears in a daemon beacon — a months-lived process's run_id would be
// exactly the longitudinal key this design refuses to produce.
package telemetry

import (
	"regexp"
	"strings"
	"time"
)

// SchemaVersion is bumped when a field is added; the ingestion side keys off
// it. Additive evolution only — removing a field is a breaking change for
// existing aggregates.
const SchemaVersion = 2

// Event types.
const (
	EventCommandRun   = "command_run"
	EventCommandError = "command_error"
	EventDaemonBeacon = "daemon_beacon"
)

// Outcomes.
const (
	OutcomeOK    = "ok"
	OutcomeError = "error"
)

// Event is the ENTIRE wire payload. Adding a field here without adding it to
// AllowedFields fails the allowlist test; adding it to both is a deliberate
// act that must also update TELEMETRY.md.
//
// Never add: DSNs, hostnames, IPs, schema/table/column names, PK values, row
// data, query text, file paths, server ids or UUIDs, GTIDs, binlog names or
// positions, flag values, positional args, verbatim error or panic strings,
// row counts, or credentials.
type Event struct {
	SchemaVersion  int    `json:"schema_version"`
	EventType      string `json:"event_type"`
	Command        string `json:"command"`
	Outcome        string `json:"outcome"`
	ErrorClass     string `json:"error_class,omitempty"`
	DurationBucket string `json:"duration_bucket,omitempty"`
	Version        string `json:"version"`
	IsRelease      bool   `json:"is_release"`
	OS             string `json:"os"`
	Arch           string `json:"arch"`
	IsCI           bool   `json:"is_ci"`
	IsInteractive  bool   `json:"is_interactive"`
	RunID          string `json:"run_id,omitempty"`
	// InstallAge and PreviousVersion appear on daemon beacons only, never on
	// an event that carries a run_id. See install.go.
	InstallAge      string `json:"install_age,omitempty"`
	PreviousVersion string `json:"previous_version,omitempty"`
}

// AllowedFields is the complete set of JSON keys that may appear on the wire,
// in struct order. The allowlist test reflects over Event and fails on any
// mismatch in either direction.
var AllowedFields = []string{
	"schema_version",
	"event_type",
	"command",
	"outcome",
	"error_class",
	"duration_bucket",
	"version",
	"is_release",
	"os",
	"arch",
	"is_ci",
	"is_interactive",
	"run_id",
	"install_age",
	"previous_version",
}

// durationBucket coarsens a command's wall time. The tail collapses at >10m
// rather than reporting real durations: a precise runtime on a long-running
// index or reconstruct is a proxy for the operator's data volume.
func durationBucket(d time.Duration) string {
	switch {
	case d < 100*time.Millisecond:
		return "<100ms"
	case d < time.Second:
		return "100ms-1s"
	case d < 10*time.Second:
		return "1s-10s"
	case d < time.Minute:
		return "10s-1m"
	case d < 10*time.Minute:
		return "1m-10m"
	default:
		return ">10m"
	}
}

// releaseVersionRE matches the versions GoReleaser injects. Anything else is a
// source build.
var releaseVersionRE = regexp.MustCompile(`^v?(\d+)\.(\d+)\.\d+$`)

// minorVersion truncates a release version to major.minor. Patch-level
// adoption is answerable from release download counts, and the full triple was
// a quasi-identifier: joined with os/arch/command-mix it singles out installs
// at low-hundreds scale.
//
// Anything that is not a release version reports "unknown" rather than passing
// an arbitrary -ldflags string through — a custom or dev version string can be
// near-unique on its own. IsRelease already carries the source-build signal.
func minorVersion(v string) string {
	m := releaseVersionRE.FindStringSubmatch(strings.TrimSpace(v))
	if m == nil {
		return "unknown"
	}
	return m[1] + "." + m[2]
}

// isReleaseVersion reports whether v looks like an official build.
func isReleaseVersion(v string) bool {
	return releaseVersionRE.MatchString(strings.TrimSpace(v))
}

// coarseArch collapses GOARCH to the three buckets that inform platform
// priorities. Exotic architectures are rare enough that reporting them
// verbatim would single their operators out.
func coarseArch(a string) string {
	switch a {
	case "amd64", "arm64":
		return a
	default:
		return "other"
	}
}
