package metadata

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Source flavors a binlog capture can run as. The literals are the ones
// stream_state.flavor stores and go-mysql's BinlogSyncerConfig.Flavor takes.
const (
	FlavorMySQL   = "mysql"
	FlavorMariaDB = "mariadb"
)

// defaultFlavorSetting names where a command-line operator declares the flavor.
const defaultFlavorSetting = "--source-flavor (or BINTRAIL_SOURCE_FLAVOR)"

// ClassifyVersion maps a VERSION() string to a flavor: "mariadb" when it
// contains "mariadb" in any case, "mysql" for any other non-empty string
// (Percona, Aurora and stock MySQL all report a plain MySQL version), and ""
// for an empty one. Empty is unknown, never MySQL.
func ClassifyVersion(version string) string {
	v := strings.ToLower(strings.TrimSpace(version))
	switch {
	case v == "":
		return ""
	case strings.Contains(v, "mariadb"):
		return FlavorMariaDB
	default:
		return FlavorMySQL
	}
}

// DetectSourceFlavor asks the source server what it is. Unlike DetectFlavor it
// returns the failure instead of hiding it, so a caller that must not guess
// can refuse. version is the raw VERSION() string, for messages.
func DetectSourceFlavor(db *sql.DB) (flavor, version string, err error) {
	if err := db.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		return "", "", fmt.Errorf("SELECT VERSION() failed: %w", err)
	}
	flavor = ClassifyVersion(version)
	if flavor == "" {
		return "", version, errors.New("SELECT VERSION() returned an empty string")
	}
	return flavor, version, nil
}

// NormalizeDeclaredFlavor canonicalizes an operator-declared flavor. Empty
// means "not declared" and stays empty: detection decides.
func NormalizeDeclaredFlavor(declared string) (string, error) {
	switch f := strings.ToLower(strings.TrimSpace(declared)); f {
	case "", FlavorMySQL, FlavorMariaDB:
		return f, nil
	default:
		return "", fmt.Errorf("invalid source flavor %q: must be %q or %q, or empty to detect it from the server",
			declared, FlavorMySQL, FlavorMariaDB)
	}
}

// FlavorMismatchError is the refusal to capture when the declared flavor
// contradicts what the server reports. Capturing a MariaDB as MySQL loses the
// statement text in silence and, on MariaDB 11.4+, trips the zero-position
// guard; capturing a MySQL as MariaDB breaks the GTID handshake.
type FlavorMismatchError struct {
	Declared, Detected, Version string
	// Fix is the sentence that tells the operator what to change. The
	// resolver fills in the command-line one; a caller whose operator declares
	// the flavor somewhere else replaces it.
	Fix string
}

func (e *FlavorMismatchError) Error() string {
	return fmt.Sprintf("source flavor mismatch: declared %q, but the server reports %q (VERSION() = %q). %s",
		e.Declared, e.Detected, e.Version, e.Fix)
}

// TelemetryClass implements telemetry.Classed: a setting the operator got wrong.
func (e *FlavorMismatchError) TelemetryClass() string { return "config_invalid" }

// FlavorUndetectedError is the refusal to capture when the server could not be
// asked what it is and nobody declared it. DBTrail does not guess.
type FlavorUndetectedError struct {
	Err error
	Fix string
}

func (e *FlavorUndetectedError) Error() string {
	return fmt.Sprintf("could not detect the source flavor: %v. DBTrail does not guess it. %s", e.Err, e.Fix)
}

func (e *FlavorUndetectedError) Unwrap() error { return e.Err }

// TelemetryClass implements telemetry.Classed.
func (e *FlavorUndetectedError) TelemetryClass() string { return "config_invalid" }

// ResolveSourceFlavor decides the flavor a capture runs as, from what the
// operator declared (raw, may be empty) and what DetectSourceFlavor returned:
//
//   - detected, nothing declared: the detected flavor;
//   - detected, declared the same: that flavor;
//   - detected, declared differently: *FlavorMismatchError;
//   - detection failed, nothing declared: *FlavorUndetectedError;
//   - detection failed, declared: the declared flavor, plus a warning to show.
//
// An empty detected flavor with a nil error counts as a failed detection.
func ResolveSourceFlavor(declared, detected, version string, detectErr error) (flavor, warning string, err error) {
	decl, err := NormalizeDeclaredFlavor(declared)
	if err != nil {
		return "", "", err
	}
	if detectErr == nil && detected == "" {
		detectErr = errors.New("the server gave no usable VERSION()")
	}
	if detectErr != nil {
		if decl == "" {
			return "", "", &FlavorUndetectedError{Err: detectErr,
				Fix: "Fix the connection, or set " + defaultFlavorSetting + " to mysql or mariadb."}
		}
		return decl, fmt.Sprintf("could not detect the source flavor (%v); using the declared flavor %q without checking it", detectErr, decl), nil
	}
	if decl != "" && decl != detected {
		return "", "", &FlavorMismatchError{Declared: decl, Detected: detected, Version: version,
			Fix: fmt.Sprintf("Set %s to %s, or leave it empty so DBTrail detects it.", defaultFlavorSetting, detected)}
	}
	return detected, "", nil
}
