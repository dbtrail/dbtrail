// Package baselineintegrity provides at-rest integrity for baseline Parquet
// files: a CRC-32C _MANIFEST sidecar written next to the _SUCCESS marker,
// validated on every baseline read path — local (#636) and S3 (#698).
//
// The Parquet readers validate nothing — DuckDB's parquet_scan and parquet-go's
// OpenFile both trust the bytes, with no CRC validation and no pragma to force it
// — so a file silently corrupted on disk or in S3 (bit-rot, a partial write)
// would be read back as truth. The _MANIFEST sidecar closes that: it records a
// CRC-32C over each table's Parquet file BYTES at write time, and the baseline
// read paths re-hash and compare, failing loud on a mismatch instead of
// returning garbage rows. Local reads validate via ValidateLocalFile; S3 reads
// validate via ValidateS3File (#698), which pre-pass-streams the original
// object through CRC-32C before any DuckDB reader touches it — see s3.go.
//
// Scope is bit-rot / partial-write, NOT deliberate tampering: an attacker who
// rewrites a Parquet file can also rewrite this manifest. True tamper-evidence
// needs signing or an external registry — a separate, larger effort, deliberately
// out of scope here. The manifest is a SEPARATE file from the data it covers, so
// corruption of one does not silently mask the other, and CRC-32C (Castagnoli,
// hardware-accelerated) detects any flipped byte with no parquet-library dependency.
//
// The content_digest (#633) is a different thing: a source-fidelity fingerprint
// of the ROWS, embedded in the Parquet metadata; it neither covers the file
// encoding nor survives being rewritten. This manifest covers the bytes.
//
// It lives in its own package (not internal/baseline) so the read paths —
// reconstruct and query — can import it without a cycle through baseline's own
// test dependencies.
package baselineintegrity

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// ManifestName is the per-snapshot integrity sidecar, written under the snapshot
// directory alongside the _SUCCESS marker.
const ManifestName = "_MANIFEST"

// manifestVersion is the on-disk schema version of the manifest JSON.
const manifestVersion = 1

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// ErrIntegrity is returned when a baseline file's CRC-32C does not match the
// manifest — the file is corrupt (bit-rot or a partial write). Callers fail loud.
var ErrIntegrity = errors.New("baseline integrity check failed (file corrupt)")

// Manifest is the integrity sidecar: each Parquet file's path (relative to the
// snapshot directory, forward-slashed so it is OS- and S3-portable) → its
// CRC-32C hex digest.
type Manifest struct {
	Version int               `json:"version"`
	Algo    string            `json:"algo"` // always "crc32c"
	Files   map[string]string `json:"files"`
}

// CRC32CFile streams path through CRC-32C (Castagnoli) and returns the 8-char
// lowercase hex digest.
func CRC32CFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := crc32.New(crc32cTable)
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%08x", h.Sum32()), nil
}

// manifested reports whether a snapshot file is covered by the manifest: every
// table file, and both files of a table delta (#1638), which are Parquet under
// another suffix (baseline.TableDeltaPosdelSuffix / TableDeltaUpsertsSuffix;
// spelled out here because baseline imports this package). A delta is folded
// into a rewritten table at compaction, so an unverified one would be the one
// route by which corrupt bytes reach a freshly certified file.
func manifested(name string) bool {
	return strings.HasSuffix(name, ".parquet") || strings.HasSuffix(name, ".posdel") || strings.HasSuffix(name, ".upserts")
}

// WriteManifest hashes every .parquet file under snapshotDir and writes the
// integrity manifest. It is called on full baseline success, before the _SUCCESS
// marker, so a snapshot that has _SUCCESS also has its manifest.
func WriteManifest(snapshotDir string) error {
	m := Manifest{Version: manifestVersion, Algo: "crc32c", Files: map[string]string{}}
	err := filepath.WalkDir(snapshotDir, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() || !manifested(d.Name()) {
			return walkErr
		}
		crc, err := CRC32CFile(p)
		if err != nil {
			return fmt.Errorf("crc32c %s: %w", p, err)
		}
		rel, err := filepath.Rel(snapshotDir, p)
		if err != nil {
			return err
		}
		m.Files[filepath.ToSlash(rel)] = crc
		return nil
	})
	if err != nil {
		return err
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(snapshotDir, ManifestName), b, 0o644)
}

// LoadManifest reads the integrity manifest from snapshotDir. ok=false with a nil
// error means the manifest is ABSENT — a legacy snapshot written before #636,
// which reads as "integrity not verified" rather than failing.
func LoadManifest(snapshotDir string) (m *Manifest, ok bool, err error) {
	b, err := os.ReadFile(filepath.Join(snapshotDir, ManifestName))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var parsed Manifest
	if err := json.Unmarshal(b, &parsed); err != nil {
		return nil, false, fmt.Errorf("parse %s: %w", ManifestName, err)
	}
	return &parsed, true, nil
}

// ValidateLocalFile checks a local baseline Parquet file against its snapshot's
// integrity manifest (the snapshot directory is the file's grandparent,
// <snapshot>/<db>/<table>.parquet). Outcomes:
//   - nil (verified): the file's CRC-32C matches the manifest.
//   - ErrIntegrity (wrapped): the CRC-32C does NOT match — the file is corrupt.
//     This is the fail-loud case; the Parquet readers validate nothing.
//   - nil (skip): "cannot verify" — there is no manifest (a legacy snapshot, or a
//     path not under a snapshot directory), the file is absent from the manifest
//     (added out-of-band after the manifest was written), the manifest is
//     unreadable/unparseable (a rotted sidecar, logged), OR its version/algo is
//     unrecognized (a newer format this binary can't validate, logged). All mean
//     "unverified",
//     NOT "data corrupt", so they degrade to a skip rather than denying recovery
//     of possibly-intact data: the bit-rot guarded here is random, so a flip lands
//     on the data (caught as a CRC mismatch with the manifest intact) OR on the
//     sidecar (degraded here) — not both. A rotted sidecar must never brick a
//     good baseline, least of all in `verify` itself.
//   - a non-ErrIntegrity error only when the data file cannot be opened/read
//     (it would fail the subsequent read anyway).
func ValidateLocalFile(path string) error {
	snapshotDir := filepath.Dir(filepath.Dir(path))
	m, ok, err := LoadManifest(snapshotDir)
	if err != nil {
		// Unreadable / unparseable manifest = cannot verify, not data corruption;
		// degrade to a skip so a rotted sidecar can't deny recovery of good data.
		slog.Warn("integrity manifest unreadable; treating baseline as integrity-not-verified",
			"snapshot", snapshotDir, "error", err)
		return nil
	}
	if !ok {
		return nil // no manifest — legacy/temp/test, not verifiable, not a failure
	}
	rel, err := filepath.Rel(snapshotDir, path)
	if err != nil {
		return nil // path not under the snapshot dir — unexpected layout, skip
	}
	want, verify := m.digestFor(filepath.ToSlash(rel), snapshotDir)
	if !verify {
		return nil
	}
	got, err := CRC32CFile(path)
	if err != nil {
		return fmt.Errorf("read baseline for integrity check %s: %w", path, err)
	}
	if got != want {
		return fmt.Errorf("%w: %s (crc32c %s, manifest %s)", ErrIntegrity, path, got, want)
	}
	return nil
}

// digestFor returns the manifest's recorded digest for rel (forward-slashed,
// snapshot-relative) and whether the manifest can vouch for it at all — the
// shared core of ValidateLocalFile and ValidateS3File. verify=false means
// "cannot verify", never "data corrupt":
//   - An unrecognized version/algo (a future v2 schema, or a different digest)
//     is "cannot verify with THIS binary" — degrade to a skip, never
//     ErrIntegrity, so an older binary can't brick recovery of a
//     newer-but-intact baseline by comparing its crc32c against a foreign
//     value (logged; snapshotLabel is the directory or s3:// prefix).
//   - A file absent from the manifest (added out-of-band after the manifest
//     was written) skips rather than false-positives.
func (m *Manifest) digestFor(rel, snapshotLabel string) (want string, verify bool) {
	if m.Version != manifestVersion || m.Algo != "crc32c" {
		slog.Warn("integrity manifest version/algo unrecognized; treating baseline as integrity-not-verified",
			"snapshot", snapshotLabel, "version", m.Version, "algo", m.Algo)
		return "", false
	}
	want, verify = m.Files[rel]
	if !verify {
		// Debug, not Warn: the local path re-runs this per read (no cache), so
		// a Warn would flood loops; the signal still exists for triage — an
		// unlisted file can indicate a writer-side bug, not just an
		// out-of-band copy.
		slog.Debug("baseline file not listed in its snapshot's integrity manifest; integrity not verified",
			"snapshot", snapshotLabel, "file", rel)
	}
	return want, verify
}
