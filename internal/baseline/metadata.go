package baseline

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2" // DuckDB driver for s3:// metadata reads
	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// Parquet metadata keys for baseline binlog position and schema DDL.
const (
	MetaKeyBinlogFile     = "bintrail.baseline_binlog_file"
	MetaKeyBinlogPos      = "bintrail.baseline_binlog_position"
	MetaKeyGTIDSet        = "bintrail.baseline_gtid_set"
	MetaKeyCreateTableSQL = "bintrail.create_table_sql"
	// MetaKeyLSN anchors a PostgreSQL-source baseline: a WAL LSN floor, stored as
	// the decimal string of the uint64 LSN, such that deltas replayed AT OR AFTER
	// this point reconstruct the table's state after the snapshot correctly.
	//
	// This is the replication slot's confirmed_flush_lsn/restart_lsn as of just
	// before the snapshot transaction opened (pgcapture.SlotFloorLSN) — NOT the
	// live pg_current_wal_lsn() read when the snapshot was taken (#771). The two
	// can differ: a transaction committing concurrently with the snapshot can
	// flush its commit WAL record at or before that live LSN while still being
	// invisible to the snapshot's MVCC view (WAL flush happens before the
	// transaction is removed from the procarray), so anchoring the delta window
	// on the live LSN with a strict "after" comparison can silently exclude that
	// transaction from BOTH the baseline and the deltas. The slot floor is always
	// <= the live LSN (read earlier, before the snapshot began), so replaying
	// from it never misses that transaction; any resulting overlap with rows
	// already in the baseline is harmless because the merge is last-write-wins
	// over full-row images. Consumers should therefore treat this value as an
	// INCLUSIVE lower bound ("at or after"), not an exclusive one.
	//
	// Absent on MySQL/MariaDB baselines (which use the binlog file/pos/GTID keys
	// above) and on PG baselines taken before #593 slice A. Part of #593; the
	// floor-vs-live-LSN correction is #771.
	MetaKeyLSN = "bintrail.baseline_lsn"
	// MetaKeyRowCount and MetaKeyContentDigest record, per table, how many rows
	// the baseline ingested and an order-independent content fingerprint of them
	// (consistency.Hasher, version-tagged). The digest is byte-identical to a
	// live ConsistentTableChecksum of the same rows, so the verify capstone
	// (#634) can compare a baseline against the source. Part of epic #631 (#633).
	//
	// The digest certifies SOURCE fidelity (the dump captured the same rows as
	// the source), not Parquet-encoding fidelity: writer transforms such as
	// zero-date→NULL are invisible to it (that is #634's concern). TIMESTAMP
	// agreement assumes the dump used UTC (mydumper's default --tz-utc, which
	// matches ConsistentTableChecksum's UTC session); an externally produced
	// dump made with --skip-tz-utc on a non-UTC server would not match.
	//
	// Readers must treat RowCount as valid only when ContentDigest != ""; the
	// readers clear ContentDigest if RowCount cannot be parsed, so the two are
	// never returned in a trustworthy-digest / untrustworthy-count combination.
	MetaKeyRowCount      = "bintrail.baseline_row_count"
	MetaKeyContentDigest = "bintrail.baseline_content_digest"
	// MetaKeyRenderGUCs records the pinned PostgreSQL rendering-GUC set the
	// baseline's text was produced under (pgcapture.RenderGUCsStamp, #593
	// slice D). Its ABSENCE on an LSN-anchored (PG) baseline marks a pre-pin
	// baseline whose GUC-sensitive text may not join post-pin deltas — readers
	// warn and recommend re-baselining. Absent on MySQL/MariaDB baselines.
	MetaKeyRenderGUCs = "bintrail.render_gucs"
	// MetaKeyCaptureGap marks a snapshot that was published over a KNOWN
	// permanent capture gap — a `baseline refresh` / `reconstruct
	// --output-format parquet` run whose window contained a stamped
	// stream_state.gap_lost_at (or an index that could not answer the question)
	// and which proceeded anyway under --allow-gaps (#1170).
	//
	// Its value is one human-readable line per gap the snapshot was folded
	// across, oldest first. It is INHERITED: a snapshot derived from a gapped
	// one carries its ancestor's lines plus any of its own, because the missing
	// events are missing from every descendant too. That inheritance is the
	// whole point — an operator must never have to reconstruct the provenance
	// chain by hand to learn that a baseline is knowingly incomplete.
	//
	// Absent on every snapshot taken from a real dump, and on any reconstructed
	// snapshot whose window was verifiably gap-free.
	MetaKeyCaptureGap = "bintrail.capture_gap"
	// MetaKeyLastEventID is the highest binlog_events.event_id the fold that
	// wrote this file applied (#1720), carried forward unchanged by a run that
	// applied none. The next baseline-anchored fetch uses it as an index floor
	// (query.Options.SinceEventID) when the index was written by a stream.
	// Absent on a dump, and on any file written before this key existed.
	MetaKeyLastEventID = "bintrail.last_event_id"
)

// RenderGUCsPinned is the canonical value the capture side stamps under
// MetaKeyRenderGUCs (pgcapture.RenderGUCsStamp builds it from its pinned GUC
// list; the two are cross-pinned by pgcapture's unit test). It lives here so
// the READ layer can compare a baseline's stamp against the current pin
// without importing pgcapture (which would link pgx into the MySQL binary).
// A stamp that is absent OR different means the baseline's GUC-sensitive text
// may not join deltas rendered under the current pin.
const RenderGUCsPinned = "TimeZone=UTC;DateStyle=ISO;extra_float_digits=3;bytea_output=hex;IntervalStyle=postgres"

// DumpMetadata contains information parsed from a mydumper metadata file or
// from a baseline Parquet file's key-value metadata.
type DumpMetadata struct {
	StartedAt      time.Time
	BinlogFile     string
	BinlogPos      int64
	GTIDSet        string
	CreateTableSQL string // raw mydumper -schema.sql bytes; set for baselines written after #187
	ContentDigest  string // version-tagged content fingerprint; set after #633, empty when absent (old baselines)
	RowCount       int64  // rows ingested into this table's baseline; valid only when ContentDigest != ""
	LSN            uint64 // PostgreSQL WAL LSN delta-replay floor, inclusive (MetaKeyLSN, see its doc comment / #771); 0 = absent (MySQL baseline, or pre-#593 PG baseline)
	RenderGUCs     string // pinned rendering-GUC stamp (MetaKeyRenderGUCs, #593 slice D); "" = pre-pin PG baseline or MySQL baseline
	// DeltaChainStart / DeltaBaseAnchor / DeltaBaseSize are set only on the two
	// files of a table delta (#1638, tabledelta.go): when the chain over the
	// base started, and which exact base the row numbers were computed against.
	// Zero on every table file.
	DeltaChainStart time.Time
	DeltaBaseAnchor string
	DeltaBaseSize   int64
	// DeltaSeq is MetaKeyDeltaSeq (#1718): the pair's sequence in its chain.
	// -1 when absent (a table file, or a v0.83.0 pair). DeltaSeqLo is
	// MetaKeyDeltaSeqLo (#1723), the low end of a range pair; -1 when
	// absent (a plain pair: its low end is DeltaSeq).
	DeltaSeq   int
	DeltaSeqLo int
	// LastEventID is MetaKeyLastEventID; 0 when absent.
	LastEventID uint64
	// Producer is MetaKeySnapshotProducer: which code path wrote these bytes
	// ("dump" | "reconstruct"). Empty on any snapshot written before #1545
	// stamped it on the dump path; see ProvenanceOf, which does not guess.
	Producer string
	// DerivedFrom / DerivedFromPath are the ancestor a FOLD was built from
	// (MetaKeyDerivedFrom / MetaKeyDerivedFromPath). Zero on a dump.
	DerivedFrom     time.Time
	DerivedFromPath string
	// SnapshotTimestamp is MetaKeySnapshotTimestamp, the instant the writing
	// run stamped. NOT which snapshot the file belongs to — the directory name
	// is that (internal/snapshotdir) — and the disagreement between the two is
	// how a carried-forward table is recognised. See ProvenanceOf.
	SnapshotTimestamp time.Time
	// CreateTableAsOf is MetaKeyCreateTableAsOf; zero when absent.
	CreateTableAsOf time.Time
	// MydumperFormat is MetaKeyMydumperFormat, written only by the mydumper
	// dump path. Carried here as the one positive signal that dates a
	// pre-#1545 MySQL dump.
	MydumperFormat string
	// CaptureGap is MetaKeyCaptureGap: non-empty means this snapshot is KNOWINGLY
	// incomplete — it was folded across a permanent capture gap under
	// --allow-gaps, or inherited that state from the snapshot it was derived
	// from. Empty is the normal case; see MetaKeyCaptureGap.
	CaptureGap string
}

// StartedAtMarkerFile is a bintrail-authored sidecar written into the mydumper
// output directory by `bintrail dump`, recording that process's own UTC
// wall-clock time captured immediately before invoking mydumper.
//
// mydumper's own "Started dump at" metadata line is written in the dump
// host's LOCAL time, but ParseMetadata parses it with ParseInLocation(...,
// time.UTC) — i.e. verbatim, as if it already were UTC. On a dump host whose
// clock isn't set to UTC, every reconstruct/verify/shim consumer that anchors
// replay at this timestamp skews by the host's UTC offset (#768). When this
// marker is present, ParseMetadata prefers it over the ambiguous mydumper
// line, sidestepping the timezone question entirely. It is absent for
// mydumper dumps produced outside `bintrail dump` (manual mydumper run, a
// different tool) — see docs/dump-and-baseline.md for that case.
const StartedAtMarkerFile = "bintrail_dump_started_at_utc"

// WriteStartedAtMarker records t (converted to UTC) into inputDir as the
// authoritative dump-start time. Called by `bintrail dump` right before
// invoking mydumper; best-effort on the caller's side — a write failure just
// means ParseMetadata falls back to mydumper's own local-time line.
func WriteStartedAtMarker(inputDir string, t time.Time) error {
	path := filepath.Join(inputDir, StartedAtMarkerFile)
	if err := os.WriteFile(path, []byte(t.UTC().Format(time.RFC3339Nano)+"\n"), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", StartedAtMarkerFile, err)
	}
	return nil
}

// readStartedAtMarker reads StartedAtMarkerFile from inputDir, if present and
// parseable. Returns ok=false (never an error) when the marker is absent or
// corrupt — callers fall back to mydumper's own metadata timestamp.
func readStartedAtMarker(inputDir string) (t time.Time, ok bool) {
	path := filepath.Join(inputDir, StartedAtMarkerFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, false
	}
	t, err = time.Parse(time.RFC3339Nano, strings.TrimSpace(string(data)))
	if err != nil {
		slog.Warn("dump-start marker present but unparseable; falling back to mydumper's 'Started dump at' line",
			"path", path, "error", err)
		return time.Time{}, false
	}
	return t.UTC(), true
}

// ParseMetadata reads the mydumper "metadata" file in inputDir and returns the
// extracted dump timestamp and binlog position information.
//
// The metadata file looks like:
//
//	Started dump at: 2025-02-28 00:00:00
//	SHOW MASTER STATUS:
//	    Log: binlog.000042
//	    Pos: 12345
//	    GTID: 3e11fa47-...:1-100
//	Finished dump at: 2025-02-28 00:01:23
//
// If inputDir also carries StartedAtMarkerFile (written by `bintrail dump`),
// its process-captured UTC time is preferred over the "Started dump at" line
// above, which is ambiguous with respect to the dump host's timezone (#768).
func ParseMetadata(inputDir string) (DumpMetadata, error) {
	path := filepath.Join(inputDir, "metadata")
	f, err := os.Open(path)
	if err != nil {
		return DumpMetadata{}, fmt.Errorf("open metadata file: %w", err)
	}
	defer f.Close()

	m := DumpMetadata{DeltaSeq: -1, DeltaSeqLo: -1}
	markerStartedAt, haveMarker := readStartedAtMarker(inputDir)
	if haveMarker {
		m.StartedAt = markerStartedAt
	}

	// Where a line sits matters (#1744). The legacy format (0.10 to 0.13) puts
	// this server's position under "SHOW MASTER STATUS:" and, on a replica, the
	// UPSTREAM server's under "SHOW SLAVE STATUS:" with the same tab-indented
	// keys; reading every block let the last one win, anchoring a replica's
	// backup on another server's binlog. 0.16.x writes the position as
	// File/Position/Executed_Gtid_Set under an INI "[master]" section.
	var (
		legacyBlock string // header of the current "SHOW ...:" block
		iniSection  string // name of the current "[...]" section
		gtidCont    bool   // the legacy GTID set continues on the next line
	)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		// A legacy GTID set with several server UUIDs spans lines, and the
		// continuation lines are NOT indented (measured, 0.10):
		//	\tGTID:uuid-a:1-12,
		//	uuid-b:1-5
		if gtidCont {
			if cont := strings.TrimSpace(line); cont != "" && !strings.HasPrefix(line, "\t") {
				m.GTIDSet += cont
				gtidCont = strings.HasSuffix(cont, ",")
				continue
			}
			gtidCont = false
		}
		if strings.HasPrefix(line, "SHOW ") && strings.HasSuffix(line, ":") {
			legacyBlock = line
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			iniSection = strings.TrimSuffix(strings.TrimPrefix(line, "["), "]")
		}
		inMasterBlock := legacyBlock == "SHOW MASTER STATUS:"

		// New mydumper format (0.16+) prefixes lines with "# ".
		trimmed := strings.TrimPrefix(line, "# ")

		if after, ok := strings.CutPrefix(trimmed, "Started dump at: "); ok {
			if !haveMarker {
				t, err := time.ParseInLocation("2006-01-02 15:04:05", strings.TrimSpace(after), time.UTC)
				if err != nil {
					return DumpMetadata{}, fmt.Errorf("parse dump timestamp %q: %w", after, err)
				}
				m.StartedAt = t
			}
		} else if after, ok := strings.CutPrefix(line, "\tLog: "); ok && inMasterBlock {
			m.BinlogFile = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "\tPos: "); ok && inMasterBlock {
			pos, err := strconv.ParseInt(strings.TrimSpace(after), 10, 64)
			if err == nil {
				m.BinlogPos = pos
			}
		} else if after, ok := strings.CutPrefix(line, "\tGTID:"); ok && inMasterBlock {
			// No space after the colon in the legacy format (0.10 to at least
			// 0.13, "\tGTID:uuid:1-10", measured on Ubuntu 24.04's 0.10);
			// matching "\tGTID: " lost every such dump's GTID set (#1688).
			m.GTIDSet = strings.TrimSpace(after)
			gtidCont = strings.HasSuffix(m.GTIDSet, ",")
		} else if after, ok := strings.CutPrefix(line, "File = "); ok && iniSection == "master" {
			m.BinlogFile = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "Position = "); ok && iniSection == "master" {
			pos, err := strconv.ParseInt(strings.TrimSpace(after), 10, 64)
			if err == nil {
				m.BinlogPos = pos
			}
		} else if after, ok := strings.CutPrefix(line, "Executed_Gtid_Set = "); ok && iniSection == "master" {
			m.GTIDSet = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(trimmed, "SOURCE_LOG_FILE = "); ok {
			m.BinlogFile = unquote(strings.TrimSpace(after))
		} else if after, ok := strings.CutPrefix(trimmed, "SOURCE_LOG_POS = "); ok {
			pos, err := strconv.ParseInt(strings.TrimSpace(after), 10, 64)
			if err == nil {
				m.BinlogPos = pos
			}
		} else if after, ok := strings.CutPrefix(trimmed, "executed_gtid_set = "); ok {
			m.GTIDSet = unquote(strings.TrimSpace(after))
		}
	}
	if err := scanner.Err(); err != nil {
		return DumpMetadata{}, fmt.Errorf("read metadata file: %w", err)
	}
	if m.StartedAt.IsZero() {
		return DumpMetadata{}, fmt.Errorf("metadata file missing 'Started dump at:' line")
	}
	return m, nil
}

// RefusedDumpMarkerFile is written into a dump directory `bintrail dump`
// refused (#1744). When there was no previous dump to restore, the refused one
// stays on disk; Run reads this file and refuses to convert it.
const RefusedDumpMarkerFile = "bintrail_dump_refused"

// WriteRefusedDumpMarker records why the dump in dir was refused.
func WriteRefusedDumpMarker(dir, reason string) error {
	return os.WriteFile(filepath.Join(dir, RefusedDumpMarkerFile), []byte(reason+"\n"), 0o644)
}

// ReadRefusedDumpMarker reports whether dir holds a refused dump, and why.
func ReadRefusedDumpMarker(dir string) (reason string, refused bool) {
	b, err := os.ReadFile(filepath.Join(dir, RefusedDumpMarkerFile))
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(b)), true
}

// ErrDumpNotAnchored marks a dump whose metadata was read and names no binlog
// position (#1688). A baseline converted from it has nothing to anchor the next
// fold on, which would fall back to timestamps (#797's loss class) with no
// warning, so a caller that publishes backups must refuse it.
var ErrDumpNotAnchored = errors.New("the dump recorded no binlog position")

// RequireDumpPosition reads inputDir's mydumper metadata and returns
// ErrDumpNotAnchored when it names no binlog position. mydumper only warns and
// exits 0 when it cannot read the position: binary logging off, a dump user
// without REPLICATION CLIENT, or (measured with 0.10) a build older than 0.16.3
// against MySQL 8.4, which removed SHOW MASTER STATUS. An unreadable
// or missing metadata file is returned as ParseMetadata's own error, not as
// ErrDumpNotAnchored, so each caller decides what an unverifiable dump means.
func RequireDumpPosition(inputDir string) error {
	m, err := ParseMetadata(inputDir)
	if err != nil {
		return err
	}
	if m.BinlogFile == "" {
		return fmt.Errorf("%w: mydumper exited successfully, but its metadata names no binlog position, "+
			"so a backup made from it could not be updated from recorded changes. Three things cause this: "+
			"binary logging is off on the source; the dump user cannot read the position (it needs REPLICATION CLIENT, "+
			"or BINLOG MONITOR on MariaDB); or mydumper is older than 0.16.3 and the source is MySQL 8.4 or newer, "+
			"which removed the statement those builds read the position with", ErrDumpNotAnchored)
	}
	return nil
}

// ReadParquetMetadata opens a local Parquet file and extracts the baseline
// binlog position from its file-level key-value metadata. Returns a zero-value
// DumpMetadata (no error) when the file lacks position metadata (older baselines).
func ReadParquetMetadata(path string) (DumpMetadata, error) {
	f, err := os.Open(path)
	if err != nil {
		return DumpMetadata{}, fmt.Errorf("open baseline file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return DumpMetadata{}, fmt.Errorf("stat baseline file: %w", err)
	}

	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return DumpMetadata{}, fmt.Errorf("open parquet file: %w", err)
	}

	m := DumpMetadata{DeltaSeq: -1, DeltaSeqLo: -1}
	if v, ok := pf.Lookup(MetaKeyBinlogFile); ok {
		m.BinlogFile = v
	}
	if v, ok := pf.Lookup(MetaKeyBinlogPos); ok {
		pos, parseErr := strconv.ParseInt(v, 10, 64)
		if parseErr != nil {
			slog.Warn("corrupt baseline_binlog_position in Parquet metadata",
				"path", path, "raw_value", v, "error", parseErr)
		} else {
			m.BinlogPos = pos
		}
	}
	if v, ok := pf.Lookup(MetaKeyLSN); ok {
		lsn, parseErr := strconv.ParseUint(v, 10, 64)
		if parseErr != nil {
			slog.Warn("corrupt baseline_lsn in Parquet metadata",
				"path", path, "raw_value", v, "error", parseErr)
		} else {
			m.LSN = lsn
		}
	}
	if v, ok := pf.Lookup(MetaKeyGTIDSet); ok {
		m.GTIDSet = v
	}
	if v, ok := pf.Lookup(MetaKeyCreateTableSQL); ok {
		m.CreateTableSQL = v
	}
	if v, ok := pf.Lookup(MetaKeyContentDigest); ok {
		m.ContentDigest = v
	}
	if v, ok := pf.Lookup(MetaKeyRenderGUCs); ok {
		m.RenderGUCs = v
	}
	if v, ok := pf.Lookup(MetaKeyCaptureGap); ok {
		m.CaptureGap = v
	}
	readProvenance(path, &m, func(k string) (string, bool) { return pf.Lookup(k) })
	if v, ok := pf.Lookup(MetaKeyDeltaChainStart); ok {
		m.DeltaChainStart = parseFooterTime(path, MetaKeyDeltaChainStart, v)
	}
	if v, ok := pf.Lookup(MetaKeyDeltaBaseAnchor); ok {
		m.DeltaBaseAnchor = v
	}
	if v, ok := pf.Lookup(MetaKeyDeltaBaseSize); ok {
		m.DeltaBaseSize = parseDeltaBaseSize(path, v)
	}
	if v, ok := pf.Lookup(MetaKeyDeltaSeq); ok {
		m.DeltaSeq = parseDeltaSeq(path, v)
	}
	if v, ok := pf.Lookup(MetaKeyDeltaSeqLo); ok {
		m.DeltaSeqLo = parseDeltaSeq(path, v)
	}
	if v, ok := pf.Lookup(MetaKeyLastEventID); ok {
		m.LastEventID = parseLastEventID(path, v)
	}
	if v, ok := pf.Lookup(MetaKeyRowCount); ok {
		n, parseErr := strconv.ParseInt(v, 10, 64)
		if parseErr != nil {
			slog.Warn("corrupt baseline_row_count in Parquet metadata",
				"path", path, "raw_value", v, "error", parseErr)
			// A digest we can't pair with a trustworthy count is not usable:
			// clearing it keeps the "ContentDigest != \"\" ⇒ RowCount valid"
			// contract from ever being observed false (a 0 would otherwise read
			// as a verified-empty table).
			m.ContentDigest = ""
		} else {
			m.RowCount = n
		}
	}
	return m, nil
}

// ReadParquetMetadataAny reads baseline Parquet metadata from either a local
// path or an s3:// URL. For S3 it uses DuckDB's parquet_kv_metadata() table
// function through the httpfs extension (validation below is the one SDK-side
// touch, and it degrades to a skip when the SDK path can't reach the object).
//
// Used by the full-table reconstruct path (#187) which needs baseline
// metadata (CreateTableSQL in particular) from S3-resident Parquet files.
func ReadParquetMetadataAny(ctx context.Context, path string) (DumpMetadata, error) {
	if !strings.HasPrefix(path, "s3://") {
		return ReadParquetMetadata(path)
	}
	// At-rest integrity (#698): the footer read below acts on the same S3
	// object bytes the row paths validate — binlog anchor, CreateTableSQL —
	// so validate BEFORE any caller trusts them. This was the fourth S3
	// byte-read site next to the three row paths; the per-process verdict
	// cache makes the row read's later validation of the same object free.
	if err := baselineintegrity.ValidateS3File(ctx, path); err != nil {
		return DumpMetadata{}, err
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return DumpMetadata{}, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		return DumpMetadata{}, fmt.Errorf("load httpfs extension: %w", err)
	}
	if err := duckdbutil.EnableS3CredentialChain(ctx, db); err != nil {
		return DumpMetadata{}, err
	}

	safePath := strings.ReplaceAll(path, "'", "''")
	q := fmt.Sprintf("SELECT key, value FROM parquet_kv_metadata('%s')", safePath)
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return DumpMetadata{}, fmt.Errorf("query parquet metadata: %w", err)
	}
	defer rows.Close()

	m := DumpMetadata{DeltaSeq: -1, DeltaSeqLo: -1}
	var rowCountCorrupt bool
	for rows.Next() {
		// DuckDB returns key/value as BLOB (BYTE_ARRAY) when the Parquet
		// metadata column stores raw bytes. Scan as []byte to be safe.
		var keyBytes, valBytes []byte
		if err := rows.Scan(&keyBytes, &valBytes); err != nil {
			return DumpMetadata{}, fmt.Errorf("scan metadata row: %w", err)
		}
		key := string(keyBytes)
		val := string(valBytes)
		if applyS3FooterKV(&m, path, key, val) {
			rowCountCorrupt = true
		}
	}
	if err := rows.Err(); err != nil {
		return DumpMetadata{}, fmt.Errorf("iterate metadata rows: %w", err)
	}
	// Applied after the loop: keys arrive as rows in arbitrary order, so the
	// digest may be set after the count row. A digest we can't pair with a
	// trustworthy count is not usable (see ReadParquetMetadata).
	if rowCountCorrupt {
		m.ContentDigest = ""
	}
	return m, nil
}

// applyS3FooterKV applies one footer key/value pair to m, the way the S3
// reader sees them: as rows, in arbitrary order. It reports whether the pair
// was a corrupt row count, which the caller resolves after the loop (the
// digest may arrive before the count). Pure, so the S3 reader's key set is
// testable without S3; the local reader looks keys up by name instead.
func applyS3FooterKV(m *DumpMetadata, path, key, val string) (corrupt bool) {
	switch key {
	case MetaKeyBinlogFile:
		m.BinlogFile = val
	case MetaKeyBinlogPos:
		if pos, parseErr := strconv.ParseInt(val, 10, 64); parseErr == nil {
			m.BinlogPos = pos
		} else {
			slog.Warn("corrupt baseline_binlog_position in S3 Parquet metadata",
				"path", path, "raw_value", val, "error", parseErr)
		}
	case MetaKeyLSN:
		if lsn, parseErr := strconv.ParseUint(val, 10, 64); parseErr == nil {
			m.LSN = lsn
		} else {
			slog.Warn("corrupt baseline_lsn in S3 Parquet metadata",
				"path", path, "raw_value", val, "error", parseErr)
		}
	case MetaKeyGTIDSet:
		m.GTIDSet = val
	case MetaKeyCreateTableSQL:
		m.CreateTableSQL = val
	case MetaKeyContentDigest:
		m.ContentDigest = val
	case MetaKeyRenderGUCs:
		m.RenderGUCs = val
	case MetaKeyCaptureGap:
		m.CaptureGap = val
	case MetaKeySnapshotProducer:
		m.Producer = val
	case MetaKeyDerivedFromPath:
		m.DerivedFromPath = val
	case MetaKeyMydumperFormat:
		m.MydumperFormat = val
	case MetaKeyDerivedFrom:
		m.DerivedFrom = parseFooterTime(path, MetaKeyDerivedFrom, val)
	case MetaKeySnapshotTimestamp:
		m.SnapshotTimestamp = parseFooterTime(path, MetaKeySnapshotTimestamp, val)
	case MetaKeyDeltaChainStart:
		m.DeltaChainStart = parseFooterTime(path, MetaKeyDeltaChainStart, val)
	case MetaKeyDeltaBaseAnchor:
		m.DeltaBaseAnchor = val
	case MetaKeyDeltaBaseSize:
		m.DeltaBaseSize = parseDeltaBaseSize(path, val)
	case MetaKeyDeltaSeq:
		m.DeltaSeq = parseDeltaSeq(path, val)
	case MetaKeyDeltaSeqLo:
		m.DeltaSeqLo = parseDeltaSeq(path, val)
	case MetaKeyLastEventID:
		m.LastEventID = parseLastEventID(path, val)
	case MetaKeyRowCount:
		if n, parseErr := strconv.ParseInt(val, 10, 64); parseErr == nil {
			m.RowCount = n
		} else {
			slog.Warn("corrupt baseline_row_count in S3 Parquet metadata",
				"path", path, "raw_value", val, "error", parseErr)
			corrupt = true
		}
	}
	return corrupt
}

// unquote strips surrounding double quotes from s, if present.
func unquote(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// parseDeltaBaseSize reads MetaKeyDeltaBaseSize. A value that does not parse
// comes back as -1, never 0: the reader compares it against a real file size,
// and -1 can match none, so a corrupt footer fails the pairing check instead of
// passing it for an empty base.
// parseDeltaSeq reads MetaKeyDeltaSeq. A value that does not parse is -1,
// which readTableDelta treats as "not this layout" and sets the pair aside.
func parseDeltaSeq(path, raw string) int {
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		slog.Warn("baseline footer: unreadable table delta sequence; the pair will be set aside", "path", path, "value", raw)
		return -1
	}
	return n
}

func parseDeltaBaseSize(path, raw string) int64 {
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		slog.Warn("corrupt delta_base_size in Parquet metadata", "path", path, "raw_value", raw, "error", err)
		return -1
	}
	return n
}

// parseLastEventID reads MetaKeyLastEventID. A value that does not parse is
// 0, "absent": the fetch then keeps its coarse time floor, which is always safe.
func parseLastEventID(path, raw string) uint64 {
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		slog.Warn("baseline footer: unreadable last event id; the next fetch keeps its time floor", "path", path, "value", raw)
		return 0
	}
	return n
}
