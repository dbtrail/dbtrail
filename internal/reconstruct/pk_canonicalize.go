package reconstruct

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// canonicalizePKValue normalizes a raw Go value scanned from a baseline
// Parquet column to match the string representation that the bintrail
// indexer stored in binlog_events.pk_values for the same column.
//
// This is the linchpin of the merge-on-read PK lookup: the indexer calls
// parser.BuildPKValues with whatever types go-mysql delivered at parse time
// (INT → int32/int64, VARCHAR → string, DATETIME → pre-formatted string
// with parseTime=false as the bintrail default), while DuckDB parquet_scan
// returns its own type set (INT → int32/int64, VARCHAR → string, TIMESTAMP
// → time.Time). Without normalisation the PK strings diverge and every
// event silently misses the baseline row it was supposed to update.
//
// col carries the column's full metadata including ColumnType, which for
// DATETIME/TIMESTAMP encodes the declared fractional precision (e.g.
// "datetime(6)"). Without precision the canonicalizer cannot distinguish
// DATETIME(0) from DATETIME(6) with a whole-second value — the indexer
// stores "14:30:45" and "14:30:45.000000" for those two cases respectively,
// and both scan back to a time.Time with Nanosecond()==0.
//
// Returns an error on any condition the canonicalizer cannot translate
// losslessly: nil PK value (MySQL forbids NULL in PKs, so nil means a
// bug), DATETIME scan not a time.Time/string, or any type not in the
// supported set. Error → the caller must abort the table reconstruction
// rather than silently produce wrong output.
//
// Supported data types (v1 + #212 + #214):
//
//   - int, smallint, tinyint, mediumint, bigint (+ unsigned): pass-through
//     (indexer and DuckDB both deliver intN/uintN — fmt.Sprintf("%v", ...)
//     produces identical decimal strings)
//   - char, varchar, text, tinytext, mediumtext, longtext, enum, set:
//     pass-through (both deliver string)
//   - datetime, timestamp: time.Time → fractional-precision-aware format
//     matching go-mysql's formatDatetime output
//   - date: time.Time → "2006-01-02"
//   - year: pass-through (go-mysql int, DuckDB int32 — both stringify the
//     same via %v)
//   - binary, varbinary, tinyblob, blob, mediumblob, longblob (#1155):
//     []byte → []byte, letting event.BuildPKValues' own formatPKValue apply
//     the content-gated "0x"+uppercase-hex spelling it introduced in #1132.
//     See the binary-family note below for the one asymmetry this branch
//     has to undo.
//   - uuid, inet4, inet6 (MariaDB): the baseline's text form parsed back to
//     the full-width bytes the index keys events by (metadata.MapRow pads
//     them), then spelled by event.BuildPKValues like any []byte.
//   - decimal, numeric: pass-through string (#214). go-mysql v1.13.0's
//     decodeDecimal returns a pre-formatted string when useDecimal is
//     false — and bintrail never sets useDecimal, so every DECIMAL PK
//     lands in the indexer as a Go string like "0.00" or "-99.99". The
//     baseline writer stores DECIMAL as parquet.String() (see
//     internal/baseline/schema.go), fed from mydumper SQL output via
//     parseSQLValue's default branch — which also returns the unquoted
//     numeric literal verbatim. Both sides end up with byte-identical
//     strings, so this branch is a pure type-check + pass-through. Zero
//     values are safe: decodeDecimal writes "0" (not "") for zero-leading
//     integer parts at row_event.go:1565-1567.
//
// Unsupported types fall through to an error. The caller runs
// supportedPKType upstream to catch these at reconstruct-start before any
// real work happens, but this path is the defense-in-depth check.
//
// The binary family (#1155). Both sides now speak raw bytes, so there is no
// spelling to reconcile here: internal/baseline decodes BOTH mydumper
// spellings — the --hex-blob `0x…` literal, which parseSQLValue passes through
// as a token for convertValue→decodeBinaryLiteral to decode by column type
// (#503), and the default `_binary "…"` form, which parseSQLValue unescapes
// directly — and stores raw bytes in a Parquet BYTE_ARRAY column
// (internal/baseline/schema.go). DuckDB scans that back as []byte, and
// event.BuildPKValues → formatPKValue applies the SAME content-gated
// hex/UTF-8 rule the indexer applied at capture (#1132). Handing the bytes
// through unchanged is what makes the two sides one encoder rather than two
// that have to agree.
//
// The ONE asymmetry is fixed-width BINARY(n), and it runs the opposite way
// from the filter path in ReadBaselineRows — do not "fix" one to match the
// other:
//
//   - The baseline (and the live source) carry the full n bytes, because
//     MySQL right-pads a short BINARY(n) value with 0x00 on storage.
//   - The binlog ROW image carries the value with EVERY trailing 0x00 byte
//     stripped, because MySQL length-prefixes MYSQL_TYPE_STRING with the
//     ACTUAL stored length and go-mysql's decodeString reads exactly that —
//     the mechanism internal/verify/render.go documents for #1135 and reverses
//     by re-padding. So pk_values — what this canonicalization has to match —
//     holds the STRIPPED spelling. Corroborated against MySQL 8.0.46 (a
//     16-byte key ending in four zero bytes arrives as 12) and pinned at
//     runtime by assertPaddingStripped. MariaDB is covered by the same
//     mechanism, not by a separate observation.
//
// Hence: trim for "binary", never for varbinary/blob. VARBINARY and the BLOB
// family preserve trailing 0x00 in the ROW image (same run), so trimming them
// would collapse two distinct keys that differ only in a trailing NUL into
// one — a false match on a primary-key lookup, which is strictly worse than
// the miss it would be curing.
//
// Every other type is refused — FLOAT/DOUBLE, TIME, BIT, JSON and the spatial
// family among them. supportedPKTypes is the authoritative list; the switch
// below mirrors it arm for arm, and
// TestCanonicalizePKValue_everySupportedTypeHasAnArm walks that same slice so
// a type admitted by the gates can never fall into the default branch. None
// of the refused types was part of #1155's shape; their round-trip between
// the indexer's pk_values and the baseline Parquet is unverified, not
// known-bad.
func canonicalizePKValue(raw any, col metadata.ColumnMeta) (any, error) {
	if raw == nil {
		return nil, fmt.Errorf("canonicalizePKValue: nil PK value for column %q (MySQL forbids NULL in PK columns; baseline row may be missing the column after schema drift)", col.Name)
	}
	dt := strings.ToLower(strings.TrimSpace(col.DataType))

	switch dt {
	case "int", "integer", "smallint", "tinyint", "mediumint", "bigint", "year":
		// Both indexer and DuckDB deliver Go int/uint types; %v
		// produces identical strings. Pass-through without inspection so
		// int32/int64/uint32/uint64 differences don't matter.
		return raw, nil

	case "char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum", "set":
		// Both sides deliver Go string. Reject non-strings because that
		// would indicate a type mismatch we can't reason about.
		if _, ok := raw.(string); !ok {
			return nil, fmt.Errorf("canonicalizePKValue: %s column %q: expected string, got %T", dt, col.Name, raw)
		}
		return raw, nil

	case "decimal", "numeric":
		// go-mysql returns a pre-formatted string (useDecimal=false, the
		// bintrail default — see cmd/bintrail/stream.go and agent.go which
		// never set it). DuckDB returns the Parquet string column as a Go
		// string too (baseline stores DECIMAL as parquet.String to avoid
		// precision loss). Both sides agree byte-for-byte, so pass-through
		// is correct. Reject non-strings for the same reason as varchar
		// above — a type mismatch here means the caller passed us raw data
		// from a non-string Parquet column and we can't reason about it.
		if _, ok := raw.(string); !ok {
			return nil, fmt.Errorf("canonicalizePKValue: %s column %q: expected string, got %T", dt, col.Name, raw)
		}
		return raw, nil

	case "binary":
		// Fixed width: strip the storage padding the ROW image does not
		// carry, so this matches pk_values. See the binary-family note above.
		b, ok := pkValueBytes(raw)
		if !ok {
			return nil, fmt.Errorf("canonicalizePKValue: %s column %q: expected []byte or string, got %T", dt, col.Name, raw)
		}
		return TrimFixedBinaryPad(b), nil

	case "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		// Variable width: trailing 0x00 is data, not padding — pass through.
		b, ok := pkValueBytes(raw)
		if !ok {
			return nil, fmt.Errorf("canonicalizePKValue: %s column %q: expected []byte or string, got %T", dt, col.Name, raw)
		}
		return b, nil

	case "datetime", "timestamp":
		return canonicalizeDatetime(raw, col)
	case "date":
		return canonicalizeDate(raw, col)

	case "uuid", "inet4", "inet6":
		// MariaDB's fixed binary types. The baseline holds the text MariaDB
		// prints (mydumper dumps `_binary "123e4567-…"`), while pk_values
		// holds the captured bytes, padded to full width by metadata.MapRow
		// (#1944). Parse the text back to those bytes and let
		// event.BuildPKValues spell them, the #1155 pattern: one encoder.
		s, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("canonicalizePKValue: %s column %q: expected string, got %T", dt, col.Name, raw)
		}
		b, err := metadata.ParseMariaDBFixed(dt, s)
		if err != nil {
			return nil, fmt.Errorf("canonicalizePKValue: %s column %q: %w", dt, col.Name, err)
		}
		return b, nil

	default:
		// Render through PKTypeGateReason, the renderer verify and single-row
		// reconstruct gate with, rather than a message of this switch's own:
		// the one it used to carry drifted (#1455), hardcoding
		// "BIT/JSON/spatial" as the refused set while FLOAT/DOUBLE/TIME were
		// refused too, so those keys were blamed on a family they are not in.
		// Nothing here claims WHY a type is refused — the round-trip is
		// unverified for it, which is not the same as known-bad.
		//
		// An EMPTY DataType keeps PKTypeGateReason's wrong-index-database
		// verdict on purpose (#1009/#1198): it is the PostgreSQL snapshot
		// shape. Of the callers that can carry one, cascade Phase-2 refuses a
		// PG-shaped table upstream at fkFilterSafe, which rejects the same
		// empty type token on the FK column. The Iceberg export gates with
		// FirstUnsupportedPKType, which skips the empty type on purpose, and
		// reads no source flavor either, so it is the one path where this
		// verdict names a check that did not run; the export does not claim
		// PostgreSQL support (docs/iceberg-export.md), so that stays a
		// wording gap, not a wrong refusal.
		return nil, fmt.Errorf("canonicalizePKValue: %s; file a follow-up issue if you need this type",
			PKTypeGateReason(col, "the baseline merge", "canonicalize"))
	}
}

// pkValueBytes normalizes a binary-family PK value scanned from a baseline
// Parquet column into bytes. DuckDB returns a BYTE_ARRAY column as []byte;
// string is accepted because a caller may hand back a value that already went
// through a text round-trip. Returning bytes (rather than a pre-rendered
// string) is the point: event.BuildPKValues' formatPKValue then applies the
// same content-gated hex/UTF-8 rule it applied at capture, so both sides go
// through ONE encoder.
func pkValueBytes(raw any) ([]byte, bool) {
	switch v := raw.(type) {
	case []byte:
		return v, true
	case string:
		return []byte(v), true
	default:
		return nil, false
	}
}

// altFixedBinaryPK returns the ONE alternative spelling a baseline row's
// primary key could carry, or ok=false when there is none.
//
// A fixed BINARY(n) key has at most two byte-forms: the padded n bytes MySQL
// stores, and the trailing-0x00-stripped bytes the binlog ROW image carries
// (see canonicalizePKValue's binary-family note). A value with no trailing zero
// byte — most BINARY(16) UUIDs — has only ONE, which is why the loop below
// skips it. canonicalizePKValue produces the stripped form so it matches
// binlog_events.pk_values; an entry in the change map under the PADDED spelling
// means the two sides are keyed differently and the join silently fails (#1158).
//
// Both byte-forms render under the same rule, so there is no third spelling to
// search: formatPKValue is content-gated (valid UTF-8 → stored verbatim, no 0x
// prefix), and padding with 0x00 cannot change UTF-8 validity in either
// direction — appending NUL keeps valid input valid, and NUL is never a
// continuation byte so it cannot repair invalid input.
//
// Direction, precisely: at the only production call site the input has already
// been stripped by canonicalizePKMap, so this always runs strip→pad. The
// pad→strip half is defensive for a caller handing over an uncanonicalized map
// — mergeBaselineImages already does under PGTextPK, inert there only because
// PostgreSQL column metas carry an empty DataType.
//
// Cost, measured per row on an M1 Pro rather than argued: 7.8 ns and zero
// allocations for a non-binary PK (one DataType check per PK column, and
// TrimSpace on a clean string does not allocate); 40.6 ns and zero allocations
// for a BINARY(16) UUID with no padding; 291 ns and 5 allocations in the worst
// case, where every key carries padding and each row builds a PK-sized map plus
// the toggled key string. Ten million rows costs 78 ms in the common case, and
// even the worst case is a few percent against the DuckDB Parquet scan and the
// per-row SQL rendering that surround it. Hoisting the "does this table have a
// fixed-binary PK" decision out of the scan would buy nothing measurable and
// would add per-table state to a function whose current virtue is having none.
// No index, no per-row state that outlives the row.
//
// It cannot fire on a healthy table: pk_values only ever holds the stripped
// spelling, so no legitimate event is keyed under the padded one. And it cannot
// invent a collision between two real rows. That reduces to one fact — an
// alternate is only ever produced for a key that HAS padding, so it always ends
// in 0x00, while a canonical spelling is stripped and so never does. Both
// collision families die there, though the reasoning has to be about the
// STRINGS rather than the bytes, since formatPKValue maps two byte domains into
// one string space: in the verbatim branch the alternate carries a literal 0x00
// byte no stripped spelling has, and in the hex branch a colliding verbatim key
// would have to be the ASCII text "0x"+2n hex characters — 2n+2 bytes in an
// n-byte column, which cannot be stored.
//
// Returns false when no PK column is a fixed BINARY(n) with a known width. Note
// what that means for a pre-#212 snapshot with no COLUMN_TYPE: the CANONICAL
// key is still correct (canonicalizePKValue trims unconditionally and never
// reads the width) — it is only this detector that goes quiet, which is why
// mergeBaselineImages warns once per table rather than letting it pass unsaid.
//
// Scope limit, stated rather than wished away: with several fixed-binary
// components every togglable component is flipped together instead of
// enumerating 2^k combinations. That detects a UNIFORM disagreement — a
// canonicalization regression flips a rule, not one column — and misses a
// partial one, including the partial toggle this function itself produces when
// one component's width is unknown.
func altFixedBinaryPK(pkCols []metadata.ColumnMeta, pkMap map[string]any) (string, bool) {
	var alt map[string]any
	for _, c := range pkCols {
		if !strings.EqualFold(strings.TrimSpace(c.DataType), "binary") {
			continue
		}
		width := FixedBinaryWidth(c.ColumnType)
		if width == 0 {
			continue
		}
		v, ok := pkMap[c.Name].([]byte)
		if !ok {
			continue
		}
		var flipped []byte
		if len(v) < width {
			flipped = make([]byte, width)
			copy(flipped, v)
		} else {
			flipped = TrimFixedBinaryPad(v)
		}
		if bytes.Equal(flipped, v) {
			continue // no padding either way: one spelling only
		}
		if alt == nil {
			// PK columns only. canonicalizePKMap hands back a copy of the
			// WHOLE row, and copying that per row would scale this with the
			// table's column count for no benefit — BuildPKValues reads
			// nothing but pkCols.
			alt = make(map[string]any, len(pkCols))
			for _, p := range pkCols {
				alt[p.Name] = pkMap[p.Name]
			}
		}
		alt[c.Name] = flipped
	}
	if alt == nil {
		return "", false
	}
	return event.BuildPKValues(pkCols, alt), true
}

// FixedBinaryWidth extracts n from a "binary(n)" COLUMN_TYPE, returning 0 when
// it is absent or unparseable (a pre-#212 snapshot carries no COLUMN_TYPE).
//
// Exported because internal/cli's padFixedBinaryFilter must agree with
// altFixedBinaryPK about the pad width — a `--pk` filter and a merge join that
// disagreed would resolve the same key differently. internal/verify/render.go
// keeps its own equivalent for the #1135 render padding; the two must stay in
// agreement, so change them together.
func FixedBinaryWidth(columnType string) int {
	s := strings.ToLower(strings.TrimSpace(columnType))
	if !strings.HasPrefix(s, "binary(") || !strings.HasSuffix(s, ")") {
		return 0
	}
	n, err := strconv.Atoi(s[len("binary(") : len(s)-1])
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

// TrimFixedBinaryPad strips the trailing 0x00 bytes MySQL adds when storing a
// short value in a fixed-width BINARY(n) column, reproducing what the binlog
// ROW image carries for that value (and therefore what the indexer stored in
// binlog_events.pk_values). Values with no trailing zero byte — the common
// case, e.g. most BINARY(16) UUIDs — come back untouched.
//
// Exported for the CLI, which needs the INVERSE (pad a pk_values spelling back
// to the baseline's full width) and must agree with this function about which
// bytes are padding.
func TrimFixedBinaryPad(b []byte) []byte {
	return bytes.TrimRight(b, "\x00")
}

// canonicalizeDatetime converts a time.Time (typical DuckDB scan output for
// TIMESTAMP columns) into the string format that go-mysql's formatDatetime
// produces for DATETIME/TIMESTAMP row events when parseTime is false (the
// bintrail default): "2006-01-02 15:04:05" with a trailing "%0Nd" fraction
// where N is the column's declared precision (0-6).
//
// Precision comes from parsing col.ColumnType, e.g. "datetime(3)" → 3.
// A bare "datetime" with no precision means DATETIME(0). When ColumnType
// is empty (pre-#212 snapshot), we fall back to a Nanosecond()==0 heuristic:
// no fraction if nanoseconds are zero, full microsecond tail otherwise.
// This best-effort mode handles the common DATETIME(0) case correctly but
// is unreliable for DATETIME(N>0) PKs — users hit by that mode should
// re-run `bintrail snapshot` to refresh schema_snapshots.
func canonicalizeDatetime(raw any, col metadata.ColumnMeta) (any, error) {
	switch v := raw.(type) {
	case time.Time:
		t := v.UTC() // indexer stores UTC; guard against non-UTC DuckDB output
		dec, known := parseDatetimePrecision(col.ColumnType)
		if !known {
			// Pre-#212 snapshot fallback: best-effort formatting based on
			// whether the scanned value has sub-second content. Reliable
			// for DATETIME(0); unreliable for DATETIME(N>0) whole-second
			// values.
			if t.Nanosecond() == 0 {
				return t.Format("2006-01-02 15:04:05"), nil
			}
			return t.Format("2006-01-02 15:04:05.000000"), nil
		}
		if dec == 0 {
			return t.Format("2006-01-02 15:04:05"), nil
		}
		// Format with full microsecond tail, then slice off (6-dec) digits
		// to match go-mysql's formatDatetime output at the declared precision.
		full := t.Format("2006-01-02 15:04:05.000000")
		return full[:len(full)-(6-dec)], nil
	case string:
		return v, nil
	default:
		return nil, fmt.Errorf("canonicalizeDatetime: column %q: expected time.Time or string, got %T", col.Name, raw)
	}
}

// canonicalizeDate converts a time.Time scanned from a DATE column into the
// "2006-01-02" string format the indexer stores. Strings pass through.
func canonicalizeDate(raw any, col metadata.ColumnMeta) (any, error) {
	switch v := raw.(type) {
	case time.Time:
		return v.UTC().Format("2006-01-02"), nil
	case string:
		return v, nil
	default:
		return nil, fmt.Errorf("canonicalizeDate: column %q: expected time.Time or string, got %T", col.Name, raw)
	}
}

// parseDatetimePrecision extracts the declared fractional second precision
// from a COLUMN_TYPE string like "datetime(6)". Returns (precision, true)
// on a successful parse, (0, true) for a bare "datetime" (DATETIME(0)),
// and (0, false) when ColumnType is empty (pre-#212 snapshot without
// column_type populated).
func parseDatetimePrecision(columnType string) (int, bool) {
	s := strings.ToLower(strings.TrimSpace(columnType))
	if s == "" {
		return 0, false
	}
	// Strip off known prefixes; the precision lives in parentheses.
	for _, prefix := range []string{"datetime", "timestamp"} {
		if !strings.HasPrefix(s, prefix) {
			continue
		}
		rest := strings.TrimPrefix(s, prefix)
		if rest == "" {
			// Bare "datetime" → DATETIME(0).
			return 0, true
		}
		if !strings.HasPrefix(rest, "(") || !strings.HasSuffix(rest, ")") {
			// Malformed — fall through to "unknown".
			return 0, false
		}
		digits := rest[1 : len(rest)-1]
		n, err := strconv.Atoi(digits)
		if err != nil || n < 0 || n > 6 {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

// ErrPKColumnMissing is a sentinel matchable via errors.Is for the case
// where a PK column declared in the resolver was not found in the row map
// passed to canonicalizePKMap. Returned errors are always typed as
// *MissingPKColumnError so callers that need the offending column name
// can recover it via errors.As.
var ErrPKColumnMissing = errors.New("PK column missing from row")

// MissingPKColumnError carries the specific column name that was absent
// from the baseline row, letting callers produce actionable error messages
// without string-parsing. Returned wrapped as `errors.Is(err,
// ErrPKColumnMissing)` via the Is method below.
type MissingPKColumnError struct {
	Column string
}

func (e *MissingPKColumnError) Error() string {
	return fmt.Sprintf("%s: column %q not in baseline row (run `bintrail snapshot` to refresh the schema snapshot if the table has been altered)",
		ErrPKColumnMissing.Error(), e.Column)
}

// Is returns true when target is the ErrPKColumnMissing sentinel, so
// `errors.Is(err, ErrPKColumnMissing)` keeps working even when the
// concrete type is *MissingPKColumnError.
func (e *MissingPKColumnError) Is(target error) bool {
	return target == ErrPKColumnMissing
}

// canonicalizePKMap takes a full row map and a PK column descriptor, and
// returns a new map with only the PK columns' values canonicalised
// according to their metadata. Non-PK columns flow through untouched.
//
// The source map is not mutated. Errors propagate from canonicalizePKValue
// and block the caller from attempting a downstream lookup that would
// return a garbage key.
func canonicalizePKMap(row map[string]any, pkCols []metadata.ColumnMeta) (map[string]any, error) {
	out := make(map[string]any, len(row))
	for k, v := range row {
		out[k] = v
	}
	for _, col := range pkCols {
		raw, ok := out[col.Name]
		if !ok {
			return nil, &MissingPKColumnError{Column: col.Name}
		}
		val, err := canonicalizePKValue(raw, col)
		if err != nil {
			return nil, err
		}
		out[col.Name] = val
	}
	return out, nil
}

// SupportedPKType is the exported form of supportedPKType, for callers
// outside this package (the shim's full-table _snapshot path) that need to
// guard a PK column type before attempting a baseline merge and fall back to
// their binlog-only path when it isn't supported.
func SupportedPKType(dataType string) bool { return supportedPKType(dataType) }

// FirstUnsupportedPKType returns the first primary-key member whose DATA_TYPE
// the baseline canonicalizer cannot handle, and whether one exists. Input
// order is the caller's (PKColumnMetas() preserves ordinal order), same
// contract as GeneratedPKColumn.
//
// An EMPTY DataType is NOT a verdict here and is skipped. It is the
// PostgreSQL snapshot signature — metadata.WritePGSnapshot leaves data_type
// and column_type empty (#533) — and flagging it would tell every PostgreSQL
// operator their schema is unsupported when nothing about the column is
// wrong.
//
// Only for callers with NO upstream source-flavor check: skipping the empty
// case means they never claim a cause they cannot know. A caller that HAS
// established it is on the MySQL path (ReconstructTable, verify, the shim)
// must keep its own loop over SupportedPKType, so an empty DataType still
// reaches PKTypeGateReason's wrong-path verdict there. Do not point those
// loops at this helper: it would silently retire that verdict.
func FirstUnsupportedPKType(pkCols []metadata.ColumnMeta) (metadata.ColumnMeta, bool) {
	for _, c := range pkCols {
		if strings.TrimSpace(c.DataType) == "" {
			continue
		}
		if !supportedPKType(c.DataType) {
			return c, true
		}
	}
	return metadata.ColumnMeta{}, false
}

// CanonicalizePKMap is the exported form of canonicalizePKMap, for callers
// outside this package (cascade Phase-2) that must encode a baseline Parquet
// row's primary key to match binlog_events.pk_values for deduplication.
func CanonicalizePKMap(row map[string]any, pkCols []metadata.ColumnMeta) (map[string]any, error) {
	return canonicalizePKMap(row, pkCols)
}

// supportedPKTypes is THE list of PK column types canonicalizePKValue
// handles, and the only place it is written down (#1455 — the refusal message
// used to keep a second copy of its complement, and that copy drifted). Tests
// iterate this slice rather than restating it, so a token added here without
// a matching arm in canonicalizePKValue's switch fails
// TestCanonicalizePKValue_everySupportedTypeHasAnArm instead of shipping a
// type every gate admits and the merge then refuses per row.
//
// #1155. BLOB-family PKs reach here as prefix keys (MySQL requires a prefix
// length on a BLOB/TEXT index), which does not change the canonicalization:
// both the index and the baseline carry the FULL column value, and only the
// index definition is truncated.
var supportedPKTypes = []string{
	"int", "integer", "smallint", "tinyint", "mediumint", "bigint",
	"char", "varchar", "text", "tinytext", "mediumtext", "longtext",
	"enum", "set",
	"datetime", "timestamp", "date",
	"year",
	"decimal", "numeric",
	"binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob",
	"uuid", "inet4", "inet6",
}

// supportedPKTypeSet is supportedPKTypes as a lookup set, built once.
var supportedPKTypeSet = func() map[string]struct{} {
	m := make(map[string]struct{}, len(supportedPKTypes))
	for _, t := range supportedPKTypes {
		m[t] = struct{}{}
	}
	return m
}()

// supportedPKType returns true if dataType is in the set of PK column types
// that canonicalizePKValue handles correctly. Callers use this at the start
// of a reconstruct run to warn operators about edge cases.
//
// Only DATA_TYPE values are expected here (lowercase base type from
// information_schema.COLUMNS.DATA_TYPE, e.g. "int", "datetime"), not the
// full COLUMN_TYPE. MySQL's DATA_TYPE never contains the "unsigned"
// qualifier — that lives in COLUMN_TYPE.
func supportedPKType(dataType string) bool {
	_, ok := supportedPKTypeSet[strings.ToLower(strings.TrimSpace(dataType))]
	return ok
}
