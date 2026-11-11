// Package verify reconstructs each table's state from baseline + binlog and
// compares its content fingerprint against the live source, proving that a
// recovery would reproduce the source. Capstone of the data-consistency epic
// (#631): the on-demand "does my recovery actually work?" check.
package verify

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dbtrail/dbtrail/internal/latin1"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// renderCell renders one reconstructed value into the exact text bytes that
// internal/consistency.ConsistentTableChecksum reads from the live source —
// MySQL's text-protocol form with the session pinned to UTC and DATE/DATETIME/
// TIMESTAMP read via CAST(... AS CHAR). A nil return is SQL NULL.
//
// Reconstructed values arrive in two shapes that must render identically: from
// the baseline they are DuckDB-native (int64/uint64/float64/time.Time/[]byte/
// string); from binlog events they are JSON-decoded (json.Number/string/[]byte/
// nil). The column metadata is needed only for temporal columns — to pick
// DATE's date-only form and DATETIME/TIMESTAMP's declared fractional precision,
// which the value's Go type alone cannot convey.
//
// renderCell never errors: it always produces bytes so the digest completes,
// and the caller decides conclusiveness. A value class whose rendering can't be
// guaranteed to match the source (FLOAT/DOUBLE text; JSON containers, which
// MySQL normalizes differently than Go) renders best-effort and, for columns the
// caller marks as deferred-representation, a resulting mismatch is reported
// inconclusive rather than as a failure (see VerifyTable). FLOAT/DOUBLE are not
// in the deferred set — there is intentionally no float-inconclusive downgrade
// (that would create a masking path). Their known representation-only gap
// (MySQL's my_gcvt vs Go's strconv exponent form, #795) is closed by
// canonicalFloatText's value-preserving parse+reformat in renderCellNormalized,
// so whatever divergence survives normalization is a genuine value difference
// and surfaces as a (safe) mismatch, never as a false match. For FLOAT
// specifically, canonicalFloatText alone is not sufficient on the live-source
// side: MySQL's bare `SELECT f` already truncates FLOAT text to ~6
// significant digits (FLT_DIG) before it ever reaches this package, which is
// lossy relative to the true float32 value and cannot be recovered by
// reformatting after the fact. Closing that requires reading more precision
// from the source in the first place — internal/consistency's selectExpr
// promotes FLOAT reads to DOUBLE arithmetic (`f+0e0`) specifically for the
// cross-renderer (normalize != nil) caller VerifyTable uses, so the text
// canonicalFloatText receives already carries enough digits to identify the
// exact float32.
func renderCell(v any, col metadata.ColumnMeta) []byte {
	if v == nil {
		return nil
	}
	// A BIT value from a binlog event image arrives as a NUMBER (go-mysql
	// delivers BIT as an integer, kept as json.Number through the stored event
	// JSON), while the baseline Parquet and the live source's text protocol
	// both carry the RAW ceil(M/8)-byte big-endian string. Render the number
	// into those exact bytes so a carried-but-unchanged BIT genuinely matches
	// (#769). Baseline-origin []byte values fall through untouched.
	if strings.EqualFold(strings.TrimSpace(col.DataType), "bit") {
		if b, ok := renderBitBytes(v, col); ok {
			return b
		}
	}
	// A fixed BINARY(n) value from a binlog event image arrives STRIPPED of
	// its trailing 0x00 padding: MySQL's ROW image length-prefixes
	// MYSQL_TYPE_STRING with the ACTUAL stored length (go-mysql decodeString
	// reads exactly that), while the baseline Parquet (mydumper) and the live
	// source's protocol both carry the full padded n bytes. Right-pad to the
	// declared width so a value that merely ends in 0x00 doesn't render
	// shorter than the identical source value — every such row was a
	// conclusive false MISMATCH (#1135). Baseline-origin values are already
	// full width, so padding is a no-op there; on the live-scan side
	// (normalizeRenderedBytes) the source value is always exactly n bytes, so
	// the transform is identity by construction and needs no hook.
	if strings.EqualFold(strings.TrimSpace(col.DataType), "binary") {
		switch x := v.(type) {
		case []byte:
			return padFixedBinary(x, col)
		case string:
			return padFixedBinary([]byte(x), col)
		}
	}
	// A latin1 text column: the copy and the index hold its text as UTF-8
	// (#2634), the source and its checksum (character_set_results = binary)
	// the stored latin1 bytes. Render those bytes. Text latin1 has no byte
	// for is left as it is, and cannot match: an honest mismatch.
	if isLatin1Text(col) {
		var s string
		switch x := v.(type) {
		case string:
			s = x
		case []byte:
			s = string(x)
		}
		if s != "" {
			if enc, err := latin1.ToStored(s); err == nil {
				return []byte(enc)
			}
		}
	}
	switch x := v.(type) {
	case json.Number:
		// Binlog event integers/decimals are JSON-decoded as json.Number, whose
		// String() is the original literal — so an integer >2^53 stays exact
		// (#496) instead of rounding through float64.
		return []byte(x.String())
	case []byte:
		return x
	case string:
		return []byte(x)
	case time.Time:
		return renderTemporal(x, col)
	case int64:
		return []byte(strconv.FormatInt(x, 10))
	case int32:
		return []byte(strconv.FormatInt(int64(x), 10))
	case uint64:
		return []byte(strconv.FormatUint(x, 10))
	case uint32:
		return []byte(strconv.FormatUint(uint64(x), 10))
	case float64:
		return []byte(strconv.FormatFloat(x, 'g', -1, 64))
	case float32:
		return []byte(strconv.FormatFloat(float64(x), 'g', -1, 32))
	case bool:
		if x {
			return []byte("1")
		}
		return []byte("0")
	default:
		// JSON columns touched by an event decode to map[string]any / []any.
		// Marshal deterministically (Go sorts map keys) so the digest completes;
		// it won't match MySQL's canonical JSON text, but such columns are in the
		// deferred-representation set so the mismatch is reported inconclusive.
		if b, err := json.Marshal(v); err == nil {
			return b
		}
		return []byte(fmt.Sprintf("%v", v))
	}
}

// renderTemporal formats a time.Time the way MySQL's CAST(col AS CHAR) does for
// the column's declared type: DATE → "2006-01-02"; DATETIME/TIMESTAMP →
// "2006-01-02 15:04:05" plus the column's fractional precision (0–6 digits).
//
// Precision comes from col.ColumnType, which is empty on pre-#212 schema
// snapshots; with it empty a DATETIME(n>0) renders without its fraction and
// would spuriously mismatch the source. Modern snapshots carry ColumnType, so
// this only affects baselines/snapshots taken before #212 — re-run bintrail
// snapshot to refresh.
func renderTemporal(t time.Time, col metadata.ColumnMeta) []byte {
	t = t.UTC()
	if strings.EqualFold(strings.TrimSpace(col.DataType), "date") {
		return []byte(t.Format("2006-01-02"))
	}
	base := t.Format("2006-01-02 15:04:05")
	prec := temporalPrecision(col.ColumnType)
	if prec == 0 {
		return []byte(base)
	}
	frac := t.Nanosecond() / 1e3 // microseconds
	tail := fmt.Sprintf("%06d", frac)
	if prec > 6 {
		prec = 6
	}
	return []byte(base + "." + tail[:prec])
}

// temporalPrecision extracts the fractional-seconds precision from a column type
// like "datetime(6)" or "timestamp(3)". Returns 0 when none is declared.
func temporalPrecision(columnType string) int {
	open := strings.IndexByte(columnType, '(')
	if open < 0 {
		return 0
	}
	close := strings.IndexByte(columnType[open:], ')')
	if close < 0 {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(columnType[open+1 : open+close]))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// renderBitBytes renders a numeric BIT value as the raw byte string MySQL's
// text protocol (and mydumper, hence the baseline Parquet) carries for a
// BIT(M) column: ceil(M/8) bytes, big-endian, zero-padded at the front. The
// declared width comes from ColumnType ("bit(12)" → 2 bytes). ok=false — the
// caller falls back to the generic decimal rendering, which the deferred gate
// reports as unresolved — when the width is unavailable (pre-#212 snapshot
// with an empty ColumnType) or the value is not an exact non-negative
// integer. A value wider than the declared width is never truncated: it
// renders at its natural width, so a genuinely divergent value still differs.
func renderBitBytes(v any, col metadata.ColumnMeta) ([]byte, bool) {
	u, ok := bitUint(v)
	if !ok {
		return nil, false
	}
	width := bitByteWidth(col.ColumnType)
	if width == 0 {
		return nil, false
	}
	if need := max((bits.Len64(u)+7)/8, 1); need > width {
		width = need
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], u)
	return buf[8-width:], true
}

// padFixedBinary right-pads b with 0x00 up to the declared width of a fixed
// BINARY(n) column. When the width is unavailable (pre-#212 snapshot with an
// empty ColumnType) b returns untouched — deferredValueUnresolved reports such
// a value unresolved, so the comparison degrades to an honest Inconclusive
// instead of a false MISMATCH. A value at or beyond the declared width is
// NEVER truncated: it returns at its natural length, so a genuinely divergent
// longer value still differs.
//
// Unlike the adjacent BIT branch — which converts only numeric (i.e.
// event-origin) values, leaving baseline-origin []byte untouched — this
// padding is origin-blind: a baseline value that is short for some
// non-padding reason would be silently padded too. Accepted because MySQL
// only ever strips trailing 0x00 from a stored fixed BINARY(n) value, so
// re-padding a short value reproduces the stored value exactly regardless of
// which side produced it.
func padFixedBinary(b []byte, col metadata.ColumnMeta) []byte {
	width := fixedBinaryWidth(col.ColumnType)
	if width == 0 || len(b) >= width {
		return b
	}
	out := make([]byte, width) // zero-filled; copy leaves the 0x00 tail
	copy(out, b)
	return out
}

// fixedBinaryWidth returns the declared byte width of a "binary(N)"
// column-type declaration, 0 when unavailable. Reuses temporalPrecision's
// parenthesized-integer extraction — same "(N)" shape (cf. bitByteWidth).
// BINARY(0) is legal but reported as unavailable; its only value is the empty
// string, which renders identically on both sides anyway.
func fixedBinaryWidth(columnType string) int {
	return temporalPrecision(columnType)
}

// bitByteWidth returns ceil(M/8) for a "bit(M)" column-type declaration, 0
// when M is unavailable or outside MySQL's 1..64 range. Reuses
// temporalPrecision's parenthesized-integer extraction — same "(N)" shape.
func bitByteWidth(columnType string) int {
	m := temporalPrecision(columnType)
	if m < 1 || m > 64 {
		return 0
	}
	return (m + 7) / 8
}

// bitUint extracts an exact non-negative integer from the value shapes an
// event image can carry for a BIT column. Mirrors metadata's ordinalValue
// conservatism: fractional, negative, beyond-exact-precision, and textual
// values all report !ok.
func bitUint(v any) (uint64, bool) {
	switch n := v.(type) {
	case json.Number:
		u, err := strconv.ParseUint(n.String(), 10, 64)
		if err != nil {
			return 0, false
		}
		return u, true
	case int64:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case uint64:
		return n, true
	case int32:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case uint32:
		return uint64(n), true
	case float64:
		if n < 0 || n != math.Trunc(n) || n > 1<<53 {
			return 0, false
		}
		return uint64(n), true
	}
	return 0, false
}

// isZeroDateSentinel reports whether b is MySQL's all-zero date pseudo-NULL
// sentinel ('0000-00-00', '0000-00-00 00:00:00', or the '.000000' fractional
// variant) rendered for a DATE/DATETIME/TIMESTAMP column. Mirrors
// internal/baseline's isZeroDate check exactly (unexported there; duplicated
// here rather than adding a cross-package dependency for one string
// comparison — both must agree on what counts as the sentinel, since this
// function exists specifically to undo that package's own NULL substitution).
//
// TIME is deliberately excluded by the DataType switch, matching
// internal/baseline's own scoping: '00:00:00' is legal midnight there, not a
// pseudo-NULL, so it must never be treated as equivalent to NULL.
func isZeroDateSentinel(b []byte, col metadata.ColumnMeta) bool {
	switch strings.ToLower(strings.TrimSpace(col.DataType)) {
	case "date", "datetime", "timestamp":
	default:
		return false
	}
	return bytes.HasPrefix(bytes.TrimSpace(b), []byte("0000-00-00"))
}

// canonicalizeJSONContainer re-renders a JSON object/array value from its
// decoded form, so two byte-different-but-semantically-equal serializations
// of the same JSON content compare equal. Scoped to CONTAINERS ({...}/[...])
// only — a scalar that happens to also be valid JSON (a bare number, "true",
// or a quoted string) is left untouched, since key-order drift can only
// happen inside an object; touching scalars would only add risk (whitespace/
// escaping differences) for no benefit.
//
// This operates on the RENDERED bytes, not the column's declared SQL type, so
// it applies to any value that renders JSON-shaped — a native MySQL JSON
// column, and equally a MySQL TEXT/LONGTEXT column (not the native JSON type)
// whose stored value happens to be JSON text, a common pattern for plugins
// that json_encode() into a text field. That TEXT case is the gap this
// primarily exists to close: once an event touches such a row, its
// event-image round-trips through Go's map[string]any
// (query.UnmarshalRowImage), which loses the original key order; renderCell's
// default case then re-marshals it with Go's own (alphabetically sorted) key
// order, while a baseline dump's text preserves the source's original order
// verbatim. Two renderings of identical data disagree on bytes alone. A
// column typed as MySQL's native JSON already had a narrower version of this
// same gap, covered by isDeferredType's inconclusive downgrade — this closes
// it more precisely there too, resolving to a genuine match rather than
// merely downgrading to "can't tell": see renderCellNormalized.
//
// UseNumber preserves large-integer/decimal literals exactly, the same
// precision requirement renderCell's json.Number case already protects
// (#496). SetEscapeHTML(false) avoids gratuitously mangling '<'/'>'/'&' that
// a human reading a mismatch cell would otherwise have to puzzle over.
//
// Returns the input unchanged with ok=false when b is not a JSON
// object/array, fails to parse, is not valid UTF-8, decodes to a value
// containing U+FFFD (see the surrogate-escape note below), or contains a
// duplicate object key at any depth.
func canonicalizeJSONContainer(b []byte) ([]byte, bool) {
	t := bytes.TrimSpace(b)
	if len(t) == 0 || (t[0] != '{' && t[0] != '[') {
		return b, false
	}
	if !json.Valid(t) {
		return b, false
	}
	// Not eligible to canonicalize: raw content this transform would corrupt
	// or hide a divergence in, rather than merely reorder.
	//   - invalid UTF-8 bytes: both json.Decode/Encode replace them with
	//     U+FFFD, so two DIFFERENT invalid byte sequences could canonicalize
	//     to the SAME output — silently erasing a real difference.
	//   - a repeated key within one object: decoding into map[string]any keeps
	//     only the last occurrence (Go's stdlib behavior), which would make
	//     `{"a":1,"a":2}` and `{"a":2}` compare equal — but those are NOT the
	//     same source bytes, and if that duplicate-keyed value came from
	//     event-image reconstruction, this is exactly the kind of divergence
	//     verify exists to catch, not paper over.
	// All leave b returned unchanged, so a genuinely malformed/duplicated
	// value still falls back to the pre-fix byte comparison (conservative:
	// worst case reports a mismatch verify's caller downgrades to
	// inconclusive or a human reviews, never a silently masked one).
	if !utf8.Valid(t) {
		return b, false
	}
	if hasDuplicateObjectKeys(t) {
		return b, false
	}
	dec := json.NewDecoder(bytes.NewReader(t))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return b, false
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return b, false
	}
	out := bytes.TrimRight(buf.Bytes(), "\n")
	// An unpaired \uD800-\uDFFF surrogate escape is valid JSON syntax AND
	// valid raw UTF-8 (it's just six ASCII characters before unescaping), so
	// utf8.Valid(t) above does not catch it — the invalidity only appears
	// once json.Decode unescapes the string content, where Go silently
	// substitutes U+FFFD, same as it does for invalid raw bytes. Two
	// DIFFERENT unpaired surrogates would decode to the identical U+FFFD and
	// canonicalize to the identical output. Refuse whenever the canonical
	// form contains U+FFFD: a real, intentional U+FFFD in source data is
	// vanishingly rare, and the cost of a false refusal here is only a
	// fallback to raw-byte comparison, not a wrong answer.
	if bytes.ContainsRune(out, utf8.RuneError) {
		return b, false
	}
	return out, true
}

// hasDuplicateObjectKeys reports whether any JSON object within data (already
// confirmed json.Valid) repeats a key WITHIN THAT SAME OBJECT — a case Go's
// map[string]any decode would silently collapse to last-key-wins, exactly the
// kind of information loss canonicalizeJSONContainer must not introduce. Two
// sibling objects (or an object and its parent) reusing the same key name is
// NOT a duplicate — key uniqueness is scoped per object, not per nesting
// depth. Walks the raw token stream (not a map) so duplicates are visible
// before any collapsing decode runs.
func hasDuplicateObjectKeys(data []byte) bool {
	dup, _ := walkForDuplicateKeys(json.NewDecoder(bytes.NewReader(data)))
	return dup
}

// walkForDuplicateKeys consumes exactly one JSON value (scalar, object, or
// array) from dec and reports whether a duplicate object key was found
// anywhere within it, recursing into nested objects/arrays. err is non-nil
// only on a stream read failure, which cannot happen against
// already-json.Valid input; callers treat that case as "no duplicate found"
// (the same conservative, fall-back-to-raw-bytes behavior every other
// canonicalizeJSONContainer failure path takes).
func walkForDuplicateKeys(dec *json.Decoder) (bool, error) {
	tok, err := dec.Token()
	if err != nil {
		return false, err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return false, nil // scalar value: string/number/bool/null, no keys to check
	}
	switch delim {
	case '{':
		seen := make(map[string]bool)
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return false, err
			}
			key, _ := keyTok.(string) // object keys are always strings per the JSON grammar
			if seen[key] {
				return true, nil
			}
			seen[key] = true
			if dup, err := walkForDuplicateKeys(dec); dup || err != nil {
				return dup, err
			}
		}
		_, err := dec.Token() // consume the closing '}'
		return false, err
	case '[':
		for dec.More() {
			if dup, err := walkForDuplicateKeys(dec); dup || err != nil {
				return dup, err
			}
		}
		_, err := dec.Token() // consume the closing ']'
		return false, err
	default:
		return false, nil // stray '}'/']' — unreachable against valid JSON
	}
}

// renderCellNormalized renders a cell like renderCell, additionally
// normalizing the known representation gaps between an event-image value and
// the SAME underlying data rendered independently on the comparison's other
// side, so a pure representation difference doesn't register as a content
// difference:
//
//   - a JSON object/array value (see canonicalizeJSONContainer) — Go's
//     map[string]any decode loses object key order, which renderCell's
//     default case then re-serializes alphabetically.
//   - a TIME value's fractional suffix (see trimTimeFractionZeros) —
//     go-mysql's event-image renderer omits the fraction entirely for an
//     integer-second value while MySQL's text protocol and mydumper always
//     pad it to the declared fsp (#794).
//   - a FLOAT/DOUBLE text rendering (see canonicalFloatText) — MySQL's
//     my_gcvt and Go's strconv disagree on exponent form and thresholds for
//     the same stored value (#795). For FLOAT, this also depends on the
//     source having been read with enough precision in the first place —
//     see internal/consistency's selectExpr (promoteFloat) — since a
//     truncated MySQL text can't be recovered by reformatting alone.
//   - a DATE/DATETIME/TIMESTAMP zero-date sentinel (see isZeroDateSentinel)
//     — internal/baseline.Writer.WriteRow deliberately maps MySQL's
//     '0000-00-00'-family pseudo-NULL to Parquet NULL, unconditionally, for
//     EVERY zero-date value (Go's time parser rejects it outright). An
//     event-touched row's image still carries the literal sentinel text, so
//     recon renders the string while a same-valued baseline or live-source
//     cell can render NULL. This mapping only runs one direction (string →
//     nil): it does NOT touch a genuine NULL.
//
//     A NULL for a temporal column is NOT provably zero-date-only — the
//     writer that produced it (internal/baseline.Writer.WriteRow, or MySQL
//     itself) also NULLs a temporal column for a genuine SQL NULL, and that
//     information is already indistinguishable at rest. So this
//     normalization is safe under verify's assumption that the binlog
//     captured every write to the row: if it did, recon's zero-date-text
//     cell and the other side's NULL cell are the same underlying value,
//     whichever path produced the NULL. It stops being safe only if the
//     source transitioned zero-date -> NULL through a write the binlog never
//     saw (sql_log_bin=0, direct file manipulation, a replication gap) —
//     which already breaks verify's guarantee for every column type, not
//     something this normalization introduces. This holds identically for
//     both comparisons below — masking requires BOTH sides to already land
//     in the {NULL, zero-date} equivalence class, regardless of which side
//     is a baseline reconstruction and which is MySQL ground truth; live-
//     source does not widen the risk, it just makes "the other side" MySQL
//     itself instead of another baseline. See
//     TestVerifyBaselinePair_StaleZeroDateVsGenuineNull_AcceptedRisk and its
//     live-source sibling TestVerifyTable_StaleZeroDateVsGenuineNull_AcceptedRisk
//     for the concrete case this accepts — a deliberate, reviewed trade-off,
//     not open follow-up work.
//
// Used by both verify comparisons, each pairing it with a same-package
// renderer on the OTHER side so the normalization is applied symmetrically
// (asymmetric application would trade one false mismatch for a different
// one):
//
//   - baseline-anchored (VerifyBaselinePair, ExplainBaselinePairMismatch):
//     both operands are produced by this package's own reconstructDigest, so
//     wiring this renderer into both sides is sufficient.
//   - live-source (VerifyTable): the other operand is MySQL's own raw text
//     via internal/consistency.ConsistentTableChecksum, which this package
//     does not render. VerifyTable pairs this renderer with
//     ConsistentTableChecksumNormalized, passing normalizeRenderedBytes as
//     that function's hook — the same normalization logic, applied to
//     already-scanned bytes instead of a Go value, so both sides agree on
//     what counts as a representation-only difference.
func renderCellNormalized(v any, col metadata.ColumnMeta) []byte {
	return normalizeRenderedBytes(renderCell(v, col), col.DataType)
}

// normalizeRenderedBytes applies renderCellNormalized's representation-gap
// normalization directly to already-rendered bytes, for a context that
// doesn't go through renderCell — internal/consistency.ConsistentTableChecksum's
// raw MySQL scan (see ConsistentTableChecksumNormalized and its use in
// VerifyTable). Both call sites must apply the identical logic for the two
// sides of a comparison to be symmetric, so renderCellNormalized itself calls
// this rather than duplicating the checks.
func normalizeRenderedBytes(b []byte, dataType string) []byte {
	if b == nil {
		return nil
	}
	col := metadata.ColumnMeta{DataType: dataType}
	if isZeroDateSentinel(b, col) {
		return nil
	}
	// TIME/FLOAT/DOUBLE values are never JSON containers, so returning from
	// these arms skips canonicalizeJSONContainer harmlessly.
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "time":
		return trimTimeFractionZeros(b)
	case "float":
		return canonicalFloatText(b, 32)
	case "double":
		return canonicalFloatText(b, 64)
	}
	if canon, ok := canonicalizeJSONContainer(b); ok {
		return canon
	}
	return b
}

// trimTimeFractionZeros canonicalizes a TIME value's fractional suffix by
// trimming trailing fractional zeros (and a then-empty '.') — the minimal
// rendering of the same value. The renderers feeding a TIME comparison
// disagree only in trailing zeros: MySQL's text protocol and mydumper pad
// the fraction to the declared fsp ("09:00:00.000" for TIME(3)), while
// go-mysql v1.13.0's timeFormat — the event-image renderer — omits the
// suffix ENTIRELY when the microsecond part is zero (and pads to the fsp
// otherwise), so every integer-second TIME(fsp>0) value was a conclusive
// false MISMATCH in both verify modes (#794). Trimming rather than padding
// to the fsp because the fsp isn't available here — the live-scan hook
// carries only information_schema DATA_TYPE (see
// ConsistentTableChecksumNormalized) — and it also holds for pre-#212
// snapshots whose ColumnType is empty. Value-preserving by construction:
// removing trailing zeros after the '.' never collapses two DIFFERENT
// values. Bytes without a '.' return untouched — an unguarded TrimRight
// would eat an integer-second value's own trailing zeros ("10:00:00" →
// "10:00:").
func trimTimeFractionZeros(b []byte) []byte {
	if bytes.IndexByte(b, '.') < 0 {
		return b
	}
	t := bytes.TrimRight(b, "0")
	return bytes.TrimSuffix(t, []byte("."))
}

// canonicalFloatText re-renders a FLOAT/DOUBLE text value through Go's
// shortest-round-trip formatter at the column's own width (32-bit for FLOAT,
// 64-bit for DOUBLE), so MySQL's my_gcvt rendering ("1e16", "0.00001") and
// Go's strconv rendering ("1e+16", "1e-05") of the SAME stored value compare
// equal (#795) — live mode was a permanent conclusive MISMATCH on any intact
// float outside the coinciding-render range. Distinct storable values of the
// column's width have distinct shortest renderings, so parse+reformat
// collapses representation only, never a value divergence — which is why
// FLOAT/DOUBLE can stay OUT of the deferred-representation set (see the
// renderCell doc: no inconclusive downgrade, no masking path). Bytes that do
// not parse as a float of that width (never produced by either renderer for
// an intact value) return unchanged and fall through to the raw byte
// comparison.
//
// This function is value-preserving only when its input already carries
// enough precision to identify the stored value uniquely. For DOUBLE, MySQL's
// bare text protocol always does. For FLOAT it does NOT: a bare `SELECT f`
// truncates to ~6 significant digits (my_gcvt's FLT_DIG default), which is
// lossy for any float32 that needs more digits to round-trip — no reformat of
// already-truncated text can recover the missing digits. That gap is closed
// one layer up, at the SQL text this function receives: internal/consistency's
// selectExpr promotes FLOAT reads to DOUBLE arithmetic (`f+0e0`) for the
// cross-renderer (normalize != nil) caller, so by the time a FLOAT value's
// text reaches here it already carries full double-precision digits and
// parses back to the exact original float32.
func canonicalFloatText(b []byte, bitSize int) []byte {
	f, err := strconv.ParseFloat(string(b), bitSize)
	if err != nil {
		return b
	}
	return []byte(strconv.FormatFloat(f, 'g', -1, bitSize))
}

// isLatin1Text reports whether col is a text column (CHAR, VARCHAR, the TEXT
// types, ENUM, SET) whose character set is latin1.
func isLatin1Text(col metadata.ColumnMeta) bool {
	switch strings.ToLower(strings.TrimSpace(col.DataType)) {
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext", "enum", "set":
		return latin1.IsLatin1(col.CharacterSet)
	}
	return false
}
