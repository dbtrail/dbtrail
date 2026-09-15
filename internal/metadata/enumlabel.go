package metadata

import (
	"database/sql"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"
)

// EnumLabelMapper rewrites ENUM and SET ordinals stored in binlog row
// images back to their string labels (#472).
//
// Binlog ROW images store an ENUM value as its 1-based ordinal and a SET
// value as a bitmask, so every surface that materializes rows from
// row_before/row_after would otherwise answer `'3'` where a live
// `SELECT` answers `'shipped'` — different representations of the same
// column. The labels come from the schema snapshot's COLUMN_TYPE
// (e.g. `enum('pending','shipped')`), captured since #212.
//
// The mapping is deliberately conservative: anything that doesn't match a
// known ordinal exactly — out-of-range values (the enum shrank after the
// event), non-integral numbers, values already strings (baseline images
// carry labels from mydumper dumps) — passes through unchanged rather
// than guessing.
//
// Which snapshot supplies the labels is the caller's choice. Use
// EnumMapperSource to decode each event with the snapshot in effect at
// its timestamp (#475) — a mapper built from only the latest snapshot
// silently mislabels old ordinals after an enum reorder or
// middle-member removal.
type EnumLabelMapper struct {
	enums map[string][]string // column → labels in 1-based ordinal order
	sets  map[string][]string // column → members in bit order (bit i ↔ member i)
}

// NewEnumLabelMapper builds a mapper for the table's ENUM/SET columns.
// Returns nil — a valid no-op receiver for MapImage — when tm is nil
// (resolver unavailable) or the table has no ENUM/SET columns, so the
// per-row mapping cost on the common path is a single nil check.
// Pre-#212 snapshots have empty ColumnType and naturally fall out as
// "no ENUM/SET columns".
func NewEnumLabelMapper(tm *TableMeta) *EnumLabelMapper {
	if tm == nil {
		return nil
	}
	var m *EnumLabelMapper
	for _, c := range tm.Columns {
		labels, isSet, ok := parseEnumSetLabels(c.ColumnType)
		if !ok {
			continue
		}
		if m == nil {
			m = &EnumLabelMapper{
				enums: make(map[string][]string),
				sets:  make(map[string][]string),
			}
		}
		if isSet {
			m.sets[c.Name] = labels
		} else {
			m.enums[c.Name] = labels
		}
	}
	return m
}

// MapImage rewrites the ENUM/SET ordinals in a single row image, in
// place. The image must be request-owned (a map decoded from the index's
// JSON columns or built by a reconstruction merge) — never pass a
// cached or shared map. Safe on a nil receiver and a nil image.
func (m *EnumLabelMapper) MapImage(image map[string]any) {
	if m == nil || image == nil {
		return
	}
	for col, labels := range m.enums {
		v, present := image[col]
		if !present {
			continue
		}
		n, ok := ordinalValue(v)
		if !ok {
			continue
		}
		switch {
		case n == 0:
			// MySQL's sentinel for an invalid/empty ENUM entry.
			image[col] = ""
		case n <= uint64(len(labels)):
			image[col] = labels[n-1]
		}
	}
	for col, members := range m.sets {
		v, present := image[col]
		if !present {
			continue
		}
		n, ok := ordinalValue(v)
		if !ok {
			continue
		}
		if s, ok := setString(n, members); ok {
			image[col] = s
		}
	}
}

// ─── Snapshot epochs (#475) ─────────────────────────────────────────────────

// SnapshotEpoch is one schema snapshot's identity and the instant it was
// taken. Epochs order the snapshot history so a binlog event can be
// decoded with the table definition in effect when it happened.
type SnapshotEpoch struct {
	ID int
	At time.Time
}

// LoadSnapshotEpochs returns every snapshot's (id, taken-at) ascending by
// time. The result is small (one row per `bintrail snapshot` run) and
// snapshots are immutable, so callers may cache per-ID resolvers
// indefinitely — only this list grows.
func LoadSnapshotEpochs(db *sql.DB) ([]SnapshotEpoch, error) {
	rows, err := db.Query(`SELECT snapshot_id, MIN(snapshot_time)
		FROM schema_snapshots
		GROUP BY snapshot_id
		ORDER BY MIN(snapshot_time), snapshot_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var epochs []SnapshotEpoch
	for rows.Next() {
		var e SnapshotEpoch
		if err := rows.Scan(&e.ID, &e.At); err != nil {
			return nil, err
		}
		epochs = append(epochs, e)
	}
	return epochs, rows.Err()
}

// EpochAt returns the snapshot in effect at t: the latest epoch taken
// at-or-before t. An instant predating the first snapshot returns the
// FIRST epoch — the closest available description of the schema, and
// what the indexer itself most plausibly used to name those events'
// columns. ok is false only when epochs is empty.
func EpochAt(epochs []SnapshotEpoch, t time.Time) (id int, ok bool) {
	if len(epochs) == 0 {
		return 0, false
	}
	id = epochs[0].ID
	for _, e := range epochs {
		if e.At.After(t) {
			break
		}
		id = e.ID
	}
	return id, true
}

// EnumMapperSource hands out the EnumLabelMapper in effect at a given
// instant (#475), memoizing per (epoch, table) within one request. The
// zero value with only Fallback set degrades to latest-snapshot mapping
// (#472's original behavior); fully empty it degrades to no mapping —
// ordinals pass through honestly, never a guessed label.
type EnumMapperSource struct {
	// Epochs is the ascending snapshot history (LoadSnapshotEpochs).
	// nil/empty → every lookup uses Fallback.
	Epochs []SnapshotEpoch
	// ResolverFor loads the resolver for a specific snapshot id.
	// Snapshots are immutable, so implementations may cache forever.
	ResolverFor func(id int) (*Resolver, error)
	// Fallback resolves tables when the epoch path is unavailable
	// (no epochs, no ResolverFor, or a failed per-id load) —
	// typically the consumer's cached latest resolver.
	Fallback *Resolver

	memo map[mapperKey]*EnumLabelMapper
}

type mapperKey struct {
	id            int // -1 = the Fallback resolver
	schema, table string
}

// MapperAt returns the mapper for schema.table under the snapshot in
// effect at t. May return nil — a valid no-op receiver.
func (s *EnumMapperSource) MapperAt(schema, table string, t time.Time) *EnumLabelMapper {
	id, ok := EpochAt(s.Epochs, t)
	if !ok || s.ResolverFor == nil {
		return s.memoized(mapperKey{-1, schema, table}, s.Fallback)
	}
	key := mapperKey{id, schema, table}
	if m, seen := s.memo[key]; seen {
		return m
	}
	r, err := s.ResolverFor(id)
	if err != nil || r == nil {
		// Per-id load failed: degrade to the latest definition rather
		// than dropping mapping entirely — exactly the pre-#475
		// behavior, and never worse (string labels pass through).
		return s.memoized(key, s.Fallback)
	}
	return s.memoized(key, r)
}

// memoized builds (once) and caches the mapper for key from r's view of
// the table. A table missing from r — created after that snapshot, or r
// itself nil — memoizes nil: honest pass-through.
func (s *EnumMapperSource) memoized(key mapperKey, r *Resolver) *EnumLabelMapper {
	if m, seen := s.memo[key]; seen {
		return m
	}
	var m *EnumLabelMapper
	if r != nil {
		if tm, err := r.Resolve(key.schema, key.table); err == nil {
			m = NewEnumLabelMapper(tm)
		}
	}
	if s.memo == nil {
		s.memo = make(map[mapperKey]*EnumLabelMapper)
	}
	s.memo[key] = m
	return m
}

// ─── Parsing and value mapping ──────────────────────────────────────────────

// ordinalValue extracts a non-negative integral ordinal from the value
// types a row image can carry. JSON-decoded images yield float64; the
// defensive integer cases cover merged values that skipped the JSON
// round-trip. Textual values (string or []byte — already labels),
// NULLs, negatives, non-integral floats, and floats beyond exact
// integer precision (2^53) all report !ok — the caller leaves those
// values untouched. The 2^53 bound also means a SET mask using members
// beyond bit 53 degrades to pass-through: such a mask is already
// precision-lossy after the JSON round-trip, so refusing it is the
// honest call.
func ordinalValue(v any) (uint64, bool) {
	switch n := v.(type) {
	case json.Number:
		// Row images decoded by query.UnmarshalRowImage keep numbers as
		// json.Number (#496). Parse the exact integer literal — no 2^53 float
		// limit, so SET masks using high bits resolve correctly. A negative,
		// fractional, or out-of-range value fails ParseUint → pass-through.
		u, err := strconv.ParseUint(n.String(), 10, 64)
		if err != nil {
			return 0, false
		}
		return u, true
	case float64:
		if n < 0 || n != math.Trunc(n) || n > 1<<53 {
			return 0, false
		}
		return uint64(n), true
	case int:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case int32:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case int64:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case uint64:
		return n, true
	}
	return 0, false
}

// setString renders a SET bitmask as MySQL's comma-joined member list
// (bit i set → members[i] included, in definition order; 0 → "").
// Reports !ok when the mask carries bits beyond the known members —
// the definition changed since the event, so the number is more honest
// than a partial label list.
func setString(mask uint64, members []string) (string, bool) {
	if mask == 0 {
		return "", true
	}
	if len(members) < 64 && mask >= 1<<uint(len(members)) {
		return "", false
	}
	var picked []string
	for i, member := range members {
		if mask&(1<<uint(i)) != 0 {
			picked = append(picked, member)
		}
	}
	return strings.Join(picked, ","), true
}

// parseEnumSetLabels extracts the member labels from an
// information_schema COLUMN_TYPE declaration like
// `enum('pending','shipped')` or `set('a','b')`. Members are
// single-quoted with embedded quotes doubled (`'it''s'`), and may
// legitimately be empty (`enum('','a')`) or contain commas. MySQL
// additionally renders backslashes and control characters inside
// members with C-style escapes (verified on 8.0: `\\`, `\n`, `\r`,
// `\0`); those are decoded so the label's bytes match the live
// value. Reports !ok for any other type declaration ("int unsigned",
// "varchar(20)", the empty pre-#212 string), for malformed input,
// and for an escape sequence we don't recognize — !ok degrades to
// the honest raw ordinal, never a guessed label.
func parseEnumSetLabels(columnType string) (labels []string, isSet, ok bool) {
	ct := strings.TrimSpace(columnType)
	lower := strings.ToLower(ct)
	var inner string
	switch {
	case strings.HasPrefix(lower, "enum(") && strings.HasSuffix(lower, ")"):
		inner = ct[len("enum(") : len(ct)-1]
	case strings.HasPrefix(lower, "set(") && strings.HasSuffix(lower, ")"):
		inner = ct[len("set(") : len(ct)-1]
		isSet = true
	default:
		return nil, false, false
	}

	var cur strings.Builder
	inString := false // inside a 'quoted' member
	pending := false  // a member has been opened since the last comma
	for i := 0; i < len(inner); i++ {
		ch := inner[i]
		if inString {
			switch ch {
			case '\'':
				if i+1 < len(inner) && inner[i+1] == '\'' {
					cur.WriteByte('\'') // doubled quote → literal quote
					i++
					continue
				}
				inString = false
			case '\\':
				// Quotes are only ever doubled (never \'), so a backslash
				// always starts one of MySQL's C-style escapes.
				if i+1 >= len(inner) {
					return nil, false, false
				}
				i++
				switch inner[i] {
				case '\\':
					cur.WriteByte('\\')
				case 'n':
					cur.WriteByte('\n')
				case 'r':
					cur.WriteByte('\r')
				case '0':
					cur.WriteByte(0)
				default:
					return nil, false, false
				}
			default:
				cur.WriteByte(ch)
			}
			continue
		}
		switch ch {
		case '\'':
			inString = true
			pending = true
		case ',':
			if !pending {
				return nil, false, false
			}
			labels = append(labels, cur.String())
			cur.Reset()
			pending = false
		case ' ', '\t':
			// whitespace between members
		default:
			return nil, false, false
		}
	}
	if inString || !pending {
		// unterminated quote, trailing comma, or empty list
		return nil, false, false
	}
	labels = append(labels, cur.String())
	return labels, isSet, true
}
