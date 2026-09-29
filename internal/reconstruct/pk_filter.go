package reconstruct

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// This file holds the two reconcilers for the one spelling asymmetry a fixed
// BINARY(n) primary key has (#1155), plus the metadata resolution they need.
// They moved here from internal/cli (#1157) so every ReadBaselineRow caller —
// the CLI, the console's /api/reconstruct, and the MCP reconstruct tool —
// resolves such a key instead of only the CLI.
//
// The two run in OPPOSITE directions on purpose, because they target different
// stores — do not "unify" them:
//
//   - IndexPKSpelling TRIMS: binlog_events.pk_values holds the ROW image's
//     spelling, with every trailing 0x00 stripped.
//   - padFixedBinaryFilter RE-PADS: the baseline Parquet holds the full n
//     bytes MySQL padded on storage.

// ResolvePKMetasAt loads schema.table's primary-key column metadata from the
// schema snapshot in effect at `at` — metadata.EpochAt over the snapshot
// history, the same per-instant rule the ENUM/SET decode path uses (#475) —
// rather than from the latest snapshot (#1159). The metas' declared widths
// drive padFixedBinaryFilter, so the anchor should be the instant whose schema
// produced the bytes being matched: callers pass the BASELINE snapshot time,
// because the pad must reach the width the baseline file actually stores. A
// latest-snapshot width is wrong whenever the column was widened after that
// instant (BINARY(16) → BINARY(32)): the retry would pad to 32 against a
// 16-byte stored value and miss silently.
//
// When the instant predates every snapshot, EpochAt answers the FIRST epoch —
// the closest available description; with no epoch history at all, the latest
// snapshot is used.
//
// Best-effort by design: every caller only uses the result to IMPROVE a lookup
// or an error message, so a missing/unreadable snapshot degrades to nil (no
// reconciliation — the pre-#1155 behavior) instead of failing a reconstruct
// that would otherwise have worked.
func ResolvePKMetasAt(db *sql.DB, schema, table string, at time.Time) []metadata.ColumnMeta {
	snapshotID := 0 // latest, when the epoch history is unavailable
	if epochs, err := metadata.LoadSnapshotEpochs(db); err != nil {
		slog.Debug("could not load schema snapshot epochs for PK metadata; using the latest snapshot", "error", err)
	} else if id, ok := metadata.EpochAt(epochs, at); ok {
		snapshotID = id
	}
	// Debug only: a nil result is harmless for most keys. For a MariaDB
	// UUID/INET key it is not, and CheckUntypedMariaDBFixedPK refuses the
	// lookup instead of letting it answer wrong.
	res, err := metadata.NewResolver(db, snapshotID)
	if err != nil {
		slog.Debug("could not load schema snapshot for PK metadata", "error", err)
		return nil
	}
	tm, err := res.Resolve(schema, table)
	if err != nil {
		slog.Debug("could not resolve table for PK metadata", "error", err)
		return nil
	}
	return tm.PKColumnMetas()
}

// IndexPKSpelling rewrites a user-supplied PK value into the spelling the
// indexer stored in binlog_events.pk_values, so an event fetch matches what
// the operator typed.
//
// MariaDB UUID/INET4/INET6 components are parsed from their text form (any
// spelling metadata.ParseMariaDBFixed accepts) and spelled as the captured
// full-width bytes. Otherwise only fixed-width BINARY(n) components are
// touched, and this is the INVERSE
// of padFixedBinaryFilter — the two run in opposite directions on purpose,
// because they target different stores. Reproducing event.formatPKValue
// exactly: trailing 0x00 padding is stripped (the ROW image never carries it),
// and the hex is uppercased, but ONLY when the trimmed bytes are not valid
// UTF-8 — formatPKValue is content-gated, so a binary key whose bytes are
// printable ASCII is stored verbatim and must stay that way.
//
// Everything else — every other column type, and every component that is
// already in the stored spelling — is returned untouched, so this cannot
// disturb a lookup that resolves today.
func IndexPKSpelling(pk string, pkMetas []metadata.ColumnMeta) string {
	if pk == "" || len(pkMetas) == 0 {
		return pk
	}
	parts := strings.Split(pk, "|")
	if len(parts) != len(pkMetas) {
		// The pk/pk-columns arity is validated against the caller's column
		// list, not against the snapshot; if the two disagree, leave the
		// value alone rather than re-spell the wrong component.
		return pk
	}
	changed := false
	for i, c := range pkMetas {
		if metadata.MariaDBFixedWidth(c.DataType) > 0 {
			// MariaDB UUID/INET4/INET6: the index keys the event by the
			// full-width bytes (metadata.MapRow pads them), spelled by
			// event.BuildPKValues, escaping included. A value that does not
			// parse is left as typed; ReadBaselineRow refuses it first.
			if b, err := metadata.ParseMariaDBFixed(c.DataType, parts[i]); err == nil {
				spelled := event.BuildPKValues([]metadata.ColumnMeta{c}, map[string]any{c.Name: b})
				if spelled != parts[i] {
					parts[i] = spelled
					changed = true
				}
			}
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(c.DataType), "binary") {
			continue
		}
		raw, isHex := decodeHexPKLiteral(parts[i])
		if !isHex {
			continue // already the verbatim/stored spelling
		}
		trimmed := TrimFixedBinaryPad(raw)
		var spelled string
		if utf8.Valid(trimmed) {
			spelled = string(trimmed)
		} else {
			spelled = "0x" + strings.ToUpper(hex.EncodeToString(trimmed))
		}
		if spelled != parts[i] {
			parts[i] = spelled
			changed = true
		}
	}
	if !changed {
		return pk
	}
	return strings.Join(parts, "|")
}

// padFixedBinaryFilter re-spells a fixed-width BINARY(n) filter value back to
// the width the baseline stores, returning false when nothing needs re-spelling.
//
// This is the INVERSE of TrimFixedBinaryPad, and the direction is deliberate:
// pk_values holds the binlog ROW image's spelling, which has every trailing
// 0x00 stripped, while the baseline Parquet holds the full n bytes MySQL
// padded on storage. An operator who copies a key out of the index — the
// workflow #1155 reports — therefore hands us a value SHORTER than the one to
// match. Re-padding it is exact (MySQL only ever pads a BINARY(n) with 0x00),
// and ReadBaselineRow only ever attempts it after an exact lookup already came
// back empty, so it cannot turn a correct hit into a different row.
func padFixedBinaryFilter(pkFilter map[string]string, pkMetas []metadata.ColumnMeta) (map[string]string, bool) {
	out := make(map[string]string, len(pkFilter))
	maps.Copy(out, pkFilter)
	changed := false
	for _, c := range pkMetas {
		if !strings.EqualFold(strings.TrimSpace(c.DataType), "binary") {
			continue
		}
		width := FixedBinaryWidth(c.ColumnType)
		if width == 0 {
			// Pre-#212 snapshot with no COLUMN_TYPE: the pad width is
			// unknowable, so leave the value alone rather than guess.
			continue
		}
		// Filter keys are operator-typed and MySQL column names are
		// case-insensitive, so an exact-only match would silently skip the
		// retry for column name `K` against snapshot column `k`. (The lookup
		// underneath is case-insensitive on both links since #1155: DuckDB
		// resolves the quoted identifier, and parquetBlobColumns is keyed
		// lowercase.)
		key, ok := filterKeyFor(out, c.Name)
		if !ok {
			continue
		}
		val := out[key]
		raw, isHex := decodeHexPKLiteral(val)
		if !isHex {
			raw = []byte(val)
		}
		if len(raw) >= width {
			continue
		}
		padded := make([]byte, width)
		copy(padded, raw)
		out[key] = "0x" + strings.ToUpper(hex.EncodeToString(padded))
		changed = true
	}
	return out, changed
}

// filterKeyFor finds the filter entry naming column col, preferring an exact
// match and falling back to a case-insensitive one.
func filterKeyFor(filter map[string]string, col string) (string, bool) {
	if _, ok := filter[col]; ok {
		return col, true
	}
	for k := range filter {
		if strings.EqualFold(k, col) {
			return k, true
		}
	}
	return "", false
}

// mariaDBFixedBaselineFilter re-spells every MariaDB UUID/INET4/INET6
// component of a baseline PK filter as the text MariaDB prints, which is what
// the baseline column holds (mydumper dumps the text form). The server accepts
// other spellings (upper case, a UUID without dashes, a long-form IPv6), so an
// exact comparison against the typed value would miss a row that exists. A
// value that is not one of these types' text is refused: guessing a spelling
// could resolve a different row. With no metas the filter passes through.
func mariaDBFixedBaselineFilter(pkFilter map[string]string, pkMetas []metadata.ColumnMeta) (map[string]string, error) {
	var out map[string]string
	for _, c := range pkMetas {
		if metadata.MariaDBFixedWidth(c.DataType) == 0 {
			continue
		}
		key, ok := filterKeyFor(pkFilter, c.Name)
		if !ok {
			continue
		}
		v := pkFilter[key]
		b, err := metadata.ParseMariaDBFixed(c.DataType, v)
		if err != nil {
			return nil, fmt.Errorf("primary-key column %q: %w", c.Name, err)
		}
		text, _ := metadata.FormatMariaDBFixed(c.DataType, b)
		if out == nil {
			out = maps.Clone(pkFilter)
		}
		out[key] = text
	}
	if out == nil {
		return pkFilter, nil
	}
	return out, nil
}

// CheckUntypedMariaDBFixedPK refuses a single-row lookup that would answer
// wrong without saying so. The index keys a MariaDB UUID/INET4/INET6 row by
// the value's bytes, and only the index's schema snapshot tells a caller to
// spell the key that way (IndexPKSpelling). Without that snapshot (pkMetas
// nil) the key is looked up as text: the baseline row matches, no event does,
// and the snapshot-era row would come back as the state at the target time.
// So when pkMetas is nil and the baseline's own CREATE TABLE shows one of
// those types among the filtered columns, this returns an error naming the
// fix. An unreadable footer is an error; a CREATE the parser cannot read is
// scanned for the type names instead. Only a footer with no CREATE TABLE at
// all (a baseline older than that metadata) passes unchecked.
func CheckUntypedMariaDBFixedPK(ctx context.Context, path string, pkFilter map[string]string, pkMetas []metadata.ColumnMeta) error {
	if len(pkMetas) > 0 {
		return nil
	}
	bm, err := baseline.ReadParquetMetadataAny(ctx, path)
	if err != nil {
		// The row read needs this same file; an error here is not a reason
		// to let the lookup through.
		return fmt.Errorf("read the baseline footer to type the primary key: %w", err)
	}
	if bm.CreateTableSQL == "" {
		return nil
	}
	cols, err := baseline.ParseSchemaText(bm.CreateTableSQL)
	if err != nil {
		// A CREATE the parser cannot read (one line, say) still names its
		// types. This text is the baseline's own, never a placeholder, so a
		// UUID/INET word in it is the column type or an identifier; refusing
		// on either is the safe side of "cannot tell".
		lower := strings.ToLower(bm.CreateTableSQL)
		for _, w := range []string{"uuid", "inet4", "inet6"} {
			if strings.Contains(lower, w) {
				return fmt.Errorf("the baseline's CREATE TABLE mentions %s and could not be parsed, "+
					"and the index has no schema snapshot for this table to spell a MariaDB %s key the way it stores it; "+
					"the answer could leave out every change after the baseline, so it is refused: "+
					"run `bintrail snapshot` for this schema, then try again", strings.ToUpper(w), strings.ToUpper(w))
			}
		}
		return nil
	}
	for _, c := range cols {
		if metadata.MariaDBFixedWidth(c.MySQLType) == 0 {
			continue
		}
		for k := range pkFilter {
			if strings.EqualFold(strings.TrimSpace(k), c.Name) {
				return fmt.Errorf("primary-key column %q is a MariaDB %s, which the index stores as bytes, "+
					"and the index has no schema snapshot for this table to spell the key that way; "+
					"the answer would leave out every change after the baseline, so it is refused: "+
					"run `bintrail snapshot` for this schema, then try again", c.Name, strings.ToLower(c.MySQLType))
			}
		}
	}
	return nil
}
