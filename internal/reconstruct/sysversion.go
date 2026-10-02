package reconstruct

import (
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/sysversion"
)

// sysVersioned is how a full-table fold reads a MariaDB system-versioned
// table (#2007): as the table the application sees, its current rows, keyed
// on the declared primary key.
//
// A snapshot of such a table holds exactly that. mydumper's SELECT returns
// current rows only, and it writes neither period column (they are generated,
// so they are left out like any generated column, #863). The binlog, though,
// carries every version under a key MariaDB extended with ROW END (see package
// sysversion for the shapes). normalize turns each captured event into what it
// means for the current rows, so the rest of the fold, the merge, the spill
// and the table deltas see an ordinary table with the declared key.
type sysVersioned struct {
	schema, table string
	period        sysversion.Period
	// endIdx is ROW END's position in the stored pk_values, keyN its width.
	endIdx, keyN int
	// surface names the reader in refusals; empty is full-table reconstruct.
	surface string
}

// sysVersioningFor decides how a full-table fold keys a table whose snapshot
// PK has a generated member. It returns the key the fold uses and, for a
// system-versioned table, how to read its events (nil for every other table).
//
// The table counts as system-versioned only when BOTH sides agree: the
// snapshot's CREATE TABLE (what the rows being folded onto came from) declares
// WITH SYSTEM VERSIONING, and the PK's one generated member is that
// statement's period end. Anything else keeps the generated-PK refusal:
//
//   - versioning added by ALTER after the snapshot was taken (its CREATE says
//     nothing about it): a full read takes a snapshot that does, and the next
//     refresh works.
//   - an ordinary STORED generated column in the key: its value is not in the
//     snapshot, so no join key can be built.
//
// Transaction-precise versioning (BIGINT UNSIGNED period columns, ROW END = a
// transaction id) is refused on its own: its ROW END is no time, and this
// build does not read it.
func sysVersioningFor(schema, table, createSQL string, pkCols []metadata.ColumnMeta) (*sysVersioned, []metadata.ColumnMeta, error) {
	genIdx := -1
	for i, c := range pkCols {
		if !c.IsGenerated {
			continue
		}
		if genIdx >= 0 {
			return nil, nil, fullTableGeneratedPKRefusal(schema, table, pkCols[genIdx])
		}
		genIdx = i
	}
	period, versioned := sysversion.FromCreateTable(createSQL)
	if genIdx < 0 {
		if versioned {
			// The snapshot's rows came from a versioned table, but the
			// schema snapshot's key has no ROW END: versioning was dropped
			// after the snapshot, or the schema snapshot predates #1272.
			// Events from while it was versioned carry the extended key, so
			// a plain fold would emit their history rows as live ones.
			return nil, nil, fmt.Errorf("full-table reconstruct: %s.%s: the snapshot this starts from declares the table "+
				"WITH SYSTEM VERSIONING, but the schema snapshot's primary key has no ROW END column (versioning dropped "+
				"since, or a schema snapshot older than the hidden-column support); a new full snapshot of the table, "+
				"after `bintrail snapshot`, cures this: %w", schema, table, ErrSchemaChanged)
		}
		return nil, pkCols, nil
	}
	gen := pkCols[genIdx]
	if !versioned || !strings.EqualFold(gen.Name, period.End) {
		return nil, nil, fullTableGeneratedPKRefusal(schema, table, gen)
	}
	switch strings.ToLower(strings.TrimSpace(gen.DataType)) {
	case "timestamp":
	case "bigint":
		return nil, nil, GeneratedPKRefusalError(fmt.Sprintf(
			"full-table reconstruct: %s.%s uses transaction-precise system versioning (its period column %q is a "+
				"BIGINT UNSIGNED transaction id, not a time); the fold reads row versions by time only, so it cannot "+
				"tell this table's current rows from its history", schema, table, gen.Name))
	default:
		return nil, nil, fullTableGeneratedPKRefusal(schema, table, gen)
	}
	// The images name the columns as the snapshot spells them.
	period.End = gen.Name
	key := make([]metadata.ColumnMeta, 0, len(pkCols)-1)
	key = append(key, pkCols[:genIdx]...)
	key = append(key, pkCols[genIdx+1:]...)
	return &sysVersioned{schema: schema, table: table, period: period, endIdx: genIdx, keyN: len(pkCols)}, key, nil
}

// normalize rewrites one captured event of the versioned table into its
// meaning for the current rows, in place, and reports whether it means
// anything at all:
//
//	INSERT of a current row             → INSERT
//	INSERT of a history row             → nothing (the old version an UPDATE keeps)
//	UPDATE current → current            → UPDATE
//	UPDATE current → history            → DELETE (MariaDB's versioned delete)
//	UPDATE history → history            → nothing (an edit of history)
//	DELETE of a history row             → nothing (DELETE HISTORY, partition rotation)
//	DELETE of a current row             → DELETE
//
// Kept events lose both period columns from their images and the ROW END
// component from their key. An UPDATE that makes a history version current
// has no meaning this fold can give it, and a ROW END value nothing explains
// cannot be placed: both are refused, never guessed.
func (sv *sysVersioned) normalize(ev *query.ResultRow) (bool, error) {
	state := func(img map[string]any, which string) (sysversion.State, error) {
		if img == nil {
			return 0, sv.refuse(ev, fmt.Sprintf("its %s image is missing", which))
		}
		st, err := sysversion.RowEnd(img[sv.period.End], ev.EventTimestamp)
		if err != nil {
			return 0, sv.refuse(ev, err.Error())
		}
		return st, nil
	}
	var typ event.EventType
	switch ev.EventType {
	case event.EventInsert:
		after, err := state(ev.RowAfter, "after")
		if err != nil {
			return false, err
		}
		if after == sysversion.History {
			return false, nil
		}
		typ = event.EventInsert
	case event.EventUpdate:
		before, err := state(ev.RowBefore, "before")
		if err != nil {
			return false, err
		}
		after, err := state(ev.RowAfter, "after")
		if err != nil {
			return false, err
		}
		switch {
		case before == sysversion.Current && after == sysversion.Current:
			typ = event.EventUpdate
		case before == sysversion.Current:
			typ = event.EventDelete
		case after == sysversion.History:
			return false, nil
		default:
			return false, sv.refuse(ev, "it makes a history version of the row current again")
		}
	case event.EventDelete:
		before, err := state(ev.RowBefore, "before")
		if err != nil {
			return false, err
		}
		if before == sysversion.History {
			return false, nil
		}
		typ = event.EventDelete
	default:
		return false, sv.refuse(ev, fmt.Sprintf("event type %d is not a row change", ev.EventType))
	}

	key, ok := event.DropPKComponent(ev.PKValues, sv.endIdx, sv.keyN)
	if !ok {
		return false, sv.refuse(ev, fmt.Sprintf("its stored pk_values %q does not hold the %d key components the schema snapshot records",
			ev.PKValues, sv.keyN))
	}
	ev.EventType = typ
	ev.PKValues = key
	ev.RowBefore = sv.strip(ev.RowBefore)
	ev.RowAfter = sv.strip(ev.RowAfter)
	if typ == event.EventDelete {
		ev.RowAfter = nil
	}
	return true, nil
}

// strip returns a copy of img without the period columns. A copy, because the
// page's maps may be shared with whatever else holds the page.
func (sv *sysVersioned) strip(img map[string]any) map[string]any {
	if img == nil {
		return nil
	}
	out := maps.Clone(img)
	// MySQL identifiers are case-insensitive: the CREATE TABLE and the
	// snapshot may spell a period column differently.
	for k := range out {
		if strings.EqualFold(k, sv.period.Start) || strings.EqualFold(k, sv.period.End) {
			delete(out, k)
		}
	}
	return out
}

func (sv *sysVersioned) refuse(ev *query.ResultRow, why string) error {
	surface := sv.surface
	if surface == "" {
		surface = "full-table reconstruct"
	}
	return fmt.Errorf("%s: %s.%s is system-versioned and event %d cannot be read as a change "+
		"of its current rows: %s", surface, sv.schema, sv.table, ev.EventID, why)
}

// SingleRowVersioned is how a single-row reconstruct reads a MariaDB
// system-versioned table (#2007): the declared-key value is looked up under
// the stored spellings (ExpandKey) and the events found are read for what
// they mean to the current row (Normalize), as the full-table fold reads
// them.
type SingleRowVersioned struct {
	sv      *sysVersioned
	pkMetas []metadata.ColumnMeta
}

// SingleRowSysVersioning decides how a single-row reconstruct reads
// schema.table: nil for an ordinary table, a handle for a system-versioned
// one, or a refusal when it cannot be read safely.
//
// Two signals, because either can be missing: the snapshot's CREATE TABLE
// (empty when its metadata could not be read, or predates the embedded
// statement), and the schema snapshot's key (pkMetas). Reading needs the
// CREATE TABLE (it names the period columns); a generated key member without
// it refuses. With neither, nothing rules versioning out, so a MariaDB
// source (flavor) refuses; an unknown source only warns, since an index
// without a recorded source is the file-indexed shape, which predates
// MariaDB support for most installs; a MySQL or PostgreSQL source has no
// system versioning.
//
// pkCols are the key columns the caller looks the row up by: they must be
// the declared key, without the period end.
func SingleRowSysVersioning(schema, table, createSQL string, pkMetas []metadata.ColumnMeta, pkCols []string, flavor string) (*SingleRowVersioned, error) {
	_, versioned := sysversion.FromCreateTable(createSQL)
	gen, hasGen := GeneratedPKColumn(pkMetas)
	if !versioned && !hasGen {
		if createSQL == "" && len(pkMetas) == 0 && flavor == "" {
			slog.Warn("neither the snapshot's metadata nor a schema snapshot describes the table, and the index does not "+
				"record its source; if it is a MariaDB system-versioned table this lookup misses its changes "+
				"(take a schema snapshot, `bintrail snapshot`, to rule that out)", "schema", schema, "table", table)
		}
		if createSQL == "" && len(pkMetas) == 0 && flavor == "mariadb" {
			return nil, fmt.Errorf("cannot tell whether %s.%s is system-versioned: neither the snapshot's metadata nor a schema "+
				"snapshot describes it, and a system-versioned table's changes are stored under a key this lookup would miss, "+
				"so the answer could be the snapshot row as if nothing had changed; take a schema snapshot first (`bintrail snapshot`)",
				schema, table)
		}
		return nil, nil
	}
	if hasGen && slices.ContainsFunc(pkCols, func(c string) bool { return strings.EqualFold(strings.TrimSpace(c), gen.Name) }) {
		return nil, fmt.Errorf("%s.%s is system-versioned: look the row up by its declared primary key, without %q, which "+
			"MariaDB appends to the key; the row's current and past versions are read for you", schema, table, gen.Name)
	}
	if !versioned {
		return nil, fmt.Errorf("%s.%s looks system-versioned (its primary key has the generated column %q), but the "+
			"snapshot's metadata does not carry its CREATE TABLE, which names the period columns; reconstruct the whole "+
			"table, or take a new full snapshot and retry", schema, table, gen.Name)
	}
	sv, _, err := sysVersioningFor(schema, table, createSQL, pkMetas)
	if err != nil {
		return nil, err
	}
	if sv == nil {
		return nil, nil
	}
	sv.surface = "reconstruct"
	return &SingleRowVersioned{sv: sv, pkMetas: pkMetas}, nil
}

// ExpandKey adds the stored spellings of opts' declared-key value(s).
func (h *SingleRowVersioned) ExpandKey(opts *query.Options) {
	values := opts.PKValuesIn
	if opts.PKValues != "" {
		values = []string{opts.PKValues}
		if opts.PKValuesAlt != "" {
			values = append(values, opts.PKValuesAlt)
		}
	}
	if out, ok := expandSysVersionedKeys(h.pkMetas, values); ok {
		opts.PKValues, opts.PKValuesAlt, opts.PKValuesIn = "", "", out
	}
}

// Normalize reads the row's events for what they mean to its current
// version, dropping history versions, and refuses one it cannot read.
func (h *SingleRowVersioned) Normalize(events []query.ResultRow) ([]query.ResultRow, error) {
	out := make([]query.ResultRow, 0, len(events))
	for i := range events {
		ev := events[i]
		keep, err := h.sv.normalize(&ev)
		if err != nil {
			return nil, err
		}
		if keep {
			out = append(out, ev)
		}
	}
	return out, nil
}

// onlyPeriodColumns reports whether every column in extra is a system-
// versioning period column: the hidden row_start/row_end, or the period the
// CREATE TABLE declares.
func onlyPeriodColumns(extra []string, createSQL string) bool {
	names := []string{sysversion.ImplicitStart, sysversion.ImplicitEnd}
	if p, ok := sysversion.FromCreateTable(createSQL); ok {
		names = append(names, p.Start, p.End)
	}
	for _, col := range extra {
		if !slices.ContainsFunc(names, func(n string) bool { return strings.EqualFold(n, col) }) {
			return false
		}
	}
	return true
}
