package pgcapture

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/dbtrail/dbtrail/internal/event"
)

// DDL inference from the stream alone (#2441).
//
// pgoutput carries no DDL statement. It does send a Relation message before
// the first change to a table under a new shape, so comparing each Relation
// with the last shape seen for the same table OID tells what changed: a
// rename (same OID, new name), an ALTER (same OID, different columns), a drop
// and recreate (a new OID under a name another OID held, that OID gone from
// the catalog), a create (a new OID under an unknown name). Rows are already
// assigned to the right shape by logical decoding; what this adds is the
// record that the shape changed, for the schema-changes timeline and the
// reconstruct guards.
//
// What the stream cannot say is when the DDL committed, so each row carries
// the bracket it committed in: after the last change under the old shape
// (low), at or before the first change under the new one (high). The row's
// stream position is high, its timestamp is low, so a time-based guard
// refuses anywhere inside the bracket.
//
// Invisible by construction: a plain DROP TABLE not followed by a CREATE of
// the same name, and every change that alters neither a column's name or
// type nor the table's name (indexes, constraints, defaults, NOT NULL,
// grants, comments). Statement text and the role that ran it are never
// available.

// ShapeColumn is one column of a table shape, as a Relation message describes
// it: name, pg_type OID and atttypmod (-1 = no modifier).
type ShapeColumn struct {
	Name    string
	TypeOID uint32
	TypeMod int32
}

// KnownShape is the last shape persisted for a table before this stream
// started. OID 0 marks a shape stored before OIDs were recorded; it is
// matched by exact (schema, table).
type KnownShape struct {
	OID     uint32
	Schema  string
	Table   string
	Columns []ShapeColumn
}

// RelationFacts is what the live catalog says about a relation OID.
type RelationFacts struct {
	Exists        bool
	Schema, Table string // current name, when Exists
	IsPartition   bool
	Parent        string // "schema.table" of the parent, when IsPartition
}

// DDLInference configures inference. Lookup and LastChange are required.
type DDLInference struct {
	// Known is the last shape persisted per table, oldest first (the
	// order metadata.LoadPGShapes returns); later entries win a name.
	Known []KnownShape
	// StartOIDs are the tables the publication covered when capture
	// started. The first Relation for one of them with nothing known is
	// version zero (no row); a table outside it is new since the start.
	StartOIDs map[uint32]bool
	// Lookup reads the live catalog for an OID. An error stops the stream.
	Lookup func(oid uint32) (RelationFacts, error)
	// LastChange returns the newest indexed change to schema.table that
	// committed before beforeLSN: the lower bound when this stream has not
	// decoded one. ok is false when there is none. An error stops the
	// stream.
	LastChange func(schema, table string, beforeLSN uint64) (lsn uint64, at time.Time, ok bool, err error)
}

// WithDDLInference turns DDL inference on. It panics on a missing resolver,
// at the wiring site rather than on the first shape change.
func WithDDLInference(cfg DDLInference) DecoderOption {
	if cfg.Lookup == nil || cfg.LastChange == nil {
		panic("pgcapture: WithDDLInference requires Lookup and LastChange")
	}
	return func(d *Decoder) { d.infer = newInferState(cfg) }
}

type relName struct{ schema, table string }

func (n relName) String() string { return n.schema + "." + n.table }

type bound struct {
	lsn pglogrepl.LSN
	at  time.Time
}

type knownRel struct {
	name relName
	cols []ShapeColumn
	// low is the commit of the last change decoded under cols in this
	// stream; set by rowEvent, cleared when the shape changes.
	low    bound
	hasLow bool
}

type inferState struct {
	cfg    DDLInference
	byOID  map[uint32]*knownRel
	byName map[relName]uint32
	legacy map[relName][]ShapeColumn
}

func newInferState(cfg DDLInference) *inferState {
	s := &inferState{cfg: cfg, byOID: map[uint32]*knownRel{}, byName: map[relName]uint32{}, legacy: map[relName][]ShapeColumn{}}
	for _, k := range cfg.Known {
		n := relName{k.Schema, k.Table}
		if k.OID == 0 {
			s.legacy[n] = k.Columns
			continue
		}
		s.byOID[k.OID] = &knownRel{name: n, cols: k.Columns}
		s.byName[n] = k.OID
	}
	return s
}

// noteChange records a decoded row change as the newest under its shape.
func (s *inferState) noteChange(oid uint32, txn txnContext) {
	if k, ok := s.byOID[oid]; ok {
		k.low, k.hasLow = bound{txn.commitLSN, txn.commitTime}, true
	}
}

func shapeOf(m *pglogrepl.RelationMessage) []ShapeColumn {
	cols := make([]ShapeColumn, len(m.Columns))
	for i, c := range m.Columns {
		cols[i] = ShapeColumn{Name: c.Name, TypeOID: c.DataType, TypeMod: c.TypeModifier}
	}
	return cols
}

func equalShape(a, b []ShapeColumn) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// observe compares a Relation message with the last known shape of its table,
// updates what is known, and returns the inferred DDL events in the order they
// must be written. txn is the transaction the message arrived in.
func (s *inferState) observe(m *pglogrepl.RelationMessage, txn txnContext, warn func(msg string, args ...any)) ([]event.Event, error) {
	oid := m.RelationID
	n := relName{m.Namespace, m.RelationName}
	cols := shapeOf(m)

	// A Relation arrives inside the transaction whose change needs it. Without
	// one there is no position to place a row at: learn the shape, write
	// nothing.
	placeable := txn.open
	high := bound{txn.commitLSN, txn.commitTime}
	var out []event.Event

	if k, ok := s.byOID[oid]; ok {
		renamed := k.name != n
		altered := !equalShape(k.cols, cols)
		if (renamed || altered) && placeable {
			low, err := s.lowFor(k, k.name, high)
			if err != nil {
				return nil, err
			}
			if renamed {
				out = append(out, renameEvent(oid, k.name, n, "", low, high))
			}
			if altered {
				out = append(out, alterEvent(oid, n, k.cols, cols, "", low, high))
			}
		} else if renamed || altered {
			warn("pgcapture: relation shape changed outside a transaction; not recorded as a schema change", "oid", oid, "table", n.String())
		}
		if renamed && s.byName[k.name] == oid {
			delete(s.byName, k.name)
		}
		s.byName[n] = oid
		k.name, k.cols = n, cols
		if renamed || altered {
			k.hasLow = false
		}
		return out, nil
	}

	// A new OID.
	newRel := &knownRel{name: n, cols: cols}
	if holder, ok := s.byName[n]; ok && holder != oid {
		hk := s.byOID[holder]
		facts, err := s.cfg.Lookup(holder)
		if err != nil {
			return nil, fmt.Errorf("pgcapture: catalog lookup for relation OID %d (formerly %s): %w", holder, n, err)
		}
		if placeable {
			low, err := s.lowFor(hk, n, high)
			if err != nil {
				return nil, err
			}
			if facts.Exists {
				// Renamed away, never written to under its new name: the name
				// was freed, not the table dropped.
				to := relName{facts.Schema, facts.Table}
				out = append(out, renameEvent(holder, n, to,
					" (its new name read from the current catalog: the rename committed at or before this point)", low, high))
			} else {
				out = append(out, ddlEvent(event.DDLDropTable, holder, n, nil,
					fmt.Sprintf("DROP TABLE %s (its name now belongs to a new table and the old one is gone from the catalog: dropped here, or renamed here and dropped since)", n),
					low, high))
			}
			c, err := s.createEvent(oid, n, " under a name that belonged to another table", high)
			if err != nil {
				return nil, err
			}
			out = append(out, c)
		}
		if facts.Exists {
			to := relName{facts.Schema, facts.Table}
			hk.name, hk.hasLow = to, false
			if to != n {
				s.byName[to] = holder
			}
		} else {
			delete(s.byOID, holder)
		}
		s.byOID[oid], s.byName[n] = newRel, oid
		return out, nil
	}

	if lc, ok := s.legacy[n]; ok {
		delete(s.legacy, n)
		s.byOID[oid], s.byName[n] = newRel, oid
		if !equalShape(lc, cols) && placeable {
			low, err := s.lowFor(nil, n, high)
			if err != nil {
				return nil, err
			}
			out = append(out, alterEvent(oid, n, lc, cols,
				" (the earlier shape was stored without the table's identity, so this could also be a drop and a create)", low, high))
		}
		return out, nil
	}

	s.byOID[oid], s.byName[n] = newRel, oid
	if s.cfg.StartOIDs[oid] || !placeable {
		return nil, nil // version zero
	}
	c, err := s.createEvent(oid, n, "", high)
	if err != nil {
		return nil, err
	}
	return []event.Event{c}, nil
}

// lowFor is the lower bound of a shape change on the table k (nil when only a
// legacy shape was known) that last held name: the last change decoded in
// this stream, else the newest indexed change before high, else none.
func (s *inferState) lowFor(k *knownRel, name relName, high bound) (*bound, error) {
	if k != nil && k.hasLow {
		b := k.low
		return &b, nil
	}
	lsn, at, ok, err := s.cfg.LastChange(name.schema, name.table, uint64(high.lsn))
	if err != nil {
		return nil, fmt.Errorf("pgcapture: last indexed change to %s (lower bound of a schema change): %w", name, err)
	}
	if !ok {
		return nil, nil
	}
	return &bound{pglogrepl.LSN(lsn), at}, nil
}

func (s *inferState) createEvent(oid uint32, n relName, why string, high bound) (event.Event, error) {
	facts, err := s.cfg.Lookup(oid)
	if err != nil {
		return event.Event{}, fmt.Errorf("pgcapture: catalog lookup for relation OID %d (%s): %w", oid, n, err)
	}
	text := fmt.Sprintf("CREATE TABLE %s", n)
	if facts.IsPartition {
		text += " partition of " + facts.Parent
	}
	if why != "" {
		text += " (a new table" + why + ")"
	} else {
		text += " (first seen in the stream: created, or added to the publication)"
	}
	return ddlEvent(event.DDLCreateTable, oid, n, nil, text, nil, high), nil
}

func renameEvent(oid uint32, from, to relName, note string, low *bound, high bound) event.Event {
	text := fmt.Sprintf("RENAME TABLE %s TO %s", from, to)
	if from.schema != to.schema {
		text += " (ALTER TABLE ... SET SCHEMA"
		if from.table != to.table {
			text += " and a rename"
		}
		text += ")"
	}
	text += note
	return ddlEvent(event.DDLRenameTable, oid, to, []event.DDLTable{{Schema: from.schema, Table: from.table}, {Schema: to.schema, Table: to.table}}, text, low, high)
}

func alterEvent(oid uint32, n relName, before, after []ShapeColumn, note string, low *bound, high bound) event.Event {
	return ddlEvent(event.DDLAlterTable, oid, n, nil, fmt.Sprintf("ALTER TABLE %s: %s%s", n, describeShapeDiff(before, after), note), low, high)
}

// describeShapeDiff says, in words, how two column lists differ.
func describeShapeDiff(before, after []ShapeColumn) string {
	old := make(map[string]ShapeColumn, len(before))
	for _, c := range before {
		old[c.Name] = c
	}
	cur := make(map[string]bool, len(after))
	var added, dropped, retyped []string
	for _, c := range after {
		cur[c.Name] = true
		o, ok := old[c.Name]
		switch {
		case !ok:
			added = append(added, fmt.Sprintf("%s (%s)", c.Name, typeName(c.TypeOID, c.TypeMod)))
		case o.TypeOID != c.TypeOID || o.TypeMod != c.TypeMod:
			retyped = append(retyped, fmt.Sprintf("%s: %s -> %s", c.Name, typeName(o.TypeOID, o.TypeMod), typeName(c.TypeOID, c.TypeMod)))
		}
	}
	for _, c := range before {
		if !cur[c.Name] {
			dropped = append(dropped, fmt.Sprintf("%s (%s)", c.Name, typeName(c.TypeOID, c.TypeMod)))
		}
	}
	var parts []string
	if len(added) > 0 {
		parts = append(parts, "added "+strings.Join(added, ", "))
	}
	if len(dropped) > 0 {
		parts = append(parts, "dropped "+strings.Join(dropped, ", "))
	}
	if len(retyped) > 0 {
		parts = append(parts, "retyped "+strings.Join(retyped, ", "))
	}
	if len(parts) == 0 {
		// Same names and types in another order: a column was dropped and
		// added again under the same name.
		return "column order changed (a column was dropped and added again under the same name)"
	}
	text := strings.Join(parts, "; ")
	if len(added) > 0 && len(dropped) > 0 {
		text += " (a renamed column reads as a drop plus an add)"
	}
	return text
}

var typeNames = pgtype.NewMap()

// typeName spells a pg_type OID and atttypmod for a person, for the built-in
// types; any other type is named by its OID.
func typeName(oid uint32, mod int32) string {
	t, ok := typeNames.TypeForOID(oid)
	if !ok {
		if mod >= 0 {
			return fmt.Sprintf("type oid %d, modifier %d", oid, mod)
		}
		return fmt.Sprintf("type oid %d", oid)
	}
	name := t.Name
	switch oid {
	case pgtype.VarcharOID, pgtype.BPCharOID:
		if mod >= 4 {
			return fmt.Sprintf("%s(%d)", name, mod-4)
		}
	case pgtype.NumericOID:
		if mod >= 4 {
			return fmt.Sprintf("%s(%d,%d)", name, (mod-4)>>16, (mod-4)&0xffff)
		}
	case pgtype.TimestampOID, pgtype.TimestamptzOID, pgtype.TimeOID, pgtype.TimetzOID, pgtype.BitOID, pgtype.VarbitOID:
		if mod >= 0 {
			return fmt.Sprintf("%s(%d)", name, mod)
		}
	default:
		if mod >= 0 {
			return fmt.Sprintf("%s, modifier %d", name, mod)
		}
	}
	return name
}

// ddlEvent builds one inferred schema_changes row. Its stream position is the
// upper bound; its timestamp is the lower bound when there is one, else the
// upper bound. The query text starts with a machine-readable marker.
func ddlEvent(kind event.DDLKind, oid uint32, n relName, tables []event.DDLTable, text string, low *bound, high bound) event.Event {
	lowText, ts := "none", high.at
	if low != nil {
		lowText, ts = fmtBound(*low), low.at
	}
	marker := fmt.Sprintf("/* dbtrail: inferred from relation shape; oid=%d; instant=at-or-before; low=%s; high=%s */ ",
		oid, lowText, fmtBound(high))
	lsn := high.lsn.String()
	return event.Event{
		BinlogFile: lsn,
		StartPos:   uint64(high.lsn),
		EndPos:     uint64(high.lsn),
		Timestamp:  ts,
		GTID:       lsn,
		Schema:     n.schema,
		Table:      n.table,
		EventType:  event.EventDDL,
		DDLType:    kind,
		DDLQuery:   marker + capInferredText(text),
		DDLTables:  tables,
	}
}

// maxInferredText bounds the human part of an inferred row, well inside
// schema_changes.ddl_query (TEXT, 64 KiB): the column list of a very wide
// table must not make the insert fail and stop capture.
const maxInferredText = 16 << 10

const inferredTruncated = " ... (truncated)"

func capInferredText(s string) string {
	if len(s) <= maxInferredText {
		return s
	}
	cut := maxInferredText - len(inferredTruncated)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + inferredTruncated
}

func fmtBound(b bound) string {
	return b.lsn.String() + "@" + b.at.UTC().Format(time.RFC3339)
}
