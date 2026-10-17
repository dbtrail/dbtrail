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
	// SeenLSN/SeenAt: the commit of the transaction the shape was last
	// described in (0 = unknown, a shape stored before this was recorded).
	// A position with a zero time is a shape seen while inference was off.
	SeenLSN uint64
	SeenAt  time.Time
}

// KnownState is what the index holds for DDL inference at start.
type KnownState struct {
	Shapes   []KnownShape
	Releases []KnownRelease
}

// KnownRelease is an inferred DROP TABLE or RENAME TABLE already in the
// index: table OID gave up the name Schema.Table in the transaction that
// committed at LSN.
type KnownRelease struct {
	OID           uint32
	Schema, Table string
	LSN           uint64
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
	// Releases are the inferred DROP and RENAME rows already recorded: the
	// points where a table gave up a name (see KnownRelease).
	Releases []KnownRelease
	// Known is what the index holds of each table's shapes: for each OID
	// the versions that a replay from the slot's position can be compared
	// against (see metadata.LoadPGShapes), each with where it was seen.
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

// version is one shape a table had, and the commit of the transaction the
// stream described it in. live marks a version observed by this process.
type version struct {
	name relName
	cols []ShapeColumn
	seen bound // lsn 0 = unknown (a shape stored before positions were)
	live bool
	// blind: seen while DDL inference was off (a position, no time). Never a
	// reference; it marks a stretch where indexed rows cannot bound a change.
	blind bool
}

type knownRel struct {
	// versions in stream order (by seen, live after stored at one LSN).
	versions []version
	// low is the commit of the last change decoded in this stream under the
	// current shape; cleared when the shape changes.
	low    bound
	hasLow bool
	// releases are the points where another table took a name this one
	// had, recorded as a DROP or RENAME then (its new name, if any, came
	// from the live catalog). Persisted as those schema_changes rows.
	releases []release
}

// release: the table gave up name at the transaction committing at lsn.
type release struct {
	name relName
	lsn  pglogrepl.LSN
	live bool // recorded by this process
}

// released reports whether the table gave up name at or after from and
// before the transaction committing at before, or in that same transaction
// as seen by this process (the same rule as knownRel.at: a stored release at
// before is that transaction from an earlier run, which a resend is now
// deriving again).
func (k *knownRel) released(name relName, from, before pglogrepl.LSN) bool {
	for _, r := range k.releases {
		if r.name == name && r.lsn >= from && (r.lsn < before || (r.live && r.lsn == before)) {
			return true
		}
	}
	return false
}

func (k *knownRel) addRelease(r release) {
	for i, x := range k.releases {
		if x.name == r.name && x.lsn == r.lsn {
			k.releases[i].live = k.releases[i].live || r.live
			return
		}
	}
	k.releases = append(k.releases, r)
}

// at returns the shape in effect for a Relation in the transaction that
// commits at lsn: the newest version described before that transaction, or
// one this process described in that same transaction (a shape that changes
// twice in one transaction). A stored version at lsn itself is that same
// transaction seen by an earlier run, which a replay is now describing
// again, so it is not the reference.
func (k *knownRel) at(lsn pglogrepl.LSN) (version, bool) {
	for i := len(k.versions) - 1; i >= 0; i-- {
		v := k.versions[i]
		if v.blind {
			continue
		}
		if v.seen.lsn < lsn || (v.live && v.seen.lsn == lsn) {
			return v, true
		}
	}
	return version{}, false
}

// add records a version observed now, keeping stream order.
func (k *knownRel) add(v version) {
	i := len(k.versions)
	for i > 0 && k.versions[i-1].seen.lsn > v.seen.lsn {
		i--
	}
	k.versions = append(k.versions, version{})
	copy(k.versions[i+1:], k.versions[i:])
	k.versions[i] = v
}

type inferState struct {
	cfg    DDLInference
	byOID  map[uint32]*knownRel
	legacy map[relName][]ShapeColumn
}

func newInferState(cfg DDLInference) *inferState {
	s := &inferState{cfg: cfg, byOID: map[uint32]*knownRel{}, legacy: map[relName][]ShapeColumn{}}
	for _, k := range cfg.Known {
		n := relName{k.Schema, k.Table}
		if k.OID == 0 {
			s.legacy[n] = k.Columns
			continue
		}
		r := s.byOID[k.OID]
		if r == nil {
			r = &knownRel{}
			s.byOID[k.OID] = r
		}
		r.add(version{name: n, cols: k.Columns, seen: bound{pglogrepl.LSN(k.SeenLSN), k.SeenAt},
			blind: k.SeenLSN != 0 && k.SeenAt.IsZero()})
	}
	for _, rel := range cfg.Releases {
		r := s.byOID[rel.OID]
		if r == nil {
			r = &knownRel{}
			s.byOID[rel.OID] = r
		}
		r.addRelease(release{name: relName{rel.Schema, rel.Table}, lsn: pglogrepl.LSN(rel.LSN)})
	}
	return s
}

// holderAt returns the OID other than oid whose shape in effect at lsn
// carries name n.
func (s *inferState) holderAt(n relName, oid uint32, lsn pglogrepl.LSN) (uint32, *knownRel, version, bool) {
	var best uint32
	var bestK *knownRel
	var bestV version
	found := false
	for o, k := range s.byOID {
		if o == oid {
			continue
		}
		v, ok := k.at(lsn)
		if !ok || v.name != n || k.released(n, v.seen.lsn, lsn) {
			continue
		}
		if !found || v.seen.lsn > bestV.seen.lsn {
			best, bestK, bestV, found = o, k, v, true
		}
	}
	return best, bestK, bestV, found
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

// observe compares a Relation message with the shape its table had just
// before it in the stream, records the new shape, and returns the inferred
// DDL events in the order they must be written. txn is the transaction the
// message arrived in. inScope says whether a table name is captured: a change
// that touches no captured name updates what is known without asking the
// catalog or the index.
//
// The comparison is by stream position, never with "the newest shape
// known": after a restart PostgreSQL resends transactions from its own
// confirmed position, which can be before what this index already holds, and
// a replayed transaction must be compared exactly as it was the first time.
func (s *inferState) observe(m *pglogrepl.RelationMessage, txn txnContext, inScope func(relName) bool, warn func(msg string, args ...any)) ([]event.Event, error) {
	oid := m.RelationID
	n := relName{m.Namespace, m.RelationName}
	cols := shapeOf(m)

	// A Relation arrives inside the transaction whose change needs it.
	// Without one there is no position to place a row at, or to compare by.
	if !txn.open {
		warn("pgcapture: relation description outside a transaction; schema changes for it cannot be placed", "oid", oid, "table", n.String())
		return nil, nil
	}
	high := bound{txn.commitLSN, txn.commitTime}
	cur := version{name: n, cols: cols, seen: high, live: true}
	var out []event.Event

	if k, ok := s.byOID[oid]; ok {
		if prev, ok := k.at(high.lsn); ok {
			// A name given up earlier, when another table took it, was
			// recorded then (with the new name read from the catalog): the
			// table's own next description is compared by columns only.
			renamed := prev.name != n && !k.released(prev.name, prev.seen.lsn, high.lsn)
			altered := !equalShape(prev.cols, cols)
			if (renamed || altered) && (inScope(prev.name) || inScope(n)) {
				low, err := s.lowFor(k, prev, high)
				if err != nil {
					return nil, err
				}
				if renamed {
					out = append(out, renameEvent(oid, prev.name, n, "", low, high))
				}
				if altered {
					out = append(out, alterEvent(oid, n, prev.cols, cols, "", low, high))
				}
			}
			if renamed || altered {
				k.hasLow = false
			}
			k.add(cur)
			return out, nil
		}
		// Every version known is from this point on: PostgreSQL is resending
		// the start of what this index already holds. What it was compared
		// with the first time is not loaded, and whatever that comparison
		// found was recorded then. Learn, write nothing.
		k.add(cur)
		return nil, nil
	}

	k := s.byOID[oid]
	if k == nil {
		k = &knownRel{}
		s.byOID[oid] = k
	}
	k.add(cur)

	if holder, hk, hv, ok := s.holderAt(n, oid, high.lsn); ok {
		// The name moved to a new table, so the old one was dropped or
		// renamed at or before this point. Which, and to what, only the live
		// catalog can say, and it may be ahead of the stream: whatever it
		// says, the old table's own next Relation is compared by columns
		// only.
		hk.addRelease(release{n, high.lsn, true})
		hk.hasLow = false
		if !inScope(n) {
			return nil, nil
		}
		facts, err := s.cfg.Lookup(holder)
		if err != nil {
			return nil, fmt.Errorf("pgcapture: catalog lookup for relation OID %d (formerly %s): %w", holder, n, err)
		}
		low, err := s.lowFor(hk, hv, high)
		if err != nil {
			return nil, err
		}
		if facts.Exists {
			out = append(out, renameEvent(holder, n, relName{facts.Schema, facts.Table},
				" (the new name is the one in the current catalog, which can be later than this point: the table was renamed at or before here, and may have been renamed again since)", low, high))
		} else {
			out = append(out, ddlEvent(event.DDLDropTable, holder, n, nil,
				fmt.Sprintf("DROP TABLE %s (its name now belongs to a new table and the old one is gone from the current catalog: dropped here, or renamed here and dropped since)", n),
				low, high))
		}
		c, err := s.createEvent(oid, n, " under a name that belonged to another table", high)
		if err != nil {
			return nil, err
		}
		return append(out, c), nil
	}

	if lc, ok := s.legacy[n]; ok {
		delete(s.legacy, n)
		if !equalShape(lc, cols) && inScope(n) {
			low, err := s.lowFor(nil, version{name: n}, high)
			if err != nil {
				return nil, err
			}
			out = append(out, alterEvent(oid, n, lc, cols,
				" (the earlier shape was stored without the table's identity, so this could also be a drop and a create)", low, high))
		}
		return out, nil
	}

	if s.cfg.StartOIDs[oid] || !inScope(n) {
		return nil, nil // version zero
	}
	c, err := s.createEvent(oid, n, "", high)
	if err != nil {
		return nil, err
	}
	return []event.Event{c}, nil
}

// lowFor is the lower bound of a change away from shape prev of table k
// (nil when only a legacy shape was known): the last change decoded under it
// in this stream, else the newest indexed change to its name before high,
// else the commit prev was described in, else none.
func (s *inferState) lowFor(k *knownRel, prev version, high bound) (*bound, error) {
	if k != nil && k.hasLow && k.low.lsn < high.lsn {
		b := k.low
		return &b, nil
	}
	// Inference was off somewhere between the old shape and now: rows the
	// index holds from then may already be under the new shape, so only the
	// old shape's own position bounds the change.
	if k != nil {
		for _, v := range k.versions {
			if v.blind && v.seen.lsn >= prev.seen.lsn && v.seen.lsn < high.lsn {
				if prev.seen.lsn == 0 {
					return nil, nil
				}
				b := prev.seen
				return &b, nil
			}
		}
	}
	lsn, at, ok, err := s.cfg.LastChange(prev.name.schema, prev.name.table, uint64(high.lsn))
	if err != nil {
		return nil, fmt.Errorf("pgcapture: last indexed change to %s (lower bound of a schema change): %w", prev.name, err)
	}
	if prev.seen.lsn != 0 && prev.seen.lsn < high.lsn && (!ok || pglogrepl.LSN(lsn) < prev.seen.lsn) {
		b := prev.seen
		return &b, nil
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
		text += " partition of " + facts.Parent + " (per the current catalog)"
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
	marker := fmt.Sprintf("%s oid=%d; instant=at-or-before; low=%s; high=%s */ ",
		event.InferredDDLPrefix, oid, lowText, fmtBound(high))
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
	// Fractions are kept: flooring the high end to the second would narrow
	// the bracket, and a guard reading it would miss a change in that second.
	return b.lsn.String() + "@" + b.at.UTC().Format(time.RFC3339Nano)
}
