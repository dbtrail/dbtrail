package pgcapture_test

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

func slogDiscard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// col is one column of a test Relation message or known shape.
type col struct {
	name string
	oid  uint32
	mod  int32
}

func int4(n string) col      { return col{n, 23, -1} }
func text(n string) col      { return col{n, 25, -1} }
func vc(n string, l int) col { return col{n, 1043, int32(l + 4)} }

func typedRel(oid uint32, schema, table string, cols ...col) *pglogrepl.RelationMessage {
	rc := make([]*pglogrepl.RelationMessageColumn, len(cols))
	for i, c := range cols {
		rc[i] = &pglogrepl.RelationMessageColumn{Name: c.name, DataType: c.oid, TypeModifier: c.mod}
	}
	return &pglogrepl.RelationMessage{RelationID: oid, Namespace: schema, RelationName: table,
		ReplicaIdentity: 'f', ColumnNum: uint16(len(rc)), Columns: rc}
}

func known(oid uint32, schema, table string, cols ...col) pgcapture.KnownShape {
	k := pgcapture.KnownShape{OID: oid, Schema: schema, Table: table}
	for _, c := range cols {
		k.Columns = append(k.Columns, pgcapture.ShapeColumn{Name: c.name, TypeOID: c.oid, TypeMod: c.mod})
	}
	return k
}

var (
	lowLSN  = pglogrepl.LSN(0x1000)
	lowTime = time.Date(2026, 6, 21, 11, 0, 0, 0, time.UTC)
)

// inferRig is a decoder with DDL inference and recording stubs.
type inferRig struct {
	d           *pgcapture.Decoder
	facts       map[uint32]pgcapture.RelationFacts
	lastCalls   []string
	lastOK      bool
	lookupErr   error
	lastErr     error
	lookupCalls int
}

func newInferRig(t *testing.T, start []uint32, shapes ...pgcapture.KnownShape) *inferRig {
	t.Helper()
	startSet := map[uint32]bool{}
	for _, o := range start {
		startSet[o] = true
	}
	return newInferRigWith(t, pgcapture.DDLInference{Known: shapes, StartOIDs: startSet})
}

// newInferRigWith is newInferRig with Known, StartOIDs and Releases from cfg.
func newInferRigWith(t *testing.T, cfg pgcapture.DDLInference) *inferRig {
	t.Helper()
	r := &inferRig{facts: map[uint32]pgcapture.RelationFacts{}, lastOK: true}
	r.d = pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, slogDiscard(), pgcapture.WithDDLInference(pgcapture.DDLInference{
		Known:     cfg.Known,
		StartOIDs: cfg.StartOIDs,
		Releases:  cfg.Releases,
		Lookup: func(oid uint32) (pgcapture.RelationFacts, error) {
			r.lookupCalls++
			return r.facts[oid], r.lookupErr
		},
		LastChange: func(schema, table string, before uint64) (uint64, time.Time, bool, error) {
			r.lastCalls = append(r.lastCalls, schema+"."+table+"<"+pglogrepl.LSN(before).String())
			if r.lastErr != nil {
				return 0, time.Time{}, false, r.lastErr
			}
			return uint64(lowLSN), lowTime, r.lastOK, nil
		},
	}))
	return r
}

// relation decodes msg inside a fresh transaction and returns every event the
// Relation message produced, in order (the return value, then TakePending).
func (r *inferRig) relation(t *testing.T, msg *pglogrepl.RelationMessage) []event.Event {
	t.Helper()
	mustDecode(t, r.d, beginMsg())
	return r.decodeAll(t, msg)
}

func (r *inferRig) decodeAll(t *testing.T, msg pglogrepl.Message) []event.Event {
	t.Helper()
	ev, emit, err := r.d.Decode(msg)
	if err != nil {
		t.Fatalf("Decode(%T): %v", msg, err)
	}
	var out []event.Event
	if emit {
		out = append(out, ev)
	}
	return append(out, r.d.TakePending()...)
}

func ddls(evs []event.Event) []event.Event {
	var out []event.Event
	for _, e := range evs {
		if e.EventType == event.EventDDL {
			out = append(out, e)
		}
	}
	return out
}

func kinds(evs []event.Event) string {
	var k []string
	for _, e := range evs {
		switch e.EventType {
		case event.EventDDL:
			k = append(k, string(e.DDLType)+" "+e.Schema+"."+e.Table)
		case event.EventRelation:
			k = append(k, "relation "+e.Schema+"."+e.Table)
		}
	}
	return strings.Join(k, " | ")
}

func mustContain(t *testing.T, s string, parts ...string) {
	t.Helper()
	for _, p := range parts {
		if !strings.Contains(s, p) {
			t.Errorf("%q does not contain %q", s, p)
		}
	}
}

// ─── no change ───────────────────────────────────────────────────────────────

// Without inference configured, a shape change is cached silently, as before.
func TestInfer_offByDefault(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, slogDiscard())
	mustDecode(t, d, beginMsg())
	mustDecode(t, d, typedRel(10, "s", "a", int4("id")))
	ev, _ := mustDecode(t, d, typedRel(10, "s", "a", int4("id"), text("x")))
	if ev.EventType != event.EventRelation || len(d.TakePending()) != 0 {
		t.Fatalf("got %v, want only the relation snapshot", ev.EventType)
	}
}

// A reconnect re-sends every table's Relation: equal to the last known shape,
// it writes nothing.
func TestInfer_equalShapeWritesNothing(t *testing.T) {
	r := newInferRig(t, nil, known(10, "s", "a", int4("id"), vc("v", 10)))
	evs := r.relation(t, typedRel(10, "s", "a", int4("id"), vc("v", 10)))
	if got := kinds(evs); got != "relation s.a" {
		t.Fatalf("events = %s", got)
	}
	if len(r.lastCalls) != 0 || r.lookupCalls != 0 {
		t.Errorf("no change must not touch the catalog or the index: last %v lookups %d", r.lastCalls, r.lookupCalls)
	}
}

// First sight of a table that was in the publication when capture started,
// with nothing known about it: version zero, no row.
func TestInfer_firstSightAtStartIsVersionZero(t *testing.T) {
	r := newInferRig(t, []uint32{10})
	if got := kinds(r.relation(t, typedRel(10, "s", "a", int4("id")))); got != "relation s.a" {
		t.Fatalf("events = %s", got)
	}
}

// ─── ALTER ───────────────────────────────────────────────────────────────────

func TestInfer_alterAddDropRetype(t *testing.T) {
	r := newInferRig(t, nil, known(10, "s", "a", int4("id"), text("legacy"), vc("v", 10)))
	evs := r.relation(t, typedRel(10, "s", "a", int4("id"), vc("v", 20), text("note")))
	if got := kinds(evs); got != "ALTER TABLE s.a | relation s.a" {
		t.Fatalf("events = %s (DDL must come before the snapshot)", got)
	}
	e := evs[0]
	t.Logf("ddl_query: %s", e.DDLQuery)
	mustContain(t, e.DDLQuery,
		"/* dbtrail: inferred from relation shape; oid=10; instant=at-or-before; low=0/1000@2026-06-21T11:00:00Z; high=0/19DF9E8@2026-06-21T12:00:00Z */",
		"ALTER TABLE s.a:", "added note (text)", "dropped legacy (text)", "retyped v: varchar(10) -> varchar(20)")
	// Stream position = the upper bound; the time = the lower bound.
	if e.BinlogFile != txnLSN.String() || e.EndPos != uint64(txnLSN) || e.GTID != txnLSN.String() {
		t.Errorf("position = %s/%d/%s, want the transaction's commit %s", e.BinlogFile, e.EndPos, e.GTID, txnLSN)
	}
	if !e.Timestamp.Equal(lowTime) {
		t.Errorf("detected_at = %v, want the lower bound %v", e.Timestamp, lowTime)
	}
	if want := []string{"s.a<" + txnLSN.String()}; strings.Join(r.lastCalls, ",") != strings.Join(want, ",") {
		t.Errorf("LastChange calls = %v, want %v", r.lastCalls, want)
	}
}

// A column rename is a drop plus an add in the stream; ONE row says so.
func TestInfer_columnRenameIsOneRow(t *testing.T) {
	r := newInferRig(t, nil, known(10, "s", "a", int4("id"), text("old")))
	d := ddls(r.relation(t, typedRel(10, "s", "a", int4("id"), text("new"))))
	if len(d) != 1 || d[0].DDLType != event.DDLAlterTable {
		t.Fatalf("got %s, want one ALTER TABLE", kinds(d))
	}
	mustContain(t, d[0].DDLQuery, "dropped old (text)", "added new (text)", "a renamed column reads as a drop plus an add")
	if strings.Contains(d[0].DDLQuery, "retyped") {
		t.Errorf("a rename is not a retype: %s", d[0].DDLQuery)
	}
}

// Rows decoded in this session give the lower bound; the index is not asked.
func TestInfer_inSessionLowerBound(t *testing.T) {
	r := newInferRig(t, []uint32{10})
	r.relation(t, typedRel(10, "s", "a", int4("id")))
	mustDecode(t, r.d, &pglogrepl.InsertMessage{RelationID: 10, Tuple: tuple(textCol("1"))})
	mustDecode(t, r.d, &pglogrepl.CommitMessage{CommitLSN: txnLSN, CommitTime: txnTime})
	second := pglogrepl.LSN(0x2000000)
	secondTime := txnTime.Add(time.Hour)
	mustDecode(t, r.d, &pglogrepl.BeginMessage{FinalLSN: second, CommitTime: secondTime})
	d := ddls(r.decodeAll(t, typedRel(10, "s", "a", int4("id"), text("x"))))
	if len(d) != 1 {
		t.Fatalf("got %s", kinds(d))
	}
	mustContain(t, d[0].DDLQuery, "low=0/19DF9E8@2026-06-21T12:00:00Z", "high=0/2000000@2026-06-21T13:00:00Z")
	if !d[0].Timestamp.Equal(txnTime) {
		t.Errorf("detected_at = %v, want %v", d[0].Timestamp, txnTime)
	}
	if len(r.lastCalls) != 0 {
		t.Errorf("LastChange called %v, want the in-session bound", r.lastCalls)
	}
}

// No lower bound anywhere: the row says so and sits at the upper bound.
func TestInfer_noLowerBound(t *testing.T) {
	r := newInferRig(t, nil, known(10, "s", "a", int4("id")))
	r.lastOK = false
	d := ddls(r.relation(t, typedRel(10, "s", "a", int4("id"), text("x"))))
	if len(d) != 1 {
		t.Fatalf("got %s", kinds(d))
	}
	mustContain(t, d[0].DDLQuery, "low=none", "high=0/19DF9E8@")
	if !d[0].Timestamp.Equal(txnTime) {
		t.Errorf("detected_at = %v, want the upper bound %v", d[0].Timestamp, txnTime)
	}
}

// ─── RENAME ──────────────────────────────────────────────────────────────────

func TestInfer_renameSameOID(t *testing.T) {
	r := newInferRig(t, nil, known(10, "s", "a", int4("id")))
	evs := r.relation(t, typedRel(10, "s", "b", int4("id")))
	if got := kinds(evs); got != "RENAME TABLE s.b | relation s.b" {
		t.Fatalf("events = %s", got)
	}
	e := evs[0]
	t.Logf("ddl_query: %s", e.DDLQuery)
	mustContain(t, e.DDLQuery, "RENAME TABLE s.a TO s.b")
	// Both names get a row, like a MySQL RENAME, so a reconstruct of either
	// name sees it.
	if len(e.DDLTables) != 2 || e.DDLTables[0] != (event.DDLTable{Schema: "s", Table: "a"}) || e.DDLTables[1] != (event.DDLTable{Schema: "s", Table: "b"}) {
		t.Errorf("DDLTables = %v", e.DDLTables)
	}
	// The lower bound is the last change under the OLD name.
	if len(r.lastCalls) != 1 || !strings.HasPrefix(r.lastCalls[0], "s.a<") {
		t.Errorf("LastChange calls = %v", r.lastCalls)
	}
}

func TestInfer_setSchemaReadsAsRename(t *testing.T) {
	r := newInferRig(t, nil, known(10, "s", "a", int4("id")))
	d := ddls(r.relation(t, typedRel(10, "t", "a", int4("id"))))
	if len(d) != 1 || d[0].DDLType != event.DDLRenameTable {
		t.Fatalf("got %s", kinds(d))
	}
	mustContain(t, d[0].DDLQuery, "RENAME TABLE s.a TO t.a", "SET SCHEMA")
}

func TestInfer_renameAndAlter(t *testing.T) {
	r := newInferRig(t, nil, known(10, "s", "a", int4("id")))
	evs := r.relation(t, typedRel(10, "s", "b", int4("id"), text("x")))
	if got := kinds(evs); got != "RENAME TABLE s.b | ALTER TABLE s.b | relation s.b" {
		t.Fatalf("events = %s", got)
	}
}

// ─── DROP + CREATE, and rename that is not a drop ────────────────────────────

func TestInfer_dropThenCreateSameName(t *testing.T) {
	r := newInferRig(t, nil, known(10, "s", "a", int4("id")))
	r.facts[10] = pgcapture.RelationFacts{Exists: false}
	evs := r.relation(t, typedRel(11, "s", "a", int4("id"), text("x")))
	if got := kinds(evs); got != "DROP TABLE s.a | CREATE TABLE s.a | relation s.a" {
		t.Fatalf("events = %s", got)
	}
	for _, e := range evs[:2] {
		t.Logf("ddl_query: %s", e.DDLQuery)
		if e.EndPos != uint64(txnLSN) {
			t.Errorf("%s at %d, want %d", e.DDLType, e.EndPos, uint64(txnLSN))
		}
	}
	mustContain(t, evs[0].DDLQuery, "oid=10", "DROP TABLE s.a")
	mustContain(t, evs[1].DDLQuery, "oid=11", "CREATE TABLE s.a")
	// The drop sits at the lower bound so reconstruct refuses inside the bracket.
	if !evs[0].Timestamp.Equal(lowTime) {
		t.Errorf("DROP detected_at = %v, want %v", evs[0].Timestamp, lowTime)
	}
}

// RENAME A→B with no write to B, then CREATE A: the old OID still exists,
// so it is a rename plus a create, never a drop.
func TestInfer_renamedAwayIsNotADrop(t *testing.T) {
	r := newInferRig(t, nil, known(10, "s", "a", int4("id")))
	r.facts[10] = pgcapture.RelationFacts{Exists: true, Schema: "s", Table: "b"}
	evs := r.relation(t, typedRel(11, "s", "a", int4("id")))
	if got := kinds(evs); got != "RENAME TABLE s.b | CREATE TABLE s.a | relation s.a" {
		t.Fatalf("events = %s", got)
	}
	mustContain(t, evs[0].DDLQuery, "oid=10", "RENAME TABLE s.a TO s.b")
	// Then a write to B under the same shape writes nothing more.
	if d := ddls(r.decodeAll(t, typedRel(10, "s", "b", int4("id")))); len(d) != 0 {
		t.Errorf("B's first Relation after the inferred rename wrote %s", kinds(d))
	}
}

// ─── CREATE ──────────────────────────────────────────────────────────────────

func TestInfer_createAfterStart(t *testing.T) {
	r := newInferRig(t, []uint32{10})
	evs := r.relation(t, typedRel(12, "s", "new", int4("id")))
	if got := kinds(evs); got != "CREATE TABLE s.new | relation s.new" {
		t.Fatalf("events = %s", got)
	}
	t.Logf("ddl_query: %s", evs[0].DDLQuery)
	mustContain(t, evs[0].DDLQuery, "oid=12", "low=none", "CREATE TABLE s.new", "or added to the publication")
	if len(r.lastCalls) != 0 {
		t.Errorf("a new table has no earlier shape to bound: %v", r.lastCalls)
	}
}

func TestInfer_newPartitionIsLabelled(t *testing.T) {
	r := newInferRig(t, nil)
	r.facts[13] = pgcapture.RelationFacts{Exists: true, IsPartition: true, Parent: "s.events"}
	d := ddls(r.relation(t, typedRel(13, "s", "events_2026_10", int4("id"))))
	if len(d) != 1 || d[0].DDLType != event.DDLCreateTable {
		t.Fatalf("got %s", kinds(d))
	}
	mustContain(t, d[0].DDLQuery, "partition of s.events")
}

// ─── shapes written before the OID was stored ────────────────────────────────

func TestInfer_legacyShapeByName(t *testing.T) {
	r := newInferRig(t, nil, known(0, "s", "a", int4("id")))
	if d := ddls(r.relation(t, typedRel(10, "s", "a", int4("id")))); len(d) != 0 {
		t.Fatalf("equal legacy shape wrote %s", kinds(d))
	}
	r = newInferRig(t, nil, known(0, "s", "a", int4("id")))
	d := ddls(r.relation(t, typedRel(10, "s", "a", int4("id"), text("x"))))
	if len(d) != 1 || d[0].DDLType != event.DDLAlterTable {
		t.Fatalf("got %s", kinds(d))
	}
	mustContain(t, d[0].DDLQuery, "added x (text)", "could also be a drop and a create")
}

// PostgreSQL names are case-sensitive: "A" is not a.
func TestInfer_caseDistinctNames(t *testing.T) {
	r := newInferRig(t, []uint32{11}, known(10, "s", "a", int4("id")))
	if d := ddls(r.relation(t, typedRel(11, "s", "A", int4("id"), text("x")))); len(d) != 0 {
		t.Fatalf("\"A\" was compared with a: %s", kinds(d))
	}
}

// ─── scope, transactions, failures ───────────────────────────────────────────

func TestInfer_filteredOutWritesNothingButTracks(t *testing.T) {
	start := map[uint32]bool{}
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{Schemas: map[string]bool{"other": true}}, slogDiscard(),
		pgcapture.WithDDLInference(pgcapture.DDLInference{
			Known: []pgcapture.KnownShape{known(10, "s", "a", int4("id"))}, StartOIDs: start,
			Lookup:     func(uint32) (pgcapture.RelationFacts, error) { return pgcapture.RelationFacts{}, nil },
			LastChange: func(string, string, uint64) (uint64, time.Time, bool, error) { return 0, time.Time{}, false, nil },
		}))
	mustDecode(t, d, beginMsg())
	if _, emit := mustDecode(t, d, typedRel(10, "s", "a", int4("id"), text("x"))); emit || len(d.TakePending()) != 0 {
		t.Fatal("an out-of-scope table produced events")
	}
}

func TestInfer_relationOutsideTransactionWritesNoRow(t *testing.T) {
	r := newInferRig(t, nil, known(10, "s", "a", int4("id")))
	evs := r.decodeAll(t, typedRel(10, "s", "a", int4("id"), text("x")))
	if len(ddls(evs)) != 0 {
		t.Fatalf("a Relation with no open transaction cannot be placed, got %s", kinds(evs))
	}
}

func TestInfer_lookupAndIndexFailuresFailLoud(t *testing.T) {
	r := newInferRig(t, nil, known(10, "s", "a", int4("id")))
	r.lookupErr = errors.New("catalog down")
	mustDecode(t, r.d, beginMsg())
	if _, _, err := r.d.Decode(typedRel(11, "s", "a", int4("id"))); err == nil || !strings.Contains(err.Error(), "catalog down") {
		t.Fatalf("lookup failure: err = %v", err)
	}
	r = newInferRig(t, nil, known(10, "s", "a", int4("id")))
	r.lastErr = errors.New("index down")
	mustDecode(t, r.d, beginMsg())
	if _, _, err := r.d.Decode(typedRel(10, "s", "a", int4("id"), text("x"))); err == nil || !strings.Contains(err.Error(), "index down") {
		t.Fatalf("index failure: err = %v", err)
	}
}

func TestWithDDLInference_requiresResolvers(t *testing.T) {
	lookup := func(uint32) (pgcapture.RelationFacts, error) { return pgcapture.RelationFacts{}, nil }
	last := func(string, string, uint64) (uint64, time.Time, bool, error) { return 0, time.Time{}, false, nil }
	for name, cfg := range map[string]pgcapture.DDLInference{
		"no Lookup":     {LastChange: last},
		"no LastChange": {Lookup: lookup},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: no panic", name)
				}
			}()
			pgcapture.WithDDLInference(cfg)
		}()
	}
}

// Every inferred row's marker reads back with the bounds it was written with.
func TestInfer_markerParsesBack(t *testing.T) {
	r := newInferRig(t, nil, known(10, "s", "a", int4("id")))
	r.facts[10] = pgcapture.RelationFacts{Exists: false}
	evs := ddls(r.relation(t, typedRel(11, "s", "a", int4("id"), text("x"))))
	for _, e := range evs {
		b, ok := event.ParseInferredBracket(e.DDLQuery)
		if !ok || !b.High.Equal(txnTime) {
			t.Errorf("%s: bracket %+v ok %v from %q", e.DDLType, b, ok, e.DDLQuery)
		}
		if e.DDLType == event.DDLDropTable && (!b.HasLow || !b.Low.Equal(lowTime)) {
			t.Errorf("DROP low = %+v", b)
		}
	}
}

// The live catalog can be ahead of the stream. a -> b, CREATE a, and later
// b -> c: when the new a arrives the catalog already says c. The row says the
// name is the current one; the old table's own Relations, which still say b,
// must not produce a reversed rename.
func TestInfer_liveCatalogAheadOfTheStream(t *testing.T) {
	r := newInferRig(t, nil, known(10, "s", "a", int4("id")))
	r.facts[10] = pgcapture.RelationFacts{Exists: true, Schema: "s", Table: "c"}
	evs := ddls(r.relation(t, typedRel(11, "s", "a", int4("id"))))
	if got := kinds(evs); got != "RENAME TABLE s.c | CREATE TABLE s.a" {
		t.Fatalf("events = %s", got)
	}
	mustContain(t, evs[0].DDLQuery, "current catalog")
	// The stream now catches up with the old table under b, then c.
	if d := ddls(r.decodeAll(t, typedRel(10, "s", "b", int4("id")))); len(d) != 0 {
		t.Fatalf("the old table's first Relation after a catalog-named rename wrote %s", kinds(d))
	}
	d := ddls(r.decodeAll(t, typedRel(10, "s", "c", int4("id"))))
	if got := kinds(d); got != "RENAME TABLE s.c" {
		t.Fatalf("b -> c: got %s", got)
	}
	mustContain(t, d[0].DDLQuery, "RENAME TABLE s.b TO s.c")
}

// a -> b, CREATE a, later DROP b: the catalog says the old OID is gone. When
// the stream catches up with the old table under b, that is not a new table.
func TestInfer_catalogGoneButStreamStillShowsIt(t *testing.T) {
	r := newInferRig(t, nil, known(10, "s", "a", int4("id")))
	r.facts[10] = pgcapture.RelationFacts{Exists: false}
	r.relation(t, typedRel(11, "s", "a", int4("id")))
	if d := ddls(r.decodeAll(t, typedRel(10, "s", "b", int4("id")))); len(d) != 0 {
		t.Fatalf("the old table under its stream name wrote %s", kinds(d))
	}
}

// A change that touches no captured name asks neither the catalog nor the
// index, so their failure cannot stop capture of the captured tables.
func TestInfer_outOfScopeAsksNothing(t *testing.T) {
	calls := 0
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{Schemas: map[string]bool{"s": true}}, slogDiscard(),
		pgcapture.WithDDLInference(pgcapture.DDLInference{
			Known: []pgcapture.KnownShape{known(10, "other", "a", int4("id")), known(20, "other", "b", int4("id"))},
			Lookup: func(uint32) (pgcapture.RelationFacts, error) {
				calls++
				return pgcapture.RelationFacts{}, nil
			},
			LastChange: func(string, string, uint64) (uint64, time.Time, bool, error) {
				calls++
				return 0, time.Time{}, false, nil
			},
		}))
	mustDecode(t, d, beginMsg())
	for _, m := range []*pglogrepl.RelationMessage{
		typedRel(10, "other", "a", int4("id"), text("x")),  // ALTER
		typedRel(10, "other", "a2", int4("id"), text("x")), // RENAME
		typedRel(21, "other", "b", int4("id")),             // name reused
		typedRel(30, "other", "new", int4("id")),           // CREATE
	} {
		if _, emit := mustDecode(t, d, m); emit || len(d.TakePending()) != 0 {
			t.Fatalf("out-of-scope %s.%s produced events", m.Namespace, m.RelationName)
		}
	}
	if calls != 0 {
		t.Errorf("%d resolver calls for out-of-scope changes", calls)
	}
	// Renamed INTO scope: still known by OID, so a rename, not a create.
	ev, _ := mustDecode(t, d, typedRel(10, "s", "a", int4("id"), text("x")))
	if ev.EventType != event.EventDDL || ev.DDLType != event.DDLRenameTable {
		t.Fatalf("rename into scope = %v %s", ev.EventType, ev.DDLType)
	}
}

// With no change decoded and none indexed, the commit the old shape was last
// described in is the lower bound: the shape was in effect then.
func TestInfer_seenIsTheFallbackLowerBound(t *testing.T) {
	seen := time.Date(2026, 6, 21, 9, 0, 0, 0, time.UTC)
	k := known(10, "s", "a", int4("id"))
	k.SeenLSN, k.SeenAt = 0x500, seen
	r := newInferRig(t, nil, k)
	r.lastOK = false
	d := ddls(r.relation(t, typedRel(10, "s", "a", int4("id"), text("x"))))
	if len(d) != 1 {
		t.Fatalf("got %s", kinds(d))
	}
	mustContain(t, d[0].DDLQuery, "low=0/500@2026-06-21T09:00:00Z")
	if !d[0].Timestamp.Equal(seen) {
		t.Errorf("detected_at = %v, want %v", d[0].Timestamp, seen)
	}
}

// The snapshot the decoder emits records where the shape was described, so a
// restart can tell shapes it will see again from shapes already behind it.
func TestInfer_relationSnapshotRecordsWhereItWasSeen(t *testing.T) {
	r := newInferRig(t, []uint32{10})
	evs := r.relation(t, typedRel(10, "s", "a", int4("id")))
	rel := evs[len(evs)-1]
	if rel.EventType != event.EventRelation || rel.Relation.SeenLSN != uint64(txnLSN) || !rel.Relation.SeenAt.Equal(txnTime) || rel.Relation.OID != 10 {
		t.Fatalf("relation event = %+v", rel.Relation)
	}
}

// A very wide ALTER fits schema_changes.ddl_query; the marker stays whole.
func TestInfer_wideAlterIsCapped(t *testing.T) {
	before, after := []col{int4("id")}, []col{int4("id")}
	for i := range 3000 {
		before = append(before, text(fmt.Sprintf("c%04d_ñ", i)))
		after = append(after, int4(fmt.Sprintf("c%04d_ñ", i)))
	}
	r := newInferRig(t, nil, known(10, "s", "a", before...))
	d := ddls(r.relation(t, typedRel(10, "s", "a", after...)))
	if len(d) != 1 {
		t.Fatalf("got %s", kinds(d))
	}
	q := d[0].DDLQuery
	if len(q) > 20<<10 || !utf8.ValidString(q) || !strings.HasSuffix(q, "(truncated)") || !strings.HasPrefix(q, "/* dbtrail: inferred") || !strings.Contains(q, " */ ALTER TABLE s.a: retyped") {
		t.Fatalf("len %d, valid %v, head %.120q, tail %q", len(q), utf8.ValidString(q), q, q[len(q)-40:])
	}
	if _, ok := event.ParseInferredBracket(q); !ok {
		t.Error("a capped row's marker no longer parses")
	}
}

func seenKnown(oid uint32, lsn uint64, cols ...col) pgcapture.KnownShape {
	k := known(oid, "s", "a", cols...)
	k.SeenLSN, k.SeenAt = lsn, txnTime.Add(-time.Hour)
	return k
}

func relAt(t *testing.T, r *inferRig, lsn uint64, msgs ...*pglogrepl.RelationMessage) []event.Event {
	t.Helper()
	mustDecode(t, r.d, &pglogrepl.BeginMessage{FinalLSN: pglogrepl.LSN(lsn), CommitTime: txnTime})
	var out []event.Event
	for _, m := range msgs {
		out = append(out, ddls(r.decodeAll(t, m))...)
	}
	mustDecode(t, r.d, &pglogrepl.CommitMessage{CommitLSN: pglogrepl.LSN(lsn), CommitTime: txnTime})
	return out
}

// PostgreSQL resends from the slot's confirmed position, which can be before
// shapes the index already holds. Each Relation is compared with the shape in
// effect just before it, so the resend finds what the first pass found, and
// never the reverse.
func TestInfer_resendComparesByPosition(t *testing.T) {
	s0 := []col{int4("id")}
	s1 := []col{int4("id"), text("c")}
	// The index holds S0 (seen at 100) and S1 (seen at 200); the resend
	// starts at 150, so the shape in effect there is S0.
	r := newInferRig(t, nil, seenKnown(10, 100, s0...), seenKnown(10, 200, s1...))
	if d := relAt(t, r, 180, typedRel(10, "s", "a", s0...)); len(d) != 0 {
		t.Fatalf("resent old shape wrote %s (a reversed change)", kinds(d))
	}
	d := relAt(t, r, 200, typedRel(10, "s", "a", s1...))
	if len(d) != 1 || d[0].DDLType != event.DDLAlterTable {
		t.Fatalf("resent ALTER: got %s", kinds(d))
	}
	mustContain(t, d[0].DDLQuery, "added c (text)")
	if d := relAt(t, r, 300, typedRel(10, "s", "a", s1...)); len(d) != 0 {
		t.Fatalf("after the resend: %s", kinds(d))
	}
}

// The resend starts at the very first shape the index holds for a table:
// there is nothing loaded before it, and it writes nothing.
func TestInfer_resendOfTheFirstKnownShapeWritesNothing(t *testing.T) {
	r := newInferRig(t, nil, seenKnown(10, 100, int4("id")))
	if d := relAt(t, r, 100, typedRel(10, "s", "a", int4("id"))); len(d) != 0 {
		t.Fatalf("got %s", kinds(d))
	}
}

// A shape that changes twice in one transaction: the second is compared with
// the first, not with the shape before the transaction.
func TestInfer_twoShapesInOneTransaction(t *testing.T) {
	r := newInferRig(t, nil, seenKnown(10, 100, int4("id")))
	d := relAt(t, r, 300, typedRel(10, "s", "a", int4("id"), text("x")), typedRel(10, "s", "a", int4("id"), text("x"), text("y")))
	if len(d) != 2 {
		t.Fatalf("got %s", kinds(d))
	}
	mustContain(t, d[0].DDLQuery, "added x (text)")
	mustContain(t, d[1].DDLQuery, "added y (text)")
	if strings.Contains(d[1].DDLQuery, "added x") {
		t.Errorf("second change compared with the shape before the transaction: %s", d[1].DDLQuery)
	}
}

// A table whose name was read from the live catalog still has its columns
// compared: only the rename is left out.
func TestInfer_catalogNamedTableStillComparesColumns(t *testing.T) {
	r := newInferRig(t, nil, known(10, "s", "a", int4("id"), text("x")))
	r.facts[10] = pgcapture.RelationFacts{Exists: true, Schema: "s", Table: "a_old"}
	r.relation(t, typedRel(11, "s", "a", int4("id")))
	d := ddls(r.decodeAll(t, typedRel(10, "s", "a_old", int4("id"))))
	if got := kinds(d); got != "ALTER TABLE s.a_old" {
		t.Fatalf("got %s", got)
	}
	mustContain(t, d[0].DDLQuery, "dropped x (text)")
}

// The bounds keep their fraction of a second: flooring the high end would
// narrow the bracket a guard reads.
func TestInfer_boundsKeepFractions(t *testing.T) {
	r := newInferRig(t, nil, known(10, "s", "a", int4("id")))
	r.lastOK = false
	mustDecode(t, r.d, &pglogrepl.BeginMessage{FinalLSN: txnLSN, CommitTime: txnTime.Add(500 * time.Millisecond)})
	d := ddls(r.decodeAll(t, typedRel(10, "s", "a", int4("id"), text("x"))))
	if len(d) != 1 {
		t.Fatalf("got %s", kinds(d))
	}
	mustContain(t, d[0].DDLQuery, "high=0/19DF9E8@2026-06-21T12:00:00.5Z")
	if b, ok := event.ParseInferredBracket(d[0].DDLQuery); !ok || !b.High.Equal(txnTime.Add(500*time.Millisecond)) {
		t.Errorf("bracket %+v ok %v", b, ok)
	}
}

// A name that changes hands twice: n is dropped and recreated (OID 1 -> 2),
// OID 2 is renamed to m, then a new OID 3 takes n. OID 1 gave n up at the
// first hand-over, so it is not the holder the second time: no second DROP.
func TestInfer_nameReusedTwiceAfterARename(t *testing.T) {
	r := newInferRig(t, nil, known(1, "s", "n", int4("id")))
	r.facts[1] = pgcapture.RelationFacts{Exists: false}
	if got := kinds(relAt(t, r, 20, typedRel(2, "s", "n", int4("id")))); got != "DROP TABLE s.n | CREATE TABLE s.n" {
		t.Fatalf("first hand-over: %s", got)
	}
	if got := kinds(relAt(t, r, 40, typedRel(2, "s", "m", int4("id")))); got != "RENAME TABLE s.m" {
		t.Fatalf("rename: %s", got)
	}
	if got := kinds(relAt(t, r, 50, typedRel(3, "s", "n", int4("id")))); got != "CREATE TABLE s.n" {
		t.Fatalf("second take of n: %s (a gone table must not hold its old name)", got)
	}
}

// After a restart the release comes back from the recorded DROP/RENAME row:
// the old table neither holds its old name nor re-records the rename when
// its own description, under the new name, finally arrives.
func TestInfer_releaseSurvivesARestart(t *testing.T) {
	k1 := seenKnown(1, 10, int4("id"))
	k1.Table = "n"
	k2 := seenKnown(2, 20, int4("id"))
	k2.Table = "n"
	r := newInferRigWith(t, pgcapture.DDLInference{
		Known:    []pgcapture.KnownShape{k1, k2},
		Releases: []pgcapture.KnownRelease{{OID: 1, Schema: "s", Table: "n", LSN: 20}},
	})
	// OID 1, renamed to b before OID 2 took n, is written to under b.
	if d := relAt(t, r, 30, typedRel(1, "s", "b", int4("id"))); len(d) != 0 {
		t.Fatalf("the rename recorded at the hand-over was recorded again: %s", kinds(d))
	}
	// A new OID taking n later finds OID 2 as the holder, not OID 1.
	r.facts[2] = pgcapture.RelationFacts{Exists: false}
	d := relAt(t, r, 50, typedRel(3, "s", "n", int4("id")))
	if got := kinds(d); got != "DROP TABLE s.n | CREATE TABLE s.n" {
		t.Fatalf("got %s", got)
	}
	mustContain(t, d[0].DDLQuery, "oid=2;")
}

// While inference is off, shapes are stored without a position, so the next
// start compares against the last shape inference saw.
func TestInfer_shapesSeenWithoutInferenceHaveNoPosition(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, slogDiscard())
	mustDecode(t, d, beginMsg())
	ev, _ := mustDecode(t, d, typedRel(10, "s", "a", int4("id")))
	if ev.Relation.SeenLSN != uint64(txnLSN) || !ev.Relation.SeenAt.IsZero() || ev.Relation.OID != 10 {
		t.Fatalf("relation = %+v, want the OID, the position and no time", ev.Relation)
	}
}

// A run with inference off left shapes with a position and no time. The next
// run compares against the last shape inference saw, and does not take the
// index's rows from that stretch as the lower bound: they may already be
// under the new shape.
func TestInfer_afterARunWithInferenceOff(t *testing.T) {
	old := seenKnown(10, 100, int4("id"))
	blind := seenKnown(10, 200, int4("id"), text("x"))
	blind.SeenAt = time.Time{}
	r := newInferRig(t, nil, old, blind)
	d := relAt(t, r, 300, typedRel(10, "s", "a", int4("id"), text("x")))
	if len(d) != 1 || d[0].DDLType != event.DDLAlterTable {
		t.Fatalf("got %s, want the ALTER made while inference was off", kinds(d))
	}
	mustContain(t, d[0].DDLQuery, "added x (text)", "low=0/64@")
	if len(r.lastCalls) != 0 {
		t.Errorf("the index was asked for a lower bound over a stretch it cannot bound: %v", r.lastCalls)
	}
}
