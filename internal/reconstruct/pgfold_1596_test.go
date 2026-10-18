package reconstruct

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

var pgRel = baseline.PGRelation{OID: 16411, Columns: []baseline.PGColumn{
	{Name: "id", TypeOID: 23, TypeMod: -1, Type: "integer"},
	{Name: "note", TypeOID: 1043, TypeMod: 14, Type: "character varying(10)"},
}}

func pgShape(id int, oid uint32, table string, seen uint64, blind bool, cols ...metadata.PGRelationColumn) metadata.PGShape {
	at := time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC).Add(time.Duration(seen) * time.Second)
	if blind {
		at = time.Time{}
	}
	return metadata.PGShape{SnapshotID: id, Relation: metadata.PGRelationSchema{OID: oid, Schema: "public", Table: table,
		Columns: cols, SeenLSN: seen, SeenAt: at}}
}

func col(name string, oid uint32, mod int32) metadata.PGRelationColumn {
	return metadata.PGRelationColumn{Name: name, TypeOID: oid, TypeMod: mod}
}

var same = []metadata.PGRelationColumn{col("id", 23, -1), col("note", 1043, 14)}

// The shape the baseline recorded, against every shape the stream saw from
// the baseline's floor up to the cut (#1596): any change refuses.
func TestCheckPGShape(t *testing.T) {
	const floor, cut = 100, 500
	cases := []struct {
		name   string
		shapes []metadata.PGShape
		refuse string // "" = passes
	}{
		{"no shapes seen", nil, ""},
		{"same shape before and after the floor", []metadata.PGShape{pgShape(1, 16411, "orders", 50, false, same...), pgShape(2, 16411, "orders", 300, false, same...)}, ""},
		// Only the shape in effect at the floor counts among the older ones:
		// an older, different shape was before the snapshot.
		{"an older shape before the one in effect at the floor", []metadata.PGShape{pgShape(1, 16411, "orders", 20, false, col("id", 23, -1)), pgShape(2, 16411, "orders", 50, false, same...)}, ""},
		{"the shape in effect at the floor differs", []metadata.PGShape{pgShape(1, 16411, "orders", 50, false, col("id", 23, -1))}, "dropped note"},
		{"a change after the cut does not count", []metadata.PGShape{pgShape(1, 16411, "orders", 600, false, col("id", 23, -1))}, ""},
		{"a change at the cut does not count", []metadata.PGShape{pgShape(1, 16411, "orders", 500, false, col("id", 23, -1))}, ""},
		{"just before the cut counts", []metadata.PGShape{pgShape(1, 16411, "orders", 499, false, col("id", 23, -1))}, "dropped note"},
		// Shapes stored before OIDs were recorded come first; the table's
		// first shape with its OID replaces them, and the other way round.
		{"a legacy shape before the first one with the OID", []metadata.PGShape{pgShape(1, 0, "orders", 20, false, col("id", 23, -1)), pgShape(2, 16411, "orders", 50, false, same...)}, ""},
		{"a shape with the OID before a legacy one", []metadata.PGShape{pgShape(1, 16411, "orders", 20, false, col("id", 23, -1)), pgShape(2, 0, "orders", 50, false, same...)}, ""},
		{"column dropped", []metadata.PGShape{pgShape(1, 16411, "orders", 300, false, col("id", 23, -1))}, "dropped note"},
		{"column added", []metadata.PGShape{pgShape(1, 16411, "orders", 300, false, append(same[:2:2], col("x", 25, -1))...)}, "added x"},
		{"type modifier changed", []metadata.PGShape{pgShape(1, 16411, "orders", 300, false, col("id", 23, -1), col("note", 1043, 24))}, "retyped note"},
		{"type changed", []metadata.PGShape{pgShape(1, 16411, "orders", 300, false, col("id", 20, -1), col("note", 1043, 14))}, "retyped id"},
		// A shape seen with detection off is still an observation: compared.
		{"seen while inference was off, same columns", []metadata.PGShape{pgShape(1, 16411, "orders", 300, true, same...)}, ""},
		{"seen while inference was off, other columns", []metadata.PGShape{pgShape(1, 16411, "orders", 300, true, col("id", 23, -1))}, "dropped note"},
		// A table dropped and created again long before the snapshot is
		// history: the old table gave the name up before the floor.
		{"an older table under the name, before the floor", []metadata.PGShape{pgShape(1, 100, "orders", 40, false, same...), pgShape(2, 16411, "orders", 60, false, same...)}, ""},
		// ...but still holding it at the floor: dropped and created after it.
		{"another table holds the name at the floor", []metadata.PGShape{pgShape(1, 100, "orders", 60, false, same...), pgShape(2, 16411, "orders", 40, false, same...)}, "different table"},
		// Stored before table OIDs were recorded: compared by name and columns.
		{"a shape with no OID, same columns", []metadata.PGShape{pgShape(1, 0, "orders", 0, false, same...)}, ""},
		{"a shape with no OID, other columns", []metadata.PGShape{pgShape(1, 0, "orders", 0, false, col("id", 23, -1))}, "dropped note"},
		{"another table under the same name", []metadata.PGShape{pgShape(1, 99999, "orders", 300, false, same...)}, "different table"},
		{"another table under another name is unrelated", []metadata.PGShape{pgShape(1, 99999, "other", 300, false, col("z", 25, -1))}, ""},
		// PostgreSQL 18 can stream STORED generated columns the baseline left out.
		{"a streamed generated column", []metadata.PGShape{pgShape(1, 16411, "orders", 300, false,
			append(same[:2:2], metadata.PGRelationColumn{Name: "g", TypeOID: 23, TypeMod: -1, IsGenerated: true})...)}, ""},
		{"renamed table, same OID, same columns", []metadata.PGShape{pgShape(1, 16411, "orders_v2", 300, false, same...)}, "renamed"},
	}
	for _, c := range cases {
		err := checkPGShape("public", "orders", pgRel, floor, cut, c.shapes)
		switch {
		case c.refuse == "" && err != nil:
			t.Errorf("%s: refused: %v", c.name, err)
		case c.refuse != "" && err == nil:
			t.Errorf("%s: passed, want a refusal mentioning %q", c.name, c.refuse)
		case c.refuse != "" && !strings.Contains(err.Error(), c.refuse):
			t.Errorf("%s: %v, want it to mention %q", c.name, err, c.refuse)
		}
		if err != nil && !strings.Contains(err.Error(), "bintrail-pg baseline") {
			t.Errorf("%s: refusal does not say how to get past it: %v", c.name, err)
		}
	}
}

// The columns of a PostgreSQL fold are the baseline file's own, all text.
func TestRawTextColumns(t *testing.T) {
	cols := rawTextColumns([]string{"id", "note"})
	if len(cols) != 2 || cols[0].Name != "id" || !cols[0].RawText || !cols[1].RawText || cols[0].MySQLType != "" {
		t.Fatalf("cols = %+v", cols)
	}
}

// The footer of a PostgreSQL fold (#1596): its LSN anchor and the shape, and
// none of the MySQL anchor keys, which would make the next run compare an LSN
// as a binlog file name.
func TestPGSnapshotFileMetadata(t *testing.T) {
	relText, err := pgRel.Encode()
	if err != nil {
		t.Fatal(err)
	}
	in := mergeInput{Schema: "public", Table: "orders", PG: true, PGCutLSN: 0x1000,
		SnapshotAt: time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC),
		SourceBaseline: baselineMeta{Path: "/b/2026-10-17T00-00-00Z/public/orders.parquet", Time: time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC),
			Metadata: baseline.DumpMetadata{LSN: 0x500, PGRelation: relText, RenderGUCs: baseline.RenderGUCsPinned, LockMode: baseline.LockStampPGRepeatableRead}}}
	md := snapshotFileMetadata(in)
	// The exact key set: a key added to the MySQL builder must not reach a
	// PostgreSQL footer.
	allowed := map[string]bool{baseline.MetaKeySnapshotTimestamp: true, "bintrail.source_database": true, "bintrail.source_table": true,
		"bintrail.bintrail_version": true, MetaKeySnapshotProducer: true, MetaKeyDerivedFrom: true, MetaKeyDerivedFromPath: true,
		baseline.MetaKeyCaptureGap: true, baseline.MetaKeyLockMode: true, baseline.MetaKeyLastDumpAt: true, baseline.MetaKeyFoldGeneration: true,
		baseline.MetaKeyLSN: true, baseline.MetaKeyPGRelation: true, baseline.MetaKeyRenderGUCs: true, baseline.MetaKeyPGDeltaSince: true}
	for k := range md {
		if !allowed[k] {
			t.Errorf("footer key %s on a PostgreSQL fold", k)
		}
	}
	for k, want := range map[string]string{
		baseline.MetaKeyLSN:              "4096",
		baseline.MetaKeyPGRelation:       relText,
		baseline.MetaKeyRenderGUCs:       baseline.RenderGUCsPinned,
		baseline.MetaKeyLockMode:         baseline.LockStampPGRepeatableRead,
		baseline.MetaKeySnapshotProducer: SnapshotProducerReconstruct,
	} {
		if md[k] != want {
			t.Errorf("%s = %q, want %q", k, md[k], want)
		}
	}
	for _, k := range []string{baseline.MetaKeyBinlogFile, baseline.MetaKeyBinlogPos, baseline.MetaKeyGTIDSet,
		baseline.MetaKeyEventMark, baseline.MetaKeyDDLMark, baseline.MetaKeyCreateTableSQL, baseline.MetaKeyCreateTableAsOf} {
		if v, ok := md[k]; ok {
			t.Errorf("%s = %q present on a PostgreSQL fold", k, v)
		}
	}
	// No cut (nothing to fold): the source's own floor carries over.
	in.PGCutLSN = 0
	if md := snapshotFileMetadata(in); md[baseline.MetaKeyLSN] != "1280" {
		t.Errorf("no cut: LSN = %q, want the source's 1280", md[baseline.MetaKeyLSN])
	}
	// The bound on how far back the changes after it reach: the fold's
	// own, else the source's word for word, else none.
	in.PGDeltaSince = time.Date(2026, 10, 17, 10, 59, 0, 0, time.UTC)
	if md := snapshotFileMetadata(in); md[baseline.MetaKeyPGDeltaSince] != "2026-10-17T10:59:00Z" {
		t.Errorf("delta since = %q", md[baseline.MetaKeyPGDeltaSince])
	}
	in.PGDeltaSince = time.Time{}
	if v, ok := snapshotFileMetadata(in)[baseline.MetaKeyPGDeltaSince]; ok {
		t.Errorf("no bound anywhere: delta since = %q present", v)
	}
	in.SourceBaseline.Metadata.PGDeltaSince = "2026-10-16T23:59:00Z"
	if md := snapshotFileMetadata(in); md[baseline.MetaKeyPGDeltaSince] != "2026-10-16T23:59:00Z" {
		t.Errorf("source bound: delta since = %q", md[baseline.MetaKeyPGDeltaSince])
	}
}

func TestCheckPGFoldable(t *testing.T) {
	rel, _ := pgRel.Encode()
	ok := baseline.DumpMetadata{LSN: 5, PGRelation: rel, RenderGUCs: baseline.RenderGUCsPinned}
	if got, err := checkPGFoldable("public", "orders", ok); err != nil || got.OID != pgRel.OID {
		t.Fatalf("a good baseline: %+v, %v", got, err)
	}
	for name, md := range map[string]baseline.DumpMetadata{
		"no LSN":         {PGRelation: rel, RenderGUCs: baseline.RenderGUCsPinned},
		"no shape":       {LSN: 5, RenderGUCs: baseline.RenderGUCsPinned},
		"bad shape":      {LSN: 5, PGRelation: "{", RenderGUCs: baseline.RenderGUCsPinned},
		"other settings": {LSN: 5, PGRelation: rel, RenderGUCs: "TimeZone=Europe/Paris"},
		"no settings":    {LSN: 5, PGRelation: rel},
	} {
		if _, err := checkPGFoldable("public", "orders", md); err == nil || !strings.Contains(err.Error(), "bintrail-pg baseline") {
			t.Errorf("%s: err = %v, want a refusal saying how to get past it", name, err)
		}
	}
}

func TestCheckPGFileColumns(t *testing.T) {
	if err := checkPGFileColumns(pgRel, []string{"note", "id"}, "public", "orders"); err != nil {
		t.Fatalf("same columns, file order: %v", err)
	}
	if err := checkPGFileColumns(pgRel, []string{"id"}, "public", "orders"); err == nil || !strings.Contains(err.Error(), "only in the shape: note") {
		t.Errorf("missing column: %v", err)
	}
	if err := checkPGFileColumns(pgRel, []string{"id", "note", "x"}, "public", "orders"); err == nil || !strings.Contains(err.Error(), "only in the file: x") {
		t.Errorf("extra column: %v", err)
	}
}

// A PostgreSQL text column takes text or NULL, nothing else.
func TestRenderBaselineValue_RawText(t *testing.T) {
	c := baseline.Column{Name: "flag", RawText: true}
	if s, null, err := renderBaselineValue(c, "t"); err != nil || null || s != "t" {
		t.Fatalf("text: %q %v %v", s, null, err)
	}
	if _, null, err := renderBaselineValue(c, nil); err != nil || !null {
		t.Fatalf("nil: %v %v", null, err)
	}
	for _, v := range []any{true, float64(1), map[string]any{"x": true}} {
		if _, _, err := renderBaselineValue(c, v); err == nil {
			t.Errorf("%T accepted for a PostgreSQL text column", v)
		}
	}
}

// A PostgreSQL 18 stored generated column rides in the stream but never in
// the baseline: not a column added after it. A real added column still is.
func TestCheckPostBaselineColumns_PGGenerated(t *testing.T) {
	changes := map[string]*query.ResultRow{"1": {EventType: event.EventUpdate,
		RowAfter: map[string]any{"id": "1", "note": "x", "total": "3"}}}
	in := mergeInput{Schema: "public", Table: "orders", PG: true, PGGenerated: map[string]bool{"total": true}}
	if err := checkPostBaselineColumns(in, changes, []string{"id", "note"}); err != nil {
		t.Fatalf("generated column refused: %v", err)
	}
	in.PGGenerated = nil
	if err := checkPostBaselineColumns(in, changes, []string{"id", "note"}); err == nil || !strings.Contains(err.Error(), "total") {
		t.Fatalf("without the generated list the column must refuse: %v", err)
	}
	in.PGGenerated = map[string]bool{"total": true}
	changes["1"].RowAfter["added"] = "y"
	if err := checkPostBaselineColumns(in, changes, []string{"id", "note"}); err == nil || !strings.Contains(err.Error(), "added") ||
		strings.Contains(err.Error(), "total") {
		t.Fatalf("a real added column must refuse alone: %v", err)
	}
}
