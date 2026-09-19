package metadata

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func implicitPeriodFixture() []columnRow {
	return []columnRow{
		{schemaName: "svdb", tableName: "imp", columnName: "id", ordinalPosition: 1, columnKey: "PRI", dataType: "int", columnType: "int(11)", isNullable: "NO"},
		{schemaName: "svdb", tableName: "imp", columnName: "val", ordinalPosition: 2, dataType: "varchar", columnType: "varchar(20)", isNullable: "YES"},
		{schemaName: "svdb", tableName: "plain", columnName: "id", ordinalPosition: 1, columnKey: "PRI", dataType: "int", columnType: "int(11)", isNullable: "NO"},
		{schemaName: "svdb", tableName: "expl", columnName: "id", ordinalPosition: 1, columnKey: "PRI", dataType: "int", columnType: "int(11)", isNullable: "NO"},
		{schemaName: "svdb", tableName: "expl", columnName: "row_start", ordinalPosition: 2, dataType: "timestamp", columnType: "timestamp(6)", isNullable: "NO",
			generationExpression: sql.NullString{Valid: true, String: "ROW START"}},
		{schemaName: "svdb", tableName: "expl", columnName: "row_end", ordinalPosition: 3, columnKey: "PRI", dataType: "timestamp", columnType: "timestamp(6)", isNullable: "NO",
			generationExpression: sql.NullString{Valid: true, String: "ROW END"}},
	}
}

// TestAddImplicitPeriodColumns_synthesizesHiddenColumns pins the #1272
// synthesis shape: an implicitly-versioned table gains row_start/row_end at
// the END of its ordinal range, TIMESTAMP(6) NOT NULL, with row_end a PK
// member and BOTH carrying the ROW START/ROW END generation expressions so
// the existing is_generated derivation (and the #1266 gates downstream) treat
// them exactly like the explicit form. An EXPLICITLY-versioned table and a
// plain table pass through untouched.
func TestAddImplicitPeriodColumns_synthesizesHiddenColumns(t *testing.T) {
	in := implicitPeriodFixture()
	out := addImplicitPeriodColumns(in, []tableRef{{"svdb", "imp"}, {"svdb", "expl"}})
	if len(out) != len(in)+2 {
		t.Fatalf("got %d columns, want %d (exactly two synthetic rows for the implicit table)", len(out), len(in)+2)
	}

	byTable := make(map[string][]columnRow)
	for _, c := range out {
		byTable[c.tableName] = append(byTable[c.tableName], c)
	}
	if len(byTable["plain"]) != 1 || len(byTable["expl"]) != 3 {
		t.Fatalf("plain/explicit tables must pass through untouched: plain=%d expl=%d", len(byTable["plain"]), len(byTable["expl"]))
	}

	imp := byTable["imp"]
	if len(imp) != 4 {
		t.Fatalf("implicit table has %d columns, want 4: %+v", len(imp), imp)
	}
	rs, re := imp[2], imp[3]
	if rs.columnName != "row_start" || rs.ordinalPosition != 3 || rs.columnKey != "" ||
		rs.columnType != "timestamp(6)" || rs.isNullable != "NO" ||
		!rs.generationExpression.Valid || rs.generationExpression.String != "ROW START" {
		t.Errorf("synthetic row_start has the wrong shape: %+v", rs)
	}
	if re.columnName != "row_end" || re.ordinalPosition != 4 || re.columnKey != "PRI" ||
		re.columnType != "timestamp(6)" || re.isNullable != "NO" ||
		!re.generationExpression.Valid || re.generationExpression.String != "ROW END" {
		t.Errorf("synthetic row_end has the wrong shape: %+v", re)
	}
}

// TestAddImplicitPeriodColumns_pkLessTableGetsNoFabricatedPK is the belt
// against the review's corruption scenario: MariaDB only EXTENDS an existing
// PK with row_end — a PK-less versioned table (which validation refuses or
// excludes upstream) must never receive a fabricated one-column generated PK,
// whose sentinel row_end would collapse every live row onto one pk_values.
func TestAddImplicitPeriodColumns_pkLessTableGetsNoFabricatedPK(t *testing.T) {
	in := []columnRow{
		{schemaName: "svdb", tableName: "nopk", columnName: "x", ordinalPosition: 1, dataType: "int", columnType: "int(11)", isNullable: "YES"},
	}
	out := addImplicitPeriodColumns(in, []tableRef{{"svdb", "nopk"}})
	if len(out) != 3 {
		t.Fatalf("got %d columns, want 3", len(out))
	}
	for _, c := range out {
		if c.columnKey == "PRI" {
			t.Fatalf("PK-less versioned table must not gain a fabricated PK member, got PRI on %q", c.columnName)
		}
	}
}

// TestAddImplicitPeriodColumns_excludedTableSkipped: a versioned table with
// no kept columns (excluded by the #1051 filter) must not re-enter the
// snapshot through synthesis; no-versioned input is a no-op.
func TestAddImplicitPeriodColumns_excludedTableSkipped(t *testing.T) {
	in := implicitPeriodFixture()
	out := addImplicitPeriodColumns(in, []tableRef{{"svdb", "excluded"}})
	if len(out) != len(in) {
		t.Fatalf("excluded (unseen) versioned table changed the column count: got %d, want %d", len(out), len(in))
	}
	if got := addImplicitPeriodColumns(in, nil); len(got) != len(in) {
		t.Fatalf("no versioned tables must be a no-op, got %d columns", len(got))
	}
}

// TestInvalidTables_versionedTablesAreValidated pins the review's
// validation-bypass fix: MariaDB reports versioned tables as TABLE_TYPE
// 'SYSTEM VERSIONED', so a TABLE_TYPE='BASE TABLE' filter let them skip BOTH
// the InnoDB and the no-PK checks. The widened scan must flag a PK-less
// versioned table AND report the versioned set for the synthesis.
func TestInvalidTables_versionedTablesAreValidated(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("SYSTEM VERSIONED").WithArgs("svdb").WillReturnRows(
		sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME", "ENGINE", "TABLE_TYPE"}).
			AddRow("svdb", "imp", "InnoDB", "SYSTEM VERSIONED").
			AddRow("svdb", "nopk", "InnoDB", "SYSTEM VERSIONED").
			AddRow("svdb", "plain", "InnoDB", "BASE TABLE"))

	columns := []columnRow{
		{schemaName: "svdb", tableName: "imp", columnName: "id", ordinalPosition: 1, columnKey: "PRI"},
		{schemaName: "svdb", tableName: "nopk", columnName: "x", ordinalPosition: 1},
		{schemaName: "svdb", tableName: "plain", columnName: "id", ordinalPosition: 1, columnKey: "PRI"},
	}
	nonInnoDB, noPK, versioned, err := invalidTables(db, []string{"svdb"}, columns)
	if err != nil {
		t.Fatalf("invalidTables: %v", err)
	}
	if len(nonInnoDB) != 0 {
		t.Errorf("nonInnoDB = %v, want empty", nonInnoDB)
	}
	if len(noPK) != 1 || noPK[0] != "svdb.nopk" {
		t.Fatalf("noPK = %v, want [svdb.nopk] — the PK-less VERSIONED table must not bypass validation", noPK)
	}
	if len(versioned) != 2 || versioned[0] != (tableRef{"svdb", "imp"}) || versioned[1] != (tableRef{"svdb", "nopk"}) {
		t.Fatalf("versioned = %v, want [svdb.imp svdb.nopk]", versioned)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestInvalidTables_versionedTablesAreValidatedWithNoSchemaFilter is the same
// invariant on the OTHER branch of invalidTables — the one an operator
// monitoring every schema takes, which is the default configuration (#1611).
// Both branches carry their own copy of the TABLE_TYPE filter, and only the
// scoped one was tested: narrowing this one back to 'BASE TABLE' left the
// whole package green, so the fix could ship to the default configuration
// while the guarded branch kept it.
func TestInvalidTables_versionedTablesAreValidatedWithNoSchemaFilter(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	// No WithArgs: the unscoped branch passes no parameters, and its query
	// excludes the system schemas by name instead.
	mock.ExpectQuery("SYSTEM VERSIONED").WillReturnRows(
		sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME", "ENGINE", "TABLE_TYPE"}).
			AddRow("svdb", "imp", "InnoDB", "SYSTEM VERSIONED").
			AddRow("svdb", "nopk", "InnoDB", "SYSTEM VERSIONED").
			AddRow("svdb", "plain", "InnoDB", "BASE TABLE"))

	columns := []columnRow{
		{schemaName: "svdb", tableName: "imp", columnName: "id", ordinalPosition: 1, columnKey: "PRI"},
		{schemaName: "svdb", tableName: "nopk", columnName: "x", ordinalPosition: 1},
		{schemaName: "svdb", tableName: "plain", columnName: "id", ordinalPosition: 1, columnKey: "PRI"},
	}
	nonInnoDB, noPK, versioned, err := invalidTables(db, nil, columns)
	if err != nil {
		t.Fatalf("invalidTables: %v", err)
	}
	if len(nonInnoDB) != 0 {
		t.Errorf("nonInnoDB = %v, want empty", nonInnoDB)
	}
	if len(noPK) != 1 || noPK[0] != "svdb.nopk" {
		t.Fatalf("noPK = %v, want [svdb.nopk] — with no schema filter, a PK-less VERSIONED table must not bypass validation either", noPK)
	}
	if len(versioned) != 2 || versioned[0] != (tableRef{"svdb", "imp"}) || versioned[1] != (tableRef{"svdb", "nopk"}) {
		t.Fatalf("versioned = %v, want [svdb.imp svdb.nopk] — the synthesis reads this set", versioned)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestAddImplicitPeriodColumns_agentResolverPath pins the exported
// capture-surface sibling (the agent BYOS path, which builds its resolver
// straight from information_schema and never sees the snapshot synthesis):
// the implicit table's TableMeta gains the two hidden columns and row_end
// joins PKColumns; an explicitly-versioned table is untouched; a PK-less
// table gains the columns but never a fabricated PK member.
func TestAddImplicitPeriodColumns_agentResolverPath(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("SYSTEM VERSIONED").WithArgs("svdb").WillReturnRows(
		sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME"}).
			AddRow("svdb", "imp").AddRow("svdb", "expl").AddRow("svdb", "nopk"))
	mock.ExpectQuery("GENERATION_EXPRESSION").WithArgs("svdb").WillReturnRows(
		sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME"}).AddRow("svdb", "expl"))

	tables := map[string]*TableMeta{
		"svdb.imp": {Schema: "svdb", Table: "imp", PKColumns: []string{"id"}, Columns: []ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "val", OrdinalPosition: 2, DataType: "varchar"},
		}},
		"svdb.expl": {Schema: "svdb", Table: "expl", PKColumns: []string{"id", "row_end"}, Columns: []ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "row_start", OrdinalPosition: 2, DataType: "timestamp", IsGenerated: true},
			{Name: "row_end", OrdinalPosition: 3, IsPK: true, DataType: "timestamp", IsGenerated: true},
		}},
		"svdb.nopk": {Schema: "svdb", Table: "nopk", Columns: []ColumnMeta{
			{Name: "x", OrdinalPosition: 1, DataType: "int"},
		}},
	}
	if err := AddImplicitPeriodColumns(db, []string{"svdb"}, tables); err != nil {
		t.Fatalf("AddImplicitPeriodColumns: %v", err)
	}

	imp := tables["svdb.imp"]
	if len(imp.Columns) != 4 {
		t.Fatalf("implicit table has %d columns, want 4: %+v", len(imp.Columns), imp.Columns)
	}
	re := imp.Columns[3]
	if re.Name != "row_end" || re.OrdinalPosition != 4 || !re.IsPK || !re.IsGenerated || re.ColumnType != "timestamp(6)" {
		t.Errorf("synthetic row_end has the wrong shape: %+v", re)
	}
	if strings.Join(imp.PKColumns, ",") != "id,row_end" {
		t.Errorf("PKColumns = %v, want [id row_end]", imp.PKColumns)
	}
	if len(tables["svdb.expl"].Columns) != 3 {
		t.Errorf("explicit table must pass through untouched, got %d columns", len(tables["svdb.expl"].Columns))
	}
	nopk := tables["svdb.nopk"]
	if len(nopk.Columns) != 3 {
		t.Fatalf("PK-less table has %d columns, want 3", len(nopk.Columns))
	}
	if len(nopk.PKColumns) != 0 || nopk.Columns[2].IsPK {
		t.Errorf("PK-less table must not gain a fabricated PK member: PKColumns=%v cols=%+v", nopk.PKColumns, nopk.Columns)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}
