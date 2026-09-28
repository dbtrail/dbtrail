package reconstruct

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// The window every case below shares: the baseline is anchored at
// binlog.000001:500 and the run's cut is binlog.000002:1000.
var (
	anchor1675 = &query.BinlogPos{File: "binlog.000001", Pos: 500}
	cut1675    = &query.BinlogPos{File: "binlog.000002", Pos: 1000}
)

// A DDL on shop.t after the target by time and by position.
func ddlAfter(query string) recordedDDL {
	return recordedDDL{
		DetectedAt: time.Date(2026, 3, 1, 10, 1, 0, 0, time.UTC),
		File:       "binlog.000002", Pos: 2000,
		Schema: "shop", Table: "t", Type: "ALTER TABLE", Query: query,
		AfterTarget: true,
	}
}

// A DDL on shop.t between the baseline and the target.
func ddlInWindow(query string) recordedDDL {
	d := ddlAfter(query)
	d.DetectedAt = time.Date(2026, 3, 1, 10, 0, 10, 0, time.UTC)
	d.Pos, d.AfterTarget = 600, false
	return d
}

// A DDL on shop.t before the baseline was read.
func ddlBefore(query string) recordedDDL {
	d := ddlAfter(query)
	d.DetectedAt = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	d.File, d.Pos, d.AfterTarget, d.BeforeAsOf = "binlog.000001", 100, false, true
	return d
}

func with(d recordedDDL, f func(*recordedDDL)) recordedDDL {
	f(&d)
	return d
}

func TestPlaceAddedColumns(t *testing.T) {
	for _, tc := range []struct {
		name    string
		added   []string
		ddls    []recordedDDL
		wantWhy string // "" = every column is placed after the target
	}{
		// (a) and (b): the statement has a row; whether it has a snapshot of
		// its own is not read at all.
		{"a: the column is added after the target", []string{"c"},
			[]recordedDDL{ddlAfter("ALTER TABLE t ADD COLUMN c INT")}, ""},

		// (c): nothing records where the column came from.
		{"c: no DDL is recorded", []string{"c"}, nil,
			"no ALTER TABLE recorded after the target adds c"},
		{"c: a later ALTER adds another column", []string{"c", "d"},
			[]recordedDDL{ddlAfter("ALTER TABLE t ADD COLUMN d INT")},
			"no ALTER TABLE recorded after the target adds c"},
		{"c: only DDL from before the baseline is recorded", []string{"c"},
			[]recordedDDL{ddlBefore("ALTER TABLE t ADD COLUMN c INT")},
			"no ALTER TABLE recorded after the target adds c"},

		// (d)
		{"d: one column before the target, one after", []string{"c", "d"},
			[]recordedDDL{ddlInWindow("ALTER TABLE t ADD COLUMN c INT"), ddlAfter("ALTER TABLE t ADD COLUMN d INT")},
			"is not after the target by both its time and its binlog position"},

		// (e)
		{"e: two columns in one statement", []string{"c", "d"},
			[]recordedDDL{ddlAfter("ALTER TABLE t ADD COLUMN c INT, ADD COLUMN d INT")}, ""},
		{"e: two statements", []string{"c", "d"},
			[]recordedDDL{ddlAfter("ALTER TABLE t ADD COLUMN c INT"), ddlAfter("ALTER TABLE t ADD d INT")}, ""},
		{"e: a later statement adds a column the newest snapshot does not have yet", []string{"c"},
			[]recordedDDL{ddlAfter("ALTER TABLE t ADD COLUMN c INT"), ddlAfter("ALTER TABLE t ADD COLUMN z INT")}, ""},
		{"e: backticks, upper case, lines and a comment", []string{"my col"},
			[]recordedDDL{ddlAfter("/* migration 12 */\nALTER TABLE `shop`.`t`\n  ADD COLUMN `My Col` INT\n")}, ""},
		{"e: added and then dropped", []string{"d"},
			[]recordedDDL{ddlAfter("ALTER TABLE t ADD COLUMN c INT, ADD COLUMN d INT"), ddlAfter("ALTER TABLE t DROP COLUMN c")},
			"cannot be read as only adding columns"},
		{"e: dropped and added again with the same name", []string{"c"},
			[]recordedDDL{ddlAfter("ALTER TABLE t DROP COLUMN c"), ddlAfter("ALTER TABLE t ADD COLUMN c INT")},
			"cannot be read as only adding columns"},
		{"e: added twice, the drop in between is not recorded", []string{"c"},
			[]recordedDDL{ddlAfter("ALTER TABLE t ADD COLUMN c INT"), ddlAfter("ALTER TABLE t ADD COLUMN C INT")},
			"is added more than once after the target"},
		{"e: renamed", []string{"c"},
			[]recordedDDL{ddlAfter("ALTER TABLE t RENAME COLUMN b TO c")},
			"cannot be read as only adding columns"},
		{"e: several clauses, one is not an ADD", []string{"c"},
			[]recordedDDL{ddlAfter("ALTER TABLE t ADD COLUMN c INT, MODIFY b BIGINT")},
			"cannot be read as only adding columns"},
		{"e: CREATE TABLE LIKE", []string{"c"},
			[]recordedDDL{with(ddlAfter("CREATE TABLE t LIKE u"), func(d *recordedDDL) { d.Type = "CREATE TABLE" })},
			"a CREATE TABLE is recorded after the target"},
		{"e: RENAME TABLE", []string{"c"},
			[]recordedDDL{with(ddlAfter("RENAME TABLE u TO t"), func(d *recordedDDL) { d.Type = "RENAME TABLE" }),
				ddlAfter("ALTER TABLE t ADD COLUMN c INT")},
			"a RENAME TABLE is recorded after the target"},
		{"e: dropped and created again", []string{"c"},
			[]recordedDDL{ddlAfter("ALTER TABLE t ADD COLUMN c INT"),
				with(ddlAfter("DROP TABLE t"), func(d *recordedDDL) { d.Type = "DROP TABLE" })},
			"a DROP TABLE is recorded after the target"},
		{"e: a type that says ALTER over a text that does not", []string{"c"},
			[]recordedDDL{ddlAfter("CREATE TABLE t (id INT, c INT)")},
			"cannot be read as only adding columns"},
		{"e: a text that says ALTER under a type that does not", []string{"c"},
			[]recordedDDL{with(ddlAfter("ALTER TABLE t ADD COLUMN c INT"), func(d *recordedDDL) { d.Type = "TRUNCATE TABLE" })},
			"a TRUNCATE TABLE is recorded after the target"},

		// (f)
		{"f: the row names the table in another case", []string{"c"},
			[]recordedDDL{with(ddlAfter("ALTER TABLE T ADD COLUMN c INT"), func(d *recordedDDL) { d.Table = "T" })},
			"cannot be told apart from this table"},
		{"f: the row names another schema", []string{"c"},
			[]recordedDDL{with(ddlAfter("ALTER TABLE t ADD COLUMN c INT"), func(d *recordedDDL) { d.Schema = "Shop" })},
			"cannot be told apart from this table"},
		{"f: the row has no schema", []string{"c"},
			[]recordedDDL{with(ddlAfter("ALTER TABLE t ADD COLUMN c INT"), func(d *recordedDDL) { d.Schema = "" })},
			"cannot be told apart from this table"},
		{"f: the text names another table", []string{"c"},
			[]recordedDDL{ddlAfter("ALTER TABLE u ADD COLUMN c INT")},
			`names "shop"."u" in its text`},
		{"f: the text names another schema", []string{"c"},
			[]recordedDDL{ddlAfter("ALTER TABLE other.t ADD COLUMN c INT")},
			`names "other"."t" in its text`},

		// (g)
		{"g: the same second as the target", []string{"c"},
			[]recordedDDL{with(ddlAfter("ALTER TABLE t ADD COLUMN c INT"), func(d *recordedDDL) { d.AfterTarget = false })},
			"is not after the target by both its time and its binlog position"},
		{"g: later by time, before the cut by position", []string{"c"},
			[]recordedDDL{with(ddlAfter("ALTER TABLE t ADD COLUMN c INT"), func(d *recordedDDL) { d.Pos = 900 })},
			"is not after the target by both its time and its binlog position"},
		{"g: later by time, ends exactly at the cut", []string{"c"},
			[]recordedDDL{with(ddlAfter("ALTER TABLE t ADD COLUMN c INT"), func(d *recordedDDL) { d.Pos = 1000 })},
			"is not after the target by both its time and its binlog position"},
		{"g: later by time, in an earlier file with a larger position", []string{"c"},
			[]recordedDDL{with(ddlAfter("ALTER TABLE t ADD COLUMN c INT"), func(d *recordedDDL) { d.File, d.Pos = "binlog.000001", 9000 })},
			"is not after the target by both its time and its binlog position"},
		{"g: one byte past the cut", []string{"c"},
			[]recordedDDL{with(ddlAfter("ALTER TABLE t ADD COLUMN c INT"), func(d *recordedDDL) { d.Pos = 1001 })}, ""},
		{"g: a later file with a smaller position", []string{"c"},
			[]recordedDDL{with(ddlAfter("ALTER TABLE t ADD COLUMN c INT"), func(d *recordedDDL) { d.File, d.Pos = "binlog.000003", 4 })}, ""},

		// (h)
		{"h: empty text", []string{"c"}, []recordedDDL{ddlAfter("")},
			"cannot be read as only adding columns"},
		{"h: a text as long as the column holds", []string{"c"},
			[]recordedDDL{ddlAfter("ALTER TABLE t ADD COLUMN c INT COMMENT '" + strings.Repeat("x", maxRecordedDDLBytes-41) + "'")},
			"may be cut short"},
		{"h: no binlog file", []string{"c"},
			[]recordedDDL{with(ddlAfter("ALTER TABLE t ADD COLUMN c INT"), func(d *recordedDDL) { d.File = "" })},
			"is not after the target by both its time and its binlog position"},
		{"h: no binlog position", []string{"c"},
			[]recordedDDL{with(ddlAfter("ALTER TABLE t ADD COLUMN c INT"), func(d *recordedDDL) { d.Pos = 0 })},
			"is not after the target by both its time and its binlog position"},

		// (i) and the baseline's side of the window.
		{"i: a DROP TABLE from before the baseline does not count", []string{"c"},
			[]recordedDDL{with(ddlBefore("DROP TABLE t"), func(d *recordedDDL) { d.Type = "DROP TABLE" }),
				ddlAfter("ALTER TABLE t ADD COLUMN c INT")}, ""},
		{"i: a row with no schema from before the baseline does not count", []string{"c"},
			[]recordedDDL{with(ddlBefore("ALTER TABLE t MODIFY b INT"), func(d *recordedDDL) { d.Schema = "" }),
				ddlAfter("ALTER TABLE t ADD COLUMN c INT")}, ""},
		{"i: earlier than the baseline by time, past its anchor by position", []string{"c"},
			[]recordedDDL{with(ddlBefore("ALTER TABLE t MODIFY b INT"), func(d *recordedDDL) { d.Pos = 501 }),
				ddlAfter("ALTER TABLE t ADD COLUMN c INT")},
			"is not after the target by both its time and its binlog position"},
		{"i: exactly at the baseline's anchor", []string{"c"},
			[]recordedDDL{with(ddlBefore("ALTER TABLE t MODIFY b INT"), func(d *recordedDDL) { d.Pos = 500 }),
				ddlAfter("ALTER TABLE t ADD COLUMN c INT")}, ""},
		{"i: a DDL inside the window that adds nothing", []string{"c"},
			[]recordedDDL{ddlInWindow("ALTER TABLE t MODIFY b INT"), ddlAfter("ALTER TABLE t ADD COLUMN c INT")},
			"is not after the target by both its time and its binlog position"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.ddls) > 0 && strings.Contains(tc.name, "as long as the column holds") && len(tc.ddls[0].Query) != maxRecordedDDLBytes {
				t.Fatalf("the fixture is %d bytes, want %d", len(tc.ddls[0].Query), maxRecordedDDLBytes)
			}
			why := placeAddedColumns(tc.added, tc.ddls, "shop", "t", anchor1675, cut1675)
			switch {
			case tc.wantWhy == "" && why != "":
				t.Fatalf("not placed: %s", why)
			case tc.wantWhy != "" && why == "":
				t.Fatalf("placed after the target; want a refusal with %q", tc.wantWhy)
			case !strings.Contains(why, tc.wantWhy):
				t.Fatalf("why = %q, want it to contain %q", why, tc.wantWhy)
			}
		})
	}
}

// Without a cut the target has no binlog position, and without an anchor the
// baseline's side of the window is decided by time alone.
func TestPlaceAddedColumns_cutAndAnchor(t *testing.T) {
	add := ddlAfter("ALTER TABLE t ADD COLUMN c INT")
	if why := placeAddedColumns([]string{"c"}, []recordedDDL{add}, "shop", "t", anchor1675, nil); !strings.Contains(why, "no event to place the target") {
		t.Errorf("no cut: why = %q", why)
	}
	// Asked with nothing recorded too, so the answer is about the cut.
	if why := placeAddedColumns([]string{"c"}, nil, "shop", "t", anchor1675, nil); !strings.Contains(why, "no event to place the target") {
		t.Errorf("no cut, nothing recorded: why = %q", why)
	}
	old := with(ddlBefore("DROP TABLE t"), func(d *recordedDDL) { d.Type, d.Pos = "DROP TABLE", 900000 })
	if why := placeAddedColumns([]string{"c"}, []recordedDDL{old, add}, "shop", "t", nil, cut1675); why != "" {
		t.Errorf("a baseline with no anchor: %s", why)
	}
	// The #840 rollover: binlog.1000000 comes after binlog.999999.
	cut := &query.BinlogPos{File: "binlog.999999", Pos: 5000}
	rolled := with(add, func(d *recordedDDL) { d.File, d.Pos = "binlog.1000000", 4 })
	if why := placeAddedColumns([]string{"c"}, []recordedDDL{rolled}, "shop", "t", anchor1675, cut); why != "" {
		t.Errorf("after a file number rollover: %s", why)
	}
}

const createSQL1675 = "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `b` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"

func tm1675(extra ...metadata.ColumnMeta) *metadata.TableMeta {
	cols := []metadata.ColumnMeta{
		{Name: "id", DataType: "int", ColumnType: "int", IsPK: true},
		{Name: "b", DataType: "int", ColumnType: "int"},
	}
	return &metadata.TableMeta{Schema: "shop", Table: "t", Columns: append(cols, extra...), PKColumns: []string{"id"}}
}

var (
	asOf1675   = time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	target1675 = time.Date(2026, 3, 1, 10, 0, 30, 0, time.UTC)
)

var ddlColumns1675 = []string{"detected_at", "binlog_file", "binlog_pos", "schema_name", "table_name",
	"ddl_type", "ddl_query", "before_as_of", "after_target"}

func ddlRow(d recordedDDL) []any {
	return []any{d.DetectedAt, d.File, d.Pos, d.Schema, d.Table, d.Type, d.Query, d.BeforeAsOf, d.AfterTarget}
}

func mockDDLs(t *testing.T, ddls ...recordedDDL) (sqlmock.Sqlmock, *sql.DB) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	rows := sqlmock.NewRows(ddlColumns1675)
	for _, d := range ddls {
		rows.AddRow(toDriverValues(ddlRow(d))...)
	}
	mock.ExpectQuery("FROM schema_changes").WithArgs(asOf1675, target1675, "shop", "t").WillReturnRows(rows)
	return mock, db
}

func toDriverValues(in []any) []driver.Value {
	out := make([]driver.Value, len(in))
	for i, v := range in {
		out[i] = v
	}
	return out
}

func foldCfg1675() FullTableConfig {
	return FullTableConfig{At: target1675, cut: cut1675}
}

func bmeta1675() baseline.DumpMetadata {
	return baseline.DumpMetadata{CreateTableSQL: createSQL1675, BinlogFile: anchor1675.File, BinlogPos: int64(anchor1675.Pos)}
}

// TestFoldNamesAtTarget_publishes: the fold's schema comparison passes when
// the one added column is proven to be from after the target, and the table
// it compares keeps every other column as it was.
func TestFoldNamesAtTarget_publishes(t *testing.T) {
	mock, db := mockDDLs(t, ddlAfter("ALTER TABLE t ADD COLUMN c INT"))
	tm := tm1675(metadata.ColumnMeta{Name: "C", DataType: "int", ColumnType: "int"})
	names, why := foldNamesAtTarget(context.Background(), db, foldCfg1675(), bmeta1675(), tm, asOf1675, "shop", "t")
	if why != "" {
		t.Fatalf("not placed: %s", why)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if len(names.Columns) != 2 || names.Columns[0].Name != "id" || names.Columns[1].Name != "b" {
		t.Fatalf("names compared = %+v, want id and b", names.Columns)
	}
	if len(tm.Columns) != 3 {
		t.Fatalf("the latest snapshot's table was changed in place: %+v", tm.Columns)
	}
	if err := checkBaselineSchemaCurrent(createSQL1675, names, nil, "shop", "t"); err != nil {
		t.Fatalf("the fold refuses: %v", err)
	}
}

// TestFoldNamesAtTarget_refuses: every way the proof is missing leaves the
// latest snapshot's table in the comparison, which refuses naming the column,
// and the refusal says what is missing.
func TestFoldNamesAtTarget_refuses(t *testing.T) {
	c := metadata.ColumnMeta{Name: "c", DataType: "int", ColumnType: "int"}
	for _, tc := range []struct {
		name     string
		tm       *metadata.TableMeta
		atTarget *metadata.TableMeta
		cfg      func(*FullTableConfig)
		db       func(t *testing.T) *sql.DB
		wantWhy  string
	}{
		{
			// Scenario (c), end to end: a DDL between the baseline and the
			// target left no row, and a later snapshot has its column.
			name: "c: the column's DDL left no row", tm: tm1675(c),
			db:      func(t *testing.T) *sql.DB { _, db := mockDDLs(t); return db },
			wantWhy: "no ALTER TABLE recorded after the target adds c",
		},
		{
			name: "c: the column's DDL left no row and a later one adds another column",
			tm:   tm1675(c, metadata.ColumnMeta{Name: "d", DataType: "int", ColumnType: "int"}),
			db: func(t *testing.T) *sql.DB {
				_, db := mockDDLs(t, ddlAfter("ALTER TABLE t ADD COLUMN d INT"))
				return db
			},
			wantWhy: "no ALTER TABLE recorded after the target adds c",
		},
		{
			name: "h: the index has no schema_changes table", tm: tm1675(c),
			db: func(t *testing.T) *sql.DB {
				db, mock, _ := sqlmock.New()
				t.Cleanup(func() { db.Close() })
				mock.ExpectQuery("FROM schema_changes").WillReturnError(&mysqldriver.MySQLError{Number: 1146, Message: "Table 'idx.schema_changes' doesn't exist"})
				return db
			},
			wantWhy: "keeps no record of DDL statements",
		},
		{
			name: "h: schema_changes cannot be read", tm: tm1675(c),
			db: func(t *testing.T) *sql.DB {
				db, mock, _ := sqlmock.New()
				t.Cleanup(func() { db.Close() })
				mock.ExpectQuery("FROM schema_changes").WillReturnError(errors.New("connection refused"))
				return db
			},
			wantWhy: "could not be read: connection refused",
		},
		{
			name: "h: a row that cannot be scanned", tm: tm1675(c),
			db: func(t *testing.T) *sql.DB {
				db, mock, _ := sqlmock.New()
				t.Cleanup(func() { db.Close() })
				mock.ExpectQuery("FROM schema_changes").WillReturnRows(
					sqlmock.NewRows(ddlColumns1675).AddRow(nil, "binlog.000002", 2000, "shop", "t", "ALTER TABLE", "ALTER TABLE t ADD COLUMN c INT", false, true))
				return db
			},
			wantWhy: "could not be read",
		},
		{
			name: "h: the rows end in an error", tm: tm1675(c),
			db: func(t *testing.T) *sql.DB {
				db, mock, _ := sqlmock.New()
				t.Cleanup(func() { db.Close() })
				mock.ExpectQuery("FROM schema_changes").WillReturnRows(
					sqlmock.NewRows(ddlColumns1675).
						AddRow(toDriverValues(ddlRow(ddlAfter("ALTER TABLE t ADD COLUMN c INT")))...).
						AddRow(toDriverValues(ddlRow(ddlAfter("ALTER TABLE t DROP COLUMN c")))...).
						RowError(1, errors.New("lost connection")))
				return db
			},
			wantWhy: "could not be read: lost connection",
		},
		{
			name: "no cut", tm: tm1675(c),
			cfg: func(cfg *FullTableConfig) { cfg.cut = nil },
			db: func(t *testing.T) *sql.DB {
				_, db := mockDDLs(t, ddlAfter("ALTER TABLE t ADD COLUMN c INT"))
				return db
			},
			wantWhy: "no event to place the target",
		},
		{
			name: "the snapshot in effect at the target already has the column", tm: tm1675(c),
			atTarget: tm1675(metadata.ColumnMeta{Name: "C", DataType: "int", ColumnType: "int"}),
			db: func(t *testing.T) *sql.DB {
				_, db := mockDDLs(t, ddlAfter("ALTER TABLE t ADD COLUMN c INT"))
				return db
			},
			wantWhy: "the schema snapshot in effect at the target already has C",
		},
		{
			name: "the added column is in the primary key",
			tm:   tm1675(metadata.ColumnMeta{Name: "c", DataType: "int", ColumnType: "int", IsPK: true}),
			db: func(t *testing.T) *sql.DB {
				_, db := mockDDLs(t, ddlAfter("ALTER TABLE t ADD COLUMN c INT"))
				return db
			},
			wantWhy: "c is part of the primary key now",
		},
		{
			name: "the added column is in the primary key by name only",
			tm: func() *metadata.TableMeta {
				tm := tm1675(c)
				tm.PKColumns = []string{"id", "C"}
				return tm
			}(),
			db: func(t *testing.T) *sql.DB {
				_, db := mockDDLs(t, ddlAfter("ALTER TABLE t ADD COLUMN c INT"))
				return db
			},
			wantWhy: "C is part of the primary key now",
		},
		{
			name: "a column is gone too",
			tm: &metadata.TableMeta{Schema: "shop", Table: "t", PKColumns: []string{"id"}, Columns: []metadata.ColumnMeta{
				{Name: "id", DataType: "int", ColumnType: "int", IsPK: true}, c}},
			db: func(t *testing.T) *sql.DB {
				_, db := mockDDLs(t, ddlAfter("ALTER TABLE t ADD COLUMN c INT"))
				return db
			},
			wantWhy: "a column is also gone since the baseline",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := foldCfg1675()
			if tc.cfg != nil {
				tc.cfg(&cfg)
			}
			if tc.atTarget != nil {
				cfg.schemaAt = metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"shop.t": tc.atTarget})
			}
			names, why := foldNamesAtTarget(context.Background(), tc.db(t), cfg, bmeta1675(), tc.tm, asOf1675, "shop", "t")
			if names != tc.tm {
				t.Fatalf("the comparison lost columns without proof: %+v", names.Columns)
			}
			if !strings.Contains(why, tc.wantWhy) {
				t.Fatalf("why = %q, want it to contain %q", why, tc.wantWhy)
			}
			err := explainUnplacedColumns(checkBaselineSchemaCurrent(createSQL1675, names, nil, "shop", "t"), why)
			if err == nil || !errors.Is(err, ErrSchemaChanged) {
				t.Fatalf("the fold does not refuse as a schema change: %v", err)
			}
			for _, want := range []string{"added since: c", tc.wantWhy} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal %q does not say %q", err, want)
				}
			}
			// The refusal reaches MCP clients and the web interface as it is:
			// it must not hand them a command line flag.
			if strings.Contains(err.Error(), " --") {
				t.Errorf("the refusal names a command line flag: %q", err)
			}
		})
	}
}

// TestFoldNamesAtTarget_nothingToPlace: with no added column the index is not
// read at all, and a generated column is not an added one.
func TestFoldNamesAtTarget_nothingToPlace(t *testing.T) {
	for _, tc := range []struct {
		name string
		tm   *metadata.TableMeta
	}{
		{"same columns", tm1675()},
		{"a generated column", tm1675(metadata.ColumnMeta{Name: "g", DataType: "int", ColumnType: "int", IsGenerated: true})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			names, why := foldNamesAtTarget(context.Background(), db, foldCfg1675(), bmeta1675(), tc.tm, asOf1675, "shop", "t")
			if names != tc.tm || why != "" {
				t.Fatalf("names = %+v, why = %q", names, why)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestFoldNamesAtTarget_anchor: the baseline's side of the window uses the
// baseline's own anchor, so a DDL stamped before the baseline but written
// after its anchor is inside the window.
func TestFoldNamesAtTarget_anchor(t *testing.T) {
	pastAnchor := with(ddlBefore("ALTER TABLE t MODIFY b INT"), func(d *recordedDDL) { d.Pos = 501 })
	_, db := mockDDLs(t, pastAnchor, ddlAfter("ALTER TABLE t ADD COLUMN c INT"))
	tm := tm1675(metadata.ColumnMeta{Name: "c", DataType: "int", ColumnType: "int"})
	if names, why := foldNamesAtTarget(context.Background(), db, foldCfg1675(), bmeta1675(), tm, asOf1675, "shop", "t"); why == "" || names != tm {
		t.Fatalf("a DDL past the baseline's anchor was taken for one before the baseline")
	}
	// A baseline that recorded no position decides by time alone.
	_, db = mockDDLs(t, pastAnchor, ddlAfter("ALTER TABLE t ADD COLUMN c INT"))
	bmeta := bmeta1675()
	bmeta.BinlogFile, bmeta.BinlogPos = "", 0
	if _, why := foldNamesAtTarget(context.Background(), db, foldCfg1675(), bmeta, tm, asOf1675, "shop", "t"); why != "" {
		t.Fatalf("a baseline with no anchor: %s", why)
	}
}

// TestExplainUnplacedColumns_text is the whole refusal as an operator reads it.
func TestExplainUnplacedColumns_text(t *testing.T) {
	tm := tm1675(metadata.ColumnMeta{Name: "extra", DataType: "int", ColumnType: "int"})
	why := placeAddedColumns([]string{"extra"}, []recordedDDL{ddlInWindow("ALTER TABLE t ADD COLUMN extra INT")}, "shop", "t", anchor1675, cut1675)
	err := explainUnplacedColumns(checkBaselineSchemaCurrent(createSQL1675, tm, nil, "shop", "t"), why)
	want := "shop.t changed shape since its baseline was taken (added since: extra; gone since: none; type changed since: none) — " +
		"a snapshot emitted from it would carry the OLD CREATE TABLE forward and project every row onto the old columns and types, " +
		"so every reconstruct anchored on it would be wrong. Take a real snapshot instead: `bintrail dump` + `bintrail baseline`. " +
		"(If the schema snapshot is what is stale, run `bintrail snapshot` first and retry.): schema changed since the baseline. " +
		"A column added after the restore's target is left out only when the index proves it; here it does not: " +
		"the ALTER TABLE recorded at 2026-03-01T10:00:10Z (binlog.000002:600) is not after the target by both its time and its binlog position (the cut is binlog.000002:1000)"
	if err == nil || err.Error() != want {
		t.Fatalf("refusal:\n got %v\nwant %s", err, want)
	}
	if got := explainUnplacedColumns(nil, why); got != nil {
		t.Errorf("no refusal, but an error: %v", got)
	}
	plain := checkBaselineSchemaCurrent(createSQL1675, tm, nil, "shop", "t")
	if got := explainUnplacedColumns(plain, ""); got != plain {
		t.Errorf("nothing to explain, but the refusal changed: %v", got)
	}
}
