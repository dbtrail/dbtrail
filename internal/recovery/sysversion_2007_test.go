package recovery

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2007: reversal SQL for a MariaDB system-versioned table. The binlog of such
// a table carries history rows and versioned deletes; reversing them as if
// they were ordinary row changes resurrects old versions as current rows
// (a DELETE HISTORY reversed into INSERTs) and silently fails to restore a
// deleted row (its tombstone UPDATE reversed with WHERE row_end = <past>).

const (
	svCur = "2106-02-07 06:28:15.999999"
	svT0  = "2026-10-02 06:13:40.000001"
	svT1  = "2026-10-02 06:13:43.628276"
)

// The snapshot shape of `CREATE TABLE prices (...) WITH SYSTEM VERSIONING`:
// the hidden period columns synthesized (#1272), row_end appended to the PK.
func svGen() *Generator {
	tm := &metadata.TableMeta{Schema: "shop", Table: "prices", Columns: []metadata.ColumnMeta{
		{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
		{Name: "price", OrdinalPosition: 2, DataType: "decimal"},
		{Name: "row_start", OrdinalPosition: 3, DataType: "timestamp", ColumnType: "timestamp(6)", IsGenerated: true},
		{Name: "row_end", OrdinalPosition: 4, IsPK: true, DataType: "timestamp", ColumnType: "timestamp(6)", IsGenerated: true},
	}, PKColumns: []string{"id", "row_end"}}
	return New(nil, metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"shop.prices": tm}))
}

func svRow(id float64, price, start, end string) map[string]any {
	return map[string]any{"id": id, "price": price, "row_start": start, "row_end": end}
}

func svEvent(id uint64, typ parser.EventType, pk string, before, after map[string]any) query.ResultRow {
	return query.ResultRow{EventID: id, EventTimestamp: time.Date(2026, 10, 2, 6, 13, 43, 0, time.UTC),
		SchemaName: "shop", TableName: "prices", EventType: typ, PKValues: pk, RowBefore: before, RowAfter: after}
}

// statementFor returns the SQL emitted under the "-- [id]" header of one event,
// or "" when the event emitted no statement.
func statementFor(t *testing.T, script string, id string) (header, stmt string) {
	t.Helper()
	lines := strings.Split(script, "\n")
	for i, l := range lines {
		if strings.HasPrefix(l, "-- ["+id+"]") {
			header = l + "\n"
			for _, n := range lines[i+1:] {
				if strings.HasPrefix(n, "-- [") || strings.TrimSpace(n) == "" {
					return header, ""
				}
				if !strings.HasPrefix(n, "--") {
					return header, n
				}
				header += n + "\n"
			}
			return header, ""
		}
	}
	t.Fatalf("no header for event %s in:\n%s", id, script)
	return "", ""
}

func TestGenerate_systemVersioned_2007(t *testing.T) {
	rows := []query.ResultRow{
		svEvent(1, parser.EventInsert, "1|"+svCur, nil, svRow(1, "1.00", svT0, svCur)),
		svEvent(2, parser.EventUpdate, "1|"+svCur, svRow(1, "1.00", svT0, svCur), svRow(1, "1.50", svT1, svCur)),
		svEvent(3, parser.EventInsert, "1|"+svT1, nil, svRow(1, "1.00", svT0, svT1)),
		svEvent(4, parser.EventUpdate, "2|"+svCur, svRow(2, "2.00", svT0, svCur), svRow(2, "2.00", svT0, svT1)),
		svEvent(5, parser.EventDelete, "1|"+svT1, svRow(1, "1.00", svT0, svT1), nil),
		svEvent(6, parser.EventUpdate, "3|"+svT0, svRow(3, "3.00", svT0, svT0), svRow(3, "3.30", svT0, svT0)),
		svEvent(7, parser.EventDelete, "4|"+svCur, svRow(4, "4.00", svT0, svCur), nil),
	}
	var buf bytes.Buffer
	n, err := svGen().GenerateSQLFromRows(rows, &buf)
	if err != nil {
		t.Fatalf("GenerateSQLFromRows: %v", err)
	}
	script := buf.String()
	if n != 4 {
		t.Errorf("statements written = %d, want 4 (insert, update, versioned delete, real delete)\n%s", n, script)
	}

	for _, c := range []struct {
		id, want string // want "" = no statement
	}{
		{"1", "DELETE FROM `shop`.`prices` WHERE `id` = 1"},
		{"2", "UPDATE `shop`.`prices` SET `id` = 1, `price` = '1.00' WHERE `id` = 1"},
		{"3", ""}, // the old version an UPDATE keeps: the server owns it
		{"4", "INSERT INTO `shop`.`prices` (`id`, `price`) VALUES (2, '2.00')"},
		{"5", ""}, // DELETE HISTORY: history cannot be written back
		{"6", ""}, // an edit of history
		{"7", "INSERT INTO `shop`.`prices` (`id`, `price`) VALUES (4, '4.00')"},
	} {
		_, stmt := statementFor(t, script, c.id)
		if stmt != c.want+map[bool]string{true: "", false: ";"}[c.want == ""] {
			t.Errorf("event %s: statement = %q, want %q\n%s", c.id, stmt, c.want, script)
		}
	}
	if strings.Contains(script, "row_end` =") || strings.Contains(script, "`row_start`") {
		t.Errorf("period columns reached the reversal SQL:\n%s", script)
	}
	for _, id := range []string{"3", "5", "6"} {
		header, _ := statementFor(t, script, id)
		if !strings.Contains(header, "system-versioned") {
			t.Errorf("event %s: skipped without saying why: %q", id, header)
		}
	}
}

func TestGenerate_systemVersioned_unknownMarkerRefused_2007(t *testing.T) {
	far := "2090-01-01 00:00:00.000000"
	rows := []query.ResultRow{
		svEvent(1, parser.EventInsert, "1|"+far, nil, svRow(1, "1.00", svT0, far)),
	}
	var buf bytes.Buffer
	_, err := svGen().GenerateSQLFromRows(rows, &buf)
	if err == nil || !strings.Contains(err.Error(), "neither") {
		t.Fatalf("GenerateSQLFromRows = %v, want a refusal for an end value nothing explains\n%s", err, buf.String())
	}
}

// A MySQL table with a stored generated TIMESTAMP in its key (MariaDB refuses
// a generated PK column outright, so the shape only exists on MySQL) has no
// row start column and keeps the ordinary reversal.
func TestGenerate_generatedTimestampKeyWithoutPeriod_unchanged_2007(t *testing.T) {
	tm := &metadata.TableMeta{Schema: "shop", Table: "t", Columns: []metadata.ColumnMeta{
		{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
		{Name: "ts", OrdinalPosition: 2, IsPK: true, DataType: "timestamp", ColumnType: "timestamp(6)", IsGenerated: true},
	}, PKColumns: []string{"id", "ts"}}
	g := New(nil, metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"shop.t": tm}))
	row := query.ResultRow{EventID: 1, EventTimestamp: time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC),
		SchemaName: "shop", TableName: "t", EventType: parser.EventDelete, PKValues: "1|" + svT0,
		RowBefore: map[string]any{"id": float64(1), "ts": svT0}}
	var buf bytes.Buffer
	n, err := g.GenerateSQLFromRows([]query.ResultRow{row}, &buf)
	if err != nil || n != 1 || !strings.Contains(buf.String(), "INSERT INTO `shop`.`t` (`id`) VALUES (1)") {
		t.Fatalf("n=%d err=%v, want the ordinary reversal INSERT:\n%s", n, err, buf.String())
	}
}

// The index's source flavor decides when it is known: MariaDB refuses a
// generated column in a primary key except ROW END, so there any generated
// key member is the period end, whatever else the table holds; MySQL has no
// system versioning, so the shape rule must never fire there.
func TestGenerate_systemVersioned_flavorDecides_2007(t *testing.T) {
	cols := []metadata.ColumnMeta{
		{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
		{Name: "price", OrdinalPosition: 2, DataType: "decimal"},
		{Name: "expires", OrdinalPosition: 3, DataType: "timestamp", IsGenerated: true}, // an ordinary generated TIMESTAMP
		{Name: "row_start", OrdinalPosition: 4, DataType: "timestamp", IsGenerated: true},
		{Name: "row_end", OrdinalPosition: 5, IsPK: true, DataType: "timestamp", IsGenerated: true},
	}
	gen := func(flavor string, cols []metadata.ColumnMeta) *Generator {
		tm := &metadata.TableMeta{Schema: "shop", Table: "prices", Columns: cols}
		for _, c := range cols {
			if c.IsPK {
				tm.PKColumns = append(tm.PKColumns, c.Name)
			}
		}
		g := New(nil, metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"shop.prices": tm}))
		g.flavor, g.flavorRead = flavor, true
		return g
	}
	tomb := svEvent(1, parser.EventUpdate, "2|"+svCur, svRow(2, "2.00", svT0, svCur), svRow(2, "2.00", svT0, svT1))

	// MariaDB: versioned despite the extra generated TIMESTAMP.
	var buf bytes.Buffer
	if _, err := gen("mariadb", cols).GenerateSQLFromRows([]query.ResultRow{tomb}, &buf); err != nil ||
		!strings.Contains(buf.String(), "INSERT INTO `shop`.`prices` (`id`, `price`) VALUES (2, '2.00')") {
		t.Fatalf("mariadb: err=%v, want the versioned delete reversed with an INSERT:\n%s", err, buf.String())
	}

	// MySQL: the exact versioned shape is an ordinary table there.
	mysqlCols := []metadata.ColumnMeta{cols[0], cols[1], cols[3], cols[4]}
	buf.Reset()
	hist := svEvent(2, parser.EventInsert, "1|"+svT1, nil, svRow(1, "1.00", svT0, svT1))
	if n, err := gen("mysql", mysqlCols).GenerateSQLFromRows([]query.ResultRow{hist}, &buf); err != nil || n != 1 ||
		strings.Contains(buf.String(), "skipped") {
		t.Fatalf("mysql: n=%d err=%v, want the ordinary reversal, nothing skipped:\n%s", n, err, buf.String())
	}

	// MariaDB, transaction-precise (BIGINT period): refused, not guessed.
	bigCols := []metadata.ColumnMeta{cols[0], cols[1],
		{Name: "row_start", OrdinalPosition: 3, DataType: "bigint", IsGenerated: true},
		{Name: "row_end", OrdinalPosition: 4, IsPK: true, DataType: "bigint", IsGenerated: true}}
	buf.Reset()
	trx := svEvent(3, parser.EventInsert, "1|18446744073709551615", nil,
		map[string]any{"id": float64(1), "price": "1.00", "row_start": float64(10), "row_end": float64(1.8446744073709552e19)})
	if _, err := gen("mariadb", bigCols).GenerateSQLFromRows([]query.ResultRow{trx}, &buf); err == nil ||
		!strings.Contains(err.Error(), "transaction-precise") {
		t.Fatalf("mariadb transaction-precise: err=%v, want a refusal", err)
	}
}

// Without a schema snapshot describing the table, nothing says whether it is
// system-versioned, and reversing a versioned table's events literally is the
// original #2007 bug (a delete that never comes back, DELETE HISTORY
// resurrecting old versions). A current-row marker in an image is the tell:
// refuse rather than guess.
func TestGenerate_noSnapshot_markerRefused_2007(t *testing.T) {
	tomb := svEvent(1, parser.EventUpdate, "2|"+svCur, svRow(2, "2.00", svT0, svCur), svRow(2, "2.00", svT0, svT1))
	other := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"shop.other": {Schema: "shop", Table: "other",
		Columns: []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "int"}}, PKColumns: []string{"id"}}})
	for name, g := range map[string]*Generator{
		"no resolver":                     New(nil, nil),
		"table missing from the snapshot": New(nil, other),
	} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			_, err := g.GenerateSQLFromRows([]query.ResultRow{tomb}, &buf)
			if err == nil || !strings.Contains(err.Error(), "take a schema snapshot first") ||
				!strings.Contains(err.Error(), "shop.prices") {
				t.Fatalf("err = %v, want the no-snapshot refusal naming the table\n%s", err, buf.String())
			}
			if !errors.Is(err, ErrSystemVersioned) {
				t.Fatalf("err = %v, want it classified ErrSystemVersioned", err)
			}
		})
	}
	// An ordinary row with no marker anywhere keeps the degraded reversal.
	plain := query.ResultRow{EventID: 2, EventTimestamp: svEvent(0, 0, "", nil, nil).EventTimestamp, SchemaName: "shop",
		TableName: "t", EventType: parser.EventDelete, PKValues: "1", RowBefore: map[string]any{"id": float64(1), "v": "x"}}
	var buf bytes.Buffer
	if n, err := New(nil, nil).GenerateSQLFromRows([]query.ResultRow{plain}, &buf); err != nil || n != 1 {
		t.Fatalf("plain row without a snapshot: n=%d err=%v, want the ordinary reversal", n, err)
	}
}

// A versioning refusal must read as one, not as the corruption message the
// per-event generation failures share.
func TestGenerate_versioningRefusalWording_2007(t *testing.T) {
	far := "2090-01-01 00:00:00.000000"
	var buf bytes.Buffer
	_, err := svGen().GenerateSQLFromRows([]query.ResultRow{
		svEvent(1, parser.EventInsert, "1|"+far, nil, svRow(1, "1.00", svT0, far)),
	}, &buf)
	if !errors.Is(err, ErrSystemVersioned) {
		t.Fatalf("err = %v, want ErrSystemVersioned", err)
	}
	if strings.Contains(err.Error(), "malformed") || strings.Contains(err.Error(), "truncated") {
		t.Fatalf("a versioning refusal must not send the operator hunting corruption: %v", err)
	}
	if !strings.Contains(err.Error(), "system-versioned") {
		t.Fatalf("the refusal must say it is about system versioning: %v", err)
	}
}

// Flavor unknown (file-indexed binlogs): a transaction-precise table is
// detected by its shape and refused, not reversed literally. Flavor stamped
// mysql on an index that in fact reads MariaDB: a generated key member
// holding a current-row marker is refused.
func TestGenerate_uncertainFlavor_2007(t *testing.T) {
	mk := func(flavor string, cols []metadata.ColumnMeta) *Generator {
		tm := &metadata.TableMeta{Schema: "shop", Table: "prices", Columns: cols}
		for _, c := range cols {
			if c.IsPK {
				tm.PKColumns = append(tm.PKColumns, c.Name)
			}
		}
		g := New(nil, metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"shop.prices": tm}))
		g.flavor, g.flavorRead = flavor, true
		return g
	}
	id := metadata.ColumnMeta{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"}
	price := metadata.ColumnMeta{Name: "price", OrdinalPosition: 2, DataType: "decimal"}
	big := []metadata.ColumnMeta{id, price,
		{Name: "row_start", OrdinalPosition: 3, DataType: "bigint", IsGenerated: true},
		{Name: "row_end", OrdinalPosition: 4, IsPK: true, DataType: "bigint", IsGenerated: true}}
	trx := svEvent(1, parser.EventInsert, "1|18446744073709551615", nil,
		map[string]any{"id": float64(1), "price": "1.00", "row_start": float64(10), "row_end": float64(1.8446744073709552e19)})
	var buf bytes.Buffer
	if _, err := mk("", big).GenerateSQLFromRows([]query.ResultRow{trx}, &buf); err == nil ||
		!strings.Contains(err.Error(), "transaction-precise") {
		t.Fatalf("unknown flavor, BIGINT periods: err=%v, want the transaction-precise refusal", err)
	}

	ts := []metadata.ColumnMeta{id, price,
		{Name: "row_start", OrdinalPosition: 3, DataType: "timestamp", IsGenerated: true},
		{Name: "row_end", OrdinalPosition: 4, IsPK: true, DataType: "timestamp", IsGenerated: true}}
	tomb := svEvent(2, parser.EventUpdate, "2|"+svCur, svRow(2, "2.00", svT0, svCur), svRow(2, "2.00", svT0, svT1))
	buf.Reset()
	if _, err := mk("mysql", ts).GenerateSQLFromRows([]query.ResultRow{tomb}, &buf); !errors.Is(err, ErrSystemVersioned) {
		t.Fatalf("flavor mysql, marker in a generated key member: err=%v, want ErrSystemVersioned", err)
	}
}

func TestGenerate_historySkipWording_2007(t *testing.T) {
	var buf bytes.Buffer
	if _, err := svGen().GenerateSQLFromRows([]query.ResultRow{
		svEvent(1, parser.EventInsert, "1|"+svCur, nil, svRow(1, "1.00", svT0, svCur)),
		svEvent(2, parser.EventInsert, "1|"+svT1, nil, svRow(1, "1.00", svT0, svT1)),
	}, &buf); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "owns it") || !strings.Contains(buf.String(), "history version") {
		t.Fatalf("the skip comment must describe a history version without claiming the server wrote it:\n%s", buf.String())
	}
}

// MySQL's largest TIMESTAMP is exactly the 2038 marker, a common "never
// expires" value: on a source recorded as MySQL (no system versioning), a
// row holding it must reverse normally even without a schema snapshot.
func TestGenerate_noSnapshot_mysqlSentinelNotRefused_2007(t *testing.T) {
	g := New(nil, nil)
	g.flavor, g.flavorRead = "mysql", true
	row := query.ResultRow{EventID: 1, EventTimestamp: svEvent(0, 0, "", nil, nil).EventTimestamp, SchemaName: "shop",
		TableName: "tokens", EventType: parser.EventDelete, PKValues: "1",
		RowBefore: map[string]any{"id": float64(1), "valid_until": "2038-01-19 03:14:07.999999"}}
	var buf bytes.Buffer
	if n, err := g.GenerateSQLFromRows([]query.ResultRow{row}, &buf); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v, want the ordinary reversal for a MySQL never-expires value", n, err)
	}
}
