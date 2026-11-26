package reconstruct

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #3159: a tail over a table keyed by DECIMAL, DATE, DATETIME, BINARY or
// VARBINARY. The seam is the anti-join of baseline.TableTailStateSQL:
// it drops a published row when its key EQUALS a tail row's key, compared as
// the two files store them, under the sandbox's case-folding collation. So
// every case writes a copy the way a dump does (the server's text through
// baseline's writer), folds events decoded the way the index hands them to
// a tail (go-mysql's text, binary as bytes with BINARY(n)'s trailing 0x00
// cut), and reads the state through DuckDB: a deleted row coming back or a
// row read twice is the failure.

// keyTailCase is one table: its definition, its key as the schema snapshot
// gives it, and how a state row prints (a DuckDB select list).
type keyTailCase struct {
	ddl    string
	pk     []metadata.ColumnMeta
	render string
}

// keyEvent is a change of s.t as the index returns it, decoded: the key from
// before (an UPDATE or a DELETE) or after (an INSERT) spelled by
// event.BuildPKValues, as the parser spells it.
func (c keyTailCase) event(id uint64, typ event.EventType, before, after map[string]any) query.ResultRow {
	ev := query.ResultRow{EventID: id, BinlogFile: "binlog.000003", StartPos: 100 * id, EndPos: 100*id + 10,
		SchemaName: "s", TableName: "t", EventType: typ, RowBefore: before, RowAfter: after}
	if before != nil {
		ev.PKValues = event.BuildPKValues(c.pk, before)
	} else {
		ev.PKValues = event.BuildPKValues(c.pk, after)
	}
	return ev
}

// state writes a copy with base's rows, folds pages into a tail over it and
// returns the state with the tail, each row printed by render, sorted. A
// fold that refuses fails the test.
func (c keyTailCase) state(t *testing.T, base []map[string]string, pages ...[]query.ResultRow) []string {
	t.Helper()
	tail, path, err := c.fold(t, base, pages...)
	if err != nil {
		t.Fatal(err)
	}
	return readKeyState(t, baseline.TableTailStateSQL("SELECT * FROM read_parquet("+lit(path)+")", lit(c.write(t, tail)), tail.PKColumns(), ""), c.render)
}

func (c keyTailCase) fold(t *testing.T, base []map[string]string, pages ...[]query.ResultRow) (*Tail, string, error) {
	t.Helper()
	path := writeTailBase(t, t.TempDir(), "t.parquet", c.ddl, base...)
	tail, err := NewTail("s", "t", c.ddl, path, c.pk)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pages {
		if err := tail.Apply(p); err != nil {
			return nil, path, err
		}
	}
	return tail, path, nil
}

func (c keyTailCase) write(t *testing.T, tail *Tail) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "t.tail.parquet")
	if _, err := tail.WriteFile(path); err != nil {
		t.Fatal(err)
	}
	return path
}

// readKeyState runs stateSQL in DuckDB under the sandbox's collation and
// prints each row with render.
func readKeyState(t *testing.T, stateSQL, render string) []string {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "SET default_collation = 'nocase'"); err != nil {
		t.Fatal(err)
	}
	rs, err := db.QueryContext(ctx, "SELECT "+render+" FROM ("+stateSQL+")")
	if err != nil {
		t.Fatalf("%v\n%s", err, stateSQL)
	}
	defer rs.Close()
	var out []string
	for rs.Next() {
		var s string
		if err := rs.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(out)
	return out
}

func wantKeyRows(t *testing.T, got []string, want ...string) {
	t.Helper()
	slices.Sort(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("got\n  %s\nwant\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

func img(kv ...any) map[string]any {
	m := map[string]any{}
	for i := 0; i < len(kv); i += 2 {
		m[kv[i].(string)] = kv[i+1]
	}
	return m
}

func num(s string) json.Number { return json.Number(s) }

func wantRefusal(t *testing.T, err error, says string) {
	t.Helper()
	var r *TailRefusal
	if !errors.As(err, &r) || !strings.Contains(r.Reason, says) {
		t.Fatalf("got %v, want a refusal saying %q", err, says)
	}
}

// A UUID stored as BINARY(16): bytes that are not UTF-8 (pk_values spells
// them in hex), bytes that are (spelled as themselves, '|' and '\' escaped),
// a 0x00 inside the value. Insert, update, delete and a key change, each
// against the dump's 16 bytes.
func TestTailKey_binary16(t *testing.T) {
	c := keyTailCase{
		ddl:    "CREATE TABLE `t` (\n  `id` binary(16) NOT NULL,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n",
		pk:     []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "binary", ColumnType: "binary(16)"}},
		render: `hex(id) || '=' || coalesce(CAST(v AS VARCHAR), 'NULL')`,
	}
	u1 := mustHex(t, "9F3B00C2D1E4400A8B7C6D5E4F3A2B1C") // a 0x00 inside
	u2 := mustHex(t, "A1B2C3D4E5F60718293A4B5C6D7E8F90")
	u3 := mustHex(t, "FFEEDDCCBBAA99887766554433221101")
	text := []byte(`ab|cd\efghijklmn`) // UTF-8, with the two characters pk_values escapes
	base := []map[string]string{
		{"id": "0x" + hex.EncodeToString(u1), "v": "1"},
		{"id": "0x" + hex.EncodeToString(u2), "v": "2"},
		{"id": "0x" + hex.EncodeToString(u3), "v": "3"},
		{"id": "0x" + hex.EncodeToString(text), "v": "4"},
	}
	u4 := mustHex(t, "0102030405060708090A0B0C0D0E0F10")
	u5 := mustHex(t, "1112131415161718191A1B1C1D1E1F20")
	got := c.state(t, base, []query.ResultRow{
		c.event(1, event.EventUpdate, img("id", u1, "v", num("1")), img("id", u1, "v", num("10"))),
		c.event(2, event.EventDelete, img("id", u2, "v", num("2")), nil),
		c.event(3, event.EventInsert, nil, img("id", u4, "v", num("40"))),
		c.event(4, event.EventUpdate, img("id", u3, "v", num("3")), img("id", u5, "v", num("3"))),
		c.event(5, event.EventDelete, img("id", text, "v", num("4")), nil),
	})
	h := func(b []byte) string { return strings.ToUpper(hex.EncodeToString(b)) }
	wantKeyRows(t, got, h(u1)+"=10", h(u4)+"=40", h(u5)+"=3")
}

// BINARY(n) refuses a value shorter than n: MySQL stored it right-padded
// with 0x00, the binary log carries it without the padding, and a row a
// refresh rewrote holds it without too, so the tail cannot know which the
// copy holds. A value that is all 0x00 is the shortest of them.
func TestTailKey_binaryShortValueRefuses(t *testing.T) {
	c := keyTailCase{
		ddl: "CREATE TABLE `t` (\n  `id` binary(4) NOT NULL,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n",
		pk:  []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "binary", ColumnType: "binary(4)"}},
	}
	for _, v := range [][]byte{{0x01, 0x02, 0x03}, {0x01}} {
		_, _, err := c.fold(t, nil, []query.ResultRow{c.event(1, event.EventInsert, nil, img("id", v, "v", num("1")))})
		wantRefusal(t, err, "ends in 0x00 bytes")
	}
	// All 0x00: the binary log carries the empty value.
	_, _, err0 := c.fold(t, nil, []query.ResultRow{c.event(1, event.EventInsert, nil, img("id", []byte{}, "v", num("1")))})
	wantRefusal(t, err0, "the empty value")
	// Seen on the old half of a key change too: the tombstone it files
	// would carry the short spelling.
	_, _, err := c.fold(t, nil, []query.ResultRow{c.event(1, event.EventUpdate,
		img("id", []byte{0x01, 0x02}, "v", num("1")), img("id", []byte{0x01, 0x02, 0x03, 0x04}, "v", num("1")))})
	wantRefusal(t, err, "ends in 0x00 bytes")
}

// VARBINARY: a trailing 0x00 is part of the value, so 01 and 0100 are two
// rows; bytes that are UTF-8 with '|' and '\' in them; bytes that are not
// UTF-8. The empty value is refused (below); one published and untouched
// stays.
func TestTailKey_varbinary(t *testing.T) {
	c := keyTailCase{
		ddl:    "CREATE TABLE `t` (\n  `id` varbinary(20) NOT NULL,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n",
		pk:     []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "varbinary", ColumnType: "varbinary(20)"}},
		render: `'[' || hex(id) || ']=' || coalesce(CAST(v AS VARCHAR), 'NULL')`,
	}
	base := []map[string]string{
		{"id": "0x01", "v": "1"},
		{"id": "0x0100", "v": "2"},
		{"id": "", "v": "3"},
		{"id": "0xfffe", "v": "4"},
		{"id": "0x" + hex.EncodeToString([]byte(`a|b\c`)), "v": "5"},
		{"id": "0x" + hex.EncodeToString([]byte("Ab")), "v": "6"},
	}
	got := c.state(t, base, []query.ResultRow{
		c.event(1, event.EventDelete, img("id", []byte{0x01, 0x00}, "v", num("2")), nil),
		c.event(3, event.EventUpdate, img("id", []byte{0xff, 0xfe}, "v", num("4")), img("id", []byte{0xff, 0xfe}, "v", num("40"))),
		c.event(4, event.EventDelete, img("id", []byte(`a|b\c`), "v", num("5")), nil),
		c.event(5, event.EventInsert, nil, img("id", []byte{0x00}, "v", num("7"))),
		// Not the published "Ab": bytes compare without a collation.
		c.event(6, event.EventInsert, nil, img("id", []byte("ab"), "v", num("8"))),
	})
	wantKeyRows(t, got, "[01]=1", "[]=3", "[FFFE]=40", "[00]=7", "["+strings.ToUpper(hex.EncodeToString([]byte("Ab")))+"]=6", "["+strings.ToUpper(hex.EncodeToString([]byte("ab")))+"]=8")
}

// A binary value whose own text starts with 0x is refused: pk_values spells
// a value that is not UTF-8 that way too, so two rows could share one key.
// So is one the index left undecoded (base64 text, no schema snapshot said
// the column is binary).
func TestTailKey_binaryAmbiguousOrUndecodedRefuses(t *testing.T) {
	c := keyTailCase{
		ddl: "CREATE TABLE `t` (\n  `id` varbinary(20) NOT NULL,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n",
		pk:  []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "varbinary"}},
	}
	_, _, err := c.fold(t, nil, []query.ResultRow{c.event(1, event.EventInsert, nil, img("id", []byte("0xFF"), "v", num("1")))})
	wantRefusal(t, err, "starts with the text 0x")
	_, _, err = c.fold(t, nil, []query.ResultRow{c.event(1, event.EventInsert, nil, img("id", "AQI=", "v", num("1")))})
	wantRefusal(t, err, "was not decoded")
	// The empty value, inserted or deleted: an earlier version wrote it into
	// the copy as the bytes 0x, this one empty, and the two do not match.
	// The bytes 0x themselves are the text case above.
	for _, ev := range []query.ResultRow{
		c.event(1, event.EventInsert, nil, img("id", []byte{}, "v", num("1"))),
		c.event(1, event.EventDelete, img("id", []byte{}, "v", num("1")), nil),
		c.event(1, event.EventUpdate, img("id", []byte{}, "v", num("1")), img("id", []byte{1}, "v", num("1"))),
	} {
		_, _, err = c.fold(t, nil, []query.ResultRow{ev})
		wantRefusal(t, err, "the empty value")
	}
	_, _, err = c.fold(t, nil, []query.ResultRow{c.event(1, event.EventInsert, nil, img("id", []byte("0x"), "v", num("1")))})
	wantRefusal(t, err, "starts with the text 0x")
	// The text "0X…" is not formatPKValue's mark (lower case x): kept.
	if _, _, err = c.fold(t, nil, []query.ResultRow{c.event(1, event.EventInsert, nil, img("id", []byte("0XFF"), "v", num("1")))}); err != nil {
		t.Fatal(err)
	}
}

// DATETIME with and without fractional seconds, before and after the epoch,
// and a dump that printed .000000 or nothing: the anti-join compares
// microseconds, not text. (TIMESTAMP has no tail: TestNewTail_refusals.)
func TestTailKey_datetime(t *testing.T) {
	for _, typ := range []string{"datetime(6)"} {
		t.Run(typ, func(t *testing.T) {
			c := keyTailCase{
				ddl:    "CREATE TABLE `t` (\n  `at` " + typ + " NOT NULL,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`at`)\n);\n",
				pk:     []metadata.ColumnMeta{{Name: "at", IsPK: true, DataType: strings.TrimSuffix(typ, "(6)"), ColumnType: typ}},
				render: `strftime(make_timestamp(epoch_us("at")), '%Y-%m-%d %H:%M:%S.%f') || '=' || CAST(v AS VARCHAR)`,
			}
			base := []map[string]string{
				{"at": "2024-01-02 03:04:05.000000", "v": "1"},
				{"at": "2024-01-02 03:04:06", "v": "2"}, // a dump that printed no fraction
				{"at": "2024-01-02 03:04:05.123456", "v": "3"},
				{"at": "1969-12-31 23:59:59.999999", "v": "4"},
			}
			got := c.state(t, base, []query.ResultRow{
				c.event(1, event.EventDelete, img("at", "2024-01-02 03:04:05.000000", "v", num("1")), nil),
				c.event(2, event.EventUpdate, img("at", "2024-01-02 03:04:06.000000", "v", num("2")), img("at", "2024-01-02 03:04:06.000000", "v", num("20"))),
				c.event(3, event.EventUpdate, img("at", "1969-12-31 23:59:59.999999", "v", num("4")), img("at", "1969-12-31 23:59:59.999998", "v", num("4"))),
				c.event(4, event.EventInsert, nil, img("at", "2024-01-02 03:04:05.000001", "v", num("5"))),
			})
			wantKeyRows(t, got,
				"2024-01-02 03:04:06.000000=20", "2024-01-02 03:04:05.123456=3",
				"1969-12-31 23:59:59.999998=4", "2024-01-02 03:04:05.000001=5")
		})
	}
	t.Run("datetime(0)", func(t *testing.T) {
		c := keyTailCase{
			ddl:    "CREATE TABLE `t` (\n  `at` datetime NOT NULL,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`at`)\n);\n",
			pk:     []metadata.ColumnMeta{{Name: "at", IsPK: true, DataType: "datetime", ColumnType: "datetime"}},
			render: `strftime(make_timestamp(epoch_us("at")), '%Y-%m-%d %H:%M:%S') || '=' || CAST(v AS VARCHAR)`,
		}
		got := c.state(t, []map[string]string{{"at": "2024-01-02 03:04:05", "v": "1"}, {"at": "2024-01-02 03:04:07", "v": "2"}}, []query.ResultRow{
			c.event(1, event.EventDelete, img("at", "2024-01-02 03:04:05", "v", num("1")), nil),
			c.event(2, event.EventUpdate, img("at", "2024-01-02 03:04:07", "v", num("2")), img("at", "2024-01-02 03:04:08", "v", num("2"))),
		})
		wantKeyRows(t, got, "2024-01-02 03:04:08=2")
	})
}

// DATE, the leap day and the day before the epoch among them. The zero date
// is refused: the copy stores it as NULL, which no tombstone matches.
func TestTailKey_date(t *testing.T) {
	c := keyTailCase{
		ddl:    "CREATE TABLE `t` (\n  `d` date NOT NULL,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`d`)\n);\n",
		pk:     []metadata.ColumnMeta{{Name: "d", IsPK: true, DataType: "date", ColumnType: "date"}},
		render: `CAST(d AS VARCHAR) || '=' || CAST(v AS VARCHAR)`,
	}
	got := c.state(t, []map[string]string{{"d": "2024-02-29", "v": "1"}, {"d": "1969-12-31", "v": "2"}, {"d": "2024-03-01", "v": "3"}}, []query.ResultRow{
		c.event(1, event.EventDelete, img("d", "2024-02-29", "v", num("1")), nil),
		c.event(2, event.EventUpdate, img("d", "1969-12-31", "v", num("2")), img("d", "1969-12-31", "v", num("20"))),
		c.event(3, event.EventInsert, nil, img("d", "1970-01-01", "v", num("4"))),
	})
	wantKeyRows(t, got, "1969-12-31=20", "2024-03-01=3", "1970-01-01=4")

	for _, zero := range []string{"0000-00-00", "2024-00-10", "2024-02-30"} {
		_, _, err := c.fold(t, nil, []query.ResultRow{c.event(1, event.EventInsert, nil, img("d", zero, "v", num("1")))})
		wantRefusal(t, err, "not a date the copy can store")
	}
	dt := keyTailCase{
		ddl: "CREATE TABLE `t` (\n  `at` datetime NOT NULL,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`at`)\n);\n",
		pk:  []metadata.ColumnMeta{{Name: "at", IsPK: true, DataType: "datetime"}},
	}
	_, _, err := dt.fold(t, nil, []query.ResultRow{dt.event(1, event.EventDelete, img("at", "0000-00-00 00:00:00", "v", num("1")), nil)})
	wantRefusal(t, err, "not a date the copy can store")
}

// DECIMAL: the copy and go-mysql both print the server's text with the
// column's scale, so 1.5 is "1.50" on both sides; DECIMAL(n,0) has no point.
// A negative zero is refused.
func TestTailKey_decimal(t *testing.T) {
	c := keyTailCase{
		ddl:    "CREATE TABLE `t` (\n  `x` decimal(10,2) NOT NULL,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`x`)\n);\n",
		pk:     []metadata.ColumnMeta{{Name: "x", IsPK: true, DataType: "decimal", ColumnType: "decimal(10,2)"}},
		render: `CAST(x AS VARCHAR) || '=' || CAST(v AS VARCHAR)`,
	}
	got := c.state(t, []map[string]string{{"x": "1.50", "v": "1"}, {"x": "-1.25", "v": "2"}, {"x": "0.00", "v": "3"}, {"x": "10.00", "v": "4"}}, []query.ResultRow{
		c.event(1, event.EventDelete, img("x", "1.50", "v", num("1")), nil),
		c.event(2, event.EventUpdate, img("x", "-1.25", "v", num("2")), img("x", "-1.25", "v", num("20"))),
		c.event(3, event.EventUpdate, img("x", "0.00", "v", num("3")), img("x", "0.50", "v", num("3"))),
		c.event(4, event.EventInsert, nil, img("x", "2.50", "v", num("5"))),
	})
	wantKeyRows(t, got, "-1.25=20", "0.50=3", "10.00=4", "2.50=5")

	// Minus zero, not a number, a float, and a scale other than the
	// column's (1.5 and 1.500 in a DECIMAL(10,2) are not the copy's 1.50).
	for _, bad := range []any{"-0.00", "1e2", "", 1.5, "1.5", "1.500", "2"} {
		_, _, err := c.fold(t, nil, []query.ResultRow{c.event(1, event.EventInsert, nil, img("x", bad, "v", num("1")))})
		if err == nil {
			t.Errorf("%v: folded", bad)
		}
	}
	whole := keyTailCase{
		ddl:    "CREATE TABLE `t` (\n  `x` decimal(10,0) NOT NULL,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`x`)\n);\n",
		pk:     []metadata.ColumnMeta{{Name: "x", IsPK: true, DataType: "decimal", ColumnType: "decimal(10,0)"}},
		render: c.render,
	}
	got = whole.state(t, []map[string]string{{"x": "5", "v": "1"}, {"x": "-7", "v": "2"}}, []query.ResultRow{
		whole.event(1, event.EventDelete, img("x", "-7", "v", num("2")), nil),
	})
	wantKeyRows(t, got, "5=1")
	if _, _, err := whole.fold(t, nil, []query.ResultRow{whole.event(1, event.EventInsert, nil, img("x", "5.0", "v", num("1")))}); err == nil {
		t.Error("5.0 folded into a DECIMAL(10,0)")
	}
}

// A composite key mixing an integer with the new types: every part of the
// tombstone is read back by its own column's kind.
func TestTailKey_composite(t *testing.T) {
	c := keyTailCase{
		ddl: "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `b` varbinary(8) NOT NULL,\n  `d` date NOT NULL,\n  `x` decimal(6,2) NOT NULL,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`id`,`b`,`d`,`x`)\n);\n",
		pk: []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "int"}, {Name: "b", IsPK: true, DataType: "varbinary"},
			{Name: "d", IsPK: true, DataType: "date"}, {Name: "x", IsPK: true, DataType: "decimal"}},
		render: `CAST(id AS VARCHAR) || '/' || hex(b) || '/' || CAST(d AS VARCHAR) || '/' || CAST(x AS VARCHAR) || '=' || CAST(v AS VARCHAR)`,
	}
	base := []map[string]string{
		{"id": "1", "b": "0xff", "d": "2024-01-01", "x": "1.00", "v": "1"},
		{"id": "1", "b": "0x" + hex.EncodeToString([]byte("a|")), "d": "2024-01-01", "x": "1.00", "v": "2"},
		{"id": "-2", "b": "0xff", "d": "2024-01-02", "x": "-1.00", "v": "3"},
	}
	got := c.state(t, base, []query.ResultRow{
		c.event(1, event.EventDelete, img("id", num("1"), "b", []byte{0xff}, "d", "2024-01-01", "x", "1.00", "v", num("1")), nil),
		c.event(2, event.EventDelete, img("id", num("1"), "b", []byte("a|"), "d", "2024-01-01", "x", "1.00", "v", num("2")), nil),
		c.event(3, event.EventUpdate, img("id", num("-2"), "b", []byte{0xff}, "d", "2024-01-02", "x", "-1.00", "v", num("3")),
			img("id", num("-2"), "b", []byte{0xff}, "d", "2024-01-02", "x", "-1.00", "v", num("30"))),
	})
	wantKeyRows(t, got, "-2/FF/2024-01-02/-1.00=30")
}

// The copy must store a key column as this version writes it: a DATE a
// producer wrote as text would compare unequal to every day number the tail
// writes, so the deleted rows would stay.
func TestNewTail_keyStoredAsAnotherTypeRefuses(t *testing.T) {
	asText := "CREATE TABLE `t` (\n  `d` varchar(10) NOT NULL,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`d`)\n);\n"
	asDate := strings.Replace(asText, "varchar(10)", "date", 1)
	path := writeTailBase(t, t.TempDir(), "t.parquet", asText, map[string]string{"d": "2024-01-01", "v": "1"})
	_, err := NewTail("s", "t", asDate, path, []metadata.ColumnMeta{{Name: "d", IsPK: true, DataType: "date"}})
	wantRefusal(t, err, "stores key column(s) d as another type")
}

// The tombstone's split: escaped parts, hex read back only where pk_values
// writes hex, and every part checked by its column's kind.
func TestTailKeyValues_byKind(t *testing.T) {
	bin := keyCol{name: "b", kind: keyBinary}
	fixed := keyCol{name: "f", kind: keyFixedBinary, width: 2}
	dec := keyCol{name: "x", kind: keyDecimal, scale: 2}
	date := keyCol{name: "d", kind: keyDate}
	for _, c := range []struct {
		key  string
		keys []keyCol
		want string // fmt of the values, or "" for an error
	}{
		{"0xFFFE", []keyCol{bin}, "[[255 254]]"},
		{`a\|b`, []keyCol{bin}, "[[97 124 98]]"},
		{"", []keyCol{bin}, ""}, // the empty value
		{"0x", []keyCol{bin}, ""}, // hex of nothing: the empty value again
		{"0xZZ", []keyCol{bin}, ""},
		{"0xFF", []keyCol{fixed}, ""}, // shorter than BINARY(2)
		{"0xFF01", []keyCol{fixed}, "[[255 1]]"},
		{"1.50|2024-01-01", []keyCol{dec, date}, "[1.50 2024-01-01]"},
		{"-0.00", []keyCol{dec}, ""},
		{"1.50", []keyCol{dec, date}, ""},
		{"0000-00-00", []keyCol{date}, ""},
	} {
		vals, err := tailKeyValues(c.key, c.keys)
		got := ""
		if err == nil {
			got = fmt.Sprint(vals)
		}
		if got != c.want {
			t.Errorf("%q: got %q (err %v), want %q", c.key, got, err, c.want)
		}
	}
}

// The key an event is filed under is its stored pk_values; the image the
// tail writes its tombstone and its row from must spell exactly that key,
// or one row is dropped and another written (an image decoded under another
// schema snapshot than capture used). Checked on every event of a key that
// is not all integers: a DELETE, both sides of an UPDATE, an INSERT.
func TestTailKey_imageMustSpellTheStoredKey(t *testing.T) {
	c := keyTailCase{
		ddl: "CREATE TABLE `t` (\n  `d` date NOT NULL,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`d`)\n);\n",
		pk:  []metadata.ColumnMeta{{Name: "d", IsPK: true, DataType: "date"}},
	}
	for _, ev := range []query.ResultRow{
		c.event(1, event.EventDelete, img("d", "2024-01-02", "v", num("1")), nil),
		c.event(1, event.EventUpdate, img("d", "2024-01-02", "v", num("1")), img("d", "2024-01-02", "v", num("2"))),
		c.event(1, event.EventInsert, nil, img("d", "2024-01-02", "v", num("1"))),
	} {
		ev.PKValues = "2024-01-03"
		_, _, err := c.fold(t, nil, []query.ResultRow{ev})
		wantRefusal(t, err, "its stored key is")
	}
	// A binary key stored in hex and decoded to other bytes.
	b := keyTailCase{
		ddl: "CREATE TABLE `t` (\n  `id` varbinary(8) NOT NULL,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n",
		pk:  []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "varbinary"}},
	}
	ev := b.event(1, event.EventDelete, img("id", []byte{0xff, 0x01}, "v", num("1")), nil)
	ev.PKValues = "0xFF02"
	_, _, err := b.fold(t, nil, []query.ResultRow{ev})
	wantRefusal(t, err, "its stored key is")
	// Integer keys are not checked, as before.
	tail, err := NewTail("shop", "orders", tailOrdersDDL, writeTailBase(t, t.TempDir(), "orders.parquet", tailOrdersDDL), tailOrdersPK)
	if err != nil {
		t.Fatal(err)
	}
	if err := tail.Apply([]query.ResultRow{tailEvent(1, event.EventInsert, "7", "x", 10)}); err != nil {
		t.Fatal(err)
	}
}

// A tombstone that cannot be built ends the tail (a *TailRefusal the
// keeper drops the tail on), not an error that leaves the last version up
// and is tried again at every write.
func TestTail_tombstoneThatCannotBeBuiltRefuses(t *testing.T) {
	c := keyTailCase{
		ddl: "CREATE TABLE `t` (\n  `x` decimal(10,2) NOT NULL,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`x`)\n);\n",
		pk:  []metadata.ColumnMeta{{Name: "x", IsPK: true, DataType: "decimal"}},
	}
	tail, _, err := c.fold(t, nil, []query.ResultRow{c.event(1, event.EventDelete, img("x", "1.50", "v", num("1")), nil)})
	if err != nil {
		t.Fatal(err)
	}
	// A key no event of this build files: what a fold bug would leave.
	ev := tail.fold.Changes["1.50"]
	delete(tail.fold.Changes, "1.50")
	tail.fold.Changes["1.5"] = ev
	_, err = tail.WriteFile(filepath.Join(t.TempDir(), "t.tail.parquet"))
	wantRefusal(t, err, "not 2 digits after the point")
	// So is an upsert with no row image.
	tail2, _, err := c.fold(t, nil, []query.ResultRow{c.event(1, event.EventInsert, nil, img("x", "1.50", "v", num("1")))})
	if err != nil {
		t.Fatal(err)
	}
	tail2.fold.Changes["1.50"].RowAfter = nil
	_, err = tail2.WriteFile(filepath.Join(t.TempDir(), "t.tail.parquet"))
	wantRefusal(t, err, "has no row image")
}

// An empty binary value is written as the empty text, which the copy's
// writer stores as the empty value; "0x" alone it would store as two bytes.
// For a column that is not a key (an empty key has no tail).
func TestRenderBaselineValue_emptyBinary(t *testing.T) {
	for _, typ := range []string{"varbinary", "blob", "binary"} {
		got, isNull, err := renderBaselineValue(baseline.Column{Name: "b", MySQLType: typ}, []byte{})
		if err != nil || isNull || got != "" {
			t.Errorf("%s: %q null=%v err=%v, want the empty text", typ, got, isNull, err)
		}
	}
	if got, _, _ := renderBaselineValue(baseline.Column{Name: "b", MySQLType: "varbinary"}, []byte{0x30, 0x78}); got != "0x3078" {
		t.Errorf("the bytes 0x: %q", got)
	}
}
