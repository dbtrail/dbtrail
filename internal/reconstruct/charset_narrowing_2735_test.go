package reconstruct

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
)

func TestCharsetHolds(t *testing.T) {
	for _, c := range []struct {
		cs, s        string
		holds, known bool
	}{
		{"utf8mb4", "Ñandú ✓😀", true, true},
		{"utf32", "😀", true, true},
		{"gb18030", "😀", true, true},
		{"utf8mb3", "Ñandú ✓", true, true},
		{"utf8mb3", "x😀", false, true},
		{"ucs2", "😀", false, true},
		{"latin1", "Ñandú €Ÿ", true, true},
		{"latin1", "\u0081\u009d", true, true}, // the bytes Windows-1252 leaves empty
		{"latin1", "\u0080", false, true},
		{"latin1", "Ñandú ✓", false, true},
		{"latin1", "", true, true},
		{"ascii", "plain", true, true},
		{"ascii", "Ñ", false, true},
		{"cp1251", "plain", false, false},
		{"", "plain", false, false},
	} {
		holds, known := charsetHolds(c.cs, c.s)
		if holds != c.holds || known != c.known {
			t.Errorf("charsetHolds(%q, %q) = %v, %v; want %v, %v", c.cs, c.s, holds, known, c.holds, c.known)
		}
	}
}

func TestCharsetChangeKind(t *testing.T) {
	for _, c := range []struct {
		from, to string
		want     int
	}{
		{"utf8mb4", "utf8mb4", charsetKept},
		{"latin1", "utf8mb4", charsetKept}, // widening: every value kept
		{"utf8mb3", "utf8mb4", charsetKept},
		{"ascii", "latin1", charsetKept},
		{"latin1", "utf8mb3", charsetKept},
		{"utf8mb3", "ucs2", charsetKept},
		{"utf8mb4", "", charsetKept}, // the snapshot says nothing
		{"utf8mb4", "latin1", charsetChecked},
		{"utf8mb4", "utf8mb3", charsetChecked},
		{"utf8mb4", "ascii", charsetChecked},
		{"latin1", "ascii", charsetChecked},
		{"", "latin1", charsetChecked}, // set before not recorded: the value decides
		{"utf8mb4", "cp1251", charsetUncheckable},
		{"cp1251", "latin1", charsetUncheckable}, // its values may be stored bytes
		{"", "cp1251", charsetUncheckable},
	} {
		if got := charsetChangeKind(c.from, c.to); got != c.want {
			t.Errorf("charsetChangeKind(%q, %q) = %d, want %d", c.from, c.to, got, c.want)
		}
	}
}

func TestWrittenBefore(t *testing.T) {
	at := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	ddl := destructiveDDL{ID: 7, File: "binlog.000003", Pos: 1000, DetectedAt: at}
	ev := func(file string, start uint64, ts time.Time) *query.ResultRow {
		return &query.ResultRow{BinlogFile: file, StartPos: start, EventTimestamp: ts}
	}
	for _, c := range []struct {
		label string
		ev    *query.ResultRow
		d     destructiveDDL
		want  bool
	}{
		{"a baseline row", nil, ddl, true},
		{"earlier in the same file", ev("binlog.000003", 500, at.Add(time.Hour)), ddl, true},
		{"starting where the ALTER ends", ev("binlog.000003", 1000, at.Add(-time.Hour)), ddl, false},
		{"later in the same file", ev("binlog.000003", 1500, at), ddl, false},
		{"an earlier file", ev("binlog.000002", 99999, at), ddl, true},
		{"a later file", ev("binlog.000004", 4, at), ddl, false},
		{"another sequence, earlier time", ev("mysql-bin.000009", 4, at.Add(-time.Second)), ddl, true},
		{"another sequence, the same second", ev("mysql-bin.000009", 4, at), ddl, true},
		{"another sequence, later time", ev("mysql-bin.000009", 4, at.Add(time.Second)), ddl, false},
		{"an ALTER with no position, later time", ev("binlog.000003", 4, at.Add(time.Second)), destructiveDDL{DetectedAt: at}, false},
		{"a spilled change with no time, another sequence", ev("mysql-bin.000009", 4, time.Time{}), ddl, true},
	} {
		if got := writtenBefore(c.ev, c.d); got != c.want {
			t.Errorf("%s: writtenBefore = %v, want %v", c.label, got, c.want)
		}
	}
}

func TestCheckRow(t *testing.T) {
	at := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	narrow := destructiveDDL{ID: 7, Type: "ALTER TABLE", File: "binlog.000003", Pos: 1000, DetectedAt: at}
	n := &CharsetNarrowings{schema: "shop", table: "t", checked: []charsetNarrowing{
		{Column: "name", From: "utf8mb4", To: "latin1", DDL: narrow},
		{Column: "note", From: "utf8mb4", To: "utf8mb3", DDL: narrow},
		{Column: "code", From: "latin1", To: "ascii", DDL: narrow},
	}}
	before := &query.ResultRow{BinlogFile: "binlog.000003", StartPos: 400}
	after := &query.ResultRow{BinlogFile: "binlog.000003", StartPos: 1200}
	afterUndo := &query.ResultRow{BinlogFile: "binlog.000003", StartPos: 2400}
	for _, c := range []struct {
		label  string
		row    map[string]any
		ev     *query.ResultRow
		refuse bool
	}{
		{"baseline row with a letter latin1 cannot hold", map[string]any{"id": 1, "name": "Ñandú ✓"}, nil, true},
		{"baseline row latin1 holds", map[string]any{"id": 1, "name": "Ñandú €"}, nil, false},
		{"change before the ALTER that lost a letter", map[string]any{"name": "✓"}, before, true},
		{"change after the ALTER", map[string]any{"name": "✓"}, after, false},
		{"emoji into utf8mb3", map[string]any{"name": "ok", "note": "hi 😀"}, nil, true},
		{"three-byte letter into utf8mb3", map[string]any{"name": "ok", "note": "✓"}, nil, false},
		{"NULL", map[string]any{"name": nil}, nil, false},
		{"column absent from the row", map[string]any{"id": 1}, nil, false},
		{"column spelled in another case", map[string]any{"NAME": "✓"}, nil, true},
		{"bytes", map[string]any{"name": []byte("✓")}, nil, true},
		{"stored bytes that are not UTF-8", map[string]any{"name": []byte{0xff, 0xfe}}, nil, false},
		{"stored bytes that are not UTF-8, into ascii", map[string]any{"code": []byte{'a', 0xe9}}, nil, true},
		{"ascii bytes into ascii", map[string]any{"code": []byte("abc")}, nil, false},
		{"a number", map[string]any{"name": 42}, nil, false},
	} {
		err := n.CheckRow(c.row, c.ev)
		if (err != nil) != c.refuse {
			t.Errorf("%s: CheckRow = %v, want refuse=%v", c.label, err, c.refuse)
			continue
		}
		if err == nil {
			continue
		}
		if !errors.Is(err, ErrDestructiveDDL) {
			t.Errorf("%s: %v does not wrap ErrDestructiveDDL", c.label, err)
		}
		for _, w := range []string{"shop.t", "binlog.000003:1000", "row 7 of schema_changes", "'?'"} {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%s: %q does not name %q", c.label, err, w)
			}
		}
		// Never the value: the refusal reaches sessions that may not read it.
		for _, v := range []string{"✓", "😀", "Ñandú"} {
			if strings.Contains(err.Error(), v) {
				t.Errorf("%s: the refusal %q carries the row's value", c.label, err)
			}
		}
	}

	// A narrowing and its undo: a value written after the undo is in a
	// utf8mb4 column again and is not checked against latin1.
	undone := &CharsetNarrowings{schema: "shop", table: "t", checked: []charsetNarrowing{
		{Column: "name", From: "utf8mb4", To: "latin1", DDL: narrow},
	}}
	if err := undone.CheckRow(map[string]any{"name": "✓"}, afterUndo); err != nil {
		t.Errorf("a value written after the column was widened again was refused: %v", err)
	}
	if err := undone.CheckRow(map[string]any{"name": "✓"}, nil); err == nil {
		t.Error("a baseline value from before the narrowing passed although latin1 cannot hold it")
	}

	var none *CharsetNarrowings
	if err := none.CheckRow(map[string]any{"name": "✓"}, nil); err != nil || none.Checked() || none.Notes() != nil || none.UntouchedNote() != "" {
		t.Errorf("a nil CharsetNarrowings checked something: %v", err)
	}
	if note := n.UntouchedNote(); !strings.Contains(note, "column name to latin1") || !strings.Contains(note, "column note to utf8mb3") {
		t.Errorf("UntouchedNote = %q, want both columns and their sets", note)
	}
}

func TestMayChangeCharset(t *testing.T) {
	for text, want := range map[string]bool{
		"ALTER TABLE t CONVERT TO CHARACTER SET latin1":  true,
		"alter table t modify name varchar(20)":          true,
		"ALTER TABLE t CHANGE name name2 VARCHAR(20)":    true,
		"ALTER TABLE t DEFAULT CHARSET=latin1":           true,
		"ALTER TABLE t MODIFY x INT COLLATE utf8mb4_bin": true,
		"ALTER TABLE t ADD INDEX idx_name (name)":        false,
		"ALTER TABLE t ADD COLUMN n INT, DROP COLUMN m":  false,
		"ALTER TABLE t ENGINE=InnoDB":                    false,
	} {
		if got := mayChangeCharset(text); got != want {
			t.Errorf("mayChangeCharset(%q) = %v, want %v", text, got, want)
		}
	}
}
