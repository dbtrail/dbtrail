package reconstruct

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/keyidx"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2496 slice 3: behind BINTRAIL_KEY_INDEX the lookup resolves the window's
// keys against <table>.keyidx when one is beside the base and bound to it;
// the DuckDB join and scan remain the net for every other case, and the
// three paths must return the same positions on the same inputs.

// useKeyIndex sets the switch for the test, bypassing the environment.
func useKeyIndex(t *testing.T, on bool) {
	t.Helper()
	prev := keyIndexEnabled
	keyIndexEnabled = func() bool { return on }
	t.Cleanup(func() { keyIndexEnabled = prev })
}

// countIndexOpens wraps the index-open seam and returns a counter.
func countIndexOpens(t *testing.T) *int {
	t.Helper()
	n := 0
	prev := keyIndexOpen
	keyIndexOpen = func(base string) (*keyidx.Reader, error) {
		n++
		return prev(base)
	}
	t.Cleanup(func() { keyIndexOpen = prev })
	return &n
}

// indexBeside builds the key index beside base on names.
func indexBeside(t *testing.T, base string, names ...string) {
	t.Helper()
	res, err := keyidx.Build(context.Background(), base, names, duckdbutil.DefaultTuning())
	if err != nil || res.Skipped != "" {
		t.Fatalf("keyidx.Build: %v, skipped %q", err, res.Skipped)
	}
}

// threeWays is bothWays (join and scan agree, with their own invariants)
// plus the index path, which must agree with the scan too. It returns the
// index path's stats and positions; the caller says which path it expects.
func threeWays(t *testing.T, base string, pkCols []metadata.ColumnMeta, changes map[string]*query.ResultRow) (lookupStats, []int64) {
	t.Helper()
	_, _, want := bothWays(t, base, pkCols, changes)
	got, st, err := lookupBasePositionsVia(context.Background(), base, "s", "t", pkCols, changes, duckdbutil.DefaultTuning(), true, true)
	if err != nil {
		t.Fatalf("index path: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("index path = %v, scan = %v", got, want)
	}
	if st.Path == "index" {
		if st.Joined || st.BaseReads != 0 || st.Examined != len(got) {
			t.Fatalf("index path stats %+v for %d positions", st, len(got))
		}
	} else if st.IndexBlocks != 0 {
		t.Fatalf("path %q read %d index blocks", st.Path, st.IndexBlocks)
	}
	return st, got
}

// captureDebugLog captures slog at Debug for the test, process-global.
func captureDebugLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func TestKeyIndexOn_parsing_2496r(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		on   bool
		warn bool
	}{
		{"", false, false}, {"0", false, false}, {"false", false, false}, {"FALSE", false, false}, {"no", false, false}, {"off", false, false},
		{"1", true, false}, {"true", true, false}, {"TRUE", true, false}, {" yes ", true, false}, {"on", true, false},
		{"2", false, true}, {"maybe", false, true},
	} {
		on, warning := keyIndexOn(tc.raw)
		if on != tc.on || (warning != "") != tc.warn {
			t.Errorf("%q: on=%v warning=%q; want on=%v warning=%v", tc.raw, on, warning, tc.on, tc.warn)
		}
		if tc.warn && !strings.Contains(warning, KeyIndexEnv) {
			t.Errorf("%q: the warning does not name the variable: %s", tc.raw, warning)
		}
	}
}

// TestLookupBasePositions_switchOffNeverOpensTheIndex_2496r: with the switch
// off the production entry point takes the join, as today, and never opens
// the index beside the base; with it on, the index is opened once per lookup
// and taken, with its own stats shape.
func TestLookupBasePositions_switchOffNeverOpensTheIndex_2496r(t *testing.T) {
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	indexBeside(t, base, "id")
	opens := countIndexOpens(t)
	useKeyIndex(t, false)
	got, st, err := lookupBasePositions(context.Background(), base, "s", "t", pkCols, touched("1", "3"), duckdbutil.DefaultTuning())
	if err != nil || !reflect.DeepEqual(got, []int64{0, 2}) || st.Path != "join" || !st.Joined || *opens != 0 {
		t.Fatalf("switch off: got=%v st=%+v opens=%d err=%v", got, st, *opens, err)
	}
	useKeyIndex(t, true)
	got, st, err = lookupBasePositions(context.Background(), base, "s", "t", pkCols, touched("1", "3"), duckdbutil.DefaultTuning())
	if err != nil || !reflect.DeepEqual(got, []int64{0, 2}) || st.Path != "index" || st.Joined || st.BaseReads != 0 || st.IndexBlocks != 1 || st.Examined != 2 || *opens != 1 {
		t.Fatalf("switch on: got=%v st=%+v opens=%d err=%v", got, st, *opens, err)
	}
	// The scan path with the switch on and no join: the index still goes first.
	got, st, err = lookupBasePositionsVia(context.Background(), base, "s", "t", pkCols, touched("2"), duckdbutil.DefaultTuning(), false, true)
	if err != nil || !reflect.DeepEqual(got, []int64{1}) || st.Path != "index" {
		t.Fatalf("no join, index on: got=%v st=%+v err=%v", got, st, err)
	}
	// Empty window: not called at all (as today), nothing opened.
	before := *opens
	if _, st, err := lookupBasePositions(context.Background(), base, "s", "t", pkCols, nil, duckdbutil.DefaultTuning()); err != nil || st.Path != "" || *opens != before {
		t.Fatalf("empty window: st=%+v opens=%d err=%v", st, *opens, err)
	}
}

// TestLookupBasePositions_indexAgreesWithJoinAndScan_2496r: every fixture
// the join tests use, and the shapes the index adds, resolved three ways.
func TestLookupBasePositions_indexAgreesWithJoinAndScan_2496r(t *testing.T) {
	t.Run("negatives, absent, present", func(t *testing.T) {
		var rows [][]string
		for i := range 100 {
			rows = append(rows, []string{fmt.Sprint(i*3 - 50), "x"})
		}
		base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, rows)
		indexBeside(t, base, "id")
		st, got := threeWays(t, base, pkCols, touched("-50", "1", "247", "999999", "-51"))
		if st.Path != "index" || !reflect.DeepEqual(got, []int64{0, 17, 99}) || st.IndexBlocks != 1 {
			t.Fatalf("st=%+v got=%v", st, got)
		}
	})
	t.Run("composite key, header order is not the key's order", func(t *testing.T) {
		// Columns named so the file's (alphabetical) order is the reverse of
		// the PRIMARY KEY's: the order_line shape.
		ddl := "CREATE TABLE `t` (\n  `w_id` smallint NOT NULL,\n  `d_id` bigint unsigned NOT NULL,\n  `o_id` int NOT NULL,\n  `v` varchar(8) DEFAULT NULL,\n  PRIMARY KEY (`w_id`,`o_id`,`d_id`)\n) ENGINE=InnoDB;\n"
		base, pkCols := writeKeyedBaseline(t, ddl, []string{"w_id", "o_id", "d_id"}, [][]string{
			{"1", "1", "10", "a"}, {"1", "2", "10", "b"}, {"2", "1", "20", "c"}, {"2", "18446744073709551615", "1", "d"}, {"-3", "0", "-7", "e"},
			{"-3", "0", "-7", "e again"},
		})
		indexBeside(t, base, "w_id", "o_id", "d_id")
		r, err := keyidx.Open(base)
		if err != nil {
			t.Fatal(err)
		}
		names := r.Names()
		r.Close()
		if reflect.DeepEqual(names, []string{"w_id", "o_id", "d_id"}) {
			t.Fatalf("fixture: the header's order %v is the key's order; the test needs them to differ", names)
		}
		st, got := threeWays(t, base, pkCols, touched("1|10|2", "2|1|18446744073709551615", "-3|-7|0", "1|10|99", "10|1|2"))
		if st.Path != "index" || !reflect.DeepEqual(got, []int64{1, 3, 4, 5}) {
			t.Fatalf("st=%+v got=%v", st, got)
		}
	})
	t.Run("every integer width at its edges", func(t *testing.T) {
		ddl := "CREATE TABLE `t` (\n  `a` tinyint NOT NULL,\n  `b` int unsigned NOT NULL,\n  `c` year NOT NULL,\n  `d` mediumint NOT NULL,\n  `e` bigint NOT NULL,\n  PRIMARY KEY (`a`,`b`,`c`,`d`,`e`)\n) ENGINE=InnoDB;\n"
		base, pkCols := writeKeyedBaseline(t, ddl, []string{"a", "b", "c", "d", "e"}, [][]string{
			{"-128", "4294967295", "2026", "-8388608", "-9223372036854775808"}, {"127", "0", "1901", "8388607", "9223372036854775807"}, {"0", "7", "2000", "0", "0"},
		})
		indexBeside(t, base, "a", "b", "c", "d", "e")
		st, got := threeWays(t, base, pkCols, touched("-128|4294967295|2026|-8388608|-9223372036854775808", "0|7|2000|0|0", "127|1|1901|8388607|9223372036854775807", "127|0|1901|8388607|9223372036854775807"))
		if st.Path != "index" || !reflect.DeepEqual(got, []int64{0, 1, 2}) {
			t.Fatalf("st=%+v got=%v", st, got)
		}
	})
	t.Run("duplicates in the base yield every position", func(t *testing.T) {
		base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"5", "a"}, {"1", "b"}, {"5", "c"}, {"2", "d"}, {"5", "e"}})
		indexBeside(t, base, "id")
		st, got := threeWays(t, base, pkCols, touched("5", "2"))
		if st.Path != "index" || !reflect.DeepEqual(got, []int64{0, 2, 3, 4}) {
			t.Fatalf("st=%+v got=%v", st, got)
		}
	})
	t.Run("keys all absent, all out of range, and a rejected spelling", func(t *testing.T) {
		base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
		indexBeside(t, base, "id")
		st, got := threeWays(t, base, pkCols, touched("7", "9"))
		if st.Path != "index" || len(got) != 0 || st.Examined != 0 || st.IndexBlocks != 1 {
			t.Fatalf("absent: st=%+v got=%v", st, got)
		}
		st, got = threeWays(t, base, pkCols, touched("-1"))
		if st.Path != "index" || len(got) != 0 || st.IndexBlocks != 0 {
			t.Fatalf("absent below the first key: st=%+v got=%v", st, got)
		}
		st, got = threeWays(t, base, pkCols, touched("5000000000", "-5000000000"))
		if st.Path != "index" || len(got) != 0 || st.IndexBlocks != 0 {
			t.Fatalf("out of range: st=%+v got=%v", st, got)
		}
		st, got = threeWays(t, base, pkCols, touched("3", "5000000000"))
		if st.Path != "index" || !reflect.DeepEqual(got, []int64{2}) {
			t.Fatalf("one in range: st=%+v got=%v", st, got)
		}
		// A spelling the join refuses sends the WHOLE lookup to the scan, as
		// today: the index never decides on a key it could not read exactly.
		for _, odd := range []string{"007", "-0", "+2", " 2", "2.0", "1|2", "", "abc"} {
			st, got = threeWays(t, base, pkCols, touched("1", odd))
			if st.Path != "scan" || !reflect.DeepEqual(got, []int64{0}) {
				t.Fatalf("key %q: st=%+v got=%v, want the scan", odd, st, got)
			}
		}
	})
	t.Run("block boundaries and non-adjacent blocks", func(t *testing.T) {
		var rows [][]string
		for i := range 10000 {
			rows = append(rows, []string{fmt.Sprint((i * 7919) % 10000), "x"}) // a permutation: scattered
		}
		base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, rows)
		indexBeside(t, base, "id")
		for _, tc := range []struct {
			keys   []string
			blocks int
			n      int
		}{
			{[]string{"0"}, 1, 1}, {[]string{"9999"}, 1, 1},
			{[]string{"4095"}, 1, 1}, {[]string{"4096"}, 2, 1}, {[]string{"4097"}, 1, 1},
			{[]string{"8191"}, 1, 1}, {[]string{"8192"}, 2, 1},
			{[]string{"10", "9000"}, 2, 2}, {[]string{"4000", "4200", "9000"}, 3, 3},
			{[]string{"5000", "6000", "7000"}, 1, 3},
		} {
			st, got := threeWays(t, base, pkCols, touched(tc.keys...))
			if st.Path != "index" || st.IndexBlocks != tc.blocks || len(got) != tc.n {
				t.Fatalf("keys %v: st=%+v got %d positions; want %d blocks, %d positions", tc.keys, st, len(got), tc.blocks, tc.n)
			}
		}
	})
	t.Run("many keys over every block", func(t *testing.T) {
		var rows [][]string
		for i := range 20000 {
			rows = append(rows, []string{fmt.Sprint(i), "x"})
		}
		base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, rows)
		indexBeside(t, base, "id")
		var keys []string
		for i := 0; i < 20000; i += 3 {
			keys = append(keys, fmt.Sprint(i))
		}
		keys = append(keys, "20001", "-1")
		st, got := threeWays(t, base, pkCols, touched(keys...))
		if st.Path != "index" || len(got) != 6667 || st.IndexBlocks != 5 {
			t.Fatalf("st=%+v n=%d", st, len(got))
		}
	})
	t.Run("single block and empty base", func(t *testing.T) {
		base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}})
		indexBeside(t, base, "id")
		st, got := threeWays(t, base, pkCols, touched("1", "2"))
		if st.Path != "index" || !reflect.DeepEqual(got, []int64{0}) || st.IndexBlocks != 1 {
			t.Fatalf("one row: st=%+v got=%v", st, got)
		}
		empty, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, nil)
		indexBeside(t, empty, "id")
		st, got = threeWays(t, empty, pkCols, touched("1", "2"))
		if st.Path != "index" || len(got) != 0 || st.IndexBlocks != 0 {
			t.Fatalf("empty: st=%+v got=%v", st, got)
		}
	})
	t.Run("quoted column names", func(t *testing.T) {
		ddl := "CREATE TABLE `t` (\n  `order` int NOT NULL,\n  `it's` int NOT NULL,\n  PRIMARY KEY (`order`,`it's`)\n) ENGINE=InnoDB;\n"
		base, pkCols := writeKeyedBaseline(t, ddl, []string{"order", "it's"}, [][]string{{"1", "1"}, {"1", "2"}, {"2", "2"}})
		indexBeside(t, base, "order", "it's")
		st, got := threeWays(t, base, pkCols, touched("1|2", "2|2"))
		if st.Path != "index" || !reflect.DeepEqual(got, []int64{1, 2}) {
			t.Fatalf("st=%+v got=%v", st, got)
		}
	})
}

// retrailIndex rewrites the trailing CRC of a forged index (one int key of
// keyWidth bytes, records records) so only the forgery is what the reader
// sees. Mirrors the format in the keyidx package doc.
func retrailIndex(raw []byte, name string, keyWidth int, records int64) []byte {
	headerLen := keyidx.HeaderSize([]string{name})
	nblocks := (records + keyidx.BlockRecords - 1) / keyidx.BlockRecords
	skipLen := nblocks * int64(keyWidth+4)
	tailStart := headerLen + records*int64(keyWidth+8)
	tbl := crc32.MakeTable(crc32.Castagnoli)
	got := crc32.Checksum(raw[:headerLen], tbl)
	got = crc32.Update(got, tbl, raw[tailStart:tailStart+skipLen])
	binary.BigEndian.PutUint32(raw[tailStart+skipLen:], got)
	return raw
}

// TestLookupBasePositions_indexFallbacks_2496r: every way the index can be
// unusable falls back to the join (or scan) with the same positions, absence
// at Debug and damage at WARN, naming the cause; never an error.
func TestLookupBasePositions_indexFallbacks_2496r(t *testing.T) {
	rows := [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}, {"-4", "d"}}
	keys := touched("1", "3", "-4", "9")
	want := []int64{0, 2, 3}
	type tc struct {
		name    string
		prepare func(t *testing.T, base string)
		level   string // "DEBUG" or "WARN"
		text    string // what the log line must carry
	}
	for _, c := range []tc{
		{"no index beside the base", func(t *testing.T, base string) {}, "DEBUG", "no key index"},
		{"index unbound: base rewritten under it", func(t *testing.T, base string) {
			indexBeside(t, base, "id")
			cols, _ := baseline.ParseSchemaText(intKeyDDL)
			w, err := baseline.NewWriter(base, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 2})
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range rows {
				if err := w.WriteRow(r, make([]bool, len(r))); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
		}, "WARN", "not bound"},
		{"corrupt body", func(t *testing.T, base string) {
			indexBeside(t, base, "id")
			flip(t, keyidx.PathFor(base), hdr("id")+5)
		}, "WARN", "corrupt"},
		{"corrupt skip table", func(t *testing.T, base string) {
			indexBeside(t, base, "id")
			flip(t, keyidx.PathFor(base), hdr("id")+4*12+1)
		}, "WARN", "corrupt"},
		{"corrupt header name", func(t *testing.T, base string) {
			indexBeside(t, base, "id")
			flip(t, keyidx.PathFor(base), hdr()+1+2)
		}, "WARN", "corrupt"},
		{"names mismatch: built on another key set", func(t *testing.T, base string) {
			indexBeside(t, base, "v")
		}, "WARN", "key columns"},
		{"type mismatch: header forged", func(t *testing.T, base string) {
			indexBeside(t, base, "id")
			p := keyidx.PathFor(base)
			raw, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			raw[hdr()] = 2 // INTEGER -> BIGINT: size no longer agrees -> corrupt, not mismatch; see the bigint case below
			if err := os.WriteFile(p, retrailIndex(raw, "id", 4, 4), 0o644); err != nil {
				t.Fatal(err)
			}
		}, "WARN", "corrupt"},
	} {
		t.Run(c.name, func(t *testing.T) {
			base, pkCols := writeKeyedBaseline(t, "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `v` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n", []string{"id"}, [][]string{{"1", "7"}, {"2", "7"}, {"3", "7"}, {"-4", "7"}})
			c.prepare(t, base)
			log := captureDebugLog(t)
			st, got := threeWays(t, base, pkCols, keys)
			if st.Path != "join" || !reflect.DeepEqual(got, want) {
				t.Fatalf("st=%+v got=%v, want the join and %v", st, got, want)
			}
			var line string
			for _, l := range strings.Split(log.String(), "\n") {
				if strings.Contains(l, "key index") && strings.Contains(l, "level="+c.level) {
					line = l
				}
			}
			if line == "" || !strings.Contains(line, c.text) {
				t.Fatalf("no %s line about the key index carrying %q:\n%s", c.level, c.text, log.String())
			}
			// Damage is said with the file to remove and what removing it
			// does; the line repeats every lookup until then.
			if c.level == "WARN" && (!strings.Contains(line, " path="+keyidx.PathFor(base)) || !strings.Contains(line, "remove it to silence this; it is rebuilt at the table's next full rewrite")) {
				t.Fatalf("the WARN lacks the path or the remediation:\n%s", line)
			}
		})
	}
	// An index whose CRCs agree but whose records are out of order inside a
	// block would answer a SUBSET; the reader refuses it and the lookup
	// falls back, with the same WARN shape.
	t.Run("records out of order inside a block", func(t *testing.T) {
		base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, rows)
		indexBeside(t, base, "id")
		p := keyidx.PathFor(base)
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		a, b := hdr("id")+0*12, hdr("id")+1*12
		tmp := append([]byte(nil), raw[a:a+12]...)
		copy(raw[a:a+12], raw[b:b+12])
		copy(raw[b:b+12], tmp)
		// Block 0's CRC in the skip table, then the trailing CRC. The
		// block's first key in the skip table is left as it was, so the
		// reader's first-key check does not fire before the order check.
		tbl := crc32.MakeTable(crc32.Castagnoli)
		tail := int64(hdr("id")) + 4*12
		binary.BigEndian.PutUint32(raw[tail+4:], crc32.Checksum(raw[hdr("id"):tail], tbl))
		copy(raw[tail:tail+4], raw[a:a+4])
		if err := os.WriteFile(p, retrailIndex(raw, "id", 4, 4), 0o644); err != nil {
			t.Fatal(err)
		}
		log := captureDebugLog(t)
		st, got := threeWays(t, base, pkCols, keys)
		if st.Path != "join" || !reflect.DeepEqual(got, want) {
			t.Fatalf("st=%+v got=%v", st, got)
		}
		if !strings.Contains(log.String(), "level=WARN") || !strings.Contains(log.String(), "order") || !strings.Contains(log.String(), "remove it to silence this") {
			t.Fatalf("no WARN about the order:\n%s", log.String())
		}
	})
	// A BIGINT key forged to UBIGINT keeps the size: the type check is what
	// refuses it, and a mutant that skipped it would return the WRONG rows
	// (the sign bit is flipped on one side only), which the equality catches.
	t.Run("type mismatch at the same width", func(t *testing.T) {
		ddl := "CREATE TABLE `t` (\n  `id` bigint NOT NULL,\n  `v` varchar(8) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
		base, pkCols := writeKeyedBaseline(t, ddl, []string{"id"}, rows)
		indexBeside(t, base, "id")
		p := keyidx.PathFor(base)
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		raw[hdr()] = 3 // BIGINT -> UBIGINT
		if err := os.WriteFile(p, retrailIndex(raw, "id", 8, 4), 0o644); err != nil {
			t.Fatal(err)
		}
		log := captureDebugLog(t)
		st, got := threeWays(t, base, pkCols, keys)
		if st.Path != "join" || !reflect.DeepEqual(got, want) {
			t.Fatalf("st=%+v got=%v", st, got)
		}
		if !strings.Contains(log.String(), "level=WARN") || !strings.Contains(log.String(), "does not describe") {
			t.Fatalf("no WARN about the types:\n%s", log.String())
		}
	})
	// A base with a NULL key has no index (the writer refuses to build one),
	// so the join's refusal stands, with its text unchanged.
	t.Run("null key: no index, the join refuses as today", func(t *testing.T) {
		base := nullKeyBase(t)
		res, err := keyidx.Build(context.Background(), base, []string{"id"}, duckdbutil.DefaultTuning())
		if err == nil {
			t.Fatalf("fixture: an index was built on a NULL key: %+v", res)
		}
		_, st, err := lookupBasePositionsVia(context.Background(), base, "s", "t", pkColsIntID(), touched("1", "3"), duckdbutil.DefaultTuning(), true, true)
		want := `canonicalize baseline PK for s.t: key column "id" holds NULL in the base file (MySQL forbids NULL in PK columns; a baseline written before #522 lost unsigned values past the signed range this way — take a fresh baseline)`
		if err == nil || err.Error() != want || st.Path != "join" {
			t.Fatalf("err=%v st=%+v", err, st)
		}
	})
	// An index beside a base on S3 is never looked for.
	t.Run("remote base", func(t *testing.T) {
		log := captureDebugLog(t)
		opens := countIndexOpens(t)
		_, st, _ := lookupBasePositionsVia(context.Background(), "s3://bucket/x/t.parquet", "s", "t", pkColsIntID(), touched("1"), duckdbutil.DefaultTuning(), false, true)
		if *opens != 0 || st.Path == "index" || !strings.Contains(log.String(), "level=DEBUG") {
			t.Fatalf("opens=%d st=%+v\n%s", *opens, st, log.String())
		}
	})
}

func flip(t *testing.T, path string, at int) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[at] ^= 1
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

// indexedBase is the previous snapshot's table for indexedChain: written by
// the rewrite (emitSnapshot), which builds an index beside it (slice 2), so
// a refresh over it is the real shape. Returned with the window's reference
// rewrite over it and the instants the chain uses.
type indexedFixture struct {
	ref1, ref2 string
	at1, at2   time.Time
	cut2       *query.BinlogPos
}

func indexedBase(t *testing.T, window map[string]*query.ResultRow) indexedFixture {
	t.Helper()
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	f := indexedFixture{at1: t0.Add(5 * time.Minute), at2: t0.Add(10 * time.Minute), cut2: &query.BinlogPos{File: "binlog.000009", Pos: 2000}}
	_, f.ref1, _ = emitSnapshot(t, src, changeMap(upd(1, "v1"), ins(5, "five")), &query.BinlogPos{File: "binlog.000009", Pos: 1000}, f.at1)
	if ok, err := keyidx.Bound(f.ref1); !ok || err != nil {
		t.Fatalf("fixture: the rewrite left no bound index beside %s (ok=%v err=%v)", f.ref1, ok, err)
	}
	_, f.ref2, _ = emitSnapshot(t, f.ref1, cloneChanges(window), f.cut2, f.at2)
	return f
}

// indexedChain is spilledChain over indexedBase's table: one refresh of
// window over it into its own root.
func indexedChain(t *testing.T, f indexedFixture, window map[string]*query.ResultRow, spill func(t *testing.T) *changeSpill) (base2 string, rep *TableReport) {
	t.Helper()
	changes := cloneChanges(window)
	var mutate func(*tableDeltaPublish)
	if spill != nil {
		changes = map[string]*query.ResultRow{}
		mutate = func(p *tableDeltaPublish) { p.fold.Spill = spill(t) }
	}
	base2, rep, err := deltaWindow(t, t.TempDir(), f.ref1, f.at1, changes, f.at2, f.cut2, mutate)
	if err != nil {
		t.Fatal(err)
	}
	return base2, rep
}

// TestTableDelta_indexPathPublishesTheSamePair_2496r: a fold with table
// deltas over a base that has an index, switch on, publishes a .posdel
// byte-identical to the one the switch-off refresh publishes, the state
// through the pair equals the full rewrite, the report says the index was
// taken, and the per-table line says so too with the blocks read.
func TestTableDelta_indexPathPublishesTheSamePair_2496r(t *testing.T) {
	noCompaction(t)
	f := indexedBase(t, bigWindow("final"))
	useKeyIndex(t, false)
	baseOff, repOff := indexedChain(t, f, bigWindow("final"), nil)
	if repOff.LookupPath != "join" || repOff.LookupBlocks != 0 {
		t.Fatalf("switch off: path %q blocks %d", repOff.LookupPath, repOff.LookupBlocks)
	}
	useKeyIndex(t, true)
	log := captureDebugLog(t)
	baseOn, repOn := indexedChain(t, f, bigWindow("final"), nil)
	if repOn.LookupPath != "index" || repOn.LookupBlocks != 1 || repOn.LookupRows != repOff.LookupRows || repOn.DeltaDeadRows != repOff.DeltaDeadRows {
		t.Fatalf("switch on: %+v; off: path %q rows %d dead %d", repOn, repOff.LookupPath, repOff.LookupRows, repOff.DeltaDeadRows)
	}
	posOff, _ := baseline.TableDeltaPaths(baseOff, repOff.DeltaSeq)
	posOn, _ := baseline.TableDeltaPaths(baseOn, repOn.DeltaSeq)
	a, err := os.ReadFile(posOff)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(posOn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf(".posdel differs between the paths: %d vs %d bytes, positions %v vs %v", len(a), len(b), readPositions(t, posOff), readPositions(t, posOn))
	}
	want, got := byID(readSnapshotRows(t, f.ref2)), byID(deltaState(t, baseOn))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state through the index path's pair differs from the full rewrite\n got: %v\nwant: %v", got, want)
	}
	var line string
	for _, l := range strings.Split(log.String(), "\n") {
		if strings.Contains(l, `msg="table published as a delta over its previous file"`) {
			line = l
		}
	}
	for _, want := range []string{" lookup_path=index ", fmt.Sprintf(" lookup_blocks=%d", repOn.LookupBlocks), fmt.Sprintf(" lookup_rows=%d ", repOn.LookupRows)} {
		if !strings.Contains(line+" ", want) {
			t.Errorf("the delta line lacks %q:\n%s", want, line)
		}
	}
	if strings.Contains(log.String(), "level=WARN") && strings.Contains(log.String(), "key index") {
		t.Errorf("a WARN about the key index on a healthy refresh:\n%s", log.String())
	}
}

// TestTableDelta_spilledWindowSumsIndexStats_2496r: a window read back from
// disk in several passes looks each pass up through the index; the report
// sums the blocks and the matches over the passes and names the one path.
func TestTableDelta_spilledWindowSumsIndexStats_2496r(t *testing.T) {
	noCompaction(t)
	useKeyIndex(t, true)
	w := bigWindow("final")
	f := indexedBase(t, w)
	spill := func(t *testing.T) *changeSpill { return spillOf(t, 4, cloneChanges(w)) }
	base2, rep := indexedChain(t, f, w, spill)
	if rep.DeltaSpillPasses < 3 {
		t.Fatalf("the spill was read in %d pass(es): the fixture has to exercise more than one", rep.DeltaSpillPasses)
	}
	if rep.LookupPath != "index" || rep.LookupBlocks != int64(rep.DeltaSpillPasses) || rep.LookupRows != rep.DeltaDeadRows || rep.LookupDuration <= 0 {
		t.Fatalf("spilled: %+v", rep)
	}
	want, got := byID(readSnapshotRows(t, f.ref2)), byID(deltaState(t, base2))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state through the spilled pair differs from the full rewrite\n got: %v\nwant: %v", got, want)
	}
	// The summing itself: blocks and rows add up over passes of one path; a
	// pass on another path makes the report say so instead of naming one.
	rep2 := newTableReport("mydb", "orders")
	rep2.addLookup(lookupStats{Path: "index", IndexBlocks: 2, Examined: 3})
	rep2.addLookup(lookupStats{Path: "index", IndexBlocks: 1, Examined: 1})
	if rep2.LookupPath != "index" || rep2.LookupBlocks != 3 || rep2.LookupRows != 4 {
		t.Fatalf("summed: %+v", rep2)
	}
	rep2.addLookup(lookupStats{Path: "scan", Examined: 10})
	if rep2.LookupPath != "mixed" || rep2.LookupBlocks != 3 || rep2.LookupRows != 14 {
		t.Fatalf("mixed: %+v", rep2)
	}
}

// TestLookupBasePositions_keyIndexTiming_2496r is the measurement, not a
// pass/fail: KEYIDX_BIG=1 writes a 10 M-row integer-keyed base in a scattered
// order with its index and times scan, join and index for 1,000 keys near the
// end of the file and 1,000 spread over it.
func TestLookupBasePositions_keyIndexTiming_2496r(t *testing.T) {
	if os.Getenv("KEYIDX_BIG") == "" {
		t.Skip("KEYIDX_BIG=1 runs the 10 M-row measurement")
	}
	const n = 10_000_000
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	base := filepath.Join(t.TempDir(), "t.parquet")
	if _, err := ddb.Exec(fmt.Sprintf("COPY (SELECT i::INTEGER AS id, md5(i::VARCHAR) AS v FROM range(%d) r(i) ORDER BY hash(i)) TO '%s' (FORMAT PARQUET, ROW_GROUP_SIZE 122880)", n, base)); err != nil {
		t.Fatal(err)
	}
	res, err := keyidx.Build(context.Background(), base, []string{"id"}, duckdbutil.DefaultTuning())
	if err != nil || res.Skipped != "" {
		t.Fatalf("build: %+v %v", res, err)
	}
	t.Logf("base %d rows; index %d bytes built in %s", n, res.Bytes, res.Build)
	pkCols := pkColsIntID()
	recent, spread := make([]string, 0, 1000), make([]string, 0, 1000)
	for i := range 1000 {
		recent = append(recent, fmt.Sprint(n-1-i))
		spread = append(spread, fmt.Sprint(i*(n/1000)))
	}
	for _, set := range []struct {
		name string
		keys []string
	}{{"1,000 recent keys", recent}, {"1,000 keys spread over the base", spread}} {
		ch := touched(set.keys...)
		for _, path := range []struct {
			name        string
			join, index bool
		}{{"scan", false, false}, {"join", true, false}, {"index", true, true}} {
			var best time.Duration
			var st lookupStats
			var got []int64
			for run := range 3 {
				start := time.Now()
				got, st, err = lookupBasePositionsVia(context.Background(), base, "s", "t", pkCols, ch, duckdbutil.DefaultTuning(), path.join, path.index)
				if err != nil {
					t.Fatal(err)
				}
				d := time.Since(start)
				if run == 0 || d < best {
					best = d
				}
			}
			t.Logf("%s / %s: %d positions, path=%s examined=%d blocks=%d, best of 3 %s", set.name, path.name, len(got), st.Path, st.Examined, st.IndexBlocks, best)
		}
	}
}

// TestLookupBasePositions_keyMetadataTypeGuard_2496r: the join refuses, in
// canonicalizePKValue, a key whose snapshot DataType is a string type while
// the base stores an integer (the column's type changed after the base was
// written; CheckDestructiveDDL does not cover ALTER ... MODIFY). The index
// path must not answer where the join would refuse: with any key column
// whose DataType is not one the canonicalizer passes through untouched it
// stands aside (Debug), and the join's refusal text is unchanged.
func TestLookupBasePositions_keyMetadataTypeGuard_2496r(t *testing.T) {
	base, _ := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	indexBeside(t, base, "id")
	for _, dt := range []string{"varchar", "char", "decimal", "binary", "datetime", "", "int unsigned", "int32"} {
		pk := []metadata.ColumnMeta{{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: dt}}
		_, joinSt, joinErr := lookupBasePositionsWith(context.Background(), base, "s", "t", pk, touched("1", "3"), duckdbutil.DefaultTuning(), true)
		log := captureDebugLog(t)
		got, st, err := lookupBasePositionsVia(context.Background(), base, "s", "t", pk, touched("1", "3"), duckdbutil.DefaultTuning(), true, true)
		if st.Path != joinSt.Path || fmt.Sprint(err) != fmt.Sprint(joinErr) {
			t.Fatalf("DataType %q: index on: path=%q err=%v; join alone: path=%q err=%v", dt, st.Path, err, joinSt.Path, joinErr)
		}
		if st.Path == "index" {
			t.Fatalf("DataType %q: the index answered %v", dt, got)
		}
		if !strings.Contains(log.String(), "level=DEBUG") || !strings.Contains(log.String(), "key index not used") {
			t.Fatalf("DataType %q: no Debug line about standing aside:\n%s", dt, log.String())
		}
	}
	// And the canonical pass-through types all take the index.
	for _, dt := range []string{"int", "integer", "smallint", "tinyint", "mediumint", "bigint", "year", "INT", " Int "} {
		pk := []metadata.ColumnMeta{{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: dt}}
		got, st, err := lookupBasePositionsVia(context.Background(), base, "s", "t", pk, touched("1", "3"), duckdbutil.DefaultTuning(), true, true)
		if err != nil || st.Path != "index" || !reflect.DeepEqual(got, []int64{0, 2}) {
			t.Fatalf("DataType %q: got=%v st=%+v err=%v", dt, got, st, err)
		}
	}
}

// TestLookupBasePositions_duplicateHeaderNamesFallBack_2496r: a header that
// names one key column twice (forged: same lengths, trailing CRC rewritten)
// has the key's column count and every name is a key column, yet it is not
// the key: the names must be a permutation of it, or the index stands
// aside with a WARN.
func TestLookupBasePositions_duplicateHeaderNamesFallBack_2496r(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n  `a` int NOT NULL,\n  `b` int NOT NULL,\n  PRIMARY KEY (`a`,`b`)\n) ENGINE=InnoDB;\n"
	base, pkCols := writeKeyedBaseline(t, ddl, []string{"a", "b"}, [][]string{{"1", "2"}, {"2", "1"}, {"3", "3"}})
	indexBeside(t, base, "a", "b")
	p := keyidx.PathFor(base)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	// Header: fixed + 2 type codes + (2+1 "a") + (2+1 "b"): the second
	// name's byte is at fixed+2+3+2.
	at := hdr() + 2 + 3 + 2
	if raw[at] != 'b' {
		t.Fatalf("fixture: byte %d is %q, not the second name", at, raw[at])
	}
	raw[at] = 'a'
	headerLen := keyidx.HeaderSize([]string{"a", "b"})
	nblocks := int64(1)
	skipLen := nblocks * (8 + 4)
	tailStart := headerLen + 3*(8+8)
	tbl := crc32.MakeTable(crc32.Castagnoli)
	crc := crc32.Checksum(raw[:headerLen], tbl)
	crc = crc32.Update(crc, tbl, raw[tailStart:tailStart+skipLen])
	binary.BigEndian.PutUint32(raw[tailStart+skipLen:], crc)
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if r, err := keyidx.Open(base); err != nil {
		t.Fatalf("fixture: the forged index must open (the forgery is the names, not the format): %v", err)
	} else {
		r.Close()
	}
	log := captureDebugLog(t)
	st, got := threeWays(t, base, pkCols, touched("1|2", "2|1", "3|3", "1|1"))
	if st.Path != "join" || !reflect.DeepEqual(got, []int64{0, 1, 2}) {
		t.Fatalf("st=%+v got=%v", st, got)
	}
	if !strings.Contains(log.String(), "level=WARN") || !strings.Contains(log.String(), "other key columns") {
		t.Fatalf("no WARN about the names:\n%s", log.String())
	}
}

// TestLookupBasePositions_missingBaseIsSaidSo_2496r: an index beside a base
// that is gone must not be logged as "no key index": the Debug line names
// the base, and the lookup then fails the way it fails today.
func TestLookupBasePositions_missingBaseIsSaidSo_2496r(t *testing.T) {
	base, pkCols := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}})
	indexBeside(t, base, "id")
	if err := os.Remove(base); err != nil {
		t.Fatal(err)
	}
	log := captureDebugLog(t)
	_, st, err := lookupBasePositionsVia(context.Background(), base, "s", "t", pkCols, touched("1"), duckdbutil.DefaultTuning(), true, true)
	if err == nil || st.Path == "index" {
		t.Fatalf("missing base: err=%v st=%+v", err, st)
	}
	if strings.Contains(log.String(), "no key index") || !strings.Contains(log.String(), "the base file is not there") {
		t.Fatalf("the log blames the index, or does not name the base:\n%s", log.String())
	}
}

// TestKeyIndexSwitch_readsOnceAndLogsOnce_2496r: the switch reads the
// environment once and says once what it read; the daemon calls it when
// the refresh loop starts so the line is at boot, not at the first refresh.
func TestKeyIndexSwitch_readsOnceAndLogsOnce_2496r(t *testing.T) {
	for _, tc := range []struct {
		raw   string
		on    bool
		level string
	}{
		{"1", true, "level=INFO"}, {"maybe", false, "level=WARN"}, {"", false, ""},
	} {
		log := captureDebugLog(t)
		reads := 0
		sw := newKeyIndexSwitch(func(name string) string {
			if name != KeyIndexEnv {
				t.Fatalf("read %q, want %s", name, KeyIndexEnv)
			}
			reads++
			return tc.raw
		})
		for range 3 {
			if sw() != tc.on {
				t.Fatalf("%q: on=%v, want %v", tc.raw, sw(), tc.on)
			}
		}
		if reads != 1 {
			t.Fatalf("%q: the environment was read %d times", tc.raw, reads)
		}
		if n := strings.Count(log.String(), KeyIndexEnv); (tc.level == "" && n != 0) || (tc.level != "" && (n != 1 || !strings.Contains(log.String(), tc.level))) {
			t.Fatalf("%q: %d lines naming the switch (want %q once):\n%s", tc.raw, n, tc.level, log.String())
		}
	}
}

// hdr is the index header's size for the named key columns (none: the
// fixed part alone), for a test that forges bytes at known offsets.
func hdr(names ...string) int { return int(keyidx.HeaderSize(names)) }

// chainStep is one refresh of a chain in TestTableDelta_carriesTheIndexForward_2496r.
type chainStep struct {
	window func() map[string]*query.ResultRow
	at     time.Time
	cut    *query.BinlogPos
}

// TestTableDelta_carriesTheIndexForward_2496r (A): the index travels with
// the base it describes when the base is carried forward by hard link, so
// every refresh of a chain resolves through it, not only the first after
// the rewrite. Switch on and off are run as two chains from the same
// rewritten snapshot; at every step the pair's dead positions and upsert
// rows must agree, and at the first step, where both chains still hang off
// the one previous file, the .posdel bytes too (from the second step on
// each chain's pair names its own previous file in the footer, so the
// bytes cannot be equal and the positions are what is compared).
func TestTableDelta_carriesTheIndexForward_2496r(t *testing.T) {
	noCompaction(t)
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	f := indexedBase(t, changeMap(upd(1, "a")))
	steps := []chainStep{
		{func() map[string]*query.ResultRow { return changeMap(upd(1, "s2"), ins(10, "ten")) }, f.at2, f.cut2},
		{func() map[string]*query.ResultRow { return changeMap(del(2), upd(10, "ten-b"), ins(11, "eleven")) }, t0.Add(15 * time.Minute), &query.BinlogPos{File: "binlog.000009", Pos: 3000}},
		{func() map[string]*query.ResultRow { return changeMap(upd(3, "s4"), del(11), ins(12, "twelve")) }, t0.Add(20 * time.Minute), &query.BinlogPos{File: "binlog.000009", Pos: 4000}},
	}
	type run struct {
		on       bool
		base     string
		prevTime time.Time
	}
	runs := []*run{{on: false, base: f.ref1, prevTime: f.at1}, {on: true, base: f.ref1, prevTime: f.at1}}
	for i, step := range steps {
		var posdel [2][]byte
		var dead [2][]int64
		var ups [2][]string
		for j, r := range runs {
			useKeyIndex(t, r.on)
			log := captureDebugLog(t)
			newBase, rep, err := deltaWindow(t, t.TempDir(), r.base, r.prevTime, step.window(), step.at, step.cut, nil)
			if err != nil {
				t.Fatalf("step %d on=%v: %v", i+1, r.on, err)
			}
			wantPath := "join"
			if r.on {
				wantPath = "index"
			}
			if rep.LookupPath != wantPath {
				t.Fatalf("step %d on=%v: lookup_path=%q, want %q:\n%s", i+1, r.on, rep.LookupPath, wantPath, log.String())
			}
			if ok, err := keyidx.Bound(newBase); !ok || err != nil {
				t.Fatalf("step %d on=%v: the carried base has no bound index beside it (ok=%v err=%v)", i+1, r.on, ok, err)
			}
			if strings.Contains(log.String(), "level=WARN") && strings.Contains(log.String(), "key index") {
				t.Fatalf("step %d on=%v: a WARN about the key index:\n%s", i+1, r.on, log.String())
			}
			pd, _ := baseline.TableDeltaPaths(newBase, rep.DeltaSeq)
			posdel[j], err = os.ReadFile(pd)
			if err != nil {
				t.Fatal(err)
			}
			dead[j], ups[j] = readPositions(t, pd), sortedUpserts(t, newBase, rep.DeltaSeq)
			t.Logf("step %d on=%v: seq=%d lookup_path=%s lookup_blocks=%d lookup_rows=%d dead=%d upserts=%d", i+1, r.on, rep.DeltaSeq, rep.LookupPath, rep.LookupBlocks, rep.LookupRows, rep.DeltaDeadRows, rep.DeltaUpsertRows)
			r.base, r.prevTime = newBase, step.at
		}
		if !reflect.DeepEqual(dead[0], dead[1]) || !reflect.DeepEqual(ups[0], ups[1]) {
			t.Fatalf("step %d: the pair differs between switch off and on: dead %v vs %v, upserts %v vs %v", i+1, dead[0], dead[1], ups[0], ups[1])
		}
		if len(dead[0]) == 0 {
			t.Fatalf("step %d: the fixture killed no base row, so the lookup is not exercised", i+1)
		}
		if i == 0 && !bytes.Equal(posdel[0], posdel[1]) {
			t.Fatalf("step %d: .posdel differs between switch off and on", i+1)
		}
	}
}

// TestCarryForward_indexTravelsOnlyByLink_2496r (A): the copy arm leaves no
// index beside the copied base (another inode: the index would be unbound
// anyway) and says so at Debug; a damaged or unbound index beside the
// previous base is not carried and a WARN says so, once, with the path; a
// link that cannot be made is a Debug line, never the table's failure.
func TestCarryForward_indexTravelsOnlyByLink_2496r(t *testing.T) {
	ctx := context.Background()
	t.Run("copy arm", func(t *testing.T) {
		base, _ := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}})
		indexBeside(t, base, "id")
		t.Cleanup(StubLinkFileForTest(func(string, string) error { return syscall.EXDEV }))
		log := captureDebugLog(t)
		dir := t.TempDir()
		linked, err := carryForward(ctx, base, dir, "s", "t")
		if err != nil || linked {
			t.Fatalf("linked=%v err=%v", linked, err)
		}
		if _, err := os.Stat(keyidx.PathFor(filepath.Join(dir, "s", "t.parquet"))); !os.IsNotExist(err) {
			t.Fatalf("an index beside the COPIED base: %v", err)
		}
		if !strings.Contains(log.String(), "level=DEBUG") || !strings.Contains(log.String(), "copied") {
			t.Fatalf("no Debug line about the index not travelling with a copy:\n%s", log.String())
		}
	})
	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T, base string)
	}{
		// The trailing CRC: what Bound checks (header, skip table, their
		// CRC). A block's own damage is found by the lookup that reads the
		// block, which warns with the same remediation.
		{"damaged", func(t *testing.T, base string) {
			raw, err := os.ReadFile(keyidx.PathFor(base))
			if err != nil {
				t.Fatal(err)
			}
			flip(t, keyidx.PathFor(base), len(raw)-1)
		}},
		{"unbound", func(t *testing.T, base string) {
			other, _ := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"2", "b"}})
			indexBeside(t, other, "id")
			raw, err := os.ReadFile(keyidx.PathFor(other))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(keyidx.PathFor(base), raw, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name+" index is not carried", func(t *testing.T) {
			base, _ := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}})
			indexBeside(t, base, "id")
			tc.prepare(t, base)
			log := captureDebugLog(t)
			dir := t.TempDir()
			linked, err := carryForward(ctx, base, dir, "s", "t")
			if err != nil || !linked {
				t.Fatalf("linked=%v err=%v", linked, err)
			}
			if _, err := os.Stat(keyidx.PathFor(filepath.Join(dir, "s", "t.parquet"))); !os.IsNotExist(err) {
				t.Fatalf("the %s index was carried: %v", tc.name, err)
			}
			warns := 0
			for _, l := range strings.Split(log.String(), "\n") {
				if strings.Contains(l, "level=WARN") && strings.Contains(l, "key index") {
					warns++
					if !strings.Contains(l, " path="+keyidx.PathFor(base)) || !strings.Contains(l, "remove it to silence this") {
						t.Fatalf("the WARN lacks the path or the remediation:\n%s", l)
					}
				}
			}
			if warns != 1 {
				t.Fatalf("%d WARN lines about the key index, want 1:\n%s", warns, log.String())
			}
		})
	}
	t.Run("absent index is a Debug line", func(t *testing.T) {
		base, _ := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}})
		log := captureDebugLog(t)
		if _, err := carryForward(ctx, base, t.TempDir(), "s", "t"); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(log.String(), "level=WARN") || !strings.Contains(log.String(), "no key index") {
			t.Fatalf("absent index:\n%s", log.String())
		}
	})
	t.Run("index link failure is a Debug line, not the table's failure", func(t *testing.T) {
		base, _ := writeKeyedBaseline(t, intKeyDDL, []string{"id"}, [][]string{{"1", "a"}})
		indexBeside(t, base, "id")
		t.Cleanup(StubLinkFileForTest(func(old, new string) error {
			if strings.HasSuffix(old, keyidx.Suffix) {
				return os.ErrPermission
			}
			return os.Link(old, new)
		}))
		log := captureDebugLog(t)
		dir := t.TempDir()
		linked, err := carryForward(ctx, base, dir, "s", "t")
		if err != nil || !linked {
			t.Fatalf("linked=%v err=%v", linked, err)
		}
		if _, err := os.Stat(keyidx.PathFor(filepath.Join(dir, "s", "t.parquet"))); !os.IsNotExist(err) {
			t.Fatalf("an index beside the base after a failed link: %v", err)
		}
		if !strings.Contains(log.String(), "level=DEBUG") || !strings.Contains(log.String(), "could not link") || strings.Contains(log.String(), "level=WARN") {
			t.Fatalf("link failure:\n%s", log.String())
		}
	})
}

// TestTableDelta_failedPublishRemovesTheCarriedIndex_2496r (A): when the
// publish fails after the base and its index were carried, the index goes
// with the base: nothing of the failed table stays in the new snapshot.
func TestTableDelta_failedPublishRemovesTheCarriedIndex_2496r(t *testing.T) {
	noCompaction(t)
	useKeyIndex(t, true)
	f := indexedBase(t, changeMap(upd(1, "a")))
	root := t.TempDir()
	base2, rep, err := deltaWindow(t, root, f.ref1, f.at1, changeMap(upd(1, "s2")), f.at2, f.cut2, nil)
	if err != nil || rep.LookupPath != "index" {
		t.Fatalf("fixture: err=%v path=%q", err, rep.LookupPath)
	}
	// The next refresh carries the chain's pair forward too; removing it
	// between the read and the publish fails the carry of the chain, after
	// the base and its index were linked.
	at3 := f.at2.Add(5 * time.Minute)
	newBase := filepath.Join(root, SnapshotDirName(at3), "mydb", "orders.parquet")
	_, _, err = deltaWindow(t, root, base2, f.at2, changeMap(upd(3, "s3")), at3, &query.BinlogPos{File: "binlog.000009", Pos: 3000}, func(p *tableDeltaPublish) {
		pd, _ := baseline.TableDeltaPaths(base2, rep.DeltaSeq)
		if err := os.Remove(pd); err != nil {
			t.Fatal(err)
		}
	})
	if err == nil {
		t.Fatal("the publish succeeded without the chain's pair")
	}
	for _, p := range []string{newBase, keyidx.PathFor(newBase)} {
		if _, serr := os.Stat(p); !os.IsNotExist(serr) {
			t.Fatalf("%s is still in the failed snapshot (stat err %v); publish err: %v", p, serr, err)
		}
	}
}
