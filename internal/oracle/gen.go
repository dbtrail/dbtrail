// Package oracle is the correctness oracle (#2499): random DML on a real
// MySQL source through the real capture, snapshot and refresh pipeline,
// compared row by row with the source at the same cut.
//
// The package holds the deterministic generator (gen.go), the read of the
// source and the barrier on capture (source.go), the comparison and its
// failure message (compare.go) and the run loop with the steps a run applies
// to the copy between two reads (step.go, run.go). The integration test under
// -tags integration drives them against a live server.
//
// The oracle knows nothing about budgets, frozen tails or segments: a step is
// whatever a mechanism does to the copy, and the read after it must equal the
// source. A seed that fails never widens the oracle; the product is fixed or
// an issue is filed with the seed.
package oracle

import (
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Kind is what an op does.
type Kind string

const (
	KindUpdate   Kind = "update"
	KindInsert   Kind = "insert"
	KindDelete   Kind = "delete"
	KindReinsert Kind = "reinsert"  // insert of a key deleted earlier
	KindPKUpdate Kind = "pk_update" // UPDATE that moves a row to another key
	KindMulti    Kind = "multi"     // one statement over several rows
	KindTxn      Kind = "txn"       // 2-4 statements in one transaction
	KindBurst    Kind = "burst"     // MinBurst..MaxBurst statements in one transaction
)

// Weights are the operation weights, drawn over their sum. The PK-changing
// update has the 7 the issue gave it: the fold folds such an UPDATE as a
// DELETE of the old key and an INSERT of the new one (#2501), so a window
// holding one is refreshed like any other and the chain of delta pairs, the
// thing this net watches, keeps growing. Before #2501 the fold refused every
// such window and the run took a snapshot instead, so the weight was held at
// 2 to keep the chain growing at all.
var Weights = []struct {
	Kind   Kind
	Weight int
}{
	{KindUpdate, 30}, {KindInsert, 20}, {KindDelete, 15}, {KindReinsert, 10},
	{KindPKUpdate, 7}, {KindMulti, 8}, {KindTxn, 8}, {KindBurst, 2},
}

const (
	// MinKeys..MaxKeys bounds the seed's key set: small, so keys collide.
	MinKeys = 30
	MaxKeys = 60
	// HotKeys is how many keys a seed draws far more often than the rest.
	HotKeys = 2
	// MinBurst..MaxBurst bounds the statements of a burst.
	MinBurst = 100
	MaxBurst = 300
	// noopEvery: one update in this many changes nothing (same values), the
	// statement MySQL writes no row event for.
	noopEvery = 6
)

// Table is one of the five tables a seed plays on. Cols is every column in
// CREATE TABLE order, PK the key columns, Datetimes the DATETIME columns
// (read as wall clocks); key renders a key index as the PK literals, in PK
// order.
type Table struct {
	Name      string
	DDL       string
	PK        []string
	Cols      []string
	Datetimes []string
	// Render reads a column as text by an expression of its own on each
	// side, where the default cast would print one value two ways (binary
	// bytes, fractional seconds, a TIMESTAMP in the session's zone). Nil
	// for the default.
	Render map[string]Render
	key    func(k int) []string
}

// Render is one column's read as text: Source is a MySQL expression and View
// a DuckDB one, each with %[1]s for the quoted column. Source is also what
// a snapshot writes (SnapshotStep stores the source's text), so it must be
// text the copy's writer reads back as the value: a binary value as 0x and
// its hex, a TIMESTAMP in UTC, as a dump writes them.
type Render struct {
	Source, View string
}

// Renders of the key types of #3159.
var (
	// RenderBinary: 0x and the hex of the bytes, the empty value as ''
	// ("0x" alone is not read back as hex).
	RenderBinary = Render{
		Source: "IF(LENGTH(%[1]s) = 0, '', CONCAT('0x', HEX(%[1]s)))",
		View:   "CASE WHEN octet_length(%[1]s) = 0 THEN '' ELSE '0x' || hex(%[1]s) END",
	}
	// RenderMicros: a DATETIME(6) with its six digits of fraction. The view's microseconds since the epoch are
	// the same whether it reads the column as a wall clock or an instant.
	RenderMicros = Render{
		Source: "CAST(%[1]s AS CHAR)",
		View:   "strftime(make_timestamp(epoch_us(%[1]s)), '%%Y-%%m-%%d %%H:%%M:%%S.%%f')",
	}
)

// Tables returns the five tables, fresh copies.
func Tables() []Table {
	return []Table{
		{
			Name: "t_int",
			DDL:  "CREATE TABLE `t_int` (`id` INT NOT NULL, `v` INT NOT NULL, `note` VARCHAR(32) NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB",
			PK:   []string{"id"}, Cols: []string{"id", "v", "note"},
			key: func(k int) []string { return []string{fmt.Sprint(k + 1)} },
		},
		{
			Name: "t_comp",
			DDL:  "CREATE TABLE `t_comp` (`a` INT NOT NULL, `b` INT NOT NULL, `v` INT NOT NULL, PRIMARY KEY (`a`, `b`)) ENGINE=InnoDB",
			PK:   []string{"a", "b"}, Cols: []string{"a", "b", "v"},
			key: func(k int) []string { return []string{fmt.Sprint(k/8 + 1), fmt.Sprint(k%8 + 1)} },
		},
		{
			Name: "t_text",
			DDL:  "CREATE TABLE `t_text` (`code` VARCHAR(16) NOT NULL, `v` INT NOT NULL, `note` VARCHAR(32) NULL, PRIMARY KEY (`code`)) ENGINE=InnoDB",
			PK:   []string{"code"}, Cols: []string{"code", "v", "note"},
			key: func(k int) []string { return []string{fmt.Sprintf("'k%03d'", k+1)} },
		},
		{
			Name: "t_types",
			DDL:  "CREATE TABLE `t_types` (`id` INT NOT NULL, `v` INT NOT NULL, `amount` DECIMAL(12,2) NULL, `seen` DATETIME NULL, `note` VARCHAR(32) NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB",
			PK:   []string{"id"}, Cols: []string{"id", "v", "amount", "seen", "note"}, Datetimes: []string{"seen"},
			key: func(k int) []string { return []string{fmt.Sprint(k + 1)} },
		},
		{
			Name: "t_uniq",
			DDL:  "CREATE TABLE `t_uniq` (`id` INT NOT NULL, `code` VARCHAR(16) NOT NULL, `v` INT NOT NULL, PRIMARY KEY (`id`), UNIQUE KEY `uq_code` (`code`)) ENGINE=InnoDB",
			PK:   []string{"id"}, Cols: []string{"id", "code", "v"},
			key: func(k int) []string { return []string{fmt.Sprint(k + 1)} },
		},
	}
}

// KeyTypeTables returns the tables keyed by the types a tail takes besides
// integers (#3159), fresh copies: BINARY(16) (a UUID), VARBINARY, DATE,
// DATETIME, DATETIME(6), DECIMAL and a composite of an integer, a VARBINARY
// and a DATE. Not in Tables: a seed draws its table by index, so
// a table added there would change every seed's draws.
//
// Their keys are ones a tail takes: no BINARY(16) value ends in 0x00, no
// binary value is empty or has text that starts with 0x (reconstruct
// refuses those, and the oracle over tails takes a refusal for a finding).
func KeyTypeTables() []Table {
	return []Table{
		{
			Name: "t_uuid",
			DDL:  "CREATE TABLE `t_uuid` (`id` BINARY(16) NOT NULL, `v` INT NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB",
			PK:   []string{"id"}, Cols: []string{"id", "v"}, Render: map[string]Render{"id": RenderBinary},
			key: func(k int) []string { return []string{binaryLiteral(uuidKey(k))} },
		},
		{
			Name: "t_vbin",
			DDL:  "CREATE TABLE `t_vbin` (`id` VARBINARY(20) NOT NULL, `v` INT NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB",
			PK:   []string{"id"}, Cols: []string{"id", "v"}, Render: map[string]Render{"id": RenderBinary},
			key: func(k int) []string { return []string{binaryLiteral(varbinaryKey(k))} },
		},
		{
			Name: "t_date",
			DDL:  "CREATE TABLE `t_date` (`d` DATE NOT NULL, `v` INT NOT NULL, PRIMARY KEY (`d`)) ENGINE=InnoDB",
			PK:   []string{"d"}, Cols: []string{"d", "v"},
			key: func(k int) []string { return []string{"'" + dateKey(k) + "'"} },
		},
		{
			Name: "t_dt0",
			DDL:  "CREATE TABLE `t_dt0` (`at` DATETIME NOT NULL, `v` INT NOT NULL, PRIMARY KEY (`at`)) ENGINE=InnoDB",
			PK:   []string{"at"}, Cols: []string{"at", "v"}, Datetimes: []string{"at"},
			key: func(k int) []string {
				return []string{"'" + time.Date(1969, 12, 31, 23, 59, 0, 0, time.UTC).Add(time.Duration(k*7)*time.Second).Format("2006-01-02 15:04:05") + "'"}
			},
		},
		{
			Name: "t_dt6",
			DDL:  "CREATE TABLE `t_dt6` (`at` DATETIME(6) NOT NULL, `v` INT NOT NULL, PRIMARY KEY (`at`)) ENGINE=InnoDB",
			PK:   []string{"at"}, Cols: []string{"at", "v"}, Render: map[string]Render{"at": RenderMicros},
			key: func(k int) []string { return []string{"'" + microsKey(k) + "'"} },
		},
		{
			Name: "t_dec",
			DDL:  "CREATE TABLE `t_dec` (`x` DECIMAL(10,2) NOT NULL, `v` INT NOT NULL, PRIMARY KEY (`x`)) ENGINE=InnoDB",
			PK:   []string{"x"}, Cols: []string{"x", "v"},
			key: func(k int) []string { return []string{decimalKey(k)} },
		},
		{
			Name: "t_kmix",
			DDL:  "CREATE TABLE `t_kmix` (`a` INT NOT NULL, `b` VARBINARY(8) NOT NULL, `d` DATE NOT NULL, `v` INT NOT NULL, PRIMARY KEY (`a`, `b`, `d`)) ENGINE=InnoDB",
			PK:   []string{"a", "b", "d"}, Cols: []string{"a", "b", "d", "v"}, Render: map[string]Render{"b": RenderBinary},
			key: func(k int) []string {
				return []string{fmt.Sprint(k%3 - 1), binaryLiteral(varbinaryKey(k / 3)), "'" + dateKey(k%2) + "'"}
			},
		},
	}
}

// uuidKey is key k of t_uuid: 16 bytes, a 0x00 inside every other one, the
// last byte never 0x00, and one in seven made of text (UTF-8, spelled as
// itself in pk_values, with a '|' and a '\' pk_values escapes).
func uuidKey(k int) []byte {
	if k%7 == 0 {
		return []byte(fmt.Sprintf(`u|\%013d`, k))
	}
	b := make([]byte, 16)
	for i := range b {
		b[i] = byte(0x80 + (k*37+i*11)%0x7f)
	}
	if k%2 == 0 {
		b[3] = 0
	}
	b[15] = byte(k + 1)
	return b
}

// varbinaryKey is key k of t_vbin: a value of one byte 0x7f, values ending in 0x00
// beside the same bytes without it, bytes that are not UTF-8, and text with
// '|' and '\' in it.
func varbinaryKey(k int) []byte {
	switch {
	case k == 0:
		return []byte{0x7f} // not the empty value, which has no tail
	case k%5 == 1:
		return []byte{byte(k), 0}
	case k%5 == 2:
		return []byte{byte(k - 1)} // key k-1 without its trailing 0x00
	case k%5 == 3:
		return []byte{0xff, byte(k)}
	case k%5 == 4:
		return []byte{0xfe, byte(k), 0, 0}
	default:
		return []byte(fmt.Sprintf(`v|\%d`, k))
	}
}

func binaryLiteral(b []byte) string { return "X'" + hex.EncodeToString(b) + "'" }

// dateKey is key k of t_date: every 13th day from 1969-12-01, across the
// epoch and the leap day of 1972.
func dateKey(k int) string {
	return time.Date(1969, 12, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, k*13).Format("2006-01-02")
}

// microsKey is key k of t_dt6: a second apart, every third with no
// fraction (written without one, so the server pads it to .000000).
func microsKey(k int) string {
	at := time.Date(2024, 3, 10, 1, 0, 0, 0, time.UTC).Add(time.Duration(k) * time.Second)
	if k%3 == 0 {
		return at.Format("2006-01-02 15:04:05")
	}
	return at.Add(time.Duration((k*123457)%1000000) * time.Microsecond).Format("2006-01-02 15:04:05.000000")
}

// decimalKey is key k of t_dec: -0.00 for key 0, which the server stores as
// zero, and for the others k*1.25 with every other sign, written as short as
// it goes (2.5, 5), so the server's scale is what pads it.
func decimalKey(k int) string {
	if k == 0 {
		return "-0.00"
	}
	v := float64(k) * 1.25
	if k%2 == 1 {
		v = -v
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// RefusedKeyTables returns tables keyed by a type that looks like one a tail
// takes and is not: a DECIMAL with ZEROFILL, which a dump prints padded
// ("0001.25") and the binary log does not (#3159). The oracle over tails
// checks that no read lays a tail over them, and that their reads still
// equal the source.
func RefusedKeyTables() []Table {
	return []Table{
		{
			Name: "t_deczf",
			DDL:  "CREATE TABLE `t_deczf` (`x` DECIMAL(6,2) UNSIGNED ZEROFILL NOT NULL, `v` INT NOT NULL, PRIMARY KEY (`x`)) ENGINE=InnoDB",
			PK:   []string{"x"}, Cols: []string{"x", "v"},
			// The source prints the zeros; the view reads the number. Both
			// as the number: CAST(x + 0) is MySQL's, without the padding.
			Render: map[string]Render{"x": {Source: "CAST(%[1]s + 0 AS CHAR)", View: "CAST(%[1]s AS VARCHAR)"}},
			key:    func(k int) []string { return []string{strconv.FormatFloat(float64(k)*1.25+0.5, 'f', -1, 64)} },
		},
	}
}

// TableByName returns the table called name, of Tables, KeyTypeTables or
// RefusedKeyTables; it
// panics on an unknown name, which only a test can spell.
func TableByName(name string) Table {
	for _, t := range slices.Concat(Tables(), KeyTypeTables(), RefusedKeyTables()) {
		if t.Name == name {
			return t
		}
	}
	panic("oracle: no table " + name)
}

// Change is one row-level effect an op has, for the model and the tests: the
// key it touches, the key it moves the row to (a PK change) and, on t_uniq,
// the code literal the inserted row carries.
type Change struct {
	Kind   Kind
	Key    int
	NewKey int
	Code   string
}

// Op is one draw of the generator: one or more SQL statements on one table.
// More than one statement runs inside one transaction. Kind is the kind
// DRAWN, which follows Weights; what each statement did is in Changes, since
// a kind that cannot apply (a delete on an empty table, an insert with every
// key live) falls back to one that can. Keys are the key indexes the op
// touches; Noop marks an update whose SET repeats the row.
type Op struct {
	Kind    Kind
	Table   string
	SQL     []string
	Keys    []int
	Noop    bool
	Changes []Change
}

// Generator draws ops deterministically from a seed, single-threaded. It
// keeps a model of each table's live rows so every statement it emits is
// valid against a source that executed the previous ones in order.
type Generator struct {
	seed   uint64
	r      *rand.Rand
	tables []Table
	k      int
	hot    []int
	model  map[string]*tableModel
	// changes counts single-row changes drawn, for the no-op cadence.
	changes int
}

type tableModel struct {
	rows   map[int][]string // live rows: key → literals in Cols order
	gone   map[int]bool     // keys deleted or vacated and not live since
	codeOf map[int]int      // t_uniq: key → code index
}

// NewGenerator returns the generator of seed over tables.
func NewGenerator(seed uint64, tables []Table) *Generator {
	if len(tables) == 0 {
		panic("oracle: NewGenerator needs at least one table")
	}
	g := &Generator{seed: seed, r: rand.New(rand.NewPCG(seed, 0)), tables: slices.Clone(tables), model: map[string]*tableModel{}}
	g.k = MinKeys + g.r.IntN(MaxKeys-MinKeys+1)
	for len(g.hot) < HotKeys {
		h := g.r.IntN(g.k)
		if !slices.Contains(g.hot, h) {
			g.hot = append(g.hot, h)
		}
	}
	for _, t := range tables {
		g.model[t.Name] = &tableModel{rows: map[int][]string{}, gone: map[int]bool{}, codeOf: map[int]int{}}
	}
	return g
}

// Seed returns the seed.
func (g *Generator) Seed() uint64 { return g.seed }

// Keys returns K, the size of the key set.
func (g *Generator) Keys() int { return g.k }

// HotKeys returns the seed's hot keys.
func (g *Generator) HotKeys() []int { return slices.Clone(g.hot) }

// Row returns a copy of the model's live row of key on table, nil when the
// key is not live.
func (g *Generator) Row(table string, key int) []string {
	m := g.model[table]
	if m == nil {
		return nil
	}
	r, ok := m.rows[key]
	if !ok {
		return nil
	}
	return slices.Clone(r)
}

// Next draws the next op.
func (g *Generator) Next() Op {
	tb := g.tables[g.r.IntN(len(g.tables))]
	m := g.model[tb.Name]
	kind := g.drawKind()
	switch kind {
	case KindMulti:
		if op, ok := g.multi(tb, m); ok {
			return op
		}
		kind = KindUpdate
	case KindTxn:
		return g.compound(tb, m, KindTxn, 2+g.r.IntN(3))
	case KindBurst:
		return g.compound(tb, m, KindBurst, MinBurst+g.r.IntN(MaxBurst-MinBurst+1))
	}
	sql, ch, noop := g.single(tb, m, kind)
	op := Op{Kind: kind, Table: tb.Name, SQL: []string{sql}, Keys: []int{ch.Key}, Noop: noop, Changes: []Change{ch}}
	if ch.Kind == KindPKUpdate {
		op.Keys = append(op.Keys, ch.NewKey)
	}
	return op
}

func (g *Generator) drawKind() Kind {
	total := 0
	for _, w := range Weights {
		total += w.Weight
	}
	n := g.r.IntN(total)
	for _, w := range Weights {
		if n < w.Weight {
			return w.Kind
		}
		n -= w.Weight
	}
	panic("unreachable")
}

// compound draws n single-row changes in one transaction.
func (g *Generator) compound(tb Table, m *tableModel, kind Kind, n int) Op {
	op := Op{Kind: kind, Table: tb.Name}
	kinds := []Kind{KindUpdate, KindInsert, KindDelete, KindReinsert}
	for range n {
		sql, ch, _ := g.single(tb, m, kinds[g.r.IntN(len(kinds))])
		op.SQL = append(op.SQL, sql)
		op.Changes = append(op.Changes, ch)
		if !slices.Contains(op.Keys, ch.Key) {
			op.Keys = append(op.Keys, ch.Key)
		}
	}
	return op
}

// single draws one single-row statement of kind, falling back to an insert
// when kind cannot apply (no live row, no free key) and to a delete when
// every key is live. The fallback is what the Change reports.
func (g *Generator) single(tb Table, m *tableModel, kind Kind) (sql string, ch Change, noop bool) {
	g.changes++
	for _, k := range []Kind{kind, KindInsert, KindDelete} {
		if sql, ch, noop, ok := g.try(tb, m, k); ok {
			return sql, ch, noop
		}
	}
	panic("oracle: neither an insert nor a delete applies, which needs an empty key set")
}

func (g *Generator) try(tb Table, m *tableModel, kind Kind) (sql string, ch Change, noop, ok bool) {
	switch kind {
	case KindInsert, KindReinsert:
		var pool []int
		if kind == KindInsert {
			pool = m.free(g.k)
		} else {
			pool = m.deleted()
		}
		if len(pool) == 0 {
			return "", Change{}, false, false
		}
		k := g.choose(pool)
		row, code := g.newRow(tb, m, k)
		m.rows[k] = row
		m.codeOf[k] = code
		delete(m.gone, k)
		ch = Change{Kind: kind, Key: k}
		if tb.Name == "t_uniq" {
			ch.Code = row[1]
		}
		return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", tb.Name, cols(tb.Cols), strings.Join(row, ", ")), ch, false, true
	case KindDelete:
		live := m.live()
		if len(live) == 0 {
			return "", Change{}, false, false
		}
		k := g.choose(live)
		delete(m.rows, k)
		delete(m.codeOf, k)
		m.gone[k] = true
		return fmt.Sprintf("DELETE FROM `%s` WHERE %s", tb.Name, where(tb, tb.key(k))), Change{Kind: KindDelete, Key: k}, false, true
	case KindUpdate:
		live := m.live()
		if len(live) == 0 {
			return "", Change{}, false, false
		}
		k := g.choose(live)
		row := m.rows[k]
		noop = g.changes%noopEvery == 0
		if !noop {
			fresh, _ := g.newRow(tb, m, k)
			for i, c := range tb.Cols {
				if slices.Contains(tb.PK, c) || (tb.Name == "t_uniq" && c == "code") {
					continue
				}
				row[i] = fresh[i]
			}
		}
		var set []string
		for i, c := range tb.Cols {
			if slices.Contains(tb.PK, c) || (tb.Name == "t_uniq" && c == "code") {
				continue
			}
			set = append(set, fmt.Sprintf("`%s` = %s", c, row[i]))
		}
		return fmt.Sprintf("UPDATE `%s` SET %s WHERE %s", tb.Name, strings.Join(set, ", "), where(tb, tb.key(k))), Change{Kind: KindUpdate, Key: k}, noop, true
	case KindPKUpdate:
		live, free := m.live(), m.free(g.k)
		if len(live) == 0 || len(free) == 0 {
			return "", Change{}, false, false
		}
		k, nk := g.choose(live), g.choose(free)
		row := m.rows[k]
		newKey := tb.key(nk)
		var set []string
		for i, c := range tb.PK {
			set = append(set, fmt.Sprintf("`%s` = %s", c, newKey[i]))
			row[slices.Index(tb.Cols, c)] = newKey[i]
		}
		sql = fmt.Sprintf("UPDATE `%s` SET %s WHERE %s", tb.Name, strings.Join(set, ", "), where(tb, tb.key(k)))
		delete(m.rows, k)
		m.gone[k] = true
		m.rows[nk] = row
		delete(m.gone, nk)
		if code, ok := m.codeOf[k]; ok {
			delete(m.codeOf, k)
			m.codeOf[nk] = code
		}
		return sql, Change{Kind: KindPKUpdate, Key: k, NewKey: nk}, false, true
	}
	return "", Change{}, false, false
}

// multi draws one statement over 2..5 rows: an INSERT of several rows, an
// UPDATE of `v` over several keys, or a DELETE of several keys.
func (g *Generator) multi(tb Table, m *tableModel) (Op, bool) {
	n := 2 + g.r.IntN(4)
	op := Op{Kind: KindMulti, Table: tb.Name}
	switch g.r.IntN(3) {
	case 0:
		pool := m.free(g.k)
		if len(pool) < 2 {
			return Op{}, false
		}
		var values []string
		for _, k := range g.chooseN(pool, min(n, len(pool))) {
			row, code := g.newRow(tb, m, k)
			m.rows[k] = row
			m.codeOf[k] = code
			delete(m.gone, k)
			values = append(values, "("+strings.Join(row, ", ")+")")
			ch := Change{Kind: KindInsert, Key: k}
			if tb.Name == "t_uniq" {
				ch.Code = row[1]
			}
			op.Changes = append(op.Changes, ch)
			op.Keys = append(op.Keys, k)
		}
		op.SQL = []string{fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s", tb.Name, cols(tb.Cols), strings.Join(values, ", "))}
	case 1:
		live := m.live()
		if len(live) < 2 {
			return Op{}, false
		}
		keys := g.chooseN(live, min(n, len(live)))
		vi := slices.Index(tb.Cols, "v")
		for _, k := range keys {
			var v int
			fmt.Sscan(m.rows[k][vi], &v)
			m.rows[k][vi] = fmt.Sprint(v + 1)
			op.Changes = append(op.Changes, Change{Kind: KindUpdate, Key: k})
		}
		op.Keys = keys
		op.SQL = []string{fmt.Sprintf("UPDATE `%s` SET `v` = `v` + 1 WHERE %s", tb.Name, whereIn(tb, keys))}
	default:
		live := m.live()
		if len(live) < 2 {
			return Op{}, false
		}
		keys := g.chooseN(live, min(n, len(live)))
		for _, k := range keys {
			delete(m.rows, k)
			delete(m.codeOf, k)
			m.gone[k] = true
			op.Changes = append(op.Changes, Change{Kind: KindDelete, Key: k})
		}
		op.Keys = keys
		op.SQL = []string{fmt.Sprintf("DELETE FROM `%s` WHERE %s", tb.Name, whereIn(tb, keys))}
	}
	return op, true
}

// newRow renders a fresh row of key k: the PK literals and new values for
// the other columns. On t_uniq the code is one no live row holds, returned
// as its index for the caller to record when the row is inserted (an update
// keeps the row's code, so it records nothing).
func (g *Generator) newRow(tb Table, m *tableModel, k int) (row []string, code int) {
	row = make([]string, len(tb.Cols))
	pk := tb.key(k)
	for i, c := range tb.Cols {
		if j := slices.Index(tb.PK, c); j >= 0 {
			row[i] = pk[j]
			continue
		}
		switch c {
		case "v":
			row[i] = fmt.Sprint(g.r.IntN(1000))
		case "note":
			row[i] = g.nullable(func() string { return fmt.Sprintf("'n%d'", g.r.IntN(100000)) })
		case "amount":
			row[i] = g.nullable(func() string {
				sign := ""
				if g.r.IntN(4) == 0 {
					sign = "-"
				}
				return fmt.Sprintf("%s%d.%02d", sign, g.r.IntN(1000000), g.r.IntN(100))
			})
		case "seen":
			row[i] = g.nullable(func() string {
				return fmt.Sprintf("'%04d-%02d-%02d %02d:%02d:%02d'", 2020+g.r.IntN(10), 1+g.r.IntN(12), 1+g.r.IntN(28),
					g.r.IntN(24), g.r.IntN(60), g.r.IntN(60))
			})
		case "code":
			code = g.freeCode(m, k)
			row[i] = fmt.Sprintf("'c%03d'", code+1)
		default:
			panic("oracle: no value rule for column " + c)
		}
	}
	return row, code
}

func (g *Generator) nullable(f func() string) string {
	if g.r.IntN(5) == 0 {
		return "NULL"
	}
	return f()
}

// freeCode picks a t_uniq code index no live row holds. The row of k itself
// does not count: a fresh row replaces it.
func (g *Generator) freeCode(m *tableModel, k int) int {
	used := map[int]bool{}
	for key, c := range m.codeOf {
		if key != k {
			used[c] = true
		}
	}
	var free []int
	for c := range g.k {
		if !used[c] {
			free = append(free, c)
		}
	}
	return free[g.r.IntN(len(free))]
}

// choose picks one key of set, a hot one a third of the time when set holds
// one.
func (g *Generator) choose(set []int) int {
	if g.r.IntN(3) == 0 {
		var hot []int
		for _, h := range g.hot {
			if slices.Contains(set, h) {
				hot = append(hot, h)
			}
		}
		if len(hot) > 0 {
			return hot[g.r.IntN(len(hot))]
		}
	}
	return set[g.r.IntN(len(set))]
}

// chooseN picks n distinct keys of set, in draw order.
func (g *Generator) chooseN(set []int, n int) []int {
	pool := slices.Clone(set)
	var out []int
	for len(out) < n {
		k := g.choose(pool)
		out = append(out, k)
		pool = slices.DeleteFunc(pool, func(x int) bool { return x == k })
	}
	return out
}

// fillAll makes every key live in the model without emitting SQL (tests of
// the fallbacks only).
func (g *Generator) fillAll(table string) {
	tb := TableByName(table)
	m := g.model[table]
	for k := range g.k {
		if _, ok := m.rows[k]; !ok {
			m.rows[k], m.codeOf[k] = g.newRow(tb, m, k)
			delete(m.gone, k)
		}
	}
}

func (m *tableModel) live() []int {
	keys := make([]int, 0, len(m.rows))
	for k := range m.rows {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func (m *tableModel) free(k int) []int {
	var keys []int
	for i := range k {
		if _, live := m.rows[i]; !live {
			keys = append(keys, i)
		}
	}
	return keys
}

func (m *tableModel) deleted() []int {
	var keys []int
	for k := range m.gone {
		if _, live := m.rows[k]; !live {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys
}

func cols(names []string) string {
	return "`" + strings.Join(names, "`, `") + "`"
}

func where(tb Table, key []string) string {
	parts := make([]string, len(tb.PK))
	for i, c := range tb.PK {
		parts[i] = fmt.Sprintf("`%s` = %s", c, key[i])
	}
	return strings.Join(parts, " AND ")
}

func whereIn(tb Table, keys []int) string {
	lits := make([]string, len(keys))
	for i, k := range keys {
		key := tb.key(k)
		if len(key) == 1 {
			lits[i] = key[0]
		} else {
			lits[i] = "(" + strings.Join(key, ", ") + ")"
		}
	}
	if len(tb.PK) == 1 {
		return fmt.Sprintf("`%s` IN (%s)", tb.PK[0], strings.Join(lits, ", "))
	}
	return fmt.Sprintf("(%s) IN (%s)", cols(tb.PK), strings.Join(lits, ", "))
}
