//go:build integration

package oracle_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/dbtrail/dbtrail/internal/oracle"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestOracle_pkChangeShapes_2501 is a scripted run: every #782 shape, in ONE
// refresh window, on four tables, resolved through the delta pair and compared
// row by row with the source. The fold folds an UPDATE that moves a row to
// another key as a DELETE of the old key and an INSERT of the new one
// (#2501); before, every one of these windows was refused and the run took a
// snapshot instead. A refresh that refuses now fails the run (Run.Do has no
// fallback), so the real checks are the run's error and that both windows
// were applied as refreshes.
//
// It skips without MySQL unless BINTRAIL_ORACLE_REQUIRE=1.
func TestOracle_pkChangeShapes_2501(t *testing.T) {
	if os.Getenv("BINTRAIL_ORACLE_REQUIRE") == "1" {
		t.Setenv("BINTRAIL_REQUIRE_MYSQL", "1")
	}
	testutil.SkipIfNoMySQL(t)

	tables := []oracle.Table{oracle.TableByName("t_int"), oracle.TableByName("t_comp"), oracle.TableByName("t_text"), oracle.TableByName("t_types"), tDatetimeKey}
	run := prepareRun(t, 2501, 3, tables)
	// Step 1 is always the snapshot; the draws after it must be refreshes.
	run.Between[0].(*oracle.SnapshotStep).W = 0
	run.Between[1].(*oracle.RefreshStep).W = 1
	run.Script = [][]oracle.Op{seedOps(), shapeOps(0), shapeOps(100)}
	run.Steps = len(run.Script)

	res, err := run.Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied["refresh"] != 2 || res.Applied["snapshot"] != 1 {
		t.Fatalf("steps applied %v; want one snapshot and two refreshes", res.Applied)
	}
}

// op is one statement on table, with the row-level effects the cut tracks.
func op(kind oracle.Kind, table, sql string, keys ...int) oracle.Op {
	o := oracle.Op{Kind: kind, Table: table, SQL: []string{sql}, Keys: keys}
	for _, k := range keys {
		o.Changes = append(o.Changes, oracle.Change{Kind: kind, Key: k})
	}
	if kind == oracle.KindPKUpdate && len(keys) == 2 {
		o.Changes = []oracle.Change{{Kind: kind, Key: keys[0], NewKey: keys[1]}}
	}
	return o
}

// Each table's rows are keyed by an int n: t_int and t_types by id = n,
// t_comp by (a, b) = (n / 1000 + 1, n % 1000), t_text by code = 'kNNNN'.
func keyOf(table string, n int) string {
	switch table {
	case "t_comp":
		return fmt.Sprintf("`a` = %d AND `b` = %d", n/1000+1, n%1000)
	case "t_text":
		return fmt.Sprintf("`code` = 'k%04d'", n)
	case "t_dt":
		return "`at` = " + dtLiteral(n)
	default:
		return fmt.Sprintf("`id` = %d", n)
	}
}

func setKey(table string, n int) string {
	switch table {
	case "t_comp":
		return fmt.Sprintf("`a` = %d, `b` = %d", n/1000+1, n%1000)
	case "t_text":
		return fmt.Sprintf("`code` = 'k%04d'", n)
	case "t_dt":
		return "`at` = " + dtLiteral(n)
	default:
		return fmt.Sprintf("`id` = %d", n)
	}
}

func insertOf(table string, n int, v int) string {
	switch table {
	case "t_comp":
		return fmt.Sprintf("INSERT INTO `t_comp` (`a`, `b`, `v`) VALUES (%d, %d, %d)", n/1000+1, n%1000, v)
	case "t_text":
		return fmt.Sprintf("INSERT INTO `t_text` (`code`, `v`, `note`) VALUES ('k%04d', %d, 'n%d')", n, v, v)
	case "t_dt":
		return fmt.Sprintf("INSERT INTO `t_dt` (`at`, `v`) VALUES (%s, %d)", dtLiteral(n), v)
	case "t_types":
		return fmt.Sprintf("INSERT INTO `t_types` (`id`, `v`, `amount`, `seen`, `note`) VALUES (%d, %d, %d.25, '2026-04-01 08:00:%02d', 'n%d')", n, v, v, v%60, v)
	default:
		return fmt.Sprintf("INSERT INTO `t_int` (`id`, `v`, `note`) VALUES (%d, %d, 'n%d')", n, v, v)
	}
}

// tDatetimeKey is a table of this test alone: a DATETIME key, so one key the
// parser spells from a string (not an integer) goes through real capture.
// Keyed by n as '2026-04-01 00:00:00' + n seconds.
var tDatetimeKey = oracle.Table{
	Name: "t_dt",
	DDL:  "CREATE TABLE `t_dt` (`at` DATETIME NOT NULL, `v` INT NOT NULL, PRIMARY KEY (`at`)) ENGINE=InnoDB",
	PK:   []string{"at"}, Cols: []string{"at", "v"}, Datetimes: []string{"at"},
}

func dtLiteral(n int) string {
	return fmt.Sprintf("'2026-04-01 %02d:%02d:%02d'", n/3600, n/60%60, n%60)
}

var shapeTables = []string{"t_int", "t_comp", "t_text", "t_types", "t_dt"}

// seedOps inserts keys 1..12 and 101..112 on every table: the rows the
// shapes move, delete and update.
func seedOps() []oracle.Op {
	var ops []oracle.Op
	for _, tb := range shapeTables {
		for _, base := range []int{0, 100} {
			for n := base + 1; n <= base+12; n++ {
				ops = append(ops, op(oracle.KindInsert, tb, insertOf(tb, n, n), n))
			}
		}
	}
	return ops
}

// shapeOps is the six #782 shapes on keys base+1..base+12 of every table, in
// one window: C `UPDATE 1→21; INSERT 1`, E `DELETE 2; UPDATE 3→2`, D
// `UPDATE 4→24; UPDATE 24→44`, F `UPDATE 5→25; UPDATE 25→5`, A `UPDATE
// 6→26; DELETE 26`, B `UPDATE 7→27; UPDATE 27 (non-key column)`, plus a lone
// move `UPDATE 8→28` and, on t_types, a move whose DECIMAL and DATETIME
// columns travel with the row.
func shapeOps(base int) []oracle.Op {
	var ops []oracle.Op
	mv := func(tb string, from, to int) oracle.Op {
		return op(oracle.KindPKUpdate, tb, fmt.Sprintf("UPDATE `%s` SET %s WHERE %s", tb, setKey(tb, to), keyOf(tb, from)), from, to)
	}
	del := func(tb string, n int) oracle.Op {
		return op(oracle.KindDelete, tb, fmt.Sprintf("DELETE FROM `%s` WHERE %s", tb, keyOf(tb, n)), n)
	}
	upd := func(tb string, n int, v int) oracle.Op {
		return op(oracle.KindUpdate, tb, fmt.Sprintf("UPDATE `%s` SET `v` = %d WHERE %s", tb, v, keyOf(tb, n)), n)
	}
	ins := func(tb string, n int, v int) oracle.Op {
		return op(oracle.KindInsert, tb, insertOf(tb, n, v), n)
	}
	for _, tb := range shapeTables {
		k := func(n int) int { return base + n }
		ops = append(ops,
			mv(tb, k(1), k(21)), ins(tb, k(1), 901), // C
			del(tb, k(2)), mv(tb, k(3), k(2)), // E
			mv(tb, k(4), k(24)), mv(tb, k(24), k(44)), // D
			mv(tb, k(5), k(25)), mv(tb, k(25), k(5)), // F
			mv(tb, k(6), k(26)), del(tb, k(26)), // A
			mv(tb, k(7), k(27)), upd(tb, k(27), 907), // B
			mv(tb, k(8), k(28)), // a lone move
		)
	}
	return ops
}
