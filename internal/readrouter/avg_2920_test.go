package readrouter

import (
	"strings"
	"testing"
)

// routedOpts are the options the routing port runs with (shim copyOpts).
var routedOpts = CopyOptions{MySQLClockKeywords: true, MySQLDateFunctions: true, MySQLDateSub: true, MySQLDateAdd: true, MySQLTimestampDiff: true, MySQLAvg: true}

// #2920: under routing AVG(x) and AVG(DISTINCT x) are sent to the copy as its
// exact average of whole numbers. Every call is written, or the statement is
// refused; the port without routing (no MySQLAvg) sends AVG as written.
func TestForCopyOpts_avg_2920(t *testing.T) {
	written := map[string]string{
		"SELECT AVG(k) FROM t":                         "SELECT _bintrail_avg(k) FROM t",
		"SELECT avg (k) FROM t":                        "SELECT _bintrail_avg(k) FROM t",
		"SELECT Avg(\nk\n) FROM t":                     "SELECT _bintrail_avg(k\n) FROM t",
		"SELECT AVG(`k`) FROM `t`":                     `SELECT _bintrail_avg("k") FROM "t"`,
		"SELECT AVG(DISTINCT k) FROM t":                "SELECT _bintrail_avg_distinct(k) FROM t",
		"SELECT AVG(distinct(k)) FROM t":               "SELECT _bintrail_avg_distinct((k)) FROM t",
		"SELECT AVG(DISTINCT t.k), AVG(t.b) a FROM t":  "SELECT _bintrail_avg_distinct(t.k), _bintrail_avg(t.b) a FROM t",
		"SELECT g, AVG(k) FROM t GROUP BY g":           "SELECT g, _bintrail_avg(k) FROM t GROUP BY g",
		"SELECT AVG(k) FROM t WHERE s = 'AVG(k)'":      "SELECT _bintrail_avg(k) FROM t WHERE s = 'AVG(k)'",
		"SELECT AVG(k) /* AVG(x) */ FROM t":            "SELECT _bintrail_avg(k) /* AVG(x) */ FROM t",
		"SELECT avg, avg_k FROM t":                     "SELECT avg, avg_k FROM t",
		"SELECT myavg(k), average(k) FROM t":           "SELECT myavg(k), average(k) FROM t",
		"SELECT AVG(DISTINCTROW k) FROM t":             "SELECT _bintrail_avg(DISTINCTROW k) FROM t", // MySQL's error; the copy's too
		"SELECT AVG(distinct_k) FROM t":                "SELECT _bintrail_avg(distinct_k) FROM t",
		"SELECT ROUND(AVG(k), 2) FROM t":               "SELECT ROUND(_bintrail_avg(k), 2) FROM t", // RoundVeto keeps it on MySQL
	}
	for stmt, want := range written {
		got, why := ForCopyOpts(stmt, routedOpts)
		if why != "" || got != want {
			t.Errorf("ForCopyOpts(%q) = %q, %q; want %q", stmt, got, why, want)
		}
		// Without the option the copy gets AVG as written.
		if plain, _ := ForCopyOpts(stmt, CopyOptions{}); strings.Contains(plain, "_bintrail_avg") {
			t.Errorf("ForCopyOpts(%q) without MySQLAvg = %q, want AVG as written", stmt, plain)
		}
	}
	refused := []string{
		"SELECT AVG/**/(k) FROM t",     // a comment before the parenthesis is not read
		"SELECT t.avg(k) FROM t",       // a stored function on MySQL
		"SELECT `db`.avg(k) FROM t",    //
		"SELECT AVG(",                  // a prepared statement's piece cut at a placeholder
		"SELECT AVG(k, b) FROM t",      // two arguments: MySQL's error
		"SELECT AVG() FROM t",          //
		"SELECT AVG(k) + AVG/**/(b) FROM t", // one written, one not
	}
	for _, stmt := range refused {
		if got, why := ForCopyOpts(stmt, routedOpts); why != VetoAvgLeft {
			t.Errorf("ForCopyOpts(%q) = %q, %q; want refused %q", stmt, got, why, VetoAvgLeft)
		}
	}
}

// Under routing an AVG no longer stops at the veto list when every call is
// written as the macro: what it is given is AvgVeto's, once the types are
// known. MEDIAN still stops there, and so does an AVG left unwritten.
func TestVetoOpts_avg_2920(t *testing.T) {
	pass := []string{
		"SELECT AVG(k) FROM t",
		"SELECT avg (k) FROM t",
		"SELECT g, AVG(DISTINCT k) FROM t GROUP BY g ORDER BY g",
		"SELECT AVG(`k`) AS `a` FROM `t`",
	}
	for _, stmt := range pass {
		if got := VetoOpts(stmt, routedOpts); got != "" {
			t.Errorf("VetoOpts(%q) = %q, want none (AvgVeto decides)", stmt, got)
		}
		if got := Veto(stmt); got != vetoAvg {
			t.Errorf("Veto(%q) without the options = %q, want %q", stmt, got, vetoAvg)
		}
	}
	kept := map[string]string{
		"SELECT AVG/**/(k) FROM t":        VetoAvgLeft,
		"SELECT t.avg(k) FROM t":          VetoAvgLeft,
		"SELECT MEDIAN(k) OVER () FROM t": vetoMedian,
		"SELECT AVG(k), MEDIAN(k) OVER () FROM t": vetoMedian,
		"SELECT AVG(k) / 2 FROM t":        vetoDivision,
		"SELECT _bintrail_avg(k) FROM t":  vetoCopyMacro,
		"SELECT _bintrail_avg_of(k, 1) FROM t": vetoCopyMacro,
	}
	for stmt, want := range kept {
		if got := VetoOpts(stmt, routedOpts); got != want {
			t.Errorf("VetoOpts(%q) = %q, want %q", stmt, got, want)
		}
	}
}

// AvgVeto (#2920): the copy answers an AVG only of a bare whole-number
// column, as a whole column of the select list, named nowhere after it.
func TestAvgVeto_2920(t *testing.T) {
	whole := []string{"k", "b", "u", "ti", "bo", "Ä"}
	other := []string{"dc", "dbl", "f", "s", "dt", "id2"}
	answered := []string{
		"SELECT AVG(k) FROM t",
		"SELECT AVG(k), AVG(b), AVG(u), AVG(ti), AVG(bo) FROM t",
		"SELECT g, AVG(k) FROM t GROUP BY g",
		"SELECT g, COUNT(*), AVG(k) FROM t GROUP BY g ORDER BY g",
		"SELECT g AS bucket, COUNT(*), AVG(k) FROM t GROUP BY bucket ORDER BY bucket", // the issue's shape, over a column
		"SELECT g, AVG(k) FROM t GROUP BY g ORDER BY 1",
		"SELECT g, AVG(k) FROM t GROUP BY 1 ORDER BY 1 DESC",
		"SELECT g, AVG(k) AS a FROM t GROUP BY g ORDER BY g LIMIT 2",
		"SELECT AVG(k) a FROM t",
		`SELECT AVG(k) AS "my avg" FROM t`,
		"SELECT AVG(DISTINCT k) FROM t",
		"SELECT AVG(DISTINCT(k)) FROM t",
		"SELECT AVG(t.k), AVG(db.t.k) FROM db.t",
		`SELECT AVG("k") FROM t`,
		"SELECT AVG((k)) FROM t",
		"SELECT AVG( k ) FROM t",
		"SELECT avg (k) FROM t",
		"SELECT AVG(K) FROM t", // MySQL folds a column's name
		"SELECT AVG(ä) FROM t",
		"SELECT AVG(k) FROM t WHERE k > 0 AND s = 'AVG(dbl)'",
		"SELECT AVG(k) FROM t;",
		"SELECT AVG(k) FROM t HAVING COUNT(*) > 1",
		"SELECT avg, AVG(k) FROM t",
		"SELECT g FROM t", // no AVG
	}
	for _, s := range answered {
		if got := AvgVeto(Shape(s), whole, other); got != "" {
			t.Errorf("AvgVeto(%q) = %q, want none", s, got)
		}
	}
	kept := []string{
		// Not a whole-number column.
		"SELECT AVG(dc) FROM t",
		"SELECT AVG(dbl) FROM t",
		"SELECT AVG(f) FROM t",
		"SELECT AVG(s) FROM t",
		"SELECT AVG(dt) FROM t",
		"SELECT AVG(nowhere) FROM t",
		"SELECT AVG(id2) FROM t",
		"SELECT AVG(LENGTH(s)) FROM t", // the issue's: an expression the router does not type
		"SELECT g AS bucket, COUNT(*), AVG(LENGTH(s)) FROM t GROUP BY bucket ORDER BY bucket",
		"SELECT AVG(k + 1) FROM t",
		"SELECT AVG(-k) FROM t",
		"SELECT AVG(1) FROM t",
		"SELECT AVG(?) FROM t",
		"SELECT AVG(ALL k) FROM t",
		"SELECT AVG(DISTINCTROW k) FROM t",
		"SELECT AVG(k), AVG(dbl) FROM t",
		"SELECT AVG(k) FROM t", // below, with k a DOUBLE in another table
		// Used other than as a whole column of the select list.
		"SELECT AVG(k) * 3 FROM t",
		"SELECT AVG(k) + 0 FROM t",
		"SELECT -AVG(k) FROM t",
		"SELECT AVG(k) > 2.3333 FROM t",
		"SELECT AVG(k) = 2.3333 FROM t",
		"SELECT ROUND(AVG(k), 1) FROM t",
		"SELECT CASE WHEN AVG(k) > 1 THEN 1 END FROM t",
		"SELECT COALESCE(AVG(k), 0) FROM t",
		"SELECT (AVG(k)) FROM t",
		"SELECT AVG(k) OVER () FROM t",
		"SELECT AVG(k) OVER w FROM t WINDOW w AS ()",
		"SELECT AVG(k) over FROM t",
		"SELECT DISTINCT AVG(k) FROM t",
		"SELECT DISTINCT g, AVG(k) FROM t GROUP BY g",
		"SELECT AVG(k) AS '' FROM t",
		"SELECT AVG(k) 'a' FROM t",
		"SELECT g FROM t GROUP BY g HAVING AVG(k) > 1",
		"SELECT g, AVG(k) a FROM t GROUP BY g HAVING a > 2.3333",
		"SELECT g, AVG(k) a FROM t GROUP BY g ORDER BY a",
		"SELECT g, AVG(k) AS `A` FROM t GROUP BY g ORDER BY `a` DESC",
		"SELECT g, AVG(k) FROM t GROUP BY g ORDER BY 2",
		"SELECT g, AVG(k) FROM t GROUP BY g ORDER BY 2 DESC, 1",
		"SELECT g, AVG(k) FROM t GROUP BY g ORDER BY (2)",
		"SELECT g, AVG(k) FROM t GROUP BY g ORDER BY `AVG(k)`",
		"SELECT g FROM t ORDER BY AVG(k)",
		"SELECT g FROM t WHERE AVG(k) > 1",
		// A name the statement may define itself, or a set operation.
		"SELECT AVG(k) FROM (SELECT dbl AS k FROM t) x",
		"WITH x AS (SELECT dbl AS k FROM t) SELECT AVG(k) FROM x",
		"SELECT AVG(k) FROM t WHERE k IN (SELECT k FROM t)",
		"SELECT g, AVG(k) FROM t GROUP BY g WITH ROLLUP",
		"SELECT AVG(k) FROM t UNION SELECT AVG(b) FROM t",
		"(SELECT AVG(k) FROM t)",
		// A stored function on MySQL, or an AVG MySQL does not read as one.
		"SELECT t.avg(k) FROM t",
		`SELECT "db".avg(k) FROM t`,
		"SELECT AVG(k FROM t",
	}
	for _, s := range kept {
		o := other
		if s == "SELECT AVG(k) FROM t" {
			o = append(append([]string(nil), other...), "k")
		}
		if got := AvgVeto(Shape(s), whole, o); got != VetoAvg {
			t.Errorf("AvgVeto(%q) = %q, want %q", s, got, VetoAvg)
		}
	}
	// No column known: every AVG over a column stays on MySQL.
	if got := AvgVeto(Shape("SELECT AVG(k) FROM t"), nil, nil); got != VetoAvg {
		t.Errorf("AvgVeto with no columns known = %q, want %q", got, VetoAvg)
	}
	if got := ShapeOf("SELECT AVG(k) FROM t").AvgVeto(whole, other); got != "" {
		t.Errorf("StatementShape.AvgVeto = %q, want none", got)
	}
}

// AvgVeto reads the statement in one pass whatever its length.
func TestAvgVeto_long_2920(t *testing.T) {
	stmt := "SELECT " + strings.Repeat("AVG(k), ", 20000) + "AVG(k) FROM t ORDER BY " + strings.Repeat("1, ", 20000) + "1"
	if got := AvgVeto(Shape(stmt), []string{"k"}, nil); got != VetoAvg {
		t.Errorf("AvgVeto(20001 AVGs ordered by the first) = %q, want %q", got, VetoAvg)
	}
}
