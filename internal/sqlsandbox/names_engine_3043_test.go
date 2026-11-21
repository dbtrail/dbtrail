package sqlsandbox

import (
	"context"
	"database/sql"
	"slices"
	"testing"
)

// #3043: the console matches the names a statement writes to the copy's
// views with A-Z folded (foldName here, sqlFoldName there), and in fresh
// mode leaves aside a bare name that matched no view as no table of the
// copy. That is sound only while the engine takes a name for a view exactly
// when the two fold to the same bytes. This asks the engine, under the
// session's settings and under PostgreSQL's: every spelling it resolves to a
// view must match that view by the fold, and none it resolves may be missed.
func TestTheEngineTakesANameForAViewOnlyWhenTheFoldDoes_3043(t *testing.T) {
	const (
		cafe   = "caf\u00e9"   // one code point for the accent
		kelvin = "\u212aelvin" // the Kelvin sign: Unicode lower-cases it to k
		longS  = "\u017ftats"  // the long s: Unicode upper-cases it to S
		eszett = "stra\u00dfe" // Unicode upper-cases it to SS
		dotted = "\u0130d"     // the dotted capital I: lower-cases to i and a dot
	)
	viewNames := []string{cafe, "Orders", kelvin, longS, eszett, dotted}
	spellings := []string{
		// A letter outside ASCII, in another case, bare and quoted, and
		// with the accent as a code point of its own.
		"caf\u00e9", "CAF\u00c9", "Caf\u00c9", "\"caf\u00e9\"", "\"CAF\u00c9\"", "`caf\u00e9`", "`CAF\u00c9`",
		"cafe\u0301", "\"cafe\u0301\"", "CAFE\u0301", "cafe", "CAFE", "\"cafe\"",
		// ASCII in every quoting, mixed case.
		"orders", "ORDERS", "oRdErS", "\"Orders\"", "\"ORDERS\"", "\"orders\"", "`Orders`", "`orders`", "[orders]", "[Orders]", "'orders'", "'Orders'",
		// A space or a quote inside the quotes.
		"\"Orders \"", "\" Orders\"", "\"orders \"", "\"Or\"\"ders\"", "\"Orders\"\"\"", "\"Orders\\\"", "\"or ders\"",
		// A view whose name has a letter that Unicode folds into ASCII,
		// named by the ASCII letters: plain names, which the fold here does
		// not take for the view.
		"kelvin", "KELVIN", "Kelvin", "\"kelvin\"", "\"KELVIN\"", "stats", "STATS", "Stats", "\"STATS\"",
		"strasse", "STRASSE", "\"STRASSE\"", "id", "ID", "\"id\"", "i\u0307d",
		// And each as it is, and in the other case of its own letters.
		"\u212aelvin", "\u212aELVIN", "\u017ftats", "\u017fTATS", "stra\u00dfe", "STRA\u00dfE", "STRA\u1e9eE", "\u0130d", "\u0130D",
	}
	for _, kind := range []struct {
		name     string
		settings []string
	}{
		{"the session's settings", fixedSettings},
		{"PostgreSQL's rules", slices.Concat(fixedSettings, postgresSettings)},
	} {
		t.Run(kind.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			for _, s := range kind.settings {
				if _, err := conn.ExecContext(ctx, s); err != nil {
					t.Fatalf("%s: %v", s, err)
				}
			}
			if _, err := conn.ExecContext(ctx, "CREATE SCHEMA shop"); err != nil {
				t.Fatal(err)
			}
			for i, v := range viewNames {
				if _, err := conn.ExecContext(ctx, `CREATE VIEW shop."`+v+`" AS SELECT `+string(rune('1'+i))+` AS n`); err != nil {
					t.Fatalf("create the view %q: %v", v, err)
				}
			}
			if _, err := conn.ExecContext(ctx, "USE shop"); err != nil {
				t.Fatal(err)
			}
			resolvedSome := 0
			for _, sp := range spellings {
				text := "SELECT n FROM " + sp
				stmt, reason := parseStatement(ctx, conn, text, false)
				if reason != "" {
					t.Logf("%-12s not a statement here", sp)
					continue
				}
				refs := collectRefs(stmt)
				var n int
				resolved := conn.QueryRowContext(ctx, text).Scan(&n) == nil
				// The view the engine read, by the number it holds.
				var took string
				if resolved {
					took = viewNames[n-1]
					resolvedSome++
				}
				matched := ""
				sure := !refs.Unsure && len(refs.Tables) == 1 && refs.Tables[0].Catalog == "" && refs.Tables[0].Schema == ""
				if sure {
					for _, v := range viewNames {
						if foldName(v) == foldName(refs.Tables[0].Name) {
							matched = v
						}
					}
				}
				t.Logf("%-12s engine: %-8q fold: %-8q (parsed as %+v, unsure %v)", sp, took, matched, refs.Tables, refs.Unsure)
				switch {
				case !resolved:
					// Not a table to the engine: the statement fails,
					// whatever the fold said.
				case !sure:
					t.Errorf("%s: the engine read the view %q, and the parse does not report one bare relation (%+v, unsure %v)", sp, took, refs.Tables, refs.Unsure)
				case matched != took:
					t.Errorf("%s: the engine read the view %q, and the fold matched %q: a name the engine resolves and the match misses is a table read without its tail", sp, took, matched)
				}
			}
			if resolvedSome < 8 {
				t.Fatalf("the engine resolved %d spellings: the views are not there to be found", resolvedSome)
			}
		})
	}
}
