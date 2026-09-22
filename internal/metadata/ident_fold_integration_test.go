//go:build integration

package metadata

import (
	"fmt"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The comments on IdentEqualFold and SuggestPKColumn make factual claims about
// how MySQL compares COLUMN IDENTIFIERS, and those claims decide whether the
// statement this card prints runs or dies with ERROR 1060. A comment cannot
// be checked by reading it, so the claims run here against a real server,
// through the SAME driver and connection charset the product uses.
//
// That last part is load-bearing. Probing this by hand through a mysql client
// whose default character set is latin1 (the one in the test container is)
// double-encodes İ on the way in and decodes it symmetrically on the way out:
// every statement looks right, information_schema echoes the name back
// looking right, and the answers are all wrong, because the column that was
// really created was mojibake that collides with nothing. That mistake was
// made while writing this test and cost a true claim being called false.
func TestIntegrationMySQLFoldsOnlyASCIICaseInColumnIdentifiers(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)

	// Each case creates one table with TWO column names. MySQL either accepts
	// them as distinct identifiers, or refuses with ERROR 1060.
	for _, tc := range []struct {
		name     string
		a, b     string
		wantSame bool // true = MySQL calls them ONE identifier (1060)
	}{
		{"ascii case", "i", "I", true},
		// The dotted I folds. This is what makes strings.ToLower the right
		// rule in SuggestPKColumn, since it folds it the same way.
		{"dotted I (U+0130) against i", "i", "İ", true},
		{"dotted I against upper I", "I", "İ", true},
		// The dotless i does NOT. IdentEqualFold folds it anyway, wider than
		// the server, which is safe for both of its callers and documented
		// there.
		{"dotless i (U+0131) against i", "i", "ı", false},
		{"dotless i against upper I", "I", "ı", false},
		{"accent", "e", "é", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := "identfold_" + strings.NewReplacer(" ", "_", "(", "", ")", "", "+", "").Replace(tc.name)
			if _, err := db.Exec("DROP TABLE IF EXISTS `" + table + "`"); err != nil {
				t.Fatal(err)
			}
			_, err := db.Exec(fmt.Sprintf("CREATE TABLE `%s` (`%s` INT, `%s` INT)", table, tc.a, tc.b))
			gotSame := err != nil && strings.Contains(err.Error(), "Duplicate column name")
			if err != nil && !gotSame {
				t.Fatalf("CREATE TABLE with columns %q and %q: %v", tc.a, tc.b, err)
			}
			if gotSame != tc.wantSame {
				verb := map[bool]string{true: "ONE identifier", false: "two distinct identifiers"}
				t.Errorf("MySQL treats %q and %q as %s; the source says %s",
					tc.a, tc.b, verb[gotSame], verb[tc.wantSame])
			}
		})
	}
}

// TestIntegrationSuggestPKColumnNameIsAccepted closes the loop the card
// exists for: the name SuggestPKColumn picks has to be one the server will
// actually take, on the very shapes where a case-folding rule could get it
// wrong. Over-folding only costs a less pretty name; UNDER-folding hands the
// operator a statement that dies with ERROR 1060, which is the failure this
// whole change was written to remove.
func TestIntegrationSuggestPKColumnNameIsAccepted(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)

	for _, tc := range []struct {
		name      string
		existing  string
		want      string
		idRefused bool // does the server refuse a plain `id` beside it?
	}{
		{"plain id is taken", "id", "dbtrail_id", true},
		{"ascii-case id is taken", "Id", "dbtrail_id", true},
		// ToLower maps the dotted I to i, and so does the server: `id` really
		// IS taken here. A narrower rule (plain EqualFold folds neither
		// Turkish letter) would offer `id` and reproduce ERROR 1060, which is
		// the whole failure this card exists to remove.
		{"dotted I (U+0130)", "İd", "dbtrail_id", true},
		// ToLower leaves the dotless i alone, and so does the server, so `id`
		// is offered and accepted.
		{"dotless i (U+0131)", "ıd", "id", false},
		{"nothing in the way", "happened_at", "id", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := "suggestpk_" + strings.NewReplacer(" ", "_", "(", "", ")", "", "+", "", "-", "_").Replace(tc.name)
			if _, err := db.Exec("DROP TABLE IF EXISTS `" + table + "`"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(fmt.Sprintf("CREATE TABLE `%s` (`%s` INT, v INT)", table, tc.existing)); err != nil {
				t.Fatal(err)
			}
			got := SuggestPKColumn([]string{tc.existing, "v"})
			if got != tc.want {
				t.Errorf("SuggestPKColumn(%q) = %q, want %q", tc.existing, got, tc.want)
			}
			// WHY that answer: ask the server directly whether a plain `id`
			// would have been refused beside this column. Without this, the
			// row above only says what the function returns, not that the
			// return is the one the server needs.
			probe := "idprobe_" + table
			if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", probe)); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(fmt.Sprintf("CREATE TABLE `%s` (`%s` INT)", probe, tc.existing)); err != nil {
				t.Fatal(err)
			}
			_, err := db.Exec(fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `id` INT", probe))
			refused := err != nil && strings.Contains(err.Error(), "Duplicate column name")
			if err != nil && !refused {
				t.Fatalf("probing `id` beside %q: %v", tc.existing, err)
			}
			if refused != tc.idRefused {
				verb := map[bool]string{true: "REFUSES", false: "accepts"}
				t.Errorf("the server %s a plain `id` beside %q; the source says it %s it",
					verb[refused], tc.existing, verb[tc.idRefused])
			}
			// The real test: the server takes the suggestion. Every name this
			// function can return is plain ASCII, so a backtick pair is the
			// whole quoting story here.
			stmt := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST",
				table, got)
			if _, err := db.Exec(stmt); err != nil {
				t.Errorf("the suggested name %q is one the server refuses beside %q: %v", got, tc.existing, err)
			}
		})
	}
}
