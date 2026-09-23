package metadata

import "testing"

// TestExclusionReasonsArePersistedVocabulary pins the exact strings a degraded
// snapshot writes into snapshot_exclusions.reason (#1802). They live in every
// index that ever excluded a table, and the status report classifies them to
// name each table's fix: a changed constant would still compile everywhere and
// quietly turn every row already on disk into an "unknown reason" with no fix.
func TestExclusionReasonsArePersistedVocabulary(t *testing.T) {
	for got, want := range map[string]string{
		ExclusionReasonNotInnoDB:    "not InnoDB",
		ExclusionReasonNoPrimaryKey: "no primary key",
		ExclusionReasonNotInnoDB + ExclusionReasonSeparator + ExclusionReasonNoPrimaryKey: "not InnoDB; no primary key",
	} {
		if got != want {
			t.Errorf("persisted exclusion reason = %q, want %q", got, want)
		}
	}
}

// TestSuggestPKColumn picks a column name the table does not already use
// (#1802). A plain non-key `id` column is the commonest shape of a key-less
// table, and adding a column named `id` to it fails with MySQL's ERROR 1060 —
// verified on 8.4.9 — leaving the operator with a statement that does not run.
// Column names compare case-insensitively, as MySQL compares them.
func TestSuggestPKColumn(t *testing.T) {
	cases := []struct {
		name     string
		existing []string
		want     string
	}{
		{"nothing in the way", []string{"happened_at", "actor"}, "id"},
		{"no columns at all", nil, "id"},
		{"a non-key id column", []string{"id", "v"}, "dbtrail_id"},
		{"id in another case", []string{"ID"}, "dbtrail_id"},
		{"id and dbtrail_id", []string{"id", "dbtrail_id"}, "dbtrail_id_2"},
		{"and the next one too", []string{"id", "DBTrail_ID", "dbtrail_id_2"}, "dbtrail_id_3"},
		{"an accented near-miss is a different column", []string{"íd"}, "id"},
		{"spaces around a name do not hide it", []string{" id "}, "dbtrail_id"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := SuggestPKColumn(c.existing); got != c.want {
				t.Errorf("SuggestPKColumn(%q) = %q, want %q", c.existing, got, c.want)
			}
		})
	}
}

// TestIdentEqualFold guards the shared identifier fold IN ITS OWN PACKAGE.
// Until this test existed, neutering IdentEqualFold left internal/metadata
// green: its only guards lived in internal/parser and internal/status, so the
// package that OWNS the rule could not see it break. Every case here is
// measured against a real MySQL 8.4.9 by
// TestIntegrationMySQLFoldsOnlyASCIICaseInColumnIdentifiers; the two lines
// marked DELIBERATELY WIDER are where this helper folds more than the server
// does, on purpose.
func TestIdentEqualFold(t *testing.T) {
	for _, c := range []struct {
		name string
		a, b string
		want bool
	}{
		{"identical", "orders", "orders", true},
		{"ascii case, which MySQL folds", "Orders", "orders", true},
		{"different names", "orders", "order", false},
		{"empty against a name", "", "orders", false},
		// DELIBERATELY WIDER than the server: 8.4.9 keeps these apart.
		{"dotted I (U+0130) folds here", "İstanbul", "istanbul", true},
		{"dotless i (U+0131) folds here", "ıstanbul", "istanbul", true},
		// NOT folded, by the server and by this helper alike.
		{"accents stay distinct", "événement", "evenement", false},
		{"a different letter entirely", "İstanbul", "ankara", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := IdentEqualFold(c.a, c.b); got != c.want {
				t.Errorf("IdentEqualFold(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
			}
			if got := IdentEqualFold(c.b, c.a); got != c.want {
				t.Errorf("IdentEqualFold(%q, %q) = %v, want %v (the rule has to be symmetric)", c.b, c.a, got, c.want)
			}
		})
	}
}

// TestSuggestPKColumnOnTheTurkishPair pins the two shapes where a change of
// folding rule would change the answer, so that swapping strings.ToLower for
// IdentEqualFold cannot happen silently. Both answers are verified against a
// real server by TestIntegrationSuggestPKColumnNameIsAccepted: 8.4.9 ACCEPTS
// `id` beside either Turkish letter, so the dotted-I row is this function
// being more cautious than it has to be, which costs a prettier name and
// nothing else.
func TestSuggestPKColumnOnTheTurkishPair(t *testing.T) {
	if got := SuggestPKColumn([]string{"İd"}); got != "dbtrail_id" {
		t.Errorf("SuggestPKColumn with a dotted-I `İd` column = %q, want dbtrail_id (ToLower maps İ to i, so id reads as taken)", got)
	}
	if got := SuggestPKColumn([]string{"ıd"}); got != "id" {
		t.Errorf("SuggestPKColumn with a dotless-i `ıd` column = %q, want id (ToLower leaves ı alone, and the server takes id beside it)", got)
	}
}
