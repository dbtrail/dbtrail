package sqlsandbox

import "testing"

// The walker's default, which had no guard (#2123): a star whose FROM holds
// something the walker does not read (a table function, a VALUES list, no
// FROM at all) is NOT attributed to the tables it did read. It stays a star
// over everything (Refs.Star), so the caller checks every table the statement
// reads. Attributing it to the readable part would leave out whatever the
// other part expands.
func TestCollectRefs_starOverAFromTheWalkerCannotRead(t *testing.T) {
	for _, sqlText := range []string{
		"SELECT * FROM range(3)",
		"SELECT * FROM shop.orders, range(3)",
		"SELECT * FROM shop.orders o JOIN range(3) r ON true",
		"SELECT * FROM (VALUES (1), (2)) v(x)",
		"SELECT o.* FROM shop.orders o, range(3)",
		"SELECT id FROM shop.gen WHERE id IN (SELECT * FROM shop.orders, range(3))",
	} {
		refs := parseForRefs(t, sqlText)
		if !refs.Star || len(refs.StarTables) != 0 {
			t.Errorf("%s: Star = %v, StarTables = %v; want the star left unattributed (Star true, no StarTables)", sqlText, refs.Star, refs.StarTables)
		}
	}
	// The same shapes with a readable FROM are attributed: the default is
	// what the unread part costs, not what every star gets.
	if refs := parseForRefs(t, "SELECT * FROM shop.orders o JOIN shop.gen g ON true"); refs.Star || len(refs.StarTables) != 2 {
		t.Errorf("a readable FROM: Star = %v, StarTables = %v; want both tables and no unattributed star", refs.Star, refs.StarTables)
	}
}
