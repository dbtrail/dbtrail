package pgshim

import "testing"

// A column under a nondeterministic collation, or one of text whose collation
// is not known, is the source's wherever a statement uses it (#3111).
func collationCatalog(ci, unknown Collation) []CatalogTable {
	return []CatalogTable{{Schema: "public", Name: "ct", Columns: []CatalogColumn{
		{Name: "id", Type: "INTEGER", SourceType: oidInt4, SourceTypeMod: -1, Collation: unknown},
		{Name: "t", Type: "VARCHAR", SourceType: oidText, SourceTypeMod: -1, Collation: ci},
		{Name: "v", Type: "VARCHAR", SourceType: oidVarchar, SourceTypeMod: 24, Collation: CollationDeterministic},
		{Name: "tags", Type: "VARCHAR[]", SourceType: oidTextArray, SourceTypeMod: -1, Collation: unknown},
	}}}
}

func TestCollation_used(t *testing.T) {
	for name, tables := range map[string][]CatalogTable{
		"nondeterministic": collationCatalog(CollationNondeterministic, CollationDeterministic),
		"not known":        collationCatalog(CollationUnknown, CollationUnknown),
	} {
		use := func(q string) string {
			toks, why := lexSQL(q)
			if why != "" {
				t.Fatalf("%s: %s", q, why)
			}
			return userTypeUse(toks, screenTypes(tables, tablesRead(q, tables)))
		}
		for _, q := range []string{
			`SELECT count(*) FROM ct WHERE t = 'AB'`,
			`SELECT count(*) FROM ct WHERE t IN ('ab')`,
			`SELECT count(DISTINCT t) FROM ct`,
			`SELECT DISTINCT t FROM ct`,
			`SELECT t, count(*) FROM ct GROUP BY t`,
			`SELECT count(*) FROM ct a JOIN ct b ON a.t = b.t`,
			`SELECT id FROM ct ORDER BY t`,
			`SELECT lower(t) FROM ct`,
			`SELECT min(t) FROM ct`,
			`SELECT t FROM ct ORDER BY 1`,
		} {
			if use(q) == "" {
				t.Errorf("%s: %s: left to the copy", name, q)
			}
		}
		for _, q := range []string{
			`SELECT t FROM ct WHERE id = 2`,
			`SELECT * FROM ct WHERE id = 2`,
			`SELECT count(*) FROM ct WHERE v = 'ab'`,
			`SELECT id FROM ct WHERE id > 1 ORDER BY id`,
		} {
			if veto := use(q); veto != "" {
				t.Errorf("%s: %s: %s", name, q, veto)
			}
		}
	}
	// Not known for a text array either; known deterministic, nothing.
	tables := collationCatalog(CollationDeterministic, CollationUnknown)
	if q := `SELECT id FROM ct WHERE 'a' = ANY(tags)`; CopyScreenFor(q, tables) == "" {
		t.Errorf("%s: left to the copy", q)
	}
	tables = collationCatalog(CollationDeterministic, CollationDeterministic)
	for _, q := range []string{`SELECT count(*) FROM ct WHERE t = 'AB'`, `SELECT id FROM ct WHERE 'a' = ANY(tags)`} {
		if veto := CopyScreenFor(q, tables); veto != "" {
			t.Errorf("%s: deterministic: %s", q, veto)
		}
	}
}
