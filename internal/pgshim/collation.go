package pgshim

// A collation that is not deterministic (#3111).
//
// Under a nondeterministic collation (an ICU one that ignores case or
// accents) PostgreSQL finds 'ab' and 'AB' equal, groups them as one and
// sorts them by the collation; the copy compares bytes. So a column under one
// is among those the copy does not compare as PostgreSQL does
// (userTypeColumn): used, it is the source's; shown, it stays on the copy.

// Collation is what the port knows of a column's collation.
type Collation uint8

const (
	// CollationDeterministic: equal text is equal bytes, as on the copy. A
	// column that holds no text has it too. It is the zero value: the one
	// catalog the port reads (consoleapp's pgCopyCatalog) sets every
	// column's Collation itself, starting from CollationUnknown.
	CollationDeterministic Collation = iota
	// CollationNondeterministic: pg_collation.collisdeterministic is false.
	CollationNondeterministic
	// CollationUnknown: the snapshot did not record it and the source was
	// not asked. A column of text is then taken for nondeterministic.
	CollationUnknown
)

// collatedOtherwise reports whether a column's collation may find equal what
// the copy does not.
func collatedOtherwise(c CatalogColumn) bool {
	switch c.Collation {
	case CollationNondeterministic:
		return true
	case CollationUnknown:
		switch c.SourceType {
		case oidText, oidVarchar, oidBpchar, oidName, oidTextArray, oidVarcharArray, oidBpcharArray, oidNameArray:
			return true
		}
	}
	return false
}
