package baseline

import "strings"

// declaredCollation reads a column's COLLATE and CHARACTER SET attributes from
// the text after its declared type: the collation's name, lower case, and
// whether a character set is named. Only top-level words count, so the same
// words inside a DEFAULT or a COMMENT string are not the attribute.
func declaredCollation(attrs string) (collation string, explicitCharset bool) {
	words := topLevelWords(attrs)
	for i := 0; i+1 < len(words); i++ {
		switch {
		case words[i] == "COLLATE" && collation == "":
			collation = strings.ToLower(words[i+1])
		case words[i] == "CHARSET" && words[i+1] != "",
			words[i] == "CHARACTER" && words[i+1] == "SET" && i+2 < len(words) && words[i+2] != "":
			explicitCharset = true
		}
	}
	return collation, explicitCharset
}

// tableDefaultCollation reads the table's own COLLATE option from an embedded
// CREATE TABLE, lower case, or "" when the options name none. The options sit
// on the line that closes the column list. A table that names a character set
// and no collation has that character set's default, which is never a binary
// one, so "" is the right answer for it too.
func tableDefaultCollation(createSQL string) string {
	for _, line := range strings.Split(createSQL, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, ")") {
			continue
		}
		words := topLevelWords(trimmed)
		for i := 0; i+1 < len(words); i++ {
			if words[i] == "COLLATE" && words[i+1] != "" {
				return strings.ToLower(words[i+1])
			}
		}
		return ""
	}
	return ""
}

// BinaryCollationColumns names the text columns MySQL compares byte by byte: a
// CHAR, VARCHAR, TEXT, ENUM or SET column whose collation ends in _bin, by its
// own definition or by the table's default. createSQL is the embedded CREATE
// TABLE cols was parsed from.
//
// A reader whose session folds case and accents (the copy's SQL session does,
// to match MySQL's default collation) needs these named, or `code = 'ab'`
// matches 'AB' on a column where MySQL says it does not (#2083).
//
// Only _bin. A _cs collation also tells 'a' from 'A', but it orders letters
// alphabetically ('a' before 'B') where bytes do not, so byte comparison
// would trade one difference for another; it is left to the reader's default.
func BinaryCollationColumns(createSQL string, cols []Column) []string {
	tableCollation := tableDefaultCollation(createSQL)
	var out []string
	for _, c := range cols {
		switch c.MySQLType {
		case "char", "varchar", "tinytext", "text", "mediumtext", "longtext", "enum", "set":
		default:
			continue
		}
		collation := c.Collation
		if collation == "" && !c.ExplicitCharset {
			collation = tableCollation
		}
		if strings.HasSuffix(collation, "_bin") {
			out = append(out, c.Name)
		}
	}
	return out
}
