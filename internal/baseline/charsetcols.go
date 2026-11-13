package baseline

import "strings"

// knownCharsets are the character set names of MySQL 8 and MariaDB. None holds
// an underscore, which is what lets collationCharset read the character set
// off the front of a collation's name.
var knownCharsets = map[string]bool{
	"armscii8": true, "ascii": true, "big5": true, "binary": true, "cp1250": true, "cp1251": true,
	"cp1256": true, "cp1257": true, "cp850": true, "cp852": true, "cp866": true, "cp932": true,
	"dec8": true, "eucjpms": true, "euckr": true, "gb18030": true, "gb2312": true, "gbk": true,
	"geostd8": true, "greek": true, "hebrew": true, "hp8": true, "keybcs2": true, "koi8r": true,
	"koi8u": true, "latin1": true, "latin2": true, "latin5": true, "latin7": true, "macce": true,
	"macroman": true, "sjis": true, "swe7": true, "tis620": true, "ucs2": true, "ujis": true,
	"utf16": true, "utf16le": true, "utf32": true, "utf8": true, "utf8mb3": true, "utf8mb4": true,
}

// ComparableCharset is a character set's name in the spelling two
// descriptions of the same column compare equal on: lower case, and utf8 read
// as utf8mb3, which is what it is on MySQL and MariaDB alike. A CREATE TABLE
// dumped before a server upgrade can say utf8 where information_schema says
// utf8mb3 after it, and that is not a change.
func ComparableCharset(name string) string {
	cs := strings.ToLower(strings.TrimSpace(name))
	if cs == "utf8" {
		return "utf8mb3"
	}
	return cs
}

// collationCharset is the character set a collation belongs to, read from the
// front of its name (latin1_swedish_ci is latin1's), or "" when that front is
// not a character set name this build knows.
func collationCharset(collation string) string {
	if collation == "binary" {
		return "binary"
	}
	if cs, _, ok := strings.Cut(collation, "_"); ok && knownCharsets[cs] {
		return cs
	}
	return ""
}

// ColumnCharsets names the character set of every text column of a parsed
// schema (CHAR, VARCHAR, the TEXT types, ENUM and SET), by column name, in
// ComparableCharset's spelling (#2723). createSQL is the CREATE TABLE cols was
// parsed from.
//
// SHOW CREATE TABLE writes a column's character set only when it differs from
// the table's, so it is resolved: the column's own CHARACTER SET, else the
// character set of its own COLLATE, else the table's DEFAULT CHARSET, else the
// character set of the table's COLLATE. A column none of these name is left
// out, and so is a value kept verbatim as text (RawText, a PostgreSQL source):
// its character set is not known, and nothing must be compared on it. Unlike
// TextColumns, no server default is assumed: a guess here would refuse a
// snapshot refresh, and a refused table stops the refresh of every table.
func ColumnCharsets(createSQL string, cols []Column) map[string]string {
	tableCharset := tableDefaultCharset(createSQL)
	if tableCharset == "" {
		tableCharset = collationCharset(tableDefaultCollation(createSQL))
	}
	out := make(map[string]string)
	for _, c := range cols {
		if c.RawText {
			continue
		}
		switch c.MySQLType {
		case "char", "varchar", "tinytext", "text", "mediumtext", "longtext", "enum", "set":
		default:
			continue
		}
		cs := c.Charset
		if cs == "" {
			cs = collationCharset(c.Collation)
		}
		if cs == "" {
			cs = tableCharset
		}
		if cs != "" {
			out[c.Name] = ComparableCharset(cs)
		}
	}
	return out
}
