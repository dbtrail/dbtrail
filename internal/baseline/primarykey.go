package baseline

import "strings"

// PrimaryKeyColumns names the columns of the PRIMARY KEY an embedded CREATE
// TABLE declares, in the key's order, or nil when it declares none or the
// declaration cannot be read whole (#2479). SHOW CREATE TABLE prints the key
// on a line of its own, `PRIMARY KEY (`a`,`b`)`, so only such a line counts:
// the same words inside a COMMENT or a DEFAULT string sit on a column's line,
// which starts with the column's name. A key with a prefix length (`name`(10))
// is not read: a prefix of a column does not identify a row by itself, and
// nil is never a wrong key, where a misread one would be.
func PrimaryKeyColumns(createSQL string) []string {
	var key []string
	for _, line := range strings.Split(createSQL, "\n") {
		rest, ok := strings.CutPrefix(strings.TrimSpace(line), "PRIMARY KEY (")
		if !ok {
			continue
		}
		if key != nil {
			return nil
		}
		if key = keyColumnList(rest); key == nil {
			return nil
		}
	}
	return key
}

// keyColumnList reads `a`,`b`) and what follows the parenthesis, or nil when
// it is anything else than backquoted names separated by commas.
func keyColumnList(s string) []string {
	var cols []string
	for {
		if !strings.HasPrefix(s, "`") {
			return nil
		}
		var name strings.Builder
		i := 1
		for ; i < len(s); i++ {
			if s[i] != '`' {
				name.WriteByte(s[i])
				continue
			}
			if i+1 < len(s) && s[i+1] == '`' {
				name.WriteByte('`')
				i++
				continue
			}
			break
		}
		if i >= len(s) || name.Len() == 0 {
			return nil
		}
		cols = append(cols, name.String())
		s = s[i+1:]
		switch {
		case strings.HasPrefix(s, ","):
			s = s[1:]
		case strings.HasPrefix(s, ")"):
			return cols
		default:
			return nil
		}
	}
}
