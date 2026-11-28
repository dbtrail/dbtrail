package pgshim

import "strings"

// A varchar or char(n) column returned as it is (#3219).
//
// The copy holds both as VARCHAR, which the port announces as text, so a
// client that reads the description (pgx, JDBC, psycopg's types) saw text
// where PostgreSQL says character varying(16) or character(2). An item of
// the statement's own select list that is a column of a table it reads,
// written by name and of one such type on the source, is announced with
// that type and its length. Its cells are the copy's, as before (a char(n)
// without its padding, #3219): text, varchar and bpchar are written, and
// sent in binary, as the same bytes.

// sourceTextTypes gives each column of types the copy announced as text
// the source's varchar or char(n), where text shows the column to be one
// (textItemTypes). Nothing changes when the catalog cannot be read.
func (s *session) sourceTextTypes(text string, types []colType) {
	some := false
	for _, t := range types {
		if t.oid == oidText {
			some = true
		}
	}
	if !some || s.bound == nil || s.bound.Catalog == nil {
		return
	}
	v, perr := s.catalog()
	if perr != nil {
		return
	}
	for i, t := range textItemTypes(text, v.tables, len(types)) {
		if t.oid != 0 && types[i].oid == oidText {
			types[i] = t
		}
	}
}

// textItemTypes is, for each of the n items of the select list of the
// statement text, the source type of the varchar or char(n) column the item
// returns as it is, or a zero colType. nil when the list is not read here:
// a star, a UNION, INTERSECT or EXCEPT (whose columns' type comes from
// every branch), a column list beside a table (AS t(a, b)), or not n items.
func textItemTypes(text string, tables []CatalogTable, n int) []colType {
	toks, why := lexSQL(text)
	if why != "" || columnListBesidesWith(toks) {
		return nil
	}
	sel, depth := -1, 0
	for i, t := range toks {
		switch {
		case isOpTok(toks, i, "("):
			depth++
		case isOpTok(toks, i, ")"):
			depth--
		case depth == 0 && t.kind == rtWord && (t.text == "union" || t.text == "intersect" || t.text == "except"):
			return nil
		case depth == 0 && t.kind == rtWord && t.text == "select":
			sel = i
		}
	}
	if sel < 0 {
		return nil
	}
	items := selectItems(toks, sel)
	if len(items) != n {
		return nil
	}
	read := tablesRead(text, tables)
	if read == nil {
		return nil
	}
	defs := aliasDefinitions(toks)
	out := make([]colType, n)
	for i, item := range items {
		name, ok := plainColumn(item)
		if !ok {
			continue
		}
		if _, defined := defs[name]; defined {
			// The name may stand for something the statement makes (a
			// WITH's or a subquery's expression).
			continue
		}
		out[i] = sourceTextType(name, tables, read)
	}
	return out
}

// plainColumn is the column an item writes by name alone (c_last,
// c.c_last, public.customer.c_last), lowercased unless quoted; false for
// any other item, a star among them.
func plainColumn(item []rtToken) (string, bool) {
	for j, t := range item {
		if j%2 == 1 {
			if !isOpTok(item, j, ".") {
				return "", false
			}
			continue
		}
		if t.kind != rtWord && t.kind != rtQuoted {
			return "", false
		}
	}
	if len(item)%2 == 0 {
		return "", false
	}
	last := item[len(item)-1]
	if last.kind == rtWord {
		return strings.ToLower(last.text), true
	}
	return last.text, true
}

// sourceTextType is the source's varchar or char(n) for a column name, when
// every column of that name among the tables read is of that one type and
// length on the source and a VARCHAR on the copy; a zero colType otherwise.
func sourceTextType(name string, tables []CatalogTable, read func(*CatalogTable) bool) colType {
	var found colType
	for i := range tables {
		t := &tables[i]
		if !read(t) {
			continue
		}
		for _, c := range t.Columns {
			if c.Name != name && strings.ToLower(c.Name) != name {
				continue
			}
			if c.Type != "VARCHAR" || c.SourceType != oidVarchar && c.SourceType != oidBpchar {
				return colType{}
			}
			ct := colType{oid: c.SourceType, size: -1, typmod: c.SourceTypeMod}
			if found.oid != 0 && found != ct {
				return colType{}
			}
			found = ct
		}
	}
	return found
}
