package views

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// State views are named exactly like the source (#2013): table demo.prices is
// the view demo.prices, in a DuckDB schema called demo, and a table called
// order.items is demo."order.items". No prefix and no rewriting of the name.
//
// Three things DuckDB does not let through unchanged, each measured against
// DuckDB 1.5.5 (TestSourceNames_* in sourcenames_2013_test.go):
//
//   - It compares names case-insensitively, ASCII letters only: demo.Orders and
//     demo.orders are one view there, while Ñ and ñ stay two. One of a colliding
//     pair keeps the exact name; the other gets a suffix (see stateViewPlan)
//     and the file says why.
//   - information_schema and pg_catalog are its own catalog, in every
//     database, and refuse views. Tables in a source schema named that way
//     cannot be defined at all.
//   - temp, system and memory are databases of every session, so temp.orders
//     is "ambiguous" there. Inside a per-server database (Input.Database) the
//     name has three parts, wp.temp.orders, and works.
//
// The events view is main.events, so a source table main.events is renamed too.

// statePlan pairs one baseline table with the name its view is created under.
type statePlan struct {
	table BaselineTable
	// view is the view's own name: the source table's, unless a collision
	// renamed it.
	view string
	// renamed says why view is not the source table's name; "" when it is.
	renamed string
	// skip says why the table has no view at all; "" when it has one.
	skip string
}

// stateViewPlan assigns a view name to EVERY table of the snapshot, in emission
// order.
//
// Names are assigned over every table even when only some are rendered: a
// rename depends on which names came before it, so choosing the tables first
// and naming them second would rename a view the moment its colliding sibling
// is left out, and a name that moves with the statement is a name nobody can
// write a query against.
//
// Which of two case twins keeps the plain name is a rule about the names
// alone, never about the order they were seen in: the spelling that sorts
// last byte by byte, which is the all-lowercase one when there is one
// (lowercase letters sort after capitals). Every other twin is named after
// its OWN spelling plus a short hash of it (Orders_b7e8ac), so its name does
// not depend on which other twins exist: adding a twin renames nothing that
// already had a suffix, and renames the plain-named one only when the new
// table's spelling sorts after it (a lowercase spelling appearing).
//
// Winners are settled before any loser is named, so a suffixed name never
// takes a real table's name.
func stateViewPlan(in Input) []statePlan {
	tables := append([]BaselineTable(nil), in.Baselines...)
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Schema != tables[j].Schema {
			return tables[i].Schema < tables[j].Schema
		}
		return tables[i].Table < tables[j].Table
	})
	spelling := func(t BaselineTable) string { return t.Schema + "\x00" + t.Table }
	// winner maps a name as DuckDB compares it to the table that keeps it.
	winner := map[string]BaselineTable{}
	for _, t := range tables {
		if in.reservedSchema(t.Schema) != "" {
			continue
		}
		k := nameKey(t.Schema, t.Table)
		if w, ok := winner[k]; !ok || spelling(t) > spelling(w) {
			winner[k] = t
		}
	}
	// holder maps a name as DuckDB compares it to who has it, for the note
	// the loser carries. main.events is the events view's whether or not this
	// render defines that view, so the table's name does not change when it
	// is switched on.
	holder := map[string]string{
		nameKey("main", eventsViewName): "the events view (the change log)",
	}
	plan := make([]statePlan, 0, len(tables))
	for _, t := range tables {
		p := statePlan{table: t, view: t.Table}
		if why := in.reservedSchema(t.Schema); why != "" {
			p.skip = why
		} else if k := nameKey(t.Schema, t.Table); k != nameKey("main", eventsViewName) && spelling(winner[k]) == spelling(t) {
			holder[k] = in.stateLabel(p)
		}
		plan = append(plan, p)
	}
	for i := range plan {
		p := &plan[i]
		k := nameKey(p.table.Schema, p.table.Table)
		if p.skip != "" || holder[k] == in.stateLabel(*p) {
			continue
		}
		taken := holder[k]
		sum := sha256.Sum256([]byte(spelling(p.table)))
		base := p.table.Table + "_" + hex.EncodeToString(sum[:])[:6]
		v := base
		for n := 2; ; n++ {
			if _, used := holder[nameKey(p.table.Schema, v)]; !used {
				break
			}
			v = fmt.Sprintf("%s_%d", base, n)
		}
		p.view = v
		holder[nameKey(p.table.Schema, v)] = in.stateLabel(*p)
		p.renamed = fmt.Sprintf("the table %s.%s. DuckDB does not tell names apart by letter case, and %s already has that name",
			p.table.Schema, p.table.Table, taken)
	}
	return plan
}

// reservedSchema says why a source schema cannot hold views in this file, or
// "" when it can. Compared the way DuckDB compares names.
func (in Input) reservedSchema(schema string) string {
	s := asciiLower(schema)
	if s == "information_schema" || s == "pg_catalog" {
		return fmt.Sprintf("not defined. DuckDB keeps a %s of its own in every database and refuses views in it", schema)
	}
	if in.Database != "" {
		return ""
	}
	if s == "temp" || s == "system" || s == "memory" || s == liveAttachAlias {
		return fmt.Sprintf("not defined. DuckDB has a database called %s, so %s.<table> would be ambiguous. "+
			"A file generated with `bintrail views --database <name>` reaches it as <name>.%s.<table>", s, schema, schema)
	}
	return ""
}

// stateRef is the name a state view is CREATED under: every part quoted.
func (in Input) stateRef(p statePlan) string {
	ref := quoteIdent(p.table.Schema) + "." + quoteIdent(p.view)
	if in.Database != "" {
		ref = quoteIdent(in.Database) + "." + ref
	}
	return ref
}

// stateLabel is the name a reader TYPES for a state view: each part bare where
// DuckDB reads it bare, quoted where it would not. It is what the console's
// SQL card lists and puts in its first query.
func (in Input) stateLabel(p statePlan) string {
	label := typedIdent(p.table.Schema) + "." + typedIdent(p.view)
	if in.Database != "" {
		label = in.Database + "." + label
	}
	return label
}

// stateKey is a state view's key in a ViewSet: its label without the
// database, compared the way DuckDB compares names.
func stateKey(p statePlan) string {
	return asciiLower(typedIdent(p.table.Schema) + "." + typedIdent(p.view))
}

// NamingNotes lists, one line each, every table whose view is not named
// exactly like the source or that has no view at all, and why. A producer
// shows them to the person who asked for the file: the same lines are in the
// file, but a table that is simply absent from it is easy not to notice.
func (in Input) NamingNotes() []string {
	var notes []string
	for _, p := range stateViewPlan(in) {
		switch {
		case p.skip != "":
			notes = append(notes, in.sourceLabel(p)+": "+p.skip)
		case p.renamed != "":
			notes = append(notes, in.stateLabel(p)+": "+p.renamed)
		}
	}
	return notes
}

// sourceLabel names a table that has no view, the way a reader would have
// typed it.
func (in Input) sourceLabel(p statePlan) string {
	return p.table.Schema + "." + p.table.Table
}

// writeNamingNotes puts the tables left out of the file in the file, so a
// reader looking for one finds the reason where the view would have been. Only
// in a full render: a filtered one (the console SQL card) is executed, not
// read.
func writeNamingNotes(b *strings.Builder, in Input) {
	if in.OnlyViews != nil {
		return
	}
	for _, p := range stateViewPlan(in) {
		if p.skip != "" {
			fmt.Fprintf(b, "-- %s: %s\n", commentSafe(in.sourceLabel(p)), commentSafe(p.skip))
		}
	}
}

// writeStateSchemas creates the schemas the views below are created in, once
// each. main exists in every database. Spelled as the first table of that
// schema spells it; DuckDB would treat Demo and demo as one schema anyway.
func writeStateSchemas(b *strings.Builder, in Input, wanted []statePlan) {
	seen := map[string]bool{"main": true}
	for _, p := range wanted {
		k := asciiLower(p.table.Schema)
		if seen[k] {
			continue
		}
		seen[k] = true
		ref := quoteIdent(p.table.Schema)
		if in.Database != "" {
			ref = quoteIdent(in.Database) + "." + ref
		}
		fmt.Fprintf(b, "CREATE SCHEMA IF NOT EXISTS %s;\n", ref)
	}
}

// nameKey is a schema.table as DuckDB compares it. The separator is a byte no
// name can hold, so a dot inside a name cannot make two pairs one key.
func nameKey(schema, table string) string {
	return asciiLower(schema) + "\x00" + asciiLower(table)
}

// asciiLower folds A-Z only, which is what DuckDB does when it compares names:
// Ñ and ñ are two different views there, and strings.ToLower would make them
// one here.
func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}

// BareKeywords lists the SQL keywords DuckDB cannot read unquoted as a name,
// lowercase: the list typedIdent quotes on. The console's DuckDB card renders
// the same names in JavaScript, and its test pins its copy to this one.
func BareKeywords() []string { return slices.Clone(bareKeywords) }

// typedIdent is a name the way a reader has to type it: bare when DuckDB reads
// it bare (a letter or underscore, then letters, digits and underscores, and
// not a keyword it refuses there), quoted otherwise. Anything outside ASCII is
// quoted too: quoting is always right, and a rule about which other letters
// DuckDB takes bare is one more thing to keep true.
func typedIdent(s string) string {
	if s == "" || slices.Contains(bareKeywords, asciiLower(s)) {
		return quoteIdent(s)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return quoteIdent(s)
		}
	}
	return s
}

// SelectedCaseTwin names a table this render reads and another table of the
// copy whose name differs from it only by letter case, both as schema.table,
// or "" twice when no table it reads has such a twin.
//
// Only a source that tells such names apart has them (lower_case_table_names
// = 0). DuckDB does not, so one table keeps the name and the other's view is
// renamed (stateViewPlan). A statement written for the source names either
// one by its own spelling, and on the copy both spellings are the table that
// kept the name: the other's rows are never what it reads. A caller that
// must answer as the source would (read routing) keeps a statement over such
// a name away from the copy.
func (in Input) SelectedCaseTwin() (table, twin string) {
	spellings := map[string][]string{}
	for _, t := range in.Baselines {
		if in.reservedSchema(t.Schema) != "" {
			continue
		}
		k := nameKey(t.Schema, t.Table)
		spellings[k] = append(spellings[k], t.Schema+"."+t.Table)
	}
	for _, t := range in.SelectedBaselines() {
		own := t.Schema + "." + t.Table
		for _, other := range spellings[nameKey(t.Schema, t.Table)] {
			if other != own {
				return own, other
			}
		}
	}
	return "", ""
}
