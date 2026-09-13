package recovery

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// CascadeAdvisory is what a plain reversal cannot see (#1616): InnoDB runs an
// FK ON DELETE / ON UPDATE CASCADE or SET NULL below the binary log (MySQL
// Bug #32506), so the child rows a parent DELETE removed, and the child keys a
// parent-key UPDATE rewrote, were never captured. A script built from the
// captured events alone re-creates the parent and nothing else, and reads as
// a full restore. The CLI has warned about this since #833/#1002; the same
// check is here so every surface that generates a reversal (CLI, MCP, console)
// says the same thing from the same facts, in its response and not only in a
// log the client never sees.
type CascadeAdvisory struct {
	// ChildTables lists, as schema.table, the tables whose rows a change to
	// the target can cascade into. With a table named these are ITS children,
	// in any schema (#833: a child in another schema is the one most easily
	// lost); with no table named, every table in the schema (or the index)
	// that carries a cascading rule toward some parent.
	ChildTables []string
	// ParentOnDelete / ParentOnUpdate say whether the named target table is
	// the REFERENCED side of a cascading rule, per referential action (#1002:
	// the two rules are never merged). Both false when no table was named.
	ParentOnDelete bool
	ParentOnUpdate bool
}

// Empty reports that no cascading rule was found in scope.
func (a CascadeAdvisory) Empty() bool {
	return len(a.ChildTables) == 0 && !a.ParentOnDelete && !a.ParentOnUpdate
}

// DetectCascade reads the FK graph the index already holds, in one query.
// An error means the question could not be answered, and the caller must say
// so rather than fall silent (a silent "no cascade" over a failed probe is
// the exact shape this exists to remove).
//
// The edges are read index-wide and filtered here by their REFERENCED side,
// which is the side a reversal can cascade FROM: with a table named, the
// edges whose parent is that table, so a child in another schema (#833) is
// named and an unrelated cascade in the same schema is not (the warning
// says "this table has children (...)", so the list is that table's; with
// no schema given, a same-named table in another schema contributes too);
// with only a schema named, the edges whose parent lives in that schema,
// since those are the deletes a window scoped to it can hold. The old
// child-schema scope missed exactly the cross-schema parent. The parent
// flags come from the same edges, per referential action.
//
// A target that is only a CHILD gets no advisory: its own DELETE or UPDATE
// is a real binlogged event and its reversal is complete. Cascades fire from
// the parent's change, and that is the only side a plain reversal misses.
//
// Names compare case-insensitively: the index compares them server-side
// under a case-insensitive collation when it fetches the rows, and a
// reversal that finds its rows must not lose its warning to the spelling
// the operator typed (lower_case_table_names servers accept either).
func DetectCascade(db *sql.DB, schema, table string) (CascadeAdvisory, error) {
	var adv CascadeAdvisory
	edges, err := metadata.CascadeConstraintsInIndex(db, nil)
	if err != nil {
		return adv, fmt.Errorf("check the index for FK cascade constraints: %w", err)
	}
	seen := map[string]bool{}
	for _, e := range edges {
		if schema != "" && !strings.EqualFold(e.ReferencedSchema, schema) {
			continue
		}
		if table != "" {
			if !strings.EqualFold(e.ReferencedTable, table) {
				continue
			}
			if cascades(e.DeleteRule) {
				adv.ParentOnDelete = true
			}
			if cascades(e.UpdateRule) {
				adv.ParentOnUpdate = true
			}
		}
		if k := e.Schema + "." + e.Table; !seen[k] {
			seen[k] = true
			adv.ChildTables = append(adv.ChildTables, k)
		}
	}
	return adv, nil
}

// cascades is the referential-action test the probe query applies server-side
// (CascadeConstraintsInIndex returns an edge when EITHER rule cascades, so the
// per-action split has to be redone here).
func cascades(rule string) bool { return rule == "CASCADE" || rule == "SET NULL" }

// RowsCanCascade reports whether any row is a DELETE or an UPDATE, the only
// events a cascade can follow; callers skip the FK probe on a window of
// INSERTs, which can never have cascaded.
func RowsCanCascade(rows []query.ResultRow) bool {
	for _, r := range rows {
		if r.EventType == event.EventDelete || r.EventType == event.EventUpdate {
			return true
		}
	}
	return false
}

// AppliesTo reports whether the reversal of these rows is the kind the
// advisory is about, so a surface that returns the script to a client does
// not cry wolf over an INSERT undo on a table that happens to have children.
// With a table named, only a DELETE (delete_rule) or an UPDATE (update_rule)
// on that table can have cascaded. With no table named the scope is the
// schema, so any DELETE or UPDATE in the window may have, and the child list
// is what the caller can name. The UPDATE arm is coarse on purpose: whether
// the update moved a referenced key needs the FK column list, and the cost
// of the coarse arm is one warning too many, never a silently dangling key.
func (a CascadeAdvisory) AppliesTo(rows []query.ResultRow, table string) bool {
	if a.Empty() {
		return false
	}
	for _, r := range rows {
		if table != "" {
			if !strings.EqualFold(r.TableName, table) {
				continue
			}
			if (a.ParentOnDelete && r.EventType == event.EventDelete) ||
				(a.ParentOnUpdate && r.EventType == event.EventUpdate) {
				return true
			}
			continue
		}
		if len(a.ChildTables) > 0 && (r.EventType == event.EventDelete || r.EventType == event.EventUpdate) {
			return true
		}
	}
	return false
}

// Warning renders the advisory for a response. table is the target the
// caller named, or "" for a schema- or window-wide reversal, where the
// subject is the parents in the window rather than "this table". remedy
// names the surface's own cascade path, since the same sentence must not
// hand an MCP client a shell command or an operator a tool name:
// "`bintrail recover-cascade`" on the CLI, "the recover_cascade tool" over
// MCP.
func (a CascadeAdvisory) Warning(table, remedy string) string {
	var b strings.Builder
	if table != "" {
		b.WriteString("this table has foreign-key children with ON DELETE / ON UPDATE CASCADE or SET NULL rules")
	} else {
		b.WriteString("tables in this window have foreign-key children with ON DELETE / ON UPDATE CASCADE or SET NULL rules")
	}
	if len(a.ChildTables) > 0 {
		b.WriteString(" (" + strings.Join(a.ChildTables, ", ") + ")")
	}
	b.WriteString("; MySQL applies those below the binary log, so the child rows a delete removed, the references it cleared, or the references a key update re-pointed are NOT in this script, which reverses the parent only. Use " + remedy + " to reconstruct them")
	return b.String()
}
