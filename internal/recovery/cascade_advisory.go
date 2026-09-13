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
	// ChildTables lists, as schema.table, every table in scope that carries a
	// cascading rule toward some parent. Scope is the target schema when one
	// is named, else the whole index.
	ChildTables []string
	// ParentOnDelete / ParentOnUpdate say whether the named target table is
	// the REFERENCED side of a cascading rule, per referential action, across
	// schemas. Both false when no table was named.
	ParentOnDelete bool
	ParentOnUpdate bool
}

// Empty reports that no cascading rule was found in scope.
func (a CascadeAdvisory) Empty() bool {
	return len(a.ChildTables) == 0 && !a.ParentOnDelete && !a.ParentOnUpdate
}

// DetectCascade reads the FK graph the index already holds. An error means
// the question could not be answered; the advisory returned with it carries
// whatever was found before the failure, and the caller must say the check
// did not complete rather than fall silent (a silent "no cascade" over a
// failed probe is the exact shape this exists to remove).
func DetectCascade(db *sql.DB, schema, table string) (CascadeAdvisory, error) {
	var adv CascadeAdvisory
	var scope []string
	if schema != "" {
		scope = []string{schema}
	}
	edges, err := metadata.CascadeConstraintsInIndex(db, scope)
	if err != nil {
		return adv, fmt.Errorf("check the index for FK cascade constraints: %w", err)
	}
	seen := map[string]bool{}
	for _, e := range edges {
		if k := e.Schema + "." + e.Table; !seen[k] {
			seen[k] = true
			adv.ChildTables = append(adv.ChildTables, k)
		}
	}
	// Cross-schema parent side (#833): CascadeConstraintsInIndex scopes by
	// the CHILD schema, so a parent whose only cascade children live in a
	// different schema is invisible to it. Probe the referenced side too.
	if schema != "" && table != "" {
		od, ou, perr := metadata.CascadeParentRulesInIndex(db, schema, table)
		if perr != nil {
			return adv, fmt.Errorf("check the index for cross-schema FK cascade parents: %w", perr)
		}
		adv.ParentOnDelete, adv.ParentOnUpdate = od, ou
	}
	return adv, nil
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
			if r.TableName != table {
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

// Warning renders the advisory for a response. remedy names the surface's
// own cascade path, since the same sentence must not hand an MCP client a
// shell command or an operator a tool name: "`bintrail recover-cascade`" on
// the CLI, "the recover_cascade tool" over MCP.
func (a CascadeAdvisory) Warning(remedy string) string {
	var b strings.Builder
	b.WriteString("this table has foreign-key children with ON DELETE / ON UPDATE CASCADE or SET NULL rules")
	if len(a.ChildTables) > 0 {
		b.WriteString(" (" + strings.Join(a.ChildTables, ", ") + ")")
	}
	b.WriteString("; MySQL applies those below the binary log, so the child rows a delete removed, the references it cleared, or the references a key update re-pointed are NOT in this script, which reverses the parent only. Use " + remedy + " to reconstruct them")
	return b.String()
}
