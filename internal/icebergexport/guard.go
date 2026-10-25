package icebergexport

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
)

// ErrConcurrentCommit is a lost commit race: another writer committed to the
// table after this run read it. The run's commit was not applied, and it is
// not retried: the window was folded against the state before the other
// commit, and replaying it on top would move rows and the cursor backwards.
// The next run reads the cursor the other writer committed and folds from
// there. It deliberately does NOT wrap table.ErrCommitFailed, which is what
// makes iceberg-go retry.
var ErrConcurrentCommit = errors.New("another writer committed to this table after this run read it, so this run's commit was not applied; the next run resumes from the cursor that writer committed")

// errCommitUnknown marks a commit whose outcome the catalog could not report
// (a REST 5xx or gateway timeout): it may or may not have landed.
var errCommitUnknown = errors.New("the catalog could not say whether this run's commit was applied, so it may or may not have been applied; the next run reads whichever cursor the catalog holds and continues from it")

// guardedCatalog sits between the export and every catalog, local or
// remote, and makes each commit conditional on the table still being in the
// state the commit was computed against: the same current snapshot and the
// same export cursor.
//
// Why the export needs its own guard instead of the catalog's:
//
//   - A commit that only moves the cursor (no events to fold, or a load of
//     an empty baseline) carries no snapshot requirement in iceberg-go, so a
//     catalog would apply it on top of anything. The guard adds the
//     requirement that the current snapshot did not move.
//   - Glue validates requirements itself and reports a failed one as a
//     plain error, not as table.ErrCommitFailed; only REST's 409 and Glue's
//     ConcurrentModificationException carry the sentinel. The guard compares
//     the table's state itself, so the verdict does not depend on how a
//     catalog spells the conflict.
//   - iceberg-go retries a commit when the TABLE says so
//     (commit.retry.num-retries, which any engine can set) by refreshing and
//     replaying it on the new state. Every replay comes back through
//     CommitTable here, finds the state moved, and is refused.
//
// Tables are re-bound to the guard when loaded or created, because a table
// commits through the catalog it was built with.
type guardedCatalog struct{ catalog.Catalog }

var _ catalog.Catalog = guardedCatalog{}

func guard(c catalog.Catalog) catalog.Catalog {
	if g, ok := c.(guardedCatalog); ok {
		return g
	}
	return guardedCatalog{c}
}

func (g guardedCatalog) rebind(t *table.Table) *table.Table {
	if t == nil {
		return nil
	}
	return table.New(t.Identifier(), t.Metadata(), t.MetadataLocation(), t.FS, g)
}

func (g guardedCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	t, err := g.Catalog.LoadTable(ctx, ident)
	if err != nil {
		return nil, err
	}
	return g.rebind(t), nil
}

func (g guardedCatalog) CreateTable(ctx context.Context, ident table.Identifier, sc *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	t, err := g.Catalog.CreateTable(ctx, ident, sc, opts...)
	if err != nil {
		return nil, err
	}
	return g.rebind(t), nil
}

// commitBase is the state a commit was computed against, plus the cursor
// the commit sets (intended), which is how a reload tells this run's own
// commit from another writer's.
type commitBase struct {
	snapshot *int64
	cursor   [len(baseKeys)]string
	present  [len(baseKeys)]bool
	intended iceberg.Properties
}

// baseKeys are the properties whose change means another export run
// committed: the cursor and the layout version.
var baseKeys = [...]string{propFile, propPos, propAt, propOrigin, propVersion}

type commitBaseKey struct{}

// withCommitBase records the table state a transaction was started from.
// Every commit the export makes goes through it; the guard refuses a commit
// that does not, so a new commit path cannot silently lose the guard.
func withCommitBase(ctx context.Context, tbl *table.Table, intended iceberg.Properties) context.Context {
	b := baseOf(tbl.Metadata())
	b.intended = intended
	return context.WithValue(ctx, commitBaseKey{}, b)
}

// landed reports whether meta is this commit's own result: the cursor it
// set is there and the table is no longer at the base. Two runs that cut at
// the same binlog position from the same base compute the same window, so
// mistaking the other one's identical commit for ours reports a commit that
// exists with the content ours would have had.
func (b commitBase) landed(meta table.Metadata) bool {
	if len(b.intended) == 0 || baseOf(meta).equal(b) {
		return false
	}
	props := meta.Properties()
	for _, k := range []string{propFile, propPos, propAt, propOrigin} {
		if want, ok := b.intended[k]; !ok || props[k] != want {
			return false
		}
	}
	return true
}

func baseOf(meta table.Metadata) commitBase {
	var b commitBase
	if s := meta.CurrentSnapshot(); s != nil {
		id := s.SnapshotID
		b.snapshot = &id
	}
	props := meta.Properties()
	for i, k := range baseKeys {
		b.cursor[i], b.present[i] = props[k]
	}
	return b
}

func (b commitBase) equal(o commitBase) bool {
	if (b.snapshot == nil) != (o.snapshot == nil) || (b.snapshot != nil && *b.snapshot != *o.snapshot) {
		return false
	}
	return b.cursor == o.cursor && b.present == o.present
}

var refReqType = table.AssertRefSnapshotID(table.MainBranch, nil).GetType()

func (g guardedCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	base, ok := ctx.Value(commitBaseKey{}).(commitBase)
	if !ok {
		return nil, "", fmt.Errorf("commit to %v refused: it was made without the table state it was computed against, so a concurrent commit could not be detected", ident)
	}
	moved, err := g.moved(ctx, ident, base)
	if err != nil {
		return nil, "", err
	}
	if moved {
		return nil, "", ErrConcurrentCommit
	}
	// Make the catalog check it too, atomically where it can (a REST server
	// validates requirements under its own lock). A RowDelta already carries
	// this requirement; a cursor-only commit does not. What no requirement
	// can express is "the properties did not change", so two cursor-only
	// commits racing inside the gap between the check above and this call
	// can both land, the later one winning. Neither folded any event, so the
	// worst case is a cursor a little behind the other's, and the next run
	// re-reads that short, empty stretch.
	hasRef := false
	for _, r := range reqs {
		if r.GetType() == refReqType {
			hasRef = true
			break
		}
	}
	if !hasRef {
		reqs = append(reqs, table.AssertRefSnapshotID(table.MainBranch, base.snapshot))
	}

	meta, loc, cerr := g.Catalog.CommitTable(ctx, ident, reqs, updates)
	if cerr == nil {
		return meta, loc, nil
	}
	// The catalog reported an error. Reload and read what happened: the
	// table may carry this very commit (a 5xx or a timeout after the catalog
	// applied it), in which case it is a success and must be reported as
	// one, or another writer's, which is a lost race whatever the catalog
	// called it.
	if cur, err := g.Catalog.LoadTable(ctx, ident); err == nil {
		if base.landed(cur.Metadata()) {
			return cur.Metadata(), cur.MetadataLocation(), nil
		}
		if !baseOf(cur.Metadata()).equal(base) {
			return nil, "", fmt.Errorf("%w (the catalog said: %v)", ErrConcurrentCommit, cerr)
		}
	} else if errors.Is(err, catalog.ErrNoSuchTable) {
		return nil, "", fmt.Errorf("%w (the table was dropped; the catalog said: %v)", ErrConcurrentCommit, cerr)
	}
	switch {
	case errors.Is(cerr, table.ErrCommitFailed):
		// The catalog reports a conflict the reload did not show (the other
		// writer's commit can be invisible to a read that soon). %v, not %w:
		// table.ErrCommitFailed is what makes iceberg-go retry.
		return nil, "", fmt.Errorf("%w (the catalog said: %v)", ErrConcurrentCommit, cerr)
	case errors.Is(cerr, rest.ErrCommitStateUnknown):
		return nil, "", fmt.Errorf("%w: %w", errCommitUnknown, cerr)
	}
	return nil, "", cerr
}

// moved reports whether the table's current state differs from base. A
// table that no longer exists has moved (it was dropped under the run).
func (g guardedCatalog) moved(ctx context.Context, ident table.Identifier, base commitBase) (bool, error) {
	cur, err := g.Catalog.LoadTable(ctx, ident)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			return true, nil
		}
		return false, fmt.Errorf("reload %v before committing: %w", ident, err)
	}
	return !baseOf(cur.Metadata()).equal(base), nil
}
