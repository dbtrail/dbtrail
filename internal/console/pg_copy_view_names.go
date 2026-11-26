package console

import (
	"context"

	"github.com/dbtrail/dbtrail/internal/views"
)

// PGCopyViewNames is the schema and name of each view a statement on the
// copy can read, without one of the copy's workers: the tables the
// PostgreSQL port's catalog lists, for its capture gate. Views, not the
// snapshot's files: a table in a schema the copy cannot create has none,
// and of two names that differ only in case one has another name.
func (q *SQLOnCopy) PGCopyViewNames(ctx context.Context) ([][2]string, error) {
	in, err := q.s.buildViewsInput(ctx, q.b, viewsRequest{PinSnapshot: true, OmitEvents: true, StateOnly: true, ForStatement: true, LocalCopy: true, ApplyHidden: true})
	if err != nil {
		return nil, err
	}
	return pgViewNames(in), nil
}

// pgViewNames is what the catalog's list of columns would name: every view
// of in but those of the copy's own schema.
func pgViewNames(in views.Input) [][2]string {
	var out [][2]string
	for _, v := range in.ViewNames() {
		if v.Schema != "main" {
			out = append(out, [2]string{v.Schema, v.View})
		}
	}
	return out
}
