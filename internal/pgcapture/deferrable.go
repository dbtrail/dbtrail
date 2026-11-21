package pgcapture

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Deferrable primary keys.
//
// A primary key declared DEFERRABLE is checked at the end of the statement,
// or of the transaction, and not row by row. Inside one transaction two rows
// can therefore hold the same key: `UPDATE t SET id = id + 1` over rows 1, 2
// and 3, or inserting a new row 1 before deleting the old one. A snapshot is
// kept by key ("the last change of a key is the row"), so over such a
// transaction it can keep the wrong row, or none, and the captured changes do
// not always tell: two rows that are equal in every column share a key with
// nothing to set them apart (#2760, #2875).
//
// The fold refuses the sequences it can recognise, and for a table below
// REPLICA IDENTITY FULL it recognises fewer: with the earlier row captured by
// its key only, a change no longer shows that it starts from the row the
// change before it left (reconstruct.pgRowLink). So the catalog is asked, in
// two places:
//
//   - when capture starts, over the whole publication (CheckDeferrableKeys):
//     a table with a deferrable key refuses the start;
//   - each time PostgreSQL describes a relation (Decoder.cacheRelation, through
//     a DeferrableResolver): that table alone is skipped, under
//     SkipDeferrablePrimaryKey, and the others go on.
//
// A primary key cannot be made deferrable in place: ALTER TABLE ... ALTER
// CONSTRAINT works on foreign keys only (PostgreSQL 16 answers 42809, "is not
// a foreign key constraint"). It is dropped and added again, and PostgreSQL
// then describes the table again before its next change, in the same
// replication session, so the second check sees it while capture runs
// (TestCapturer_DeferrableKeyMidStream_Integration).
//
// The limit: both checks read the catalog as it is when they run, not as it
// was when a change was made. Capture that is behind reads old changes under
// today's catalog. A key that was deferrable while those changes were made and
// is not any more is not seen at all; the reverse (not deferrable then,
// deferrable now) skips changes that were safe. A change of the key that
// PostgreSQL does not describe the table again for is seen at the next start.

// ErrDeferrableKey marks the condition, so a caller can tell it from a failed
// catalog read.
var ErrDeferrableKey = errors.New("a primary key is DEFERRABLE")

// DeferrableKey is one table of the publication whose primary key is
// declared DEFERRABLE, with the constraint's name.
type DeferrableKey struct {
	Schema, Table, Constraint string
}

// deferrableKeysQuery lists the tables of publication $1 whose primary key
// constraint is deferrable (INITIALLY IMMEDIATE or INITIALLY DEFERRED: both
// allow two rows on one key inside a statement).
const deferrableKeysQuery = `
	SELECT pt.schemaname, pt.tablename, con.conname
	FROM pg_publication_tables pt
	JOIN pg_namespace n ON n.nspname = pt.schemaname
	JOIN pg_class c ON c.relname = pt.tablename AND c.relnamespace = n.oid
	JOIN pg_constraint con ON con.conrelid = c.oid AND con.contype = 'p' AND con.condeferrable
	WHERE pt.pubname = $1
	ORDER BY 1, 2`

// QueryDeferrableKeys returns the tables of publication whose primary key is
// deferrable; empty when there is none.
func QueryDeferrableKeys(ctx context.Context, conn *pgx.Conn, publication string) ([]DeferrableKey, error) {
	rows, err := conn.Query(ctx, deferrableKeysQuery, publication)
	if err != nil {
		return nil, fmt.Errorf("pgcapture: checking for deferrable primary keys in publication %q: %w", publication, err)
	}
	defer rows.Close()
	var found []DeferrableKey
	for rows.Next() {
		var k DeferrableKey
		if err := rows.Scan(&k.Schema, &k.Table, &k.Constraint); err != nil {
			return nil, fmt.Errorf("pgcapture: checking for deferrable primary keys in publication %q: %w", publication, err)
		}
		found = append(found, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgcapture: checking for deferrable primary keys in publication %q: %w", publication, err)
	}
	return found, nil
}

// CheckDeferrableKeys refuses a publication that holds a table with a
// deferrable primary key. The error wraps ErrDeferrableKey and says what to
// do.
func CheckDeferrableKeys(ctx context.Context, conn *pgx.Conn, publication string) error {
	found, err := QueryDeferrableKeys(ctx, conn, publication)
	if err != nil {
		return err
	}
	if len(found) == 0 {
		return nil
	}
	return fmt.Errorf("pgcapture: %s: %w", DeferrableKeysProblem(found), ErrDeferrableKey)
}

// DeferrableKeysProblem says what found means and what to do. Pure.
func DeferrableKeysProblem(found []DeferrableKey) string {
	names := make([]string, len(found))
	for i, k := range found {
		names[i] = fmt.Sprintf("%s (constraint %s)", quoteQualified(k.Schema, k.Table), pgx.Identifier{k.Constraint}.Sanitize())
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s of the publication %s a primary key declared DEFERRABLE: %s. ", countTables(len(found)), hasOrHave(len(found)), strings.Join(names, ", "))
	b.WriteString("Inside a transaction such a key lets two rows hold the same value, and the copy is kept by key, so it could keep the wrong row, or none, with nothing to tell. ")
	b.WriteString("Either make the key not deferrable, which rebuilds its index and locks the table while it does:")
	for _, k := range found {
		fmt.Fprintf(&b, " ALTER TABLE %s DROP CONSTRAINT %s, ADD PRIMARY KEY (<the same columns>);", quoteQualified(k.Schema, k.Table), pgx.Identifier{k.Constraint}.Sanitize())
	}
	b.WriteString(" Or take the table out of the publication.")
	return b.String()
}

func hasOrHave(n int) string {
	if n == 1 {
		return "has"
	}
	return "have"
}
