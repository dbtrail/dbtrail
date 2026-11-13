// Package pgcompare runs the same read statements against a PostgreSQL source
// and against the analytical copy of that source, and says, per statement,
// whether the two answers are the same (issue #2772). It is the PostgreSQL
// sibling of internal/sqlcompare, whose verdicts and comparison rules it
// reuses.
//
// The copy has no PostgreSQL-protocol port yet, so it is queried in process:
// the snapshot's views are installed in the SQL sandbox (internal/sqlsandbox),
// the engine every SQL surface of the copy runs on, and the statement is sent
// there as written. Nothing is translated: a statement the copy refuses is
// NOT_ON_COPY, and one it answers differently is DIFFERENT.
//
// What counts as the same answer:
//
//   - Cells are compared as the text PostgreSQL sends for them, under the
//     settings a snapshot is written with (TimeZone UTC, DateStyle ISO,
//     IntervalStyle postgres, bytea_output hex). The copy's typed cells are
//     written the same way first (CellText): a boolean as t or f, a timestamp
//     as 2006-01-02 15:04:05, one with a time zone ending in +00.
//   - Row order counts only when the statement has a top-level ORDER BY.
//   - Column names compare as sqlcompare compares them: an expression's name
//     is each engine's own and is left to the cells.
//   - Two numbers that agree to four decimals are reported as a "precision"
//     difference, two texts that differ only in case as a "case" difference,
//     so a report can tell them from a wrong answer. Both are still DIFFERENT.
//
// Nothing here writes to the source. A statement that does not start as a
// query, or that calls a function with side effects (ReadOnlyVeto), is
// SKIPPED without being sent; every statement that is sent runs alone in a
// read-only transaction that is rolled back, under a statement timeout.
package pgcompare

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/sqlcompare"
)

// Side is one of the two things compared: it runs a statement and returns up
// to max rows as text. over reports that a further row existed, or that the
// answer was cut in some other way and cannot be compared.
type Side interface {
	Query(ctx context.Context, stmt string, max int) (rows sqlcompare.Rows, over bool, err error)
}

// CutError is a side saying it could not give a whole answer for a reason
// that is not about the statement's meaning: it ran out of time, the result
// was too large to carry, the server cancelled it. The statement is
// INCONCLUSIVE with Kind as its kind.
type CutError struct {
	Kind string
	Err  error
}

func (e *CutError) Error() string { return e.Err.Error() }
func (e *CutError) Unwrap() error { return e.Err }

// FatalError wraps an error that says nothing about the statement: the
// connection or the worker is gone. Run stops on it instead of filing it as
// the statement's verdict, where it would read as a finding.
type FatalError struct{ Err error }

func (e *FatalError) Error() string { return e.Err.Error() }
func (e *FatalError) Unwrap() error { return e.Err }

// Options tune a run. The zero value is usable.
type Options struct {
	// MaxRows is how many rows are read per side; 0 means 1000. A statement
	// that returns more on either side is INCONCLUSIVE.
	MaxRows int
	// NoRerun skips the second read of the source that tells a live write
	// from a real difference. Set it when the source is known to be quiet.
	NoRerun bool
}

// Result is one statement's outcome.
type Result struct {
	Statement  string
	Verdict    sqlcompare.Verdict
	Kind       string
	Detail     string
	SourceRows int
	CopyRows   int
}

// Report is a whole run.
type Report struct {
	Results []Result
	Counts  map[sqlcompare.Verdict]int
}

// Run compares every statement, in order. It returns an error only when a
// side stopped working (FatalError); everything about a statement is in its
// Result.
func Run(ctx context.Context, src, cp Side, opts Options, statements []string) (*Report, error) {
	if opts.MaxRows <= 0 {
		opts.MaxRows = 1000
	}
	if len(statements) == 0 {
		return nil, errors.New("no statements to compare")
	}
	rep := &Report{Counts: map[sqlcompare.Verdict]int{}}
	for i, stmt := range statements {
		res, err := compareOne(ctx, src, cp, opts, stmt)
		if err != nil {
			return nil, fmt.Errorf("statement %d (%s): %w", i+1, stmt, err)
		}
		rep.Results = append(rep.Results, res)
		rep.Counts[res.Verdict]++
	}
	return rep, nil
}

func compareOne(ctx context.Context, src, cp Side, opts Options, stmt string) (Result, error) {
	res := Result{Statement: stmt}
	if !IsQuery(stmt) {
		res.Verdict, res.Kind, res.Detail = sqlcompare.Skipped, "not_a_query", "only SELECT, WITH, VALUES and TABLE statements are compared"
		return res, nil
	}
	if why := ReadOnlyVeto(stmt); why != "" {
		res.Verdict, res.Kind, res.Detail = sqlcompare.Skipped, "not_read_only", "not sent: "+why
		return res, nil
	}
	srcRows, srcOver, err := src.Query(ctx, stmt, opts.MaxRows)
	if err != nil {
		if fatal(ctx, err) {
			return res, fmt.Errorf("source: %w", err)
		}
		if cut(&res, "the source", err) {
			return res, nil
		}
		res.Verdict, res.Detail = sqlcompare.SourceError, err.Error()
		return res, nil
	}
	res.SourceRows = len(srcRows.Rows)
	cpRows, cpOver, err := cp.Query(ctx, stmt, opts.MaxRows)
	if err != nil {
		if fatal(ctx, err) {
			return res, fmt.Errorf("copy: %w", err)
		}
		if cut(&res, "the copy", err) {
			return res, nil
		}
		res.Verdict, res.Detail = sqlcompare.NotOnCopy, err.Error()
		return res, nil
	}
	res.CopyRows = len(cpRows.Rows)
	if srcOver || cpOver {
		res.Verdict, res.Kind = sqlcompare.Inconclusive, "over_max_rows"
		res.Detail = fmt.Sprintf("more than %d rows on a side, or a cell too long to carry; narrow the statement", opts.MaxRows)
		return res, nil
	}
	ordered := OrderedAtTop(stmt)
	res.Verdict, res.Kind, res.Detail = sqlcompare.Compare(srcRows, cpRows, ordered)
	if res.Verdict == sqlcompare.Different && !opts.NoRerun {
		// A write between the two reads looks exactly like a difference. If
		// the copy agrees with a second read of the source, the first one
		// was stale and nothing can be concluded.
		again, _, err := src.Query(ctx, stmt, opts.MaxRows)
		if err != nil {
			if fatal(ctx, err) {
				return res, fmt.Errorf("source: %w", err)
			}
			res.Detail += "; the second read of the source failed: " + err.Error()
			return res, nil
		}
		if v, _, _ := sqlcompare.Compare(again, cpRows, ordered); v == sqlcompare.Equal {
			res.Verdict, res.Kind = sqlcompare.Inconclusive, "source_changed"
			res.Detail = "the copy matches a second read of the source: the first read was before a live write"
		} else if v, _, _ := sqlcompare.Compare(srcRows, again, ordered); v != sqlcompare.Equal {
			res.Detail += "; the source also changed between two reads"
		}
	}
	return res, nil
}

// cut files a CutError as INCONCLUSIVE and reports whether err was one.
func cut(res *Result, side string, err error) bool {
	var c *CutError
	if !errors.As(err, &c) {
		return false
	}
	res.Verdict, res.Kind, res.Detail = sqlcompare.Inconclusive, c.Kind, side+": "+c.Error()
	return true
}

func fatal(ctx context.Context, err error) bool {
	var f *FatalError
	return errors.As(err, &f) || ctx.Err() != nil
}

// IsQuery reports whether the statement starts as a query: SELECT, WITH,
// VALUES or TABLE, after any comments, blanks and opening parentheses. It is
// what keeps a write from being sent at all; the source's read-only session
// is the second lock, for a WITH that writes.
func IsQuery(stmt string) bool {
	s := stmt
	for {
		s = strings.TrimLeft(s, " \t\r\n(")
		switch {
		case strings.HasPrefix(s, "--"):
			i := strings.IndexByte(s, '\n')
			if i < 0 {
				return false
			}
			s = s[i+1:]
			continue
		case strings.HasPrefix(s, "/*"):
			i := strings.Index(s, "*/")
			if i < 0 {
				return false
			}
			s = s[i+2:]
			continue
		}
		break
	}
	end := 0
	for end < len(s) && (s[end] >= 'a' && s[end] <= 'z' || s[end] >= 'A' && s[end] <= 'Z') {
		end++
	}
	switch strings.ToLower(s[:end]) {
	case "select", "with", "values", "table":
		return true
	}
	return false
}
