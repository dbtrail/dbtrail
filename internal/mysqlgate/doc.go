// Package mysqlgate holds one test: the answers a MySQL source's copy gives,
// compared byte for byte with the answers it gave when they were last
// reviewed (#2814).
//
// The copy is read through code every source family shares: internal/baseline
// writes the columns, internal/views builds the views over them, and
// internal/sqlsandbox runs the statement. Work for another family (PostgreSQL
// casts, session rules) edits those packages, and nothing else says that a
// MySQL answer stayed the same. This does: a fixed mydumper dump goes through
// the real conversion, the real views and the real worker, a fixed list of
// statements is run, and each answer (column names, types and every cell) is
// held against testdata/answers.golden.txt.
//
// It needs no database, so it runs in the unit tier of every pull request
// that changes Go code or this package's testdata (the CI path filter names
// it), and nothing in it skips.
//
// The golden file holds the COPY's answers, not MySQL's. Where the copy is
// known to answer otherwise than MySQL (docs/time-travel-sql.md lists them:
// LIKE is case-sensitive, count(DISTINCT) over folded text, the type a
// division or an aggregate comes back as), the file pins the copy's answer:
// the gate is about that answer not moving, not about it being MySQL's.
//
// What it does not reach: a table with changes over its snapshot (delta
// files, fresh mode's tails), the change-log views, more than one schema, a
// latin1 column, a MariaDB dump, and the session fields a caller decides
// with its own views (StrictStar, Types, UnchangedWithin, a session time
// zone, which needs the console's wall-clock DATETIME input).
//
// A change that alters an answer on purpose changes the golden file in the
// same pull request, where a reviewer sees which answer moved. Run with
// BINTRAIL_GATE_WRITE=<file> to write what the code answers now to that file.
// A change to the shared code that adds a cast or a session rule adds here the
// MySQL case that would have caught it.
package mysqlgate
