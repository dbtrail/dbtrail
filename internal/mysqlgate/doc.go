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
// It needs no database, so it runs in the unit tier on every pull request and
// no path filter or missing service can skip it.
//
// A change that alters an answer on purpose changes the golden file in the
// same pull request, where a reviewer sees which answer moved. Run with
// BINTRAIL_GATE_WRITE=<file> to write what the code answers now to that file.
// A change to the shared code that adds a cast or a session rule adds here the
// MySQL case that would have caught it.
package mysqlgate
