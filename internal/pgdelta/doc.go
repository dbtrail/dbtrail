// Package pgdelta is the table delta of a PostgreSQL source's snapshot
// (#2757): a changed table is published as its previous file plus two small
// files beside it, instead of being written again in full.
//
// It is its own package, with its own file names, footer keys and readers,
// and shares none of them with the table delta of MySQL and MariaDB sources
// (internal/baseline, internal/reconstruct): that code neither writes nor
// reads these files, and nothing here reads its files. The two families can
// change, and ship, apart.
//
// A pair of sequence N beside "<table>.parquet":
//
//	<table>.NNNNNN.pgdead   the row numbers of the base file a window killed
//	<table>.NNNNNN.pgnew    the newest row of every key the window changed
//
// The pairs of one base form a chain, in sequence order from 0. A table's
// state is the base's rows minus every dead row number, plus the newest
// version of every key in the .pgnew files that is not a deletion
// (StateSQL).
//
// This package writes and reads the files. It decides nothing about when a
// pair is written or a chain ends: a snapshot update writes one pair per
// window it publishes this way (internal/reconstruct, pgpair.go).
package pgdelta
