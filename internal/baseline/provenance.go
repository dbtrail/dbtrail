package baseline

import (
	"log/slog"
	"time"
)

// Provenance keys and values (#1545).
//
// The fold has stamped MetaKeySnapshotProducer / MetaKeyDerivedFrom /
// MetaKeyDerivedFromPath since #1169, but they lived in internal/reconstruct
// and NOTHING read them: the data was on disk and no surface could answer
// "where did this snapshot come from". They move here, where the rest of the
// footer vocabulary lives and where both writers and every reader can reach
// them without an import cycle.
//
// The string VALUES are unchanged, deliberately. Every snapshot already written
// carries them, and renaming a key would silently drop provenance on exactly
// the historical files this exists to explain.
const (
	// MetaKeySnapshotProducer names the code path that WROTE these bytes.
	MetaKeySnapshotProducer = "bintrail.snapshot_producer"
	// MetaKeyDerivedFrom is the RFC3339 snapshot time this one was folded from;
	// absent on a dump, which is derived from nothing.
	MetaKeyDerivedFrom = "bintrail.derived_from_snapshot"
	// MetaKeyDerivedFromPath is that ancestor's file path, for a human tracing
	// a chain by hand.
	MetaKeyDerivedFromPath = "bintrail.derived_from_path"
	// MetaKeySnapshotTimestamp is the instant the WRITING run stamped. It is
	// NOT authoritative for which snapshot a file belongs to — the directory
	// name is (see internal/snapshotdir) — and the gap between the two is what
	// identifies a carried-forward table. See TableProvenance.
	MetaKeySnapshotTimestamp = "bintrail.snapshot_timestamp"
	// MetaKeyCreateTableAsOf is when the embedded CREATE TABLE was read from
	// the live table (#1651). A dump reads it at the snapshot instant; a fold
	// carries its ancestor's value forward with the statement itself, so a
	// chain of folds still knows how old its table definition is. Absent on
	// dumps and on folds written before the key existed: readers fall back to
	// the snapshot's own time.
	MetaKeyCreateTableAsOf = "bintrail.create_table_as_of"
	// MetaKeyMydumperFormat is written only by the mydumper dump path. Kept as
	// a named constant because it is the one POSITIVE signal that dates a
	// pre-#1545 MySQL dump, which carries no producer key at all.
	MetaKeyMydumperFormat = "bintrail.mydumper_format"
)

// Producer values for MetaKeySnapshotProducer.
const (
	// ProducerDump is a real read of the source: mydumper for MySQL,
	// pgbaseline for PostgreSQL.
	ProducerDump = "dump"
	// ProducerReconstruct is the fold — the previous snapshot replayed forward
	// over the index. Its value predates this file and must not change.
	ProducerReconstruct = "reconstruct"
)

// How a table's rows got into a snapshot.
const (
	// ProducedByDump: read from the source. Independent evidence.
	ProducedByDump = "dump"
	// ProducedByFold: the previous snapshot replayed forward over the index.
	// Only as correct as the index window it folded; the source was never read.
	ProducedByFold = "fold"
	// ProducedByCarriedForward: not written at all. The table saw no changes in
	// the window, so its previous file was reused verbatim (#1471) — usually as
	// a HARD LINK, meaning these are literally the older snapshot's bytes.
	ProducedByCarriedForward = "carried_forward"
	// ProducedByUnknown: none of the signals below are present. Several causes
	// reach it and this verdict does NOT distinguish them: a snapshot written
	// before any of them existed, a producer value a newer build wrote that this
	// one does not know, and a corrupt LSN or mydumper format that parsed away.
	// Anything reporting it must not name a cause it cannot see. Never guessed
	// as one of the others — the whole point is that an operator can tell which
	// they have, and a confident wrong answer is worse than none.
	ProducedByUnknown = "unknown"
)

// TableProvenance answers, for ONE table of ONE snapshot, how its rows got
// there. Per table rather than per snapshot because carry-forward makes a
// single snapshot a mix: some tables folded, some reused whole.
type TableProvenance struct {
	// ProducedBy is one of the ProducedBy* values.
	ProducedBy string
	// From is the snapshot these rows came out of, for the two derived cases:
	// the ancestor that was folded (ProducedByFold) or the snapshot whose file
	// was reused (ProducedByCarriedForward). Zero for a dump.
	From time.Time
	// FromPath is From's own file, when the footer recorded one. EMPTY on a
	// carried table, and deliberately: derived_from_path in a carried file is
	// the footer of the snapshot the bytes came from, which for a folded
	// ancestor names ITS source — one link further back than From. Returning
	// the pair would hand an operator two timestamps that do not describe the
	// same snapshot. The footer simply does not record the carried-from file.
	FromPath string
}

// ProvenanceOf derives a table's provenance from its footer and the snapshot
// DIRECTORY time it was found under.
//
// The directory time is a parameter and not something read out of the file
// because it is the authoritative one (internal/snapshotdir says so, and every
// discovery path already works that way), and because the DISAGREEMENT between
// the two is the whole signal for carry-forward.
//
// A carried table is usually a hard link to the previous snapshot's file, so
// its footer is that snapshot's footer, unchanged and unchangeable: rewriting
// it would edit the older snapshot through the same inode. That closes off
// stamping a carried file at write time and leaves exactly one honest way to
// recognise one — its footer says it was written at a different instant than
// the snapshot it now sits in. Which is not a workaround: it is the same fact,
// read where it actually exists.
//
// Order matters. The carried check comes FIRST, because a carried file's
// producer key is its ancestor's and reporting that would name the wrong
// operation for this snapshot. Everything after it is describing bytes that
// really were written into this snapshot.
func ProvenanceOf(snapshotTime time.Time, md DumpMetadata) TableProvenance {
	if !md.SnapshotTimestamp.IsZero() && !snapshotTime.IsZero() &&
		!md.SnapshotTimestamp.Equal(snapshotTime.UTC()) {
		// No FromPath: see the field's doc. md.DerivedFromPath here belongs to
		// whoever wrote these bytes, not to the snapshot they were written for.
		return TableProvenance{
			ProducedBy: ProducedByCarriedForward,
			From:       md.SnapshotTimestamp,
		}
	}
	switch md.Producer {
	case ProducerReconstruct:
		return TableProvenance{ProducedBy: ProducedByFold, From: md.DerivedFrom, FromPath: md.DerivedFromPath}
	case ProducerDump:
		return TableProvenance{ProducedBy: ProducedByDump}
	}
	// A producer this build does not recognise is UNKNOWN, and it must not reach
	// the legacy sniff below. That block dates a file that carries no producer
	// key at all; a value a newer version stamped is a positive statement this
	// build cannot read, and grading it `dump` because the file also happens to
	// carry a mydumper format would be a confident answer about an operation
	// nobody here knows the name of.
	if md.Producer != "" {
		return TableProvenance{ProducedBy: ProducedByUnknown}
	}
	// No producer key: written before #1545 stamped one. Two positive signals
	// still date it as a dump, and both are keys the fold never writes —
	// mydumper's format for MySQL, the WAL floor for PostgreSQL. Guessing from
	// their ABSENCE is what is refused: an old fold carries neither either.
	if md.MydumperFormat != "" || md.LSN != 0 {
		return TableProvenance{ProducedBy: ProducedByDump}
	}
	return TableProvenance{ProducedBy: ProducedByUnknown}
}

// readProvenance fills the provenance half of a DumpMetadata from a footer
// lookup. Shared by the local and S3 readers so the two cannot come to hold
// different opinions about which keys exist.
func readProvenance(path string, m *DumpMetadata, lookup func(string) (string, bool)) {
	if v, ok := lookup(MetaKeySnapshotProducer); ok {
		m.Producer = v
	}
	if v, ok := lookup(MetaKeyDerivedFromPath); ok {
		m.DerivedFromPath = v
	}
	if v, ok := lookup(MetaKeyMydumperFormat); ok {
		m.MydumperFormat = v
	}
	if v, ok := lookup(MetaKeyDerivedFrom); ok {
		m.DerivedFrom = parseFooterTime(path, MetaKeyDerivedFrom, v)
	}
	if v, ok := lookup(MetaKeySnapshotTimestamp); ok {
		m.SnapshotTimestamp = parseFooterTime(path, MetaKeySnapshotTimestamp, v)
	}
	if v, ok := lookup(MetaKeyCreateTableAsOf); ok {
		m.CreateTableAsOf = parseFooterTime(path, MetaKeyCreateTableAsOf, v)
	}
	if v, ok := lookup(MetaKeyLastDumpAt); ok {
		m.LastDumpAt = parseFooterTime(path, MetaKeyLastDumpAt, v)
	}
	if v, ok := lookup(MetaKeyFoldGeneration); ok {
		m.FoldGeneration = parseFoldGeneration(path, v)
	}
}

// parseFooterTime reads an RFC3339 footer value, warning rather than failing on
// a corrupt one: provenance is a description, and losing it must never take a
// baseline READ down with it.
func parseFooterTime(path, key, val string) time.Time {
	t, err := time.Parse(time.RFC3339, val)
	if err != nil {
		slog.Warn("corrupt timestamp in baseline Parquet metadata",
			"path", path, "key", key, "raw_value", val, "error", err)
		return time.Time{}
	}
	return t.UTC()
}
