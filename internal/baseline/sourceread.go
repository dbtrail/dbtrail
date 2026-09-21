package baseline

import (
	"log/slog"
	"strconv"
	"time"
)

// Source-read keys (#1570): how far a table's rows are from the last time the
// source database was actually read.
//
// #1545 made one snapshot say how each of its tables was produced, one step
// back. The chain question ("when did we last take real evidence from the
// source?") needs every ancestor opened in turn, and an operator cannot do
// that by hand: ancestors get pruned, and a carried table's chain runs through
// a file whose footer belongs to an older snapshot entirely. So the answer is
// INHERITED, the way MetaKeyCaptureGap is: a dump stamps its own instant, a
// fold copies what it folded from, and a descendant therefore never looks
// fresher than its ancestor.
//
// A carried-forward table is not stamped at all, and cannot be: it is usually
// a hard link to the previous snapshot's file (#1545), so its values are the
// ancestor's, read where they are.
const (
	// MetaKeyLastDumpAt is the RFC3339 instant of the newest ancestor that
	// was a real read of the source (a dump), the file's own for a dump.
	MetaKeyLastDumpAt = "bintrail.last_dump_at"
	// MetaKeyFoldGeneration is how many folds sit between that read and these
	// bytes: 0 for a dump, the state it folded from plus one for a fold. With
	// table deltas (#1638) it lives on the chain's newest pair, since the
	// base under a chain is carried forward unchanged while every pair is
	// one more fold (see ChainSourceRead).
	MetaKeyFoldGeneration = "bintrail.fold_generation"
)

// SourceRead is the answer for one table: when its rows last came from a real
// read of the source database, and how many folds have been applied since.
type SourceRead struct {
	// At is when; zero when nothing on record says.
	At time.Time
	// Folds is how many folds since At; -1 when not on record. Meaningful
	// only when At is known.
	Folds int
}

// Known reports whether the read's instant is on record.
func (r SourceRead) Known() bool { return !r.At.IsZero() }

// unknownSourceRead is the zero answer: no instant, no count.
var unknownSourceRead = SourceRead{Folds: -1}

// SourceReadOf answers for the BYTES of one file, from its footer alone.
//
// The file's own keys win. Without them it is derived, and only for a dump:
// a dump IS a read of the source, at the instant its writer stamped
// (MetaKeySnapshotTimestamp), so it answers with zero folds. Everything else
// with no keys is unknown and stays unknown: a fold written before #1570
// cannot be dated without its ancestors, and guessing would be exactly the
// fresher-than-true answer the inheritance exists to rule out.
//
// Deliberately NOT ProvenanceOf. That one answers how a table got into a
// SNAPSHOT, and a carried file is "carried" there. This one describes bytes,
// which were produced however their writer produced them, wherever they sit
// now; the snapshot directory plays no part, so a carried file answers with
// its ancestor's values, which is the truth about those rows.
func SourceReadOf(md DumpMetadata) SourceRead {
	if !md.LastDumpAt.IsZero() {
		return SourceRead{At: md.LastDumpAt, Folds: md.FoldGeneration}
	}
	if bytesFromADump(md) && !md.SnapshotTimestamp.IsZero() {
		return SourceRead{At: md.SnapshotTimestamp, Folds: 0}
	}
	return unknownSourceRead
}

// bytesFromADump reports whether these bytes were written by a real read of
// the source: the producer key, or for a file older than that key the two
// positive signals ProvenanceOf dates a legacy dump by. An unknown producer
// value is not a dump, for the reason ProvenanceOf gives.
func bytesFromADump(md DumpMetadata) bool {
	switch md.Producer {
	case ProducerDump:
		return true
	case "":
		return md.MydumperFormat != "" || md.LSN != 0
	}
	return false
}

// ChainSourceRead answers for a table with a chain of table deltas beside its
// base (#1638): base is the table file's footer, last the chain's newest
// pair's (nil when there is no chain).
//
// The instant comes from the BASE, always: a pair is a fold over the base, so
// the table's last real read is the base's whatever the chain holds, and a
// pair written before #1570 cannot blur it. The fold count comes from the
// newest pair, which is one fold more than the state before it. The one
// exception is a pair written by the same run as the base (same writer
// instant): the empty sequence-0 pair a full backup or a compaction starts
// the chain with, which adds no fold and, for a dump, carries no keys of its
// own.
func ChainSourceRead(base DumpMetadata, last *DumpMetadata) SourceRead {
	r := SourceReadOf(base)
	if last == nil || !r.Known() || sameWriter(base, *last) {
		return r
	}
	r.Folds = -1
	if !last.LastDumpAt.IsZero() {
		r.Folds = last.FoldGeneration
	}
	return r
}

// sameWriter reports whether two files of one table were written by the same
// run: equal writer instants, both on record.
func sameWriter(a, b DumpMetadata) bool {
	return !a.SnapshotTimestamp.IsZero() && a.SnapshotTimestamp.Equal(b.SnapshotTimestamp)
}

// Next is what a fold over state r stamps on what it writes: the same read,
// one fold further. Unknown stays unknown.
func (r SourceRead) Next() SourceRead {
	if !r.Known() {
		return unknownSourceRead
	}
	if r.Folds >= 0 {
		r.Folds++
	}
	return r
}

// Stamp writes r into a footer being built, when it is known. A known instant
// with an unknown count stamps the instant alone: the reader then reports the
// count as not on record, never as zero.
func (r SourceRead) Stamp(md map[string]string) {
	if !r.Known() {
		return
	}
	md[MetaKeyLastDumpAt] = r.At.UTC().Format(time.RFC3339)
	if r.Folds >= 0 {
		md[MetaKeyFoldGeneration] = strconv.Itoa(r.Folds)
	}
}

// DumpSourceRead is a dump's own read: itself, zero folds. The dump writers
// stamp the same two values from their snapshot timestamp string directly.
func DumpSourceRead(at time.Time) SourceRead { return SourceRead{At: at, Folds: 0} }

// parseFoldGeneration reads MetaKeyFoldGeneration. Anything that is not a
// non-negative integer is -1, not on record: a count this build cannot read
// must not turn into "read from the source by this very file".
func parseFoldGeneration(path, raw string) int {
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		slog.Warn("baseline footer: unreadable fold generation; the count of folds since the last read of the source is not on record",
			"path", path, "value", raw)
		return -1
	}
	return n
}
