package reconstruct

import (
	"bufio"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// This file holds the on-disk side of the change map (#1107).
//
// A fold holds one row image per distinct changed row until the merge, and
// paging the event fetch (#1097) does not bound that. When a table's changes
// pass the fold's in-memory limit (FullTableConfig.MaxTouchedRows, divided by
// the tables folding at once), foldEventWindow moves them here instead of
// refusing: every change goes to one of spillBuckets files chosen by a hash of
// its pk_values, and the merge then reads a few groups at a time, as many as
// fit under the same limit, making one pass over the baseline per set. Peak
// memory stays near the limit, plus about one group while a pass is built,
// however many rows the window changed; the price
// is one baseline read per pass, and disk in the system temp directory for the
// changes.
//
// WHY NOT LET DUCKDB DO THE JOIN. DuckDB can merge two tables (MERGE INTO,
// INSERT ... ON CONFLICT) and spills its own joins to disk, so handing it the
// baseline and the changes looks like the natural fold. It is not, because the
// two sides do not carry the same key:
//
//   - the change side's key is binlog_events.pk_values, text formatted by the
//     capture parser (a DATETIME as go-mysql prints it, a BINARY(n) without its
//     trailing zeros, escaped pipes between columns);
//   - the baseline side holds the typed values mydumper dumped (a timestamp, the
//     padded bytes, a DECIMAL).
//
// canonicalizePKMap + event.BuildPKValues turn a baseline row into the change
// side's spelling, in Go, per type. Joining in DuckDB would mean rewriting that
// translation in SQL for every key type, which is where #212 (DATETIME keys
// never matched), #1155 (binary keys) and #1158 (one row under two spellings,
// a DELETE lost) lived. The change side's values need Go too: ENUM ordinals and
// base64 BLOB/TEXT are decoded against the schema in effect at each event's own
// time before they can be written. Partitioning in Go keeps both translations
// exactly as they are and changes only where the change map lives.

// spillBuckets is the number of groups. The merge refuses a table only when
// one group alone passes the limit, which takes about spillBuckets times the
// limit of changed rows; a daemon fold with two tables folding at once
// (1,000,000 per table) gets there at about 64 million rows of one table.
const spillBuckets = 64

func init() {
	// The value types a decoded row image holds besides gob's basic ones:
	// json.Number (query.UnmarshalRowImage decodes with UseNumber) and the
	// containers of a JSON column. Anything else in an image fails to encode,
	// loudly, rather than coming back as something else.
	gob.Register(json.Number(""))
	gob.Register(map[string]any{})
	gob.Register([]any{})
}

// spillRecord is what the merge reads from a change: the event type, its id
// (for the nil-image log line), the key and the after-image. Nothing else.
// A merge stage that starts reading another field of a change map entry must
// add it here, or it reads zero from every change that went through disk.
type spillRecord struct {
	PK    string
	Type  event.EventType
	ID    uint64
	After map[string]any
}

// changeSpill is one table's changes on disk, one gob stream per group.
type changeSpill struct {
	dir string
	// limit is the in-memory limit the fold passed; the merge sizes its passes
	// with it, and tables names the tables folding at once for the refusal.
	limit  int64
	tables int

	files   [spillBuckets]*os.File
	bufs    [spillBuckets]*bufio.Writer
	encs    [spillBuckets]*gob.Encoder
	written [spillBuckets]bool
	// records counts what was written, which is at least the distinct rows:
	// a row changed on several pages is written once per page.
	records int64
}

func newChangeSpill(limit int64) (*changeSpill, error) {
	dir, err := os.MkdirTemp("", "bintrail-fold-*")
	if err != nil {
		return nil, fmt.Errorf("create a directory for changed rows in the temporary directory %s: %w", os.TempDir(), err)
	}
	return &changeSpill{dir: dir, limit: limit}, nil
}

// spillBucket is the group a pk_values string belongs to. The fold and every
// merge pass call it on the same string, in the same process, so it only has
// to be deterministic, not stable across versions.
func spillBucket(pk string) int {
	h := fnv.New32a()
	_, _ = io.WriteString(h, pk)
	return int(h.Sum32() % spillBuckets)
}

func (s *changeSpill) path(b int) string {
	return filepath.Join(s.dir, fmt.Sprintf("%02d.gob", b))
}

// drain appends every entry of changes to its group. Entries of one map have
// distinct keys, so their order does not matter; calls must come in the order
// the changes happened, because reading a group back keeps the last write.
func (s *changeSpill) drain(changes map[string]*query.ResultRow) error {
	for pk, ev := range changes {
		b := spillBucket(pk)
		if s.encs[b] == nil {
			f, err := os.OpenFile(s.path(b), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				return fmt.Errorf("write changed rows to the temporary directory %s: %w", s.dir, err)
			}
			s.files[b] = f
			s.bufs[b] = bufio.NewWriterSize(f, 64<<10)
			s.encs[b] = gob.NewEncoder(s.bufs[b])
			s.written[b] = true
		}
		if err := s.encs[b].Encode(spillRecord{PK: pk, Type: ev.EventType, ID: ev.EventID, After: ev.RowAfter}); err != nil {
			return fmt.Errorf("write changed row %q to the temporary directory %s: %w", pk, s.dir, err)
		}
		s.records++
	}
	return nil
}

// finish flushes and closes every group file. Nothing may be drained after it.
func (s *changeSpill) finish() error {
	var errs []error
	for b := range s.files {
		if s.files[b] == nil {
			continue
		}
		if err := s.bufs[b].Flush(); err != nil {
			errs = append(errs, fmt.Errorf("write changed rows to the temporary directory %s: %w", s.dir, err))
		}
		if err := s.files[b].Close(); err != nil {
			errs = append(errs, fmt.Errorf("write changed rows to the temporary directory %s: %w", s.dir, err))
		}
		s.files[b], s.bufs[b] = nil, nil
	}
	return errors.Join(errs...)
}

// load reads group b back into a change map, the last write for a key
// winning. It stops with the changed-rows refusal as soon as the group alone
// holds more distinct rows than the limit, before reading the rest of it.
func (s *changeSpill) load(b int) (map[string]*query.ResultRow, error) {
	m := map[string]*query.ResultRow{}
	if !s.written[b] {
		return m, nil
	}
	f, err := os.Open(s.path(b))
	if err != nil {
		return nil, fmt.Errorf("read changed rows back from disk: %w", err)
	}
	defer f.Close()
	dec := gob.NewDecoder(bufio.NewReaderSize(f, 64<<10))
	for {
		var rec spillRecord
		if err := dec.Decode(&rec); err != nil {
			if errors.Is(err, io.EOF) {
				return m, nil
			}
			return nil, fmt.Errorf("read changed rows back from disk: %w", err)
		}
		for k, v := range rec.After {
			rec.After[k] = restoreEmpty(v)
		}
		m[rec.PK] = &query.ResultRow{EventType: rec.Type, EventID: rec.ID, PKValues: rec.PK, RowAfter: rec.After}
		if s.limit > 0 && int64(len(m)) > s.limit {
			return nil, TouchedRowBudgetError(s.limit, s.tables, true)
		}
	}
}

// diskBytes is how much the groups take on disk, or zero for what cannot be
// sized. It is the estimate a pair written from the spill gives the disk
// check (#2126): the rows are the same ones, in a format that packs less.
func (s *changeSpill) diskBytes() int64 {
	var n int64
	for b := range spillBuckets {
		if !s.written[b] {
			continue
		}
		if fi, err := os.Stat(s.path(b)); err == nil {
			n += fi.Size()
		}
	}
	return n
}

// eachPass reads the groups back in order and hands them to run a pass at a
// time: as many groups as fit under the limit, the next group's size predicted
// from the last one (the hash spreads rows evenly). Every group belongs to
// exactly one pass, the empty ones included, and owns marks the groups of the
// pass being run; neither it nor the map outlives the call. It returns how
// many passes ran.
//
// The one place the grouping is decided, for the merge that rewrites a table
// (mergeSpilledPasses) and for the pair written beside one (writeTableDelta,
// #2126): two loops would drift on the limit.
func (s *changeSpill) eachPass(ctx context.Context, run func(pass map[string]*query.ResultRow, owns *[spillBuckets]bool) error) (int, error) {
	var owns [spillBuckets]bool
	pass := map[string]*query.ResultRow{}
	passes := 0
	flush := func() error {
		if err := run(pass, &owns); err != nil {
			return err
		}
		passes++
		owns = [spillBuckets]bool{}
		pass = map[string]*query.ResultRow{}
		return nil
	}
	last := 0
	for b := range spillBuckets {
		if err := ctx.Err(); err != nil {
			return passes, err
		}
		if len(pass) > 0 && int64(len(pass)+last) > s.limit {
			if err := flush(); err != nil {
				return passes, err
			}
		}
		group, err := s.load(b)
		if err != nil {
			return passes, err
		}
		last = len(group)
		// Groups hold disjoint keys, so nothing here overwrites.
		maps.Copy(pass, group)
		owns[b] = true
	}
	if err := flush(); err != nil {
		return passes, err
	}
	return passes, nil
}

// restoreEmpty undoes gob's one loss on a row image: it can send an empty
// []byte, map or slice and hand back a nil one, which would turn an empty BLOB into
// NULL and a JSON [] or {} into null. A decoded image never holds a nil one of
// these (JSON null is a nil interface, and base64 decoding returns a non-nil
// slice), so every nil one here was an empty one.
func restoreEmpty(v any) any {
	switch x := v.(type) {
	case []byte:
		if x == nil {
			return []byte{}
		}
	case map[string]any:
		if x == nil {
			return map[string]any{}
		}
		for k, e := range x {
			x[k] = restoreEmpty(e)
		}
	case []any:
		if x == nil {
			return []any{}
		}
		for i, e := range x {
			x[i] = restoreEmpty(e)
		}
	}
	return v
}

// remove closes anything still open and deletes the spill directory. Safe to
// call more than once.
func (s *changeSpill) remove() error {
	if err := s.finish(); err != nil {
		slog.Debug("closing changed rows on disk before removing them", "dir", s.dir, "error", err)
	}
	return os.RemoveAll(s.dir)
}
