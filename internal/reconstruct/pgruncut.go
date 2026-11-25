package reconstruct

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

// PGRunCutFile is the record a PostgreSQL snapshot update leaves in the
// snapshot directory it publishes (#3084): where the run ended, and which of
// the directory's tables it brought up to that end.
//
// A table with no change in the window is carried forward with its previous
// file, footer and all, so its own floor stays where an earlier run left it,
// and a reader of the footer alone takes the new snapshot for the old one.
// The run read the table's window and found nothing, so the table's rows
// hold through the run's end as well as any file the run wrote: the record
// says so for the directory. MySQL has the same rule in the footers of the
// files a refresh rewrites (MetaKeyFoldCut, #2592); a PostgreSQL footer has
// no event id or run cut, so the run writes it once, beside _SUCCESS.
//
// Not written for a run over an index with no change (nothing was folded),
// by a full read of the source, or by a run that did not complete: a
// directory without it reads as before.
const PGRunCutFile = "_PG_RUN_CUT"

// pgRunHeld is the set of tables a run brought up to its end: folded through
// it, written as a table delta through it, or carried forward because the
// window through it held no change. A table published as an earlier read
// left it (a full read capture has not passed, a table capture dropped
// changes of) is not in it, nor is one published over a known loss.
type pgRunHeld struct {
	mu     sync.Mutex
	tables map[[2]string]bool
}

// holdThrough records that schema.table holds through the run's end. A run
// with no record (nil, or over an empty index) keeps none.
func (r *pgRunEnd) holdThrough(schema, table string) {
	if r == nil || r.held == nil {
		return
	}
	r.held.mu.Lock()
	defer r.held.mu.Unlock()
	if r.held.tables == nil {
		r.held.tables = map[[2]string]bool{}
	}
	r.held.tables[[2]string{schema, table}] = true
}

// pgRunCutRecord is PGRunCutFile as written.
type pgRunCutRecord struct {
	Version int `json:"version"`
	// LSN is where the next fold starts: the run's last LSN plus one, what
	// the footer of every file the run wrote records as its LSN.
	LSN uint64 `json:"lsn"`
	// ReachID is pgRunEnd.reachID, 0 for none.
	ReachID uint64 `json:"reach_id,omitempty"`
	// At is the directory's own time, as its name spells it: a record
	// copied into another directory says nothing about it.
	At     string      `json:"at"`
	Tables [][2]string `json:"tables"`
}

// writeCut writes the run's record into snapshotDir, the directory it is
// about to publish. Nothing is written for a run with nothing to say: no run
// (a MySQL source), an index with no change, no table brought up to the end,
// a directory whose name is not a time.
func (r *pgRunEnd) writeCut(snapshotDir string) error {
	if r == nil || r.none || r.held == nil || snapshotDir == "" {
		return nil
	}
	at, ok := snapshotdir.ParseTime(filepath.Base(filepath.Clean(snapshotDir)))
	if !ok {
		return nil
	}
	r.held.mu.Lock()
	tables := make([][2]string, 0, len(r.held.tables))
	for k := range r.held.tables {
		tables = append(tables, k)
	}
	r.held.mu.Unlock()
	if len(tables) == 0 {
		return nil
	}
	slices.SortFunc(tables, func(a, b [2]string) int {
		return cmp.Or(strings.Compare(a[0], b[0]), strings.Compare(a[1], b[1]))
	})
	b, err := json.Marshal(pgRunCutRecord{Version: 1, LSN: r.until + 1, ReachID: r.reachID, At: at.Format(time.RFC3339), Tables: tables})
	if err != nil {
		return err
	}
	tmp := filepath.Join(snapshotDir, PGRunCutFile+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, filepath.Join(snapshotDir, PGRunCutFile)); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// PGRunCut is a snapshot directory's PGRunCutFile, read.
type PGRunCut struct {
	// LSN is where the run's next fold starts, as a file it wrote records it.
	LSN uint64
	// ReachID is the id of the newest event the run held every change
	// through, 0 when it does not say.
	ReachID uint64
	// At is the time of the directory the record was written for.
	At     time.Time
	tables map[[2]string]bool
}

// Holds says whether the run brought schema.table up to its end.
func (c PGRunCut) Holds(schema, table string) bool { return c.tables[[2]string{schema, table}] }

// ReadPGRunCut reads dir's record. found is false, with no error, when dir
// has none. A record that does not parse, or that names no position, is an
// error: the caller then treats the directory as having none.
func ReadPGRunCut(dir string) (cut PGRunCut, found bool, err error) {
	b, err := os.ReadFile(filepath.Join(dir, PGRunCutFile))
	if errors.Is(err, os.ErrNotExist) {
		return PGRunCut{}, false, nil
	}
	if err != nil {
		return PGRunCut{}, false, err
	}
	var rec pgRunCutRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return PGRunCut{}, false, fmt.Errorf("%s does not parse: %w", PGRunCutFile, err)
	}
	if rec.Version != 1 {
		return PGRunCut{}, false, fmt.Errorf("%s is version %d, which this build does not read", PGRunCutFile, rec.Version)
	}
	at, err := time.Parse(time.RFC3339, rec.At)
	if err != nil || rec.LSN == 0 {
		return PGRunCut{}, false, fmt.Errorf("%s names no time or no position", PGRunCutFile)
	}
	cut = PGRunCut{LSN: rec.LSN, ReachID: rec.ReachID, At: at.UTC(), tables: make(map[[2]string]bool, len(rec.Tables))}
	for _, t := range rec.Tables {
		cut.tables[t] = true
	}
	return cut, true, nil
}

// pgHoldThroughRun records, at a table's successful end, that the table holds
// through the run's end: not when the window had a known loss of captured
// changes, or capture dropped changes of the table and the fold went on
// without them (notCovered). Its file then records the loss itself, and a
// reader that moved its start to the run's end would ask over less.
func pgHoldThroughRun(cfg FullTableConfig, schema, table string, capGap *CaptureGap, notCovered bool) {
	if capGap != nil || notCovered {
		return
	}
	cfg.pgRun.holdThrough(schema, table)
}
