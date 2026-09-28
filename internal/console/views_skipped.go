package console

import (
	"context"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// Views a snapshot left out (#1879).
//
// A view holds no rows to copy, so a snapshot has no file for it. The full
// read that skipped it records it beside the snapshot
// (baseline.ViewsSkipped), and a snapshot updated from the recorded changes
// carries that record forward, marked as carried. The Snapshots page shows
// the count on the snapshot's row and the names when the row is opened.
//
// Three rules, the same on the row and in the detail:
//
//   - No record is "not recorded", never "no views": a snapshot written
//     before the record existed says nothing about views, and neither does
//     one whose source had none.
//   - A carried record is dated. It says what the source held when it was
//     last read in full, which is not now.
//   - The names reach a session only through the listing and the detail,
//     and both are refused to a session with a data profile (#1075), so such
//     a session sees no name of a view in a schema its profile hides. The
//     run status keeps the count and no names, as it does for the tables a
//     refresh refused (#1653).

// ViewsSkippedCap is how many names the detail of one snapshot carries. The
// rest are counted, as the refused tables of a run are (RefusedTablesCap).
const ViewsSkippedCap = RefusedTablesCap

// viewsSkippedDTO is what a snapshot's detail says about the views it left
// out. Absent from the response when nothing is recorded.
type viewsSkippedDTO struct {
	Count int `json:"count"`
	// Names is up to ViewsSkippedCap names, "schema.view", sorted; Omitted
	// counts the views behind Count that are not named here. No names at
	// all means the count came from this daemon's record of the run, which
	// keeps no names: the log of that run has them.
	Names   []string `json:"names,omitempty"`
	Omitted int      `json:"omitted,omitempty"`
	// Carried: this snapshot did not read the source. ReadAt is when the
	// source was read in full and these views were found, in the listing's
	// own time format, UTC; "" when the record does not say.
	Carried bool   `json:"carried,omitempty"`
	ReadAt  string `json:"read_at,omitempty"`
}

// viewsSkippedOf turns a record into what the page is told. nil for a
// record that counts no view.
func viewsSkippedOf(rec baseline.ViewsSkipped) *viewsSkippedDTO {
	if rec.Count <= 0 {
		return nil
	}
	dto := &viewsSkippedDTO{Count: rec.Count, Carried: rec.Carried}
	for _, name := range rec.Views {
		if len(dto.Names) >= ViewsSkippedCap {
			break
		}
		if name = clipRunes(oneLine(name), refusedNameCap); name != "" {
			dto.Names = append(dto.Names, name)
		}
	}
	dto.Count = max(dto.Count, len(dto.Names))
	dto.Omitted = dto.Count - len(dto.Names)
	if at, err := time.Parse(time.RFC3339, rec.ReadAt); err == nil {
		dto.ReadAt = at.UTC().Format(consoleTSFormat)
	}
	return dto
}

// viewsSkippedFromRun is the count this daemon recorded for the full read
// that wrote a snapshot, for a snapshot that carries no record of its own
// (written before the record existed, by a version that already counted).
// Only a full read counts: no other run reads the views.
func (s *Server) viewsSkippedFromRun(serverID string, snapshotAt time.Time) *viewsSkippedDTO {
	if s.baselineHistory == nil {
		return nil
	}
	rec := s.baselineHistory.FindBySnapshot(serverID, snapshotAt.UTC().Format(time.RFC3339))
	if rec == nil || rec.Kind != BaselineRunDump || rec.ViewsSkipped <= 0 {
		return nil
	}
	return &viewsSkippedDTO{Count: rec.ViewsSkipped, Omitted: rec.ViewsSkipped}
}

// readLocalViewsSkipped reads the record of a local snapshot directory. A
// record that cannot be read is logged and reads as none: the listing is
// served, and a count from a file that was not understood is never shown.
func readLocalViewsSkipped(snapshotDir string) *viewsSkippedDTO {
	rec, ok, err := baseline.ReadViewsSkipped(snapshotDir)
	if err != nil {
		slog.Warn("console: the record of skipped views of a snapshot cannot be read; no count of views is shown for it",
			"snapshot", snapshotDir, "error", err)
		return nil
	}
	if !ok {
		return nil
	}
	return viewsSkippedOf(rec)
}

// snapshotViewsSkipped reads the record out of one snapshot's enumerated
// files, local or S3. It costs a read only when the snapshot holds the
// record.
func snapshotViewsSkipped(ctx context.Context, ss *snapshotSource, dirName string, files []baselineSnapshotFile) *viewsSkippedDTO {
	want := dirName + "/" + baseline.ViewsSkippedName
	for _, f := range files {
		if f.RelPath != want {
			continue
		}
		rc, err := ss.open(ctx, f.RelPath)
		if err != nil {
			slog.Warn("console: the record of skipped views of a snapshot cannot be opened; no count of views is shown for it",
				"snapshot", dirName, "error", err)
			return nil
		}
		defer rc.Close()
		rec, ok, err := baseline.ReadViewsSkippedFrom(rc)
		if err != nil {
			slog.Warn("console: the record of skipped views of a snapshot cannot be read; no count of views is shown for it",
				"snapshot", dirName, "error", err)
			return nil
		}
		if !ok {
			return nil
		}
		return viewsSkippedOf(rec)
	}
	return nil
}

// rowViewsSkipped fills a listing row from what is known about its
// snapshot. nil leaves the row without a word about views.
func (dto *baselineSnapshotDTO) rowViewsSkipped(v *viewsSkippedDTO) {
	if v == nil {
		return
	}
	dto.ViewsSkipped, dto.ViewsCarried, dto.ViewsReadAt = v.Count, v.Carried, v.ReadAt
}

// localSnapshotDirOf is the snapshot directory of a local table file
// (<snapshot>/<schema>/<table>.parquet).
func localSnapshotDirOf(tablePath string) string {
	return filepath.Dir(filepath.Dir(tablePath))
}
