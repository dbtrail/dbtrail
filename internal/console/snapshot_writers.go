package console

import (
	"context"
	"database/sql"
	"log/slog"
	"slices"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/serverid"
)

// snapshotWritersDTO says one snapshot location holds snapshots signed by
// more than one writer (#1762): two installations write into it, their
// snapshots mix, and a read that takes the newest one can return the other
// installation's data. The Snapshots page says so in the server's settings
// row. Nothing is refused.
//
// Unsigned snapshots, which is every snapshot written before signatures
// existed, name no writer and never appear here.
type snapshotWritersDTO struct {
	// Source is the location, as the listing's sources name it.
	Source string `json:"source"`
	// Own is the selected server's own identity, set only when it is known
	// AND it signed snapshots in this location. Others is then everyone
	// else. Empty means this server cannot be told apart, and Others names
	// every writer found.
	Own    string   `json:"own,omitempty"`
	Others []string `json:"others"`
}

// snapshotWritersSeen is reconstruct.SnapshotWritersSeen behind a seam: what
// the listings made so far saw, read from memory.
var snapshotWritersSeen = reconstruct.SnapshotWritersSeen

// ownWriterID reads the selected server's identity from its index, the same
// value its snapshots are signed with. "" when the index is not open, names
// none, or cannot be read; the warning then names every writer instead of
// "the other one", and is still shown.
func ownWriterID(ctx context.Context, db *sql.DB, serverID string) string {
	if db == nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	id, err := serverid.SnapshotWriterID(ctx, db)
	if err != nil {
		slog.Warn("console: could not read the server's bintrail_id; the shared-location warning names every writer found instead of the other one",
			"server", serverID, "error", err)
		return ""
	}
	// In the form signatures are compared in, or the same id spelled in
	// upper case by the index would read as a stranger to its own snapshots.
	return reconstruct.NormalizeSnapshotWriter(id)
}

// sharedSnapshotLocations returns one entry per source whose snapshots were
// signed by more than one writer. own is asked at most once, and only when
// there is something to report, so a location with one writer costs nothing.
func sharedSnapshotLocations(sources []string, seen func(string) []string, own func() string) []snapshotWritersDTO {
	var out []snapshotWritersDTO
	ownID, asked := "", false
	for _, src := range sources {
		writers := seen(src)
		if len(writers) < 2 {
			continue
		}
		if !asked {
			ownID, asked = own(), true
		}
		dto := snapshotWritersDTO{Source: src, Others: slices.Clone(writers)}
		if i := slices.Index(writers, ownID); ownID != "" && i >= 0 {
			dto.Own = ownID
			dto.Others = slices.Delete(dto.Others, i, i+1)
		}
		out = append(out, dto)
	}
	return out
}
