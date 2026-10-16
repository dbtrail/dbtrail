package consoleapp

import (
	"log/slog"

	"github.com/dbtrail/dbtrail/internal/console"
)

// loadConsoleRegistry loads the server registry, registers the daemon's own
// --baseline-s3 bucket with it, and gives every server that read its
// snapshots through the daemon's --baseline-dir/--baseline-s3 that location
// as its own (#1684), all before anything else sees the registry. The
// rotation, baseline-prune, refresh, staleness and verify loops start ahead
// of console.New: their first cycle reads the bucket table (#1575) and the
// entries' own locations, so both must already be final. console.New
// registers the bucket again, harmlessly; the migration runs once.
//
// cliRefreshes: this process refreshes the command-line server's snapshots
// (a command-line index and --baseline-refresh-interval, the one way it
// writes them; baselineRefreshTargets makes it a target on the same terms).
// Then that server is a writer of the startup folder, and a registry server
// naming the same folder is refused every write (#1684,
// Registry.WriteRefusal). The refresh writes the folder only, so the startup
// S3 location is not counted, exactly as the refresh loop counts it.
//
// takesSnapshots: this process takes the snapshots (watch, never serve). Then
// every server that keeps its snapshots only in S3 is given its default local
// folder (#2431), after the location migration, which can hand an entry the
// daemon's bucket alone.
func loadConsoleRegistry(path string, cliRefreshes, takesSnapshots bool, baselineDir, baselineS3 string) (*console.Registry, error) {
	reg, err := console.LoadRegistry(path)
	if err != nil {
		return nil, err
	}
	if baselineS3 != "" {
		reg.SetProcessS3Location(console.DaemonBaselineS3Label, baselineS3)
	}
	if cliRefreshes {
		reg.SetCommandLineWriter(baselineDir, "")
	}
	reg.MigrateProcessBaselineLocation(baselineDir, baselineS3)
	if takesSnapshots {
		logLocalCopyAssignments(reg.GiveEveryServerALocalCopy())
	}
	return reg, nil
}

// logLocalCopyAssignments says what GiveEveryServerALocalCopy did: once per
// server, at startup, since an entry given its folder is saved and is not
// seen again.
func logLocalCopyAssignments(done []console.LocalCopyAssignment) {
	for _, a := range done {
		switch {
		case a.Err != nil:
			slog.Error("server keeps its snapshots only in S3 and could not be given a folder on this machine, so it stays as it was; "+
				"fix the reason and restart, or give it a folder on the "+console.PageSnapshots+" page",
				"server", a.Name, "id", a.ID, "folder", a.Dir, "error", a.Err)
		case len(a.Moved) > 0:
			slog.Warn("server kept its snapshots only in S3; it now also keeps them on this machine, "+
				"and the snapshots its local mirror held were moved into its folder, so SQL on its copy reads what it read before",
				"server", a.Name, "id", a.ID, "folder", a.Dir, "moved", a.Moved, "move_error", a.MoveErr)
		default:
			slog.Warn("server kept its snapshots only in S3; it now also keeps them on this machine, "+
				"but SQL on its copy has no snapshot to read until its next one lands in the folder (take one on the "+console.PageSnapshots+" page)",
				"server", a.Name, "id", a.ID, "folder", a.Dir, "moved", a.Moved, "move_error", a.MoveErr)
		}
	}
}
