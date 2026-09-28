package consoleapp

import "github.com/dbtrail/dbtrail/internal/console"

// loadConsoleRegistry loads the server registry, registers the daemon's own
// --baseline-s3 bucket with it, and gives every server that read its
// snapshots through the daemon's --baseline-dir/--baseline-s3 that location
// as its own (#1684), all before anything else sees the registry. The
// rotation, baseline-prune, refresh, staleness and verify loops start ahead
// of console.New: their first cycle reads the bucket table (#1575) and the
// entries' own locations, so both must already be final. console.New
// registers the bucket again, harmlessly; the migration runs once.
func loadConsoleRegistry(path, baselineDir, baselineS3 string) (*console.Registry, error) {
	reg, err := console.LoadRegistry(path)
	if err != nil {
		return nil, err
	}
	if baselineS3 != "" {
		reg.SetProcessS3Location(console.DaemonBaselineS3Label, baselineS3)
	}
	reg.MigrateProcessBaselineLocation(baselineDir, baselineS3)
	return reg, nil
}
