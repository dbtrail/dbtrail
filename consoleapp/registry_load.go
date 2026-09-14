package consoleapp

import "github.com/dbtrail/dbtrail/internal/console"

// loadConsoleRegistry loads the server registry and registers the daemon's
// own --baseline-s3 bucket with it before anything else sees the registry.
// The rotation and baseline-prune loops start ahead of console.New, and their
// first cycle reads the bucket table: without this, that cycle routes the
// daemon's bucket to a server's store and every later cycle does not (#1575).
// console.New registers it again, harmlessly.
func loadConsoleRegistry(path, baselineS3 string) (*console.Registry, error) {
	reg, err := console.LoadRegistry(path)
	if err != nil {
		return nil, err
	}
	if baselineS3 != "" {
		reg.SetProcessS3Location(console.DaemonBaselineS3Label, baselineS3)
	}
	return reg, nil
}
