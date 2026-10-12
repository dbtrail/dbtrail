package consoleapp

import (
	"testing"
)

// The resolved cap reaches the console's configuration, and the real watch
// command registers the flag.
func TestUpConsoleConfig_sqlPortMaxRowsReachesTheConsole(t *testing.T) {
	cfg, err := upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/idx",
		consoleOpts{Listen: "127.0.0.1:8090", Token: "tok", SQLPortMaxRows: 4321}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SQLPortMaxRows != 4321 {
		t.Errorf("Config.SQLPortMaxRows = %d, want 4321", cfg.SQLPortMaxRows)
	}
	if got := upConsoleOpts().SQLPortMaxRows; got != upSQLPortMaxRows {
		t.Errorf("upConsoleOpts().SQLPortMaxRows = %d, want the resolved %d", got, upSQLPortMaxRows)
	}
	if f := watchCmd.Flags().Lookup("sql-port-max-rows"); f == nil || f.DefValue != "10000" {
		t.Errorf("watch --sql-port-max-rows = %+v, want it registered with default 10000", f)
	}
}
