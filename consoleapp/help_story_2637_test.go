package consoleapp

import (
	"strings"
	"testing"
)

// The root help of bintrail-console opens with what DBTrail does, the heavy
// queries, instead of "read-only web interface over the binlog index"
// (#2637): this binary runs the capture and the MySQL port too, so that line
// was not true of it.
func TestRootHelp_saysWhatTheServiceDoes(t *testing.T) {
	for _, old := range []string{"Read-only web interface", "read-only web UI over the binlog index", "NEVER executes SQL"} {
		if strings.Contains(rootCmd.Short+"\n"+rootCmd.Long, old) {
			t.Errorf("the root help still says %q:\n%s\n%s", old, rootCmd.Short, rootCmd.Long)
		}
	}
	if !strings.Contains(rootCmd.Short, "heavy MySQL queries") {
		t.Errorf("root Short does not open on the heavy queries: %q", rootCmd.Short)
	}
	if !strings.Contains(rootCmd.Long, "https://www.dbtrail.com/docs/quickstart/") {
		t.Errorf("root Long does not link the start page:\n%s", rootCmd.Long)
	}
}
