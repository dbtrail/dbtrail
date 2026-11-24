package consoleapp

import (
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// The argument list of a full read, spelled out whole for every schema
// selection. The read of some tables alone (#2640) shares the code that builds
// it, and a full read must keep sending mydumper exactly this, in this order.
func TestBuildConsoleMydumperArgs_fullReadIsUnchanged_2640(t *testing.T) {
	head := []string{
		"--host", "h",
		"--port", "3306",
		"--user", "u",
		"--threads", "4",
		"--compress-protocol",
		"--complete-insert",
	}
	lock := []string{"--sync-thread-lock-mode", "FTWRL", "--trx-tables"}
	tls := []string{"--ssl", "--ssl-mode", "REQUIRED"}
	tail := []string{"--outputdir", "/out"}

	for _, c := range []struct {
		name        string
		schemas     []string
		listSchemas bool
		selection   []string
	}{
		{"nil schemas", nil, false, []string{"--regex", systemSchemaExcludeRegex}},
		{"empty schemas", []string{}, true, []string{"--regex", systemSchemaExcludeRegex}},
		{"one schema", []string{"shop"}, false, []string{"--database", "shop"}},
		{"one schema, list allowed", []string{"shop"}, true, []string{"--database", "shop"}},
		{"two schemas, list allowed", []string{"shop", "crm"}, true, []string{"--database", "shop,crm"}},
		{"two schemas, older mydumper", []string{"shop", "crm"}, false, []string{"--regex", `^(shop|crm)\.`}},
		{"a comma in a schema", []string{"a,b", "crm"}, true, []string{"--regex", `^(a,b|crm)\.`}},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, sendLock := range []bool{true, false} {
				for _, tlsArgs := range [][]string{nil, tls} {
					want := slices.Clone(head)
					if sendLock {
						want = append(want, lock...)
					}
					want = append(want, c.selection...)
					want = append(want, tlsArgs...)
					want = append(want, tail...)

					got := buildConsoleMydumperArgs("h", 3306, "u", c.schemas, "/out", baseline.LockModeFTWRL, sendLock, tlsArgs, c.listSchemas)
					if !slices.Equal(got, want) {
						t.Errorf("sendLock=%v tls=%v:\n got %q\nwant %q", sendLock, tlsArgs, got, want)
					}
				}
			}
		})
	}
}
