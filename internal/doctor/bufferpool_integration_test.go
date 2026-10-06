//go:build integration

package doctor

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Against a real server the check reads the real variable, and whatever the
// test MySQL's pool and wherever it runs, it never FAILS. The detail always
// names the pool size it read, so a scan that silently produced 0 would show.
func TestIntegrationIndexBufferPool_readsTheServer(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	dsn := testutil.BaseDSN() + "/"
	db, err := connectWithoutDB(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var pool uint64
	if err := db.QueryRow("SELECT @@innodb_buffer_pool_size").Scan(&pool); err != nil {
		t.Fatal(err)
	}
	if pool == 0 {
		t.Fatal("the server reported a zero buffer pool; the test proves nothing")
	}
	c := checkIndexBufferPool(t.Context(), dsn)
	if c.Status == StatusFail {
		t.Fatalf("FAIL: %s", c.Detail)
	}
	if c.Name != BufferPoolCheckName {
		t.Fatalf("name = %q", c.Name)
	}
	if want := humanBytes(float64(pool)); !strings.Contains(c.Detail, want) {
		t.Errorf("detail %q does not name the pool it read (%s)", c.Detail, want)
	}
	t.Logf("pool %d: %s %s", pool, c.Status, c.Detail)
}

// Build runs the check in the index half of the report, the half `doctor`,
// the watch and up preflights and the console's server checks all print.
func TestIntegrationBuild_carriesTheBufferPoolCheck(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	_, name := testutil.CreateTestDB(t)
	r := Build(t.Context(), testutil.BaseDSN()+"/", testutil.IntegrationDSN(name), name, 0)
	for _, c := range r.Checks {
		if c.Name == BufferPoolCheckName {
			if c.Status == StatusFail {
				t.Fatalf("FAIL: %s", c.Detail)
			}
			return
		}
	}
	t.Fatalf("Build produced no %q check; checks: %+v", BufferPoolCheckName, r.Checks)
}
