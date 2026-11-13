package testutil

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestMySQLRequired verifies the BINTRAIL_REQUIRE_MYSQL env-var gate mirrors
// MariaDBRequired / PostgresRequired. This is the switch that turns a MySQL
// connectivity failure in SkipIfNoMySQL from a silent t.Skip into a t.Fatal
// (issue #949 — no green-via-skip on the flagship MySQL integration job).
//
// The actual t.Fatalf/t.Skipf call in skipOrFailMySQL is not exercised here:
// a failing subtest propagates Fail() up to its parent (by the testing
// package's own design), so simulating the "required" branch via t.Run would
// make this test intentionally — and misleadingly — report FAIL. Neither the
// MariaDB nor Postgres sibling guards test that call path either.
func TestMySQLRequired(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		t.Setenv("BINTRAIL_REQUIRE_MYSQL", "")
		if MySQLRequired() {
			t.Fatal("expected MySQLRequired() to be false when BINTRAIL_REQUIRE_MYSQL is unset")
		}
	})

	t.Run("set", func(t *testing.T) {
		t.Setenv("BINTRAIL_REQUIRE_MYSQL", "1")
		if !MySQLRequired() {
			t.Fatal("expected MySQLRequired() to be true when BINTRAIL_REQUIRE_MYSQL=1")
		}
	})

	t.Run("other_value_not_required", func(t *testing.T) {
		t.Setenv("BINTRAIL_REQUIRE_MYSQL", "true")
		if MySQLRequired() {
			t.Fatal("expected MySQLRequired() to be false for any value other than \"1\" (matches MariaDBRequired/PostgresRequired's exact-match semantics)")
		}
	})
}

// #2684 point 9. Two processes that run the same test at the same time
// against one server derived the same database name from the test name and
// the counter, so one dropped or filled the other's database mid-test. The
// name now carries a token per process.
func TestTestDBName_uniquePerProcess(t *testing.T) {
	if os.Getenv("BT_PRINT_TEST_DB_NAME") == "1" {
		fmt.Println(testDBName("TestSomething/sub_case"))
		return
	}
	names := map[string]bool{}
	for range 2 {
		cmd := exec.Command(os.Args[0], "-test.run=^TestTestDBName_uniquePerProcess$", "-test.count=1")
		cmd.Env = append(os.Environ(), "BT_PRINT_TEST_DB_NAME=1")
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("child process: %v", err)
		}
		name := strings.SplitN(string(out), "\n", 2)[0]
		t.Logf("rendered: %s", name)
		if !strings.HasPrefix(name, "bt_TestSomething_sub_case_") {
			t.Errorf("name %q lost the test name", name)
		}
		names[name] = true
	}
	if len(names) != 2 {
		t.Errorf("two processes running the same test got the same database name: %v", names)
	}
}

// Within one process the counter still separates two databases of one test,
// the token stays the same, and the name is never longer than before the
// token existed (some tests append a suffix of their own).
func TestTestDBName_inOneProcess(t *testing.T) {
	a, b := testDBName("TestSomething"), testDBName("TestSomething")
	if a == b {
		t.Errorf("two databases of one test share the name %q", a)
	}
	tokenOf := func(n string) string { parts := strings.Split(n, "_"); return parts[len(parts)-2] }
	if tokenOf(a) != tokenOf(b) || len(tokenOf(a)) != 8 {
		t.Errorf("tokens %q and %q", tokenOf(a), tokenOf(b))
	}
	long := testDBName("TestAVeryLongTestNameThatGoesOnAndOn/with_a_subtest_name_that_is_long_too")
	before := len(sanitiseDBName("TestAVeryLongTestNameThatGoesOnAndOn/with_a_subtest_name_that_is_long_too")) + len("_1000000")
	if len(long) > before {
		t.Errorf("%q is %d characters, longer than the %d of the name without the token", long, len(long), before)
	}
	if len(long) > 64 {
		t.Errorf("%q is longer than a MySQL database name may be", long)
	}
	for _, r := range long {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_') {
			t.Errorf("%q holds %q", long, r)
		}
	}
	t.Logf("rendered: %s", long)
}
