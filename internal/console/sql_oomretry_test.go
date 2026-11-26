package console

import (
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The memory hint keeps what the sandbox says beside an out-of-memory
// error: the PostgreSQL port reads its retry in the order written there.
func TestSQLWithMemoryHint_keepsTheWrittenOrderRetry(t *testing.T) {
	oom := &sqlsandbox.QueryError{Message: "Out of Memory Error: x", WrittenOrderRetry: sqlsandbox.WrittenOrderRetryFailed}
	var qe *sqlsandbox.QueryError
	if !errors.As(sqlWithMemoryHint(oom, "2GB"), &qe) {
		t.Fatal("not a QueryError")
	}
	if qe.WrittenOrderRetry != sqlsandbox.WrittenOrderRetryFailed || !strings.Contains(qe.Message, "of memory") {
		t.Errorf("%+v", qe)
	}
	if oom.Message != "Out of Memory Error: x" {
		t.Errorf("the sandbox's error was changed in place: %q", oom.Message)
	}
}
